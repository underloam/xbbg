//! Interned name type for Bloomberg BLPAPI
//!
//! Names are globally interned strings. Bloomberg handles the interning internally,
//! and this module provides a Rust-side cache to avoid repeated FFI calls.
//!
//! **Performance**: Use `Name::get_or_intern()` for automatic caching. First call
//! does FFI + caches, subsequent calls return from Rust cache (no FFI).
//!
//! # Hot Path Optimization
//!
//! For maximum performance in tight loops, pre-intern names and pass by reference:
//!
//! ```ignore
//! // Setup (once, before hot loop)
//! let px_last = Name::get_or_intern("PX_LAST");
//!
//! // Hot loop - pass &Name (no clone, no FFI overhead)
//! for msg in messages {
//!     if let Some(elem) = root.get(&px_last) { ... }
//! }
//! ```

use crate::ffi;
use rustc_hash::FxHashMap;
use std::cell::UnsafeCell;
use std::ffi::{CStr, CString};
use std::hash::{Hash, Hasher};
use std::ptr::NonNull;

// Thread-local cache for interned names.
// Uses FxHashMap for faster hashing (non-cryptographic, ~2-5x faster than SipHash).
// Box<str> keys avoid per-lookup String allocation.
//
// SAFETY: UnsafeCell is safe here because thread_local! guarantees single-threaded access.
// This eliminates the runtime borrow-checking overhead of RefCell (~5-10 instructions per access).
//
// NOTE: This cache grows unbounded. Call `clear_name_cache()` periodically
// in long-running applications that use many distinct field names.
thread_local! {
    static NAME_CACHE: UnsafeCell<FxHashMap<Box<str>, Name>> = UnsafeCell::new(FxHashMap::default());
}

/// Clear the thread-local Name cache.
///
/// In long-running applications that use many distinct field names over time,
/// the cache can grow unbounded. Call this function periodically (e.g., hourly
/// or when memory pressure is detected) to free cached Names.
///
/// # Performance
/// After clearing, the next call to `Name::get_or_intern()` for each name will
/// require an FFI call to Bloomberg. For hot paths, pre-intern names at startup
/// and they will be re-cached automatically.
///
/// # Example
/// ```ignore
/// // Clear cache when memory usage is high
/// if memory_pressure_detected() {
///     xbbg_core::clear_name_cache();
/// }
/// ```
pub fn clear_name_cache() {
    NAME_CACHE.with(|cache| {
        // SAFETY: TLS guarantees single-threaded access
        unsafe { (*cache.get()).clear() };
    });
}

/// Get the current size of the thread-local Name cache.
///
/// Useful for monitoring memory usage in long-running applications.
pub fn name_cache_size() -> usize {
    NAME_CACHE.with(|cache| {
        // SAFETY: TLS guarantees single-threaded access
        unsafe { (*cache.get()).len() }
    })
}

/// Interned string for O(1) comparison.
///
/// Names are globally interned by Bloomberg. Creation is expensive (hash table lookup),
/// but comparison is cheap (pointer equality). Create once at startup, reuse everywhere.
///
/// # Examples
///
/// ```no_run
/// use xbbg_core::Name;
///
/// let name1 = Name::new("PX_LAST").unwrap();
/// let name2 = Name::new("PX_LAST").unwrap();
/// // Note: Pointer equality only works with real Bloomberg backend (live feature)
/// assert_eq!(name1.as_str(), "PX_LAST");
/// ```
///
/// # Performance
///
/// - `new()`: Expensive (hash table lookup) - use at startup only
/// - `eq()`: O(1) pointer comparison
/// - `as_str()`: O(1) pointer dereference
///
/// # Thread Safety
/// `Name` is `Send + Sync`. Cross-thread use of `blpapi_Name_t` is the SDK's
/// own documented pattern: `blpapi_names.h` ships process-wide static
/// `blpapi::Name` instances ("Provide static blpapi::Name instances for
/// common message types") constructed lazily on whichever thread first
/// touches them and then read — and duplicated into messages — from SDK
/// dispatcher threads inside event handlers. Names are interned in a global,
/// process-lifetime table; `clone`/`drop` adjust the SDK-managed reference
/// count exactly as those vendor statics do across threads.
///
/// `get_or_intern` additionally keeps a thread-local cache so repeated
/// lookups stay allocation-free; the cached `Name`s themselves may move
/// freely between threads.
#[repr(transparent)]
pub struct Name(NonNull<ffi::blpapi_Name_t>);

// SAFETY: see "Thread Safety" above — the SDK's `blpapi::Names` statics are
// constructed, read, duplicated, and destroyed across threads by design
// (blpapi_names.h:23-59,188,445-539), which requires `blpapi_Name_t` handles
// and their reference counting to be thread-safe. `Name` adds no thread-local
// state of its own.
unsafe impl Send for Name {}
unsafe impl Sync for Name {}

impl Name {
    /// Intern a string. **Expensive** - do at startup only.
    ///
    /// Returns `None` if the string contains null bytes.
    #[cold]
    pub fn new(s: &str) -> Option<Self> {
        let c = CString::new(s).ok()?;
        // SAFETY: blpapi_Name_create returns a valid pointer or null.
        // NonNull::new handles the null case.
        let ptr = unsafe { ffi::blpapi_Name_create(c.as_ptr()) };
        NonNull::new(ptr).map(Self)
    }

    /// Find existing interned name. Returns `None` if not interned.
    pub fn find(s: &str) -> Option<Self> {
        let c = CString::new(s).ok()?;
        // SAFETY: blpapi_Name_findName returns a valid pointer or null.
        let ptr = unsafe { ffi::blpapi_Name_findName(c.as_ptr()) };
        NonNull::new(ptr).map(Self)
    }

    /// Get or intern a name with caching.
    ///
    /// This is the recommended way to get Names in hot paths:
    /// - First call: FFI call to Bloomberg + cache in Rust
    /// - Subsequent calls: Return from Rust cache (no FFI)
    ///
    /// The cache is thread-local, so no synchronization overhead.
    ///
    /// # Panics
    /// Panics if the name cannot be interned (should never happen for valid strings).
    ///
    /// # Example
    /// ```ignore
    /// // Fast after first call - no FFI overhead
    /// let name = Name::get_or_intern("PX_LAST");
    /// element.get(&name);
    /// ```
    #[inline]
    pub fn get_or_intern(s: &str) -> Self {
        NAME_CACHE.with(|cache| {
            // SAFETY: TLS guarantees single-threaded access. No borrow checking overhead.
            let cache = unsafe { &mut *cache.get() };

            // Fast path: return clone from cache
            if let Some(name) = cache.get(s) {
                return name.clone();
            }

            // Slow path: create and cache
            let name = Self::new(s).expect("failed to intern name");
            cache.insert(s.into(), name.clone());
            name
        })
    }

    /// String value. O(1) - just pointer arithmetic.
    ///
    /// # Performance
    /// This is a hot path function - returns a reference to the internal buffer.
    /// No allocation, no copy.
    ///
    /// # Panics
    /// Panics if the name contains invalid UTF-8 (should never happen for Bloomberg names).
    #[inline(always)]
    pub fn as_str(&self) -> &str {
        // SAFETY: blpapi_Name_string returns a pointer to a null-terminated C string
        // that lives as long as the Name object.
        let ptr = unsafe { ffi::blpapi_Name_string(self.0.as_ptr()) };
        // Bloomberg names are ASCII identifiers, but we use checked conversion for safety.
        // This should never fail in practice.
        unsafe { CStr::from_ptr(ptr) }
            .to_str()
            .expect("Bloomberg Name contained invalid UTF-8")
    }

    /// Construct from raw pointer (internal use only).
    ///
    /// # Safety
    /// Caller must ensure ptr is a valid blpapi_Name_t pointer.
    #[inline]
    pub(crate) unsafe fn from_raw(ptr: NonNull<ffi::blpapi_Name_t>) -> Self {
        Self(ptr)
    }

    /// Get raw pointer for FFI calls.
    #[inline(always)]
    pub fn as_ptr(&self) -> *mut ffi::blpapi_Name_t {
        self.0.as_ptr()
    }
}

impl Clone for Name {
    fn clone(&self) -> Self {
        // SAFETY: blpapi_Name_duplicate increments the reference count and returns
        // a valid pointer to the same interned name.
        let ptr = unsafe { ffi::blpapi_Name_duplicate(self.0.as_ptr()) };
        Self(NonNull::new(ptr).expect("blpapi_Name_duplicate returned null"))
    }
}

impl Drop for Name {
    fn drop(&mut self) {
        // SAFETY: We own this pointer and it's valid. blpapi_Name_destroy decrements
        // the reference count and frees if this was the last reference.
        unsafe { ffi::blpapi_Name_destroy(self.0.as_ptr()) }
    }
}

impl PartialEq for Name {
    /// O(1) pointer comparison.
    ///
    /// Bloomberg guarantees all instances of the same string intern to the same pointer.
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 // Pointer comparison
    }
}

impl Eq for Name {}

impl Hash for Name {
    #[inline(always)]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_ptr().hash(state)
    }
}

impl std::fmt::Debug for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Name").field(&self.as_str()).finish()
    }
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// Tests require Bloomberg API to be available (gated behind `live` feature)
#[cfg(all(test, feature = "live"))]
mod tests {
    use super::*;

    #[test]
    fn test_name_interning() {
        let name1 = Name::new("TEST_NAME").expect("failed to create name");
        let name2 = Name::new("TEST_NAME").expect("failed to create name");

        // Both should intern to same pointer (pointer comparison)
        assert_eq!(name1, name2);
        assert_eq!(name1.as_ptr(), name2.as_ptr());
    }

    #[test]
    fn test_name_as_str_roundtrip() {
        let name = Name::new("PX_LAST").expect("failed to create name");
        assert_eq!(name.as_str(), "PX_LAST");
    }

    #[test]
    fn test_name_display() {
        let name = Name::new("SECURITY_DATA").expect("failed to create name");
        assert_eq!(format!("{}", name), "SECURITY_DATA");
    }

    #[test]
    fn test_name_debug() {
        let name = Name::new("FIELD_DATA").expect("failed to create name");
        assert_eq!(format!("{:?}", name), "Name(\"FIELD_DATA\")");
    }
}
