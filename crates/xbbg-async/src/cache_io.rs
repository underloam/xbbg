use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde::de::{DeserializeOwned, DeserializeSeed, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const TEMP_FILE_ATTEMPTS: usize = 128;

/// Publishes snapshots in invocation order without allowing an older
/// serialization to replace a later successful publication.
#[derive(Debug, Default)]
pub(crate) struct AtomicJsonPublisher {
    next_publication: AtomicU64,
    state: Mutex<PublisherState>,
}

#[derive(Debug, Default)]
struct PublisherState {
    latest_successful: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublicationOutcome {
    Published,
    Superseded,
}

impl AtomicJsonPublisher {
    /// Reserve publication order before capturing a cache snapshot.
    pub(crate) fn begin(&self) -> AtomicJsonPublication<'_> {
        AtomicJsonPublication {
            publisher: self,
            publication: self.next_publication.fetch_add(1, Ordering::AcqRel),
        }
    }
}

pub(crate) struct AtomicJsonPublication<'a> {
    publisher: &'a AtomicJsonPublisher,
    publication: u64,
}

impl AtomicJsonPublication<'_> {
    pub(crate) fn publish<T>(self, path: &Path, value: &T) -> Result<PublicationOutcome, String>
    where
        T: Serialize + ?Sized,
    {
        let mut pending = PendingJsonWrite::prepare(path, value)?;
        let outcome = {
            let mut state = self.publisher.state.lock();
            if state
                .latest_successful
                .is_some_and(|latest| latest > self.publication)
            {
                Ok(PublicationOutcome::Superseded)
            } else {
                pending.replace().map(|()| {
                    state.latest_successful = Some(self.publication);
                    PublicationOutcome::Published
                })
            }
        }?;

        if outcome == PublicationOutcome::Published {
            sync_parent_after_commit(path);
        }
        Ok(outcome)
    }

    pub(crate) fn remove(self, path: &Path) -> Result<PublicationOutcome, String> {
        let (outcome, removed) = {
            let mut state = self.publisher.state.lock();
            if state
                .latest_successful
                .is_some_and(|latest| latest > self.publication)
            {
                (Ok(PublicationOutcome::Superseded), false)
            } else {
                match remove_cache_file_unsynced(path) {
                    Ok(removed) => {
                        state.latest_successful = Some(self.publication);
                        (Ok(PublicationOutcome::Published), removed)
                    }
                    Err(error) => (Err(error), false),
                }
            }
        };

        let outcome = outcome?;
        if removed {
            sync_parent_after_commit(path);
        }
        Ok(outcome)
    }
}

/// Atomically replace `path` with a complete JSON document.
///
/// The temporary file is created beside the destination so replacement stays
/// on one filesystem. All buffered writes and the file sync complete before
/// the destination name changes.
pub(crate) fn write_json_atomic<T>(path: &Path, value: &T) -> Result<(), String>
where
    T: Serialize + ?Sized,
{
    let mut pending = PendingJsonWrite::prepare(path, value)?;
    pending.replace()?;
    sync_parent_after_commit(path);
    Ok(())
}

pub(crate) fn remove_cache_file(path: &Path) -> Result<(), String> {
    if remove_cache_file_unsynced(path)? {
        sync_parent_after_commit(path);
    }
    Ok(())
}

fn remove_cache_file_unsynced(path: &Path) -> Result<bool, String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!(
            "cannot remove cache file '{}': {error}",
            path.display()
        )),
    }
}

fn sync_parent_after_commit(path: &Path) {
    if let Err(error) = sync_parent(path) {
        xbbg_log::warn!(
            path = %path.display(),
            error = %error,
            "cache publication committed without directory sync"
        );
    }
}

pub(crate) fn read_json_bounded<T>(path: &Path, max_bytes: u64) -> Result<T, String>
where
    T: DeserializeOwned,
{
    let reader = open_bounded(path, max_bytes)?;
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let value = T::deserialize(&mut deserializer)
        .map_err(|error| format!("cannot parse cache file '{}': {error}", path.display()))?;
    deserializer
        .end()
        .map_err(|error| format!("cannot parse cache file '{}': {error}", path.display()))?;
    Ok(value)
}

pub(crate) fn read_json_array_bounded<T>(
    path: &Path,
    max_bytes: u64,
    max_entries: usize,
) -> Result<Vec<T>, String>
where
    T: DeserializeOwned,
{
    let reader = open_bounded(path, max_bytes)?;
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let values = BoundedVecSeed::<T> {
        max_entries,
        marker: PhantomData,
    }
    .deserialize(&mut deserializer)
    .map_err(|error| format!("cannot parse cache file '{}': {error}", path.display()))?;
    deserializer
        .end()
        .map_err(|error| format!("cannot parse cache file '{}': {error}", path.display()))?;
    Ok(values)
}

fn open_bounded(path: &Path, max_bytes: u64) -> Result<BufReader<std::io::Take<File>>, String> {
    let file = File::open(path)
        .map_err(|error| format!("cannot open cache file '{}': {error}", path.display()))?;
    let length = file
        .metadata()
        .map_err(|error| format!("cannot inspect cache file '{}': {error}", path.display()))?
        .len();
    if length > max_bytes {
        return Err(format!(
            "cache file '{}' is {length} bytes, exceeding the {max_bytes}-byte limit",
            path.display()
        ));
    }
    Ok(BufReader::new(file.take(max_bytes)))
}

struct BoundedVecSeed<T> {
    max_entries: usize,
    marker: PhantomData<T>,
}

impl<'de, T> DeserializeSeed<'de> for BoundedVecSeed<T>
where
    T: Deserialize<'de>,
{
    type Value = Vec<T>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedVecVisitor::<T> {
            max_entries: self.max_entries,
            marker: PhantomData,
        })
    }
}

struct BoundedVecVisitor<T> {
    max_entries: usize,
    marker: PhantomData<T>,
}

impl<'de, T> Visitor<'de> for BoundedVecVisitor<T>
where
    T: Deserialize<'de>,
{
    type Value = Vec<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "a JSON array containing at most {} entries",
            self.max_entries
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let capacity = sequence
            .size_hint()
            .unwrap_or(0)
            .min(self.max_entries)
            .min(4096);
        let mut values = Vec::with_capacity(capacity);
        while values.len() < self.max_entries {
            let Some(value) = sequence.next_element()? else {
                return Ok(values);
            };
            values.push(value);
        }

        if sequence.next_element::<IgnoredAny>()?.is_some() {
            return Err(serde::de::Error::custom(format!(
                "cache array exceeds the {}-entry limit",
                self.max_entries
            )));
        }
        Ok(values)
    }
}

struct PendingJsonWrite {
    destination: PathBuf,
    temporary: PathBuf,
    cleanup_needed: bool,
}

impl PendingJsonWrite {
    fn prepare<T>(destination: &Path, value: &T) -> Result<Self, String>
    where
        T: Serialize + ?Sized,
    {
        let parent = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "cannot create cache directory '{}': {error}",
                parent.display()
            )
        })?;

        let (temporary, file) = create_temporary_file(parent, destination)?;
        let pending = Self {
            destination: destination.to_path_buf(),
            temporary,
            cleanup_needed: true,
        };

        let mut file = file;
        {
            let mut writer = BufWriter::new(&mut file);
            serde_json::to_writer_pretty(&mut writer, value).map_err(|error| {
                format!(
                    "cannot serialize cache snapshot for '{}': {error}",
                    destination.display()
                )
            })?;
            writer.flush().map_err(|error| {
                format!(
                    "cannot flush cache snapshot for '{}': {error}",
                    destination.display()
                )
            })?;
        }
        file.sync_all().map_err(|error| {
            format!(
                "cannot sync cache snapshot for '{}': {error}",
                destination.display()
            )
        })?;
        drop(file);

        Ok(pending)
    }

    fn replace(&mut self) -> Result<(), String> {
        replace_file(&self.temporary, &self.destination).map_err(|error| {
            format!(
                "cannot replace cache file '{}': {error}",
                self.destination.display()
            )
        })?;
        self.cleanup_needed = false;
        Ok(())
    }
}

impl Drop for PendingJsonWrite {
    fn drop(&mut self) {
        if self.cleanup_needed
            && let Err(error) = fs::remove_file(&self.temporary)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            xbbg_log::warn!(
                path = %self.temporary.display(),
                error = %error,
                "cannot remove unpublished temporary cache file"
            );
        }
    }
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            format!(
                "cannot sync cache directory '{}': {error}",
                parent.display()
            )
        })?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> Result<(), String> {
    Ok(())
}

fn create_temporary_file(parent: &Path, destination: &Path) -> Result<(PathBuf, File), String> {
    let file_name = destination.file_name().ok_or_else(|| {
        format!(
            "cache destination '{}' has no file name",
            destination.display()
        )
    })?;

    for _ in 0..TEMP_FILE_ATTEMPTS {
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let mut temporary_name = OsString::from(".");
        temporary_name.push(file_name);
        temporary_name.push(format!(".tmp-{}-{sequence}", std::process::id()));
        let temporary = parent.join(temporary_name);

        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(format!(
                    "cannot create temporary cache file beside '{}': {error}",
                    destination.display()
                ));
            }
        }
    }

    Err(format!(
        "cannot allocate a temporary cache file beside '{}' after {TEMP_FILE_ATTEMPTS} attempts",
        destination.display()
    ))
}

#[cfg(not(windows))]
fn replace_file(temporary: &Path, destination: &Path) -> std::io::Result<()> {
    fs::rename(temporary, destination)
}

#[cfg(windows)]
fn replace_file(temporary: &Path, destination: &Path) -> std::io::Result<()> {
    use std::mem::{offset_of, size_of};
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use std::ptr;

    const DELETE: u32 = 0x0001_0000;
    const FILE_SHARE_READ: u32 = 0x1;
    const FILE_SHARE_WRITE: u32 = 0x2;
    const FILE_SHARE_DELETE: u32 = 0x4;
    const FILE_RENAME_INFO_EX: i32 = 22;
    const FILE_RENAME_REPLACE_IF_EXISTS: u32 = 0x1;
    const FILE_RENAME_POSIX_SEMANTICS: u32 = 0x2;

    #[repr(C)]
    struct FileRenameInfo {
        flags: u32,
        root_directory: *mut std::ffi::c_void,
        file_name_length: u32,
        file_name: [u16; 1],
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn SetFileInformationByHandle(
            file: *mut std::ffi::c_void,
            information_class: i32,
            information: *mut std::ffi::c_void,
            buffer_size: u32,
        ) -> i32;
    }

    // FileRenameInfoEx support is required. An unsupported system or
    // filesystem fails safely instead of falling back to a replacement API
    // that can make the destination transiently unavailable to readers.
    // Renaming by handle requires DELETE access. Keep every share bit explicit:
    // Rust's ordinary File::open readers share deletion, and POSIX rename
    // semantics let their existing handles continue reading the old file while
    // subsequent opens of the destination name reach the complete new file.
    let temporary = OpenOptions::new()
        .access_mode(DELETE)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .open(temporary)?;

    let destination: Vec<u16> = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect();
    let file_name_length = destination
        .len()
        .checked_sub(1)
        .and_then(|length| length.checked_mul(size_of::<u16>()))
        .and_then(|length| u32::try_from(length).ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cache destination path is too long to rename",
            )
        })?;
    let information_size = offset_of!(FileRenameInfo, file_name)
        .checked_add(file_name_length as usize)
        .and_then(|length| length.checked_add(size_of::<u16>()))
        .and_then(|length| u32::try_from(length).ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cache destination path is too long to rename",
            )
        })?;
    let mut information_buffer =
        vec![0_usize; (information_size as usize).div_ceil(size_of::<usize>())];
    let information = information_buffer.as_mut_ptr().cast::<FileRenameInfo>();

    // SAFETY: Vec<usize> supplies FileRenameInfo's pointer alignment, and its
    // rounded allocation covers information_size bytes. The copied UTF-16
    // string (including its terminator) exactly fits that computed size. Both
    // the buffer and the open temporary-file handle remain live for the call,
    // and SetFileInformationByHandle does not retain either pointer.
    let renamed = unsafe {
        ptr::addr_of_mut!((*information).flags)
            .write(FILE_RENAME_REPLACE_IF_EXISTS | FILE_RENAME_POSIX_SEMANTICS);
        ptr::addr_of_mut!((*information).root_directory).write(ptr::null_mut());
        ptr::addr_of_mut!((*information).file_name_length).write(file_name_length);
        destination.as_ptr().copy_to_nonoverlapping(
            ptr::addr_of_mut!((*information).file_name).cast::<u16>(),
            destination.len(),
        );
        SetFileInformationByHandle(
            temporary.as_raw_handle(),
            FILE_RENAME_INFO_EX,
            information.cast::<std::ffi::c_void>(),
            information_size,
        )
    };
    if renamed == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serializer;
    use serde::ser::Error as _;
    use std::sync::{Arc, Barrier};

    struct BlockingValue {
        value: &'static str,
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }

    impl Serialize for BlockingValue {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.entered.wait();
            self.release.wait();
            serializer.serialize_str(self.value)
        }
    }

    struct FailingValue;

    impl Serialize for FailingValue {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(S::Error::custom("deliberate serialization failure"))
        }
    }

    #[test]
    fn slow_older_snapshot_cannot_replace_newer_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cache.json");
        let publisher = Arc::new(AtomicJsonPublisher::default());
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));

        let older_publisher = Arc::clone(&publisher);
        let older_path = path.clone();
        let older_entered = Arc::clone(&entered);
        let older_release = Arc::clone(&release);
        let older = std::thread::spawn(move || {
            older_publisher.begin().publish(
                &older_path,
                &BlockingValue {
                    value: "older",
                    entered: older_entered,
                    release: older_release,
                },
            )
        });

        entered.wait();
        assert_eq!(
            publisher.begin().publish(&path, "newer").unwrap(),
            PublicationOutcome::Published
        );
        release.wait();
        assert_eq!(
            older.join().unwrap().unwrap(),
            PublicationOutcome::Superseded
        );

        let published: String = serde_json::from_reader(File::open(&path).unwrap()).unwrap();
        assert_eq!(published, "newer");
    }

    #[test]
    fn failed_newer_serialization_does_not_suppress_older_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cache.json");
        let publisher = Arc::new(AtomicJsonPublisher::default());
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));

        let older_publisher = Arc::clone(&publisher);
        let older_path = path.clone();
        let older_entered = Arc::clone(&entered);
        let older_release = Arc::clone(&release);
        let older = std::thread::spawn(move || {
            older_publisher.begin().publish(
                &older_path,
                &BlockingValue {
                    value: "older",
                    entered: older_entered,
                    release: older_release,
                },
            )
        });

        entered.wait();
        assert!(publisher.begin().publish(&path, &FailingValue).is_err());
        release.wait();
        assert_eq!(
            older.join().unwrap().unwrap(),
            PublicationOutcome::Published
        );
        let published: String = serde_json::from_reader(File::open(&path).unwrap()).unwrap();
        assert_eq!(published, "older");
    }

    #[test]
    fn failed_newer_replacement_does_not_suppress_older_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cache.json");
        let publisher = Arc::new(AtomicJsonPublisher::default());
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));

        let older_publisher = Arc::clone(&publisher);
        let older_path = path.clone();
        let older_entered = Arc::clone(&entered);
        let older_release = Arc::clone(&release);
        let older = std::thread::spawn(move || {
            older_publisher.begin().publish(
                &older_path,
                &BlockingValue {
                    value: "older",
                    entered: older_entered,
                    release: older_release,
                },
            )
        });

        entered.wait();
        fs::create_dir(&path).unwrap();
        assert!(publisher.begin().publish(&path, "newer").is_err());
        fs::remove_dir(&path).unwrap();
        release.wait();
        assert_eq!(
            older.join().unwrap().unwrap(),
            PublicationOutcome::Published
        );
        let published: String = serde_json::from_reader(File::open(&path).unwrap()).unwrap();
        assert_eq!(published, "older");
    }

    #[test]
    fn successful_newer_removal_suppresses_older_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cache.json");
        write_json_atomic(&path, &"initial").unwrap();
        let publisher = Arc::new(AtomicJsonPublisher::default());
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));

        let older_publisher = Arc::clone(&publisher);
        let older_path = path.clone();
        let older_entered = Arc::clone(&entered);
        let older_release = Arc::clone(&release);
        let older = std::thread::spawn(move || {
            older_publisher.begin().publish(
                &older_path,
                &BlockingValue {
                    value: "older",
                    entered: older_entered,
                    release: older_release,
                },
            )
        });

        entered.wait();
        assert_eq!(
            publisher.begin().remove(&path).unwrap(),
            PublicationOutcome::Published
        );
        release.wait();
        assert_eq!(
            older.join().unwrap().unwrap(),
            PublicationOutcome::Superseded
        );
        assert!(!path.exists());
    }

    #[test]
    fn failed_newer_removal_does_not_suppress_older_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cache.json");
        let publisher = Arc::new(AtomicJsonPublisher::default());
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));

        let older_publisher = Arc::clone(&publisher);
        let older_path = path.clone();
        let older_entered = Arc::clone(&entered);
        let older_release = Arc::clone(&release);
        let older = std::thread::spawn(move || {
            older_publisher.begin().publish(
                &older_path,
                &BlockingValue {
                    value: "older",
                    entered: older_entered,
                    release: older_release,
                },
            )
        });

        entered.wait();
        fs::create_dir(&path).unwrap();
        assert!(publisher.begin().remove(&path).is_err());
        fs::remove_dir(&path).unwrap();
        release.wait();
        assert_eq!(
            older.join().unwrap().unwrap(),
            PublicationOutcome::Published
        );
        let published: String = serde_json::from_reader(File::open(&path).unwrap()).unwrap();
        assert_eq!(published, "older");
    }

    #[test]
    fn bounded_reader_rejects_oversized_file_and_array() {
        let directory = tempfile::tempdir().unwrap();
        let oversized = directory.path().join("oversized.json");
        fs::write(&oversized, b"\"12345\"").unwrap();
        assert!(
            read_json_bounded::<String>(&oversized, 4)
                .unwrap_err()
                .contains("exceeding")
        );

        let array = directory.path().join("array.json");
        fs::write(&array, b"[1,2,3]").unwrap();
        assert!(
            read_json_array_bounded::<u64>(&array, 64, 2)
                .unwrap_err()
                .contains("2-entry limit")
        );
    }

    #[test]
    fn concurrent_readers_only_observe_complete_json_documents() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cache.json");
        write_json_atomic(&path, &vec![0_u64; 4096]).unwrap();

        let writer_path = path.clone();
        let start = Arc::new(Barrier::new(2));
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer_start = Arc::clone(&start);
        let writer_finished = Arc::clone(&finished);
        let writer = std::thread::spawn(move || {
            writer_start.wait();
            let result = (|| {
                for value in 1..=64_u64 {
                    write_json_atomic(&writer_path, &vec![value; 4096])?;
                }
                Ok::<(), String>(())
            })();
            writer_finished.store(true, Ordering::Release);
            result
        });

        start.wait();
        let reader_result = loop {
            let file = match File::open(&path) {
                Ok(file) => file,
                Err(error) => break Err(format!("cannot open published cache file: {error}")),
            };
            let values: Vec<u64> = match serde_json::from_reader(file) {
                Ok(values) => values,
                Err(error) => break Err(format!("cannot read published JSON document: {error}")),
            };
            if values.len() != 4096 {
                break Err(format!(
                    "published JSON document has {} values instead of 4096",
                    values.len()
                ));
            }
            if !values.iter().all(|value| *value == values[0]) {
                break Err("published JSON document contains mixed snapshots".to_owned());
            }
            if finished.load(Ordering::Acquire) {
                break Ok::<(), String>(());
            }
        };

        // Join before propagating a reader failure so the temp directory and
        // primary writer result remain available as diagnostic evidence.
        let writer_result = writer.join();
        reader_result.unwrap();
        writer_result.unwrap().unwrap();
        let leftovers: Vec<_> = fs::read_dir(directory.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp-"))
            .collect();
        assert!(leftovers.is_empty());
    }

    #[test]
    fn replacement_failure_is_reported_without_damaging_destination() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("cache.json");
        fs::create_dir(&destination).unwrap();

        let error = write_json_atomic(&destination, &"snapshot").unwrap_err();

        assert!(error.contains("cannot replace cache file"));
        assert!(destination.is_dir());
        let leftovers: Vec<_> = fs::read_dir(directory.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp-"))
            .collect();
        assert!(leftovers.is_empty());
    }
}
