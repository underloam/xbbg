use std::ffi::CString;

use crate::correlation::CorrelationId;
use crate::errors::{BlpError, Result};
use crate::ffi;
use crate::sdk;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthConfig {
    User,
    App {
        app_name: String,
    },
    UserApp {
        app_name: String,
    },
    Directory {
        property_name: String,
    },
    Manual {
        app_name: String,
        user_id: String,
        ip_address: String,
    },
    Token {
        token: String,
    },
}

impl AuthConfig {
    pub fn method_name(&self) -> &'static str {
        match self {
            Self::User => "user",
            Self::App { .. } => "app",
            Self::UserApp { .. } => "userapp",
            Self::Directory { .. } => "dir",
            Self::Manual { .. } => "manual",
            Self::Token { .. } => "token",
        }
    }

    pub fn build_auth_options(&self) -> Result<AuthOptions> {
        match self {
            Self::User => {
                let user = AuthUser::with_logon_name()?;
                AuthOptions::for_user(&user)
            }
            Self::App { app_name } => {
                let app = AuthApplication::new(app_name)?;
                AuthOptions::for_app(&app)
            }
            Self::UserApp { app_name } => {
                let user = AuthUser::with_logon_name()?;
                let app = AuthApplication::new(app_name)?;
                AuthOptions::for_user_and_app(&user, &app)
            }
            Self::Directory { property_name } => {
                let user = AuthUser::with_active_directory_property(property_name)?;
                AuthOptions::for_user(&user)
            }
            Self::Manual {
                app_name,
                user_id,
                ip_address,
            } => {
                let user = AuthUser::with_manual_options(user_id, ip_address)?;
                let app = AuthApplication::new(app_name)?;
                AuthOptions::for_user_and_app(&user, &app)
            }
            Self::Token { token } => {
                let token = AuthToken::new(token)?;
                AuthOptions::for_token(&token)
            }
        }
    }
}

pub struct AuthUser {
    ptr: *mut ffi::blpapi_AuthUser_t,
}

impl AuthUser {
    pub fn with_logon_name() -> Result<Self> {
        let mut ptr = std::ptr::null_mut();
        let rc = unsafe { sdk().blpapi_AuthUser_createWithLogonName(&mut ptr) };
        if rc != 0 || ptr.is_null() {
            return Err(BlpError::InvalidArgument {
                detail: format!("failed to create AuthUser(OS_LOGON): rc={rc}"),
            });
        }
        Ok(Self { ptr })
    }

    pub fn with_active_directory_property(property_name: &str) -> Result<Self> {
        let property_name = cstring(property_name, "Active Directory property")?;
        let mut ptr = std::ptr::null_mut();
        let rc = unsafe {
            sdk()
                .blpapi_AuthUser_createWithActiveDirectoryProperty(&mut ptr, property_name.as_ptr())
        };
        if rc != 0 || ptr.is_null() {
            return Err(BlpError::InvalidArgument {
                detail: format!("failed to create AuthUser(DIRECTORY_SERVICE): rc={rc}"),
            });
        }
        Ok(Self { ptr })
    }

    pub fn with_manual_options(user_id: &str, ip_address: &str) -> Result<Self> {
        let user_id = cstring(user_id, "manual user_id")?;
        let ip_address = cstring(ip_address, "manual ip_address")?;
        let mut ptr = std::ptr::null_mut();
        let rc = unsafe {
            sdk().blpapi_AuthUser_createWithManualOptions(
                &mut ptr,
                user_id.as_ptr(),
                ip_address.as_ptr(),
            )
        };
        if rc != 0 || ptr.is_null() {
            return Err(BlpError::InvalidArgument {
                detail: format!("failed to create AuthUser(manual): rc={rc}"),
            });
        }
        Ok(Self { ptr })
    }

    pub(crate) fn as_ptr(&self) -> *const ffi::blpapi_AuthUser_t {
        self.ptr
    }
}

impl Drop for AuthUser {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { sdk().blpapi_AuthUser_destroy(self.ptr) };
            self.ptr = std::ptr::null_mut();
        }
    }
}

pub struct AuthApplication {
    ptr: *mut ffi::blpapi_AuthApplication_t,
}

impl AuthApplication {
    pub fn new(app_name: &str) -> Result<Self> {
        let app_name = cstring(app_name, "application name")?;
        let mut ptr = std::ptr::null_mut();
        let rc = unsafe { sdk().blpapi_AuthApplication_create(&mut ptr, app_name.as_ptr()) };
        if rc != 0 || ptr.is_null() {
            return Err(BlpError::InvalidArgument {
                detail: format!("failed to create AuthApplication: rc={rc}"),
            });
        }
        Ok(Self { ptr })
    }

    pub(crate) fn as_ptr(&self) -> *const ffi::blpapi_AuthApplication_t {
        self.ptr
    }
}

impl Drop for AuthApplication {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { sdk().blpapi_AuthApplication_destroy(self.ptr) };
            self.ptr = std::ptr::null_mut();
        }
    }
}

pub struct AuthToken {
    ptr: *mut ffi::blpapi_AuthToken_t,
}

impl AuthToken {
    pub fn new(token: &str) -> Result<Self> {
        let token = cstring(token, "token")?;
        let mut ptr = std::ptr::null_mut();
        let rc = unsafe { sdk().blpapi_AuthToken_create(&mut ptr, token.as_ptr()) };
        if rc != 0 || ptr.is_null() {
            return Err(BlpError::InvalidArgument {
                detail: format!("failed to create AuthToken: rc={rc}"),
            });
        }
        Ok(Self { ptr })
    }

    pub(crate) fn as_ptr(&self) -> *const ffi::blpapi_AuthToken_t {
        self.ptr
    }
}

impl Drop for AuthToken {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { sdk().blpapi_AuthToken_destroy(self.ptr) };
            self.ptr = std::ptr::null_mut();
        }
    }
}

pub struct AuthOptions {
    ptr: *mut ffi::blpapi_AuthOptions_t,
}

impl AuthOptions {
    pub fn none() -> Result<Self> {
        let mut ptr = std::ptr::null_mut();
        let rc = unsafe { sdk().blpapi_AuthOptions_create_default(&mut ptr) };
        if rc != 0 || ptr.is_null() {
            return Err(BlpError::Internal {
                detail: format!("failed to create default AuthOptions: rc={rc}"),
            });
        }
        Ok(Self { ptr })
    }

    pub fn for_user(user: &AuthUser) -> Result<Self> {
        let mut ptr = std::ptr::null_mut();
        let rc = unsafe { sdk().blpapi_AuthOptions_create_forUserMode(&mut ptr, user.as_ptr()) };
        if rc != 0 || ptr.is_null() {
            return Err(BlpError::Internal {
                detail: format!("failed to create user AuthOptions: rc={rc}"),
            });
        }
        Ok(Self { ptr })
    }

    pub fn for_app(app: &AuthApplication) -> Result<Self> {
        let mut ptr = std::ptr::null_mut();
        let rc = unsafe { sdk().blpapi_AuthOptions_create_forAppMode(&mut ptr, app.as_ptr()) };
        if rc != 0 || ptr.is_null() {
            return Err(BlpError::Internal {
                detail: format!("failed to create app AuthOptions: rc={rc}"),
            });
        }
        Ok(Self { ptr })
    }

    pub fn for_user_and_app(user: &AuthUser, app: &AuthApplication) -> Result<Self> {
        let mut ptr = std::ptr::null_mut();
        let rc = unsafe {
            sdk().blpapi_AuthOptions_create_forUserAndAppMode(&mut ptr, user.as_ptr(), app.as_ptr())
        };
        if rc != 0 || ptr.is_null() {
            return Err(BlpError::Internal {
                detail: format!("failed to create user+app AuthOptions: rc={rc}"),
            });
        }
        Ok(Self { ptr })
    }

    pub fn for_token(token: &AuthToken) -> Result<Self> {
        let mut ptr = std::ptr::null_mut();
        let rc = unsafe { sdk().blpapi_AuthOptions_create_forToken(&mut ptr, token.as_ptr()) };
        if rc != 0 || ptr.is_null() {
            return Err(BlpError::Internal {
                detail: format!("failed to create token AuthOptions: rc={rc}"),
            });
        }
        Ok(Self { ptr })
    }

    pub(crate) fn as_ptr(&self) -> *const ffi::blpapi_AuthOptions_t {
        self.ptr
    }
}

impl Drop for AuthOptions {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { sdk().blpapi_AuthOptions_destroy(self.ptr) };
            self.ptr = std::ptr::null_mut();
        }
    }
}

fn cstring(value: &str, field: &str) -> Result<CString> {
    CString::new(value).map_err(|e| BlpError::InvalidArgument {
        detail: format!("invalid {field}: {e}"),
    })
}

pub fn apply_session_identity_options(
    options: &mut crate::options::SessionOptions,
    auth_config: &AuthConfig,
) -> Result<CorrelationId> {
    let auth_options = auth_config.build_auth_options()?;
    options.set_session_identity_options(&auth_options)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_config_method_names_match_public_api() {
        assert_eq!(AuthConfig::User.method_name(), "user");
        assert_eq!(
            AuthConfig::App {
                app_name: "app".to_string(),
            }
            .method_name(),
            "app"
        );
        assert_eq!(
            AuthConfig::UserApp {
                app_name: "app".to_string(),
            }
            .method_name(),
            "userapp"
        );
        assert_eq!(
            AuthConfig::Directory {
                property_name: "mail=user@example.com".to_string(),
            }
            .method_name(),
            "dir"
        );
        assert_eq!(
            AuthConfig::Manual {
                app_name: "app".to_string(),
                user_id: "1234".to_string(),
                ip_address: "10.0.0.1".to_string(),
            }
            .method_name(),
            "manual"
        );
        assert_eq!(
            AuthConfig::Token {
                token: "token".to_string(),
            }
            .method_name(),
            "token"
        );
    }
}
