//! Python exception hierarchy and lazy exception plans for `xbbg._core`.
//!
//! Mirrors `py-xbbg/src/xbbg/exceptions.py`. Exceptions are represented as lazy
//! [`ExceptionPlan`]s so native worker threads can construct and drop errors
//! without attaching to the Python interpreter.

use std::sync::Arc;

use pyo3::PyErrArguments;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyString, PyType};
use xbbg_async::BlpAsyncError;
use xbbg_async::error_presentation::{ErrorKind, ErrorPresentation, ErrorStyle};
use xbbg_core::BlpError;
use xbbg_host::errors::{HostError, HostErrorKind};

pyo3::create_exception!(xbbg._core, BlpErrorBase, pyo3::exceptions::PyException);
pyo3::create_exception!(xbbg._core, BlpSessionError, BlpErrorBase);
pyo3::create_exception!(xbbg._core, BlpRequestError, BlpErrorBase);
pyo3::create_exception!(xbbg._core, BlpLimitError, BlpRequestError);
pyo3::create_exception!(xbbg._core, BlpSecurityError, BlpRequestError);
pyo3::create_exception!(xbbg._core, BlpFieldError, BlpRequestError);
pyo3::create_exception!(xbbg._core, BlpSubscriptionDataLossError, BlpRequestError);
pyo3::create_exception!(xbbg._core, BlpValidationError, BlpErrorBase);
pyo3::create_exception!(xbbg._core, BlpTimeoutError, BlpErrorBase);
pyo3::create_exception!(xbbg._core, BlpInternalError, BlpErrorBase);

/// Python exception classes raised for native errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExceptionClass {
    Session,
    Request,
    Limit,
    DataLoss,
    Validation,
    Timeout,
    Internal,
    /// Recipe or host argument errors.
    Value,
    /// Recipe or host failures outside the engine.
    Runtime,
}

impl From<ErrorKind> for ExceptionClass {
    fn from(kind: ErrorKind) -> Self {
        match kind {
            ErrorKind::Session => Self::Session,
            ErrorKind::Request => Self::Request,
            ErrorKind::Limit => Self::Limit,
            ErrorKind::DataLoss => Self::DataLoss,
            ErrorKind::Validation => Self::Validation,
            ErrorKind::Timeout => Self::Timeout,
            ErrorKind::Internal => Self::Internal,
        }
    }
}

impl From<HostErrorKind> for ExceptionClass {
    fn from(kind: HostErrorKind) -> Self {
        match kind {
            HostErrorKind::Session => Self::Session,
            HostErrorKind::Request => Self::Request,
            HostErrorKind::Limit => Self::Limit,
            HostErrorKind::DataLoss => Self::DataLoss,
            HostErrorKind::Validation => Self::Validation,
            HostErrorKind::Timeout => Self::Timeout,
            HostErrorKind::Internal => Self::Internal,
            HostErrorKind::Argument => Self::Value,
            HostErrorKind::State | HostErrorKind::Cancelled | HostErrorKind::SdkNotFound => {
                Self::Runtime
            }
        }
    }
}

impl ExceptionClass {
    fn type_object(self, py: Python<'_>) -> Bound<'_, PyType> {
        match self {
            Self::Session => py.get_type::<BlpSessionError>(),
            Self::Request => py.get_type::<BlpRequestError>(),
            Self::Limit => py.get_type::<BlpLimitError>(),
            Self::DataLoss => py.get_type::<BlpSubscriptionDataLossError>(),
            Self::Validation => py.get_type::<BlpValidationError>(),
            Self::Timeout => py.get_type::<BlpTimeoutError>(),
            Self::Internal => py.get_type::<BlpInternalError>(),
            Self::Value => py.get_type::<PyValueError>(),
            Self::Runtime => py.get_type::<PyRuntimeError>(),
        }
    }
}

/// One structured exception attribute value.
enum ExceptionAttr {
    None,
    Text(String),
    Texts(Vec<Arc<str>>),
}

impl From<Option<String>> for ExceptionAttr {
    fn from(value: Option<String>) -> Self {
        value.map_or(Self::None, Self::Text)
    }
}

/// Exception class, message and attributes captured without the interpreter.
///
/// The plan becomes a lazy `PyErr`: PyO3 builds the exception only when Python
/// observes it, so errors can be created and dropped on any native thread.
pub(crate) struct ExceptionPlan {
    class: ExceptionClass,
    message: String,
    attrs: Vec<(&'static str, ExceptionAttr)>,
}

impl ExceptionPlan {
    pub(crate) fn new(class: ExceptionClass, message: String) -> Self {
        Self {
            class,
            message,
            attrs: Vec::new(),
        }
    }

    fn set(&mut self, name: &'static str, value: ExceptionAttr) {
        match self
            .attrs
            .iter_mut()
            .find(|(existing, _)| *existing == name)
        {
            Some((_, slot)) => *slot = value,
            None => self.attrs.push((name, value)),
        }
    }

    /// SDK support identifiers; the singular attribute holds exactly one ID.
    pub(crate) fn set_bloomberg_request_ids(&mut self, ids: Vec<Arc<str>>) {
        let singular = match ids.as_slice() {
            [id] => ExceptionAttr::Text(id.to_string()),
            _ => ExceptionAttr::None,
        };
        self.set("bloomberg_request_id", singular);
        self.set("bloomberg_request_ids", ExceptionAttr::Texts(ids));
    }

    fn instantiate<'py>(&self, class: &Bound<'py, PyType>) -> PyResult<Bound<'py, PyAny>> {
        let py = class.py();
        let instance = class.call1((self.message.as_str(),))?;
        for (name, value) in &self.attrs {
            match value {
                ExceptionAttr::None => instance.setattr(*name, py.None())?,
                ExceptionAttr::Text(text) => instance.setattr(*name, text.as_str())?,
                ExceptionAttr::Texts(texts) => {
                    let texts: Vec<&str> = texts.iter().map(|text| &**text).collect();
                    instance.setattr(*name, texts)?
                }
            }
        }
        Ok(instance)
    }
}

impl From<ErrorPresentation> for ExceptionPlan {
    fn from(presentation: ErrorPresentation) -> Self {
        let kind = presentation.kind;
        let mut plan = Self::new(kind.into(), presentation.message);
        if let Some(context) = presentation.request_context {
            plan.set("service", context.service.into());
            plan.set("operation", context.operation.into());
            plan.set("request_id", context.request_id.into());
            plan.set_bloomberg_request_ids(context.bloomberg_request_ids);
        } else if matches!(
            kind,
            ErrorKind::Request | ErrorKind::Limit | ErrorKind::DataLoss | ErrorKind::Timeout
        ) {
            for name in ["service", "operation", "request_id"] {
                plan.set(name, ExceptionAttr::None);
            }
            plan.set_bloomberg_request_ids(Vec::new());
        }
        if let Some(context) = presentation.data_loss {
            plan.set("topic", ExceptionAttr::Text(context.topic));
            plan.set("detail", ExceptionAttr::Text(context.detail));
        }
        plan
    }
}

impl From<HostError> for ExceptionPlan {
    fn from(error: HostError) -> Self {
        let kind = error.kind;
        let mut plan = Self::new(kind.into(), error.message);
        let ctx = error.context;
        let has_request_ctx = ctx.service.is_some()
            || ctx.operation.is_some()
            || ctx.request_id.is_some()
            || !ctx.bloomberg_request_ids.is_empty();
        if has_request_ctx {
            plan.set("service", ctx.service.into());
            plan.set("operation", ctx.operation.into());
            plan.set("request_id", ctx.request_id.into());
            plan.set_bloomberg_request_ids(
                ctx.bloomberg_request_ids
                    .into_iter()
                    .map(Arc::from)
                    .collect(),
            );
        } else if matches!(
            kind,
            HostErrorKind::Request
                | HostErrorKind::Limit
                | HostErrorKind::DataLoss
                | HostErrorKind::Timeout
        ) {
            for name in ["service", "operation", "request_id"] {
                plan.set(name, ExceptionAttr::None);
            }
            plan.set_bloomberg_request_ids(Vec::new());
        }
        if let Some(topic) = ctx.topic {
            plan.set("topic", ExceptionAttr::Text(topic));
        }
        if let Some(detail) = ctx.detail {
            plan.set("detail", ExceptionAttr::Text(detail));
        }
        plan
    }
}

impl PyErrArguments for ExceptionPlan {
    /// `PyErr_SetObject` raises an instance of the error's class as-is, so the
    /// fully attributed instance is the raised exception. If attributes cannot
    /// be set the plain instance is raised; if construction fails, CPython
    /// instantiates the class from the message itself.
    fn arguments(self, py: Python<'_>) -> Py<PyAny> {
        let class = self.class.type_object(py);
        self.instantiate(&class)
            .or_else(|_| class.call1((self.message.as_str(),)))
            .map_or_else(
                |_| PyString::new(py, &self.message).into_any().unbind(),
                Bound::unbind,
            )
    }
}

impl From<ExceptionPlan> for PyErr {
    fn from(plan: ExceptionPlan) -> Self {
        match plan.class {
            ExceptionClass::Session => PyErr::new::<BlpSessionError, _>(plan),
            ExceptionClass::Request => PyErr::new::<BlpRequestError, _>(plan),
            ExceptionClass::Limit => PyErr::new::<BlpLimitError, _>(plan),
            ExceptionClass::DataLoss => PyErr::new::<BlpSubscriptionDataLossError, _>(plan),
            ExceptionClass::Validation => PyErr::new::<BlpValidationError, _>(plan),
            ExceptionClass::Timeout => PyErr::new::<BlpTimeoutError, _>(plan),
            ExceptionClass::Internal => PyErr::new::<BlpInternalError, _>(plan),
            ExceptionClass::Value => PyErr::new::<PyValueError, _>(plan),
            ExceptionClass::Runtime => PyErr::new::<PyRuntimeError, _>(plan),
        }
    }
}

/// Convert a classified error into a lazy Python exception with its attributes.
pub(crate) fn error_presentation_to_pyerr(presentation: ErrorPresentation) -> PyErr {
    ExceptionPlan::from(presentation).into()
}

/// Convert a core error into its Python exception, retaining structured attributes.
pub(crate) fn blp_error_to_pyerr(error: BlpError) -> PyErr {
    error_presentation_to_pyerr(ErrorPresentation::from_blp(error, ErrorStyle::Python))
}

/// Convert an async error into its Python exception.
pub(crate) fn blp_async_error_to_pyerr(error: BlpAsyncError) -> PyErr {
    error_presentation_to_pyerr(ErrorPresentation::from_async(error, ErrorStyle::Python))
}

/// Convert a host-layer error into a lazy Python exception.
pub(crate) fn host_error_to_pyerr(error: HostError) -> PyErr {
    ExceptionPlan::from(error).into()
}

pub(crate) fn subscription_control_error_to_pyerr(error: BlpAsyncError) -> PyErr {
    match error {
        BlpAsyncError::ChannelClosed => PyRuntimeError::new_err("subscription already closed"),
        error => blp_async_error_to_pyerr(error),
    }
}

pub(crate) fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("BlpError", py.get_type::<BlpErrorBase>())?;
    m.add("BlpSessionError", py.get_type::<BlpSessionError>())?;
    m.add("BlpRequestError", py.get_type::<BlpRequestError>())?;
    m.add("BlpLimitError", py.get_type::<BlpLimitError>())?;
    m.add("BlpSecurityError", py.get_type::<BlpSecurityError>())?;
    m.add("BlpFieldError", py.get_type::<BlpFieldError>())?;
    m.add(
        "BlpSubscriptionDataLossError",
        py.get_type::<BlpSubscriptionDataLossError>(),
    )?;
    m.add("BlpValidationError", py.get_type::<BlpValidationError>())?;
    m.add("BlpTimeoutError", py.get_type::<BlpTimeoutError>())?;
    m.add("BlpInternalError", py.get_type::<BlpInternalError>())?;
    Ok(())
}
