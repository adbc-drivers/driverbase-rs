// Copyright (c) 2025 Columnar Technologies Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Utilities for building legible, informative errors in ADBC drivers.

use std::fmt::Write;

/// A lightweight error type.
#[derive(Clone)]
pub struct Error<E>
where
    E: ErrorHelper,
{
    // boxed so that the on-stack size is small
    inner: Box<ErrorImpl<E>>,
    _marker: std::marker::PhantomData<E>,
}

#[derive(Clone)]
struct ErrorImpl<E>
where
    E: ErrorHelper,
{
    status: adbc_core::error::Status,
    // Message will be "could not <CONTEXT_MESSAGE>: <ERROR_MESSAGE>"
    error_message: String,
    context_message: Option<String>,
    location: Option<String>,
    vendor_code: i32,
    sqlstate: [std::os::raw::c_char; 5],
    #[allow(
        clippy::vec_box,
        reason = "inner is Boxed - no need to move when combining"
    )]
    rest: Vec<Box<ErrorImpl<E>>>,
    _marker: std::marker::PhantomData<E>,
}

impl<E> Error<E>
where
    E: ErrorHelper,
{
    pub fn get_vendor_code(&self) -> i32 {
        self.inner.vendor_code
    }

    pub fn to_adbc(self) -> adbc_core::error::Error {
        self.into()
    }

    /// Add a new clause to the error message.
    pub fn message(mut self, message: impl AsRef<str>) -> Self {
        if !self.inner.error_message.is_empty() {
            self.inner.error_message.push_str("; ");
        }
        write!(&mut self.inner.error_message, "{}", message.as_ref()).unwrap();
        self
    }

    /// Add a new clause to the context message.
    pub fn context(mut self, message: impl AsRef<str>) -> Self {
        self.inner.context_message = match self.inner.context_message.take() {
            None => Some(message.as_ref().to_owned()),
            Some(ctx) => Some(format!("{}: could not {ctx}", message.as_ref())),
        };
        self
    }

    /// Add a source code location to the error.
    pub fn location(mut self, location: impl AsRef<str>) -> Self {
        self.inner.location = Some(location.as_ref().to_owned());
        self
    }

    /// Add a new clause to the error message.
    pub fn format(mut self, message: std::fmt::Arguments) -> Self {
        if !self.inner.error_message.is_empty() {
            self.inner.error_message.push_str("; ");
        }
        write!(&mut self.inner.error_message, "{message}").unwrap();
        self
    }

    /// Add a vendor-specific code to the error.
    pub fn vendor_code(mut self, code: i32) -> Self {
        self.inner.vendor_code = code;
        self
    }

    /// Add an ANSI SQL-style SQLSTATE code to the error.
    pub fn sqlstate(mut self, sqlstate: [std::os::raw::c_char; 5]) -> Self {
        self.inner.sqlstate = sqlstate;
        self
    }

    /// Merge two errors.
    pub fn and(mut self, mut other: Error<E>) -> Self {
        let rest = std::mem::take(&mut other.inner.rest);
        self.inner.rest.push(other.inner);
        self.inner.rest.extend(rest);
        self
    }

    /// Merge this error with a potential error from a fallible operation.
    pub fn and_also<T>(self, other: impl FnOnce() -> Result<T, Error<E>>) -> Self {
        match other() {
            Ok(_) => self,
            Err(other) => self.and(other),
        }
    }
}

impl<E> From<Error<E>> for adbc_core::error::Error
where
    E: ErrorHelper,
{
    fn from(value: Error<E>) -> Self {
        let mut message = match value.inner.context_message {
            Some(ctx) => format!(
                "[{}] could not {}: {}",
                E::NAME,
                ctx,
                value.inner.error_message
            ),
            None => format!("[{}] {}", E::NAME, value.inner.error_message),
        };

        if let Some(location) = value.inner.location {
            write!(&mut message, " (at {location})").unwrap();
        }

        // TODO: deal with other details of 'rest'
        for (i, err) in value.inner.rest.iter().enumerate() {
            if i == 0 {
                write!(&mut message, ". While handling error: ").unwrap();
            } else {
                write!(&mut message, "; ").unwrap();
            }
            match err.context_message {
                Some(ref ctx) => {
                    write!(&mut message, "could not {}: {}", ctx, err.error_message).unwrap()
                }
                None => write!(&mut message, "{}", err.error_message).unwrap(),
            }

            if let Some(ref location) = err.location {
                write!(&mut message, " (at {location})").unwrap();
            }
        }

        adbc_core::error::Error {
            message,
            status: value.inner.status,
            vendor_code: value.inner.vendor_code,
            sqlstate: value.inner.sqlstate,
            details: None,
        }
    }
}

impl<E> From<arrow_schema::ArrowError> for Error<E>
where
    E: ErrorHelper,
{
    fn from(value: arrow_schema::ArrowError) -> Self {
        E::from_arrow(value)
    }
}

impl<E> std::fmt::Debug for Error<E>
where
    E: ErrorHelper,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl<E> std::fmt::Display for Error<E>
where
    E: ErrorHelper,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: [{}] ", self.inner.status, E::NAME)?;
        if let Some(ref ctx) = self.inner.context_message {
            write!(f, "could not {ctx}: ")?;
        }
        write!(f, "{}", self.inner.error_message)?;

        if let Some(ref location) = self.inner.location {
            write!(f, ". Location: {location}").unwrap();
        }

        if self.inner.vendor_code != 0 {
            write!(f, ". Vendor code: {}", self.inner.vendor_code)?;
        }
        Ok(())
    }
}

impl<E> std::fmt::Debug for ErrorImpl<E>
where
    E: ErrorHelper,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Error {\n")?;
        writeln!(f, "  status: {:?},", self.status)?;
        writeln!(f, "  error_message: {:?},", self.error_message)?;
        writeln!(f, "  context_message: {:?},", self.context_message)?;
        writeln!(f, "  vendor_code: {},", self.vendor_code)?;
        writeln!(f, "  sqlstate: {:?},", self.sqlstate)?;
        write!(f, "  rest: [")?;
        if !self.rest.is_empty() {
            writeln!(f)?;
            for err in &self.rest {
                writeln!(f, "    {err:?},")?;
            }
        }
        writeln!(f, "],")?;
        f.write_str("}")?;
        Ok(())
    }
}

impl<E> std::error::Error for Error<E> where E: ErrorHelper {}

/// A factory for errors with consistent formatting and messaging.
pub trait ErrorHelper: Clone + Send + Sync + Sized + 'static {
    const NAME: &'static str;

    /// Error for when a user requests an unknown option.
    fn get_unknown_option<K: std::fmt::Debug>(key: &K) -> Error<Self> {
        // TODO: don't use the Rust repr of keys/values
        Self::not_found().format(format_args!("unknown option {key:?}"))
    }

    /// Error for when when a user sets an option to an invalid value.
    fn set_invalid_option<K: std::fmt::Debug>(
        key: &K,
        value: &adbc_core::options::OptionValue,
    ) -> Error<Self> {
        // TODO: don't use the Rust repr of keys/values
        Self::invalid_argument().context(format!("invalid option {key:?}={value:?}"))
    }

    /// Error for when a user sets an unknown option.
    fn set_unknown_option<K: std::fmt::Debug>(key: &K) -> Error<Self> {
        // TODO: don't use the Rust repr of keys/values
        Self::not_implemented().format(format_args!("unknown option {key:?}"))
    }

    /// Unwrap an option value as an integer.
    fn option_as_int<K: std::fmt::Debug>(
        k: &K,
        v: &adbc_core::options::OptionValue,
    ) -> Result<i64, super::error::Error<Self>> {
        match v {
            adbc_core::options::OptionValue::String(s) => s
                .parse::<i64>()
                .map_err(|_| Self::set_invalid_option(k, v).message("must be an integer")),
            adbc_core::options::OptionValue::Int(v) => Ok(*v),
            v => Err(Self::set_invalid_option(k, v).message("must be an integer")),
        }
    }

    /// Unwrap an option value as a string.
    fn option_as_string<'v, K: std::fmt::Debug>(
        k: &K,
        v: &'v adbc_core::options::OptionValue,
    ) -> Result<&'v str, super::error::Error<Self>> {
        match v {
            adbc_core::options::OptionValue::String(s) => Ok(s.as_ref()),
            v => Err(Self::set_invalid_option(k, v).message("must be a string")),
        }
    }

    /// An error with the given status and no message.
    fn status(status: adbc_core::error::Status) -> Error<Self> {
        Error::<Self> {
            inner: Box::new(ErrorImpl::<Self> {
                status,
                error_message: String::new(),
                context_message: None,
                location: None,
                vendor_code: 0,
                sqlstate: Default::default(),
                rest: Vec::new(),
                _marker: std::marker::PhantomData,
            }),
            _marker: std::marker::PhantomData,
        }
    }

    fn cancelled() -> Error<Self> {
        Self::status(adbc_core::error::Status::Cancelled)
    }

    fn internal(location: impl AsRef<str>) -> Error<Self> {
        Self::status(adbc_core::error::Status::Internal).location(location)
    }

    fn internal_no_location() -> Error<Self> {
        Self::status(adbc_core::error::Status::Internal)
    }

    fn invalid_argument() -> Error<Self> {
        Self::status(adbc_core::error::Status::InvalidArguments)
    }

    fn invalid_data() -> Error<Self> {
        Self::status(adbc_core::error::Status::InvalidData)
    }

    fn invalid_state() -> Error<Self> {
        Self::status(adbc_core::error::Status::InvalidState)
    }

    fn io() -> Error<Self> {
        Self::status(adbc_core::error::Status::IO)
    }

    fn not_found() -> Error<Self> {
        Self::status(adbc_core::error::Status::NotFound)
    }

    fn not_implemented() -> Error<Self> {
        Self::status(adbc_core::error::Status::NotImplemented)
    }

    fn unknown() -> Error<Self> {
        Self::status(adbc_core::error::Status::Unknown)
    }

    /// Box an ADBC error as an Arrow error.
    fn to_arrow(err: adbc_core::error::Error) -> arrow_schema::ArrowError {
        arrow_schema::ArrowError::ExternalError(Box::new(err))
    }

    /// Convert an Arrow error into this error, mapping status appropriately.
    fn from_arrow(err: arrow_schema::ArrowError) -> Error<Self> {
        match err {
            arrow_schema::ArrowError::NotYetImplemented(msg) => {
                Self::not_implemented().format(format_args!("{msg}"))
            }
            arrow_schema::ArrowError::ExternalError(error) => {
                Self::unknown().format(format_args!("{error}"))
            }
            arrow_schema::ArrowError::CastError(msg) => {
                Self::internal_no_location().format(format_args!("cast error: {msg}"))
            }
            arrow_schema::ArrowError::MemoryError(msg) => {
                Self::internal_no_location().format(format_args!("memory error: {msg}"))
            }
            arrow_schema::ArrowError::ParseError(msg) => {
                Self::internal_no_location().format(format_args!("parse error: {msg}"))
            }
            arrow_schema::ArrowError::SchemaError(msg) => {
                Self::internal_no_location().format(format_args!("schema error: {msg}"))
            }
            arrow_schema::ArrowError::ComputeError(msg) => {
                Self::internal_no_location().format(format_args!("compute error: {msg}"))
            }
            arrow_schema::ArrowError::DivideByZero => {
                Self::invalid_data().format(format_args!("divide by zero",))
            }
            arrow_schema::ArrowError::ArithmeticOverflow(msg) => {
                Self::invalid_data().format(format_args!("arithmetic overflow: {msg}"))
            }
            arrow_schema::ArrowError::CsvError(msg) => {
                Self::invalid_argument().format(format_args!("CSV error: {msg}"))
            }
            arrow_schema::ArrowError::JsonError(msg) => {
                Self::invalid_argument().format(format_args!("JSON error: {msg}"))
            }
            arrow_schema::ArrowError::IoError(msg, error) => {
                Self::io().format(format_args!("I/O error: {msg}: {error}"))
            }
            arrow_schema::ArrowError::IpcError(msg) => {
                Self::io().format(format_args!("IPC error: {msg}"))
            }
            arrow_schema::ArrowError::InvalidArgumentError(msg) => {
                Self::invalid_argument().message(msg)
            }
            arrow_schema::ArrowError::ParquetError(msg) => {
                Self::io().format(format_args!("Parquet error: {msg}"))
            }
            arrow_schema::ArrowError::CDataInterface(msg) => {
                Self::io().format(format_args!("C Data interface error: {msg}"))
            }
            arrow_schema::ArrowError::DictionaryKeyOverflowError => {
                Self::invalid_data().message("dictionary key overflowed")
            }
            arrow_schema::ArrowError::RunEndIndexOverflowError => {
                Self::invalid_data().message("run end index overflowed")
            }
            arrow_schema::ArrowError::OffsetOverflowError(size) => {
                Self::invalid_data().format(format_args!("offset overflowed at size {size}"))
            }
        }
    }
}

#[macro_export]
macro_rules! location {
    () => {
        format!("{}:{}", file!(), line!())
    };
}
pub(crate) use location;

#[cfg(test)]
mod test {
    use super::ErrorHelper;

    #[derive(Clone)]
    struct TestErrorHelper;

    impl ErrorHelper for TestErrorHelper {
        const NAME: &'static str = "goosedb";
    }

    #[test]
    fn test_display() {
        let error = TestErrorHelper::invalid_argument().message("an error occurred");
        assert_eq!(
            format!("{error}"),
            "InvalidArguments: [goosedb] an error occurred"
        );

        let error = TestErrorHelper::invalid_argument()
            .message("an error occurred")
            .context("foo");
        assert_eq!(
            format!("{error}"),
            "InvalidArguments: [goosedb] could not foo: an error occurred"
        );

        let error = TestErrorHelper::invalid_argument()
            .message("an error occurred")
            .context("foo")
            .vendor_code(42);
        assert_eq!(
            format!("{error}"),
            "InvalidArguments: [goosedb] could not foo: an error occurred. Vendor code: 42"
        );

        // TODO: SQLSTATE
    }

    #[test]
    fn test_message() {
        let error = TestErrorHelper::invalid_argument().message("an error occurred");
        let adbc_error = error.to_adbc();
        assert_eq!(
            adbc_error.status,
            adbc_core::error::Status::InvalidArguments
        );
        assert_eq!(adbc_error.message, "[goosedb] an error occurred");
    }

    #[test]
    fn test_message_context() {
        let error = TestErrorHelper::invalid_argument()
            .message("an error occurred")
            .context("foo");
        let adbc_error = error.to_adbc();
        assert_eq!(
            adbc_error.status,
            adbc_core::error::Status::InvalidArguments
        );
        assert_eq!(
            adbc_error.message,
            "[goosedb] could not foo: an error occurred"
        );
    }

    #[test]
    fn test_combine() {
        let error = TestErrorHelper::invalid_argument()
            .message("an error occurred")
            .and(TestErrorHelper::invalid_state().message("another error occurred"));
        let adbc_error = error.to_adbc();
        assert_eq!(
            adbc_error.status,
            adbc_core::error::Status::InvalidArguments
        );
        assert_eq!(
            adbc_error.message,
            "[goosedb] an error occurred. While handling error: another error occurred"
        );
    }

    #[test]
    fn test_combine_multi() {
        let error = TestErrorHelper::invalid_argument()
            .message("an error occurred")
            .and(
                TestErrorHelper::invalid_state()
                    .message("another error occurred")
                    .and(
                        TestErrorHelper::internal("foobar.rs:123")
                            .message("yet another error occurred"),
                    ),
            );
        let adbc_error = error.to_adbc();
        assert_eq!(
            adbc_error.status,
            adbc_core::error::Status::InvalidArguments
        );
        assert_eq!(
            adbc_error.message,
            "[goosedb] an error occurred. While handling error: another error occurred; yet another error occurred (at foobar.rs:123)"
        );
    }
}
