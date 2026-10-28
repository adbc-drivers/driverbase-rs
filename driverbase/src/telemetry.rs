// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! OpenTelemetry tracing for ADBC drivers, following the Go driverbase
//! conventions.
//!
//! Create a [`Telemetry`], add [`Telemetry::app_layer`] to the driver's
//! tracing subscriber, choose an exporter with [`Telemetry::configure`], and
//! run each ADBC operation through [`Telemetry::traced`].

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use adbc_core::error::{Error, Result, Status};
use chrono::{DateTime, SecondsFormat, Utc};
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::{
    Link, SamplingResult, SpanKind, Status as SpanStatus, TraceContextExt, TraceId,
};
use opentelemetry::{Context, InstrumentationScope, KeyValue, Value};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{
    BatchSpanProcessor, Sampler, SdkTracerProvider, ShouldSample, Span as SdkSpan, SpanData,
    SpanExporter, SpanProcessor,
};
use serde_json::{Value as JsonValue, json};
use tracing_opentelemetry::{OpenTelemetryLayer, OpenTelemetrySpanExt};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::Layer;
use tracing_subscriber::registry::LookupSpan;

/// ADBC connection option carrying a W3C traceparent header.
pub const TRACE_PARENT: &str = "adbc.telemetry.trace_parent";
/// ADBC connection option selecting the trace exporter.
pub const TRACES_EXPORTER: &str = "adbc.telemetry.traces_exporter";
/// ADBC connection option selecting the trace file directory.
pub const TRACES_FOLDER_PATH: &str = "adbc.telemetry.traces_folder_path";
const MAX_FILE_BYTES: u64 = 1024 * 1024;
const MAX_FILES: usize = 100;

/// The destination for ADBC spans.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exporter {
    None,
    /// OTLP over HTTP/protobuf, configured by the standard
    /// `OTEL_EXPORTER_OTLP_*` environment variables.
    Otlp,
    /// JSON lines on stdout, using the field names of Go's `stdouttrace`.
    Console,
    /// Rotating JSON-lines files in [`TRACES_FOLDER_PATH`] or the platform's
    /// default ADBC traces folder, in the same format as [`Exporter::Console`].
    AdbcFile,
}

impl Exporter {
    /// Parse the value of [`TRACES_EXPORTER`] or `OTEL_TRACES_EXPORTER`.
    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "none" => Ok(Self::None),
            "otlp" => Ok(Self::Otlp),
            "console" => Ok(Self::Console),
            "adbcfile" => Ok(Self::AdbcFile),
            _ => Err(Error::with_message_and_status(
                format!("unknown traces exporter '{name}'"),
                Status::InvalidArguments,
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Otlp => "otlp",
            Self::Console => "console",
            Self::AdbcFile => "adbcfile",
        }
    }
}

struct ParentHeader<'a>(&'a str);

impl Extractor for ParentHeader<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        (key == "traceparent").then_some(self.0)
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent"]
    }
}

/// Extract a remote W3C parent for a driver operation span.
pub fn parent_context(value: &str) -> Context {
    TraceContextPropagator::new().extract(&ParentHeader(value))
}

/// Validate a W3C traceparent, or clear the configured parent when empty.
pub fn validate_trace_parent(value: &str) -> Result<Option<String>> {
    if value.is_empty() {
        return Ok(None);
    }
    if !parent_context(value).span().span_context().is_valid() {
        return Err(Error::with_message_and_status(
            format!("invalid W3C traceparent: '{value}'"),
            Status::InvalidArguments,
        ));
    }
    Ok(Some(value.to_owned()))
}

/// An ADBC tracer that can change its exporter without changing its subscriber.
/// The caller must keep the dispatch built with [`Telemetry::app_layer`] alive
/// while issuing driver operations and may compose other layers with it.
pub struct Telemetry {
    provider: SdkTracerProvider,
    processor: SwitchableProcessor,
    enabled: Arc<AtomicBool>,
    configured: Mutex<Option<(Exporter, Option<PathBuf>)>>,
    resource: Resource,
    scope: InstrumentationScope,
    file_prefix: Arc<str>,
    db_system_name: Box<str>,
}

impl Telemetry {
    /// Construct an initially disabled tracer for an ADBC driver.
    /// `driver_name` is used in the instrumentation scope and JSONL filenames;
    /// `db_system_name` is written to operation spans as `db.system.name`.
    pub fn new(driver_name: &str, driver_version: &str, db_system_name: &str) -> Self {
        let enabled = Arc::new(AtomicBool::new(false));
        let processor = SwitchableProcessor::default();
        let resource = Resource::builder()
            .with_service_name("apache.arrow.adbc")
            .build();
        let provider = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_sampler(SwitchableSampler(Arc::clone(&enabled)))
            .with_span_processor(processor.clone())
            .build();
        Self {
            provider,
            processor,
            enabled,
            configured: Mutex::new(None),
            resource,
            scope: InstrumentationScope::builder(format!("apache.arrow.adbc.{driver_name}"))
                .with_version(driver_version.to_owned())
                .with_schema_url(opentelemetry_semantic_conventions::SCHEMA_URL)
                .build(),
            file_prefix: Arc::from(format!("apache.arrow.adbc.{driver_name}-")),
            db_system_name: db_system_name.into(),
        }
    }

    /// Obtain an SDK tracer for the driver's named instrumentation scope.
    pub fn tracer(&self) -> opentelemetry_sdk::trace::SdkTracer {
        use opentelemetry::trace::TracerProvider;
        self.provider.tracer_with_scope(self.scope.clone())
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Only creates OTel data for spans targeting `driverbase`. A driver can
    /// append its own unfiltered layers (e.g. a core-span mirror) so they can
    /// still look up this layer's span context in the parent registry.
    ///
    /// Keep the subscriber and the [`tracing::Dispatch`] built from it alive
    /// for the life of the process, and call [`Telemetry::flush`] when a
    /// database is released rather than dropping them: tracing-core (0.1.36)
    /// drops other dispatchers while holding its callsite lock when a new one
    /// is created, and dropping this layer's tracer provider emits a tracing
    /// event, so dropping one dispatcher while creating another deadlocks.
    pub fn app_layer<S>(&self) -> impl Layer<S> + Send + Sync + 'static + use<S>
    where
        S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync + 'static,
    {
        OpenTelemetryLayer::new(self.tracer())
            .with_filter(Targets::new().with_target("driverbase", LevelFilter::TRACE))
    }

    /// Set the exporter; explicit connection options take precedence over the
    /// `OTEL_TRACES_EXPORTER` environment variable. Without either, tracing is
    /// disabled. `folder` is used only for [`Exporter::AdbcFile`]. The OTLP
    /// HTTP/protobuf exporter honors the OpenTelemetry OTLP environment vars.
    pub fn configure(&self, option: Option<Exporter>, folder: Option<&str>) -> Result<()> {
        let exporter = resolve_exporter(
            option,
            std::env::var("OTEL_TRACES_EXPORTER").ok().as_deref(),
        )?;
        let folder = (exporter == Exporter::AdbcFile)
            .then(|| {
                folder
                    .filter(|path| !path.trim().is_empty())
                    .map(PathBuf::from)
            })
            .flatten();
        let mut configured = self
            .configured
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if configured.as_ref() == Some(&(exporter, folder.clone())) {
            return Ok(());
        }
        let processor = match exporter {
            Exporter::None => None,
            Exporter::Otlp => {
                let exporter = opentelemetry_otlp::SpanExporter::builder()
                    .with_http()
                    .build()
                    .map_err(|error| telemetry_error(error, Status::InvalidArguments))?;
                Some(BatchSpanProcessor::builder(exporter).build())
            }
            Exporter::Console => {
                Some(BatchSpanProcessor::builder(JsonExporter::console(&self.resource)).build())
            }
            Exporter::AdbcFile => {
                let directory = match folder.as_ref() {
                    Some(directory) => directory.clone(),
                    None => default_folder()?,
                };
                let writer = RotatingFileWriter::new(
                    directory,
                    Arc::clone(&self.file_prefix),
                    MAX_FILE_BYTES,
                    MAX_FILES,
                )
                .map_err(|error| telemetry_error(error, Status::IO))?;
                Some(
                    BatchSpanProcessor::builder(JsonExporter::file(writer, &self.resource)).build(),
                )
            }
        };
        // Stop sampling before swapping the old processor, then enable the
        // newly installed processor. The subscriber and tracing parents stay fixed.
        self.enabled.store(false, Ordering::Release);
        let previous = self.processor.replace(processor);
        self.enabled
            .store(exporter != Exporter::None, Ordering::Release);
        *configured = Some((exporter, folder));
        if let Some(previous) = previous {
            let _ = previous.shutdown();
        }
        Ok(())
    }

    /// Flush pending spans. Useful before inspecting trace files and at shutdown.
    pub fn flush(&self) {
        if self.is_enabled() {
            let _ = self.provider.force_flush();
        }
    }

    /// Flush and shut down the SDK provider.
    pub fn shutdown(&self) {
        let _ = self.provider.force_flush();
        let _ = self.provider.shutdown();
    }

    /// Run an ADBC operation with a remote W3C parent and OTel status/error
    /// attributes. The driver may record `db.response.returned_rows` on the
    /// callback span, and may use `OpenTelemetrySpanExt::set_attribute` for
    /// driver-specific fields without adding them to this generic span.
    pub fn traced<T>(
        &self,
        dispatch: &tracing::Dispatch,
        name: &'static str,
        parent: Option<&str>,
        body: impl FnOnce(&tracing::Span) -> Result<T>,
    ) -> Result<T> {
        tracing::dispatcher::with_default(dispatch, || {
            if !self.is_enabled() {
                return body(&tracing::Span::none());
            }
            let span = tracing::info_span!(
                target: "driverbase",
                "adbc.operation",
                otel.name = name,
                db.system.name = &*self.db_system_name,
                db.operation.name = name,
                error.type = tracing::field::Empty,
                db.response.returned_rows = tracing::field::Empty,
            );
            if let Some(parent) = parent {
                span.set_parent(parent_context(parent));
            }
            let result = span.in_scope(|| body(&span));
            match &result {
                Ok(_) => span.set_status(SpanStatus::Ok),
                Err(error) => {
                    span.record("error.type", status_name(error.status));
                    tracing::error!(parent: &span, error = %error, "ADBC operation failed");
                    span.set_status(SpanStatus::error(error.message.clone()));
                }
            }
            result
        })
    }
}

fn resolve_exporter(option: Option<Exporter>, environment: Option<&str>) -> Result<Exporter> {
    match option {
        Some(exporter) => Ok(exporter),
        None => environment
            .filter(|value| !value.is_empty())
            .map_or(Ok(Exporter::None), Exporter::parse),
    }
}

fn telemetry_error(error: impl std::fmt::Display, status: Status) -> Error {
    Error::with_message_and_status(format!("failed to configure tracing: {error}"), status)
}

#[derive(Clone, Debug)]
struct SwitchableSampler(Arc<AtomicBool>);

impl ShouldSample for SwitchableSampler {
    fn should_sample(
        &self,
        parent_context: Option<&Context>,
        trace_id: TraceId,
        name: &str,
        span_kind: &SpanKind,
        attributes: &[KeyValue],
        links: &[Link],
    ) -> SamplingResult {
        static PARENT_BASED: LazyLock<Sampler> =
            LazyLock::new(|| Sampler::ParentBased(Box::new(Sampler::AlwaysOn)));
        if self.0.load(Ordering::Acquire) {
            PARENT_BASED.should_sample(parent_context, trace_id, name, span_kind, attributes, links)
        } else {
            Sampler::AlwaysOff.should_sample(
                parent_context,
                trace_id,
                name,
                span_kind,
                attributes,
                links,
            )
        }
    }
}

#[derive(Clone, Default, Debug)]
struct SwitchableProcessor {
    current: Arc<RwLock<Option<BatchSpanProcessor>>>,
}

impl SwitchableProcessor {
    fn replace(&self, next: Option<BatchSpanProcessor>) -> Option<BatchSpanProcessor> {
        std::mem::replace(
            &mut *self.current.write().unwrap_or_else(|err| err.into_inner()),
            next,
        )
    }
}

impl SpanProcessor for SwitchableProcessor {
    fn on_start(&self, _span: &mut SdkSpan, _context: &Context) {}

    fn on_end(&self, span: SpanData) {
        if let Some(current) = self
            .current
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
        {
            current.on_end(span);
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.current
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .map_or(Ok(()), SpanProcessor::force_flush)
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.replace(None)
            .map_or(Ok(()), |current| current.shutdown_with_timeout(timeout))
    }
}

#[derive(Debug)]
enum JsonOutput {
    Stdout,
    File(Mutex<RotatingFileWriter>),
}

#[derive(Debug)]
struct JsonExporter {
    output: JsonOutput,
    resource: Resource,
}

impl JsonExporter {
    fn console(resource: &Resource) -> Self {
        Self {
            output: JsonOutput::Stdout,
            resource: resource.clone(),
        }
    }

    fn file(writer: RotatingFileWriter, resource: &Resource) -> Self {
        Self {
            output: JsonOutput::File(Mutex::new(writer)),
            resource: resource.clone(),
        }
    }

    fn write_spans(&self, spans: &[SpanData]) -> io::Result<()> {
        match &self.output {
            JsonOutput::Stdout => {
                let stdout = io::stdout();
                let mut writer = stdout.lock();
                for span in spans {
                    serde_json::to_writer(&mut writer, &span_json(span, &self.resource))?;
                    writer.write_all(b"\n")?;
                }
                writer.flush()
            }
            JsonOutput::File(writer) => {
                let mut writer = writer.lock().unwrap_or_else(|err| err.into_inner());
                for span in spans {
                    let mut line = serde_json::to_vec(&span_json(span, &self.resource))?;
                    line.push(b'\n');
                    writer.write_line(&line)?;
                }
                writer.flush()
            }
        }
    }
}

impl SpanExporter for JsonExporter {
    async fn export(&self, spans: Vec<SpanData>) -> OTelSdkResult {
        self.write_spans(&spans)
            .map_err(|error| OTelSdkError::InternalFailure(error.to_string()))
    }
}

fn json_attribute(attribute: &KeyValue) -> JsonValue {
    let (kind, value) = match &attribute.value {
        Value::Bool(v) => ("BOOL", json!(v)),
        Value::I64(v) => ("INT64", json!(v)),
        Value::F64(v) => ("FLOAT64", json!(v)),
        Value::String(v) => ("STRING", json!(v.as_str())),
        Value::Array(opentelemetry::Array::Bool(v)) => ("BOOLSLICE", json!(v)),
        Value::Array(opentelemetry::Array::I64(v)) => ("INT64SLICE", json!(v)),
        Value::Array(opentelemetry::Array::F64(v)) => ("FLOAT64SLICE", json!(v)),
        Value::Array(opentelemetry::Array::String(v)) => (
            "STRINGSLICE",
            JsonValue::Array(v.iter().map(|value| json!(value.as_str())).collect()),
        ),
        _ => ("INVALID", JsonValue::Null),
    };
    json!({ "Key": attribute.key.as_str(), "Value": {"Type": kind, "Value": value} })
}

fn span_context_json(context: &opentelemetry::trace::SpanContext) -> JsonValue {
    json!({
        "TraceID": context.trace_id().to_string(),
        "SpanID": context.span_id().to_string(),
        "TraceFlags": format!("{:02x}", context.trace_flags().to_u8()),
        "TraceState": context.trace_state().header(),
        "Remote": context.is_remote(),
    })
}

fn timestamp(time: SystemTime) -> String {
    DateTime::<Utc>::from(time).to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn span_json(span: &SpanData, resource: &Resource) -> JsonValue {
    // Root spans get an all-zero parent, as Go's stdouttrace writes.
    let parent = if span.parent_span_id == opentelemetry::trace::SpanId::INVALID {
        opentelemetry::trace::SpanContext::empty_context()
    } else {
        opentelemetry::trace::SpanContext::new(
            span.span_context.trace_id(),
            span.parent_span_id,
            span.span_context.trace_flags(),
            true,
            span.span_context.trace_state().clone(),
        )
    };
    let (status_code, description) = match &span.status {
        SpanStatus::Unset => ("Unset", ""),
        SpanStatus::Ok => ("Ok", ""),
        SpanStatus::Error { description } => ("Error", description.as_ref()),
    };
    let events: Vec<_> = span
        .events
        .events
        .iter()
        .map(|event| {
            json!({
                "Name": event.name,
                "Attributes": event.attributes.iter().map(json_attribute).collect::<Vec<_>>(),
                "DroppedAttributeCount": event.dropped_attributes_count,
                "Time": timestamp(event.timestamp),
            })
        })
        .collect();
    json!({
        "Name": span.name,
        "SpanContext": span_context_json(&span.span_context),
        "Parent": span_context_json(&parent),
        "SpanKind": match span.span_kind {
            SpanKind::Internal => "Internal",
            SpanKind::Server => "Server",
            SpanKind::Client => "Client",
            SpanKind::Producer => "Producer",
            SpanKind::Consumer => "Consumer",
        },
        "StartTime": timestamp(span.start_time),
        "EndTime": timestamp(span.end_time),
        "Attributes": span.attributes.iter().map(json_attribute).collect::<Vec<_>>(),
        "Events": events,
        "Links": [],
        "Status": { "Code": status_code, "Description": description },
        "DroppedAttributes": span.dropped_attributes_count,
        "DroppedEvents": span.events.dropped_count,
        "DroppedLinks": span.links.dropped_count,
        "ChildSpanCount": 0,
        "Resource": resource.iter().map(|(key, value)| json_attribute(&KeyValue::new(key.clone(), value.clone()))).collect::<Vec<_>>(),
        "InstrumentationScope": {
            "Name": span.instrumentation_scope.name(),
            "Version": span.instrumentation_scope.version().unwrap_or(""),
            "SchemaURL": span.instrumentation_scope.schema_url().unwrap_or(""),
            "Attributes": span.instrumentation_scope.attributes().map(json_attribute).collect::<Vec<_>>(),
        },
        "InstrumentationLibrary": {
            "Name": span.instrumentation_scope.name(),
            "Version": span.instrumentation_scope.version().unwrap_or(""),
            "SchemaURL": span.instrumentation_scope.schema_url().unwrap_or(""),
        },
    })
}

#[derive(Debug)]
struct RotatingFileWriter {
    folder: PathBuf,
    prefix: Arc<str>,
    file: Option<File>,
    bytes: u64,
    max_bytes: u64,
    max_files: usize,
}

impl RotatingFileWriter {
    fn new(
        folder: PathBuf,
        prefix: Arc<str>,
        max_bytes: u64,
        max_files: usize,
    ) -> io::Result<Self> {
        fs::create_dir_all(&folder)?;
        // Surface directory/write failures at configuration, not after an operation.
        let probe = folder.join(format!("{}probe-{}", prefix, uuid::Uuid::new_v4()));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe)?;
        drop(file);
        fs::remove_file(probe)?;
        Ok(Self {
            folder,
            prefix,
            file: None,
            bytes: 0,
            max_bytes,
            max_files,
        })
    }

    fn write_line(&mut self, line: &[u8]) -> io::Result<()> {
        if self.file.is_none() || self.bytes > 0 && self.bytes + line.len() as u64 > self.max_bytes
        {
            self.file.take();
            self.open_file()?;
        }
        self.file
            .as_mut()
            .expect("file just opened")
            .write_all(line)?;
        self.bytes += line.len() as u64;
        Ok(())
    }

    fn open_file(&mut self) -> io::Result<()> {
        let mut timestamp = SystemTime::now();
        loop {
            let formatted = DateTime::<Utc>::from(timestamp).format("%Y-%m-%d-%H-%M-%S.%f");
            let path = self
                .folder
                .join(format!("{}{formatted}.jsonl", self.prefix));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => {
                    self.file = Some(file);
                    self.bytes = 0;
                    self.prune(&path)?;
                    return Ok(());
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    timestamp += Duration::from_nanos(1);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn prune(&self, current: &Path) -> io::Result<()> {
        let mut files = fs::read_dir(&self.folder)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.strip_prefix(self.prefix.as_ref()))
                    .and_then(|name| name.strip_suffix(".jsonl"))
                    .is_some_and(|name| {
                        chrono::NaiveDateTime::parse_from_str(name, "%Y-%m-%d-%H-%M-%S.%f").is_ok()
                    })
            })
            .collect::<Vec<_>>();
        files.sort_unstable();
        while files.len() > self.max_files {
            let index = files.iter().position(|path| path != current).unwrap_or(0);
            fs::remove_file(files.remove(index))?;
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.file {
            Some(file) => file.flush(),
            None => Ok(()),
        }
    }
}

fn default_folder() -> Result<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        Ok(home_dir()?.join("Library/Application Support/ADBC/Traces"))
    }
    #[cfg(target_os = "windows")]
    {
        std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .map(|path| path.join("ADBC/Traces"))
            .ok_or_else(|| telemetry_error("LOCALAPPDATA is unset", Status::InvalidState))
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        let state = std::env::var_os("XDG_STATE_HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .map(Ok)
            .unwrap_or_else(|| home_dir().map(|path| path.join(".local/state")))?;
        Ok(state.join("adbc/traces"))
    }
}

#[cfg(not(target_os = "windows"))]
fn home_dir() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| telemetry_error("HOME is unset", Status::InvalidState))
}

/// Return the ADBC status name used in the `error.type` span attribute.
pub fn status_name(status: Status) -> &'static str {
    match status {
        Status::Ok => "OK",
        Status::Unknown => "Unknown",
        Status::NotImplemented => "Not Implemented",
        Status::NotFound => "Not Found",
        Status::AlreadyExists => "Already Exists",
        Status::InvalidArguments => "Invalid Argument",
        Status::InvalidState => "Invalid State",
        Status::InvalidData => "Invalid Data",
        Status::Integrity => "Integrity Issue",
        Status::Internal => "Internal",
        Status::IO => "I/O",
        Status::Cancelled => "Cancelled",
        Status::Timeout => "Timeout",
        Status::Unauthenticated => "Unauthenticated",
        Status::Unauthorized => "Unauthorized",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::layer::Identity;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::registry::Registry;

    fn temp_folder() -> PathBuf {
        std::env::temp_dir().join(format!("adbc-traces-test-{}", uuid::Uuid::new_v4()))
    }

    fn attribute<'a>(span: &'a JsonValue, key: &str) -> &'a JsonValue {
        span["Attributes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|attr| attr["Key"] == key)
            .unwrap_or_else(|| panic!("missing span attribute {key}"))
    }

    fn read_spans(folder: &Path) -> Vec<JsonValue> {
        fs::read_dir(folder)
            .unwrap()
            .flat_map(|entry| {
                fs::read_to_string(entry.unwrap().path())
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect::<Vec<JsonValue>>()
            })
            .collect()
    }

    #[test]
    fn traceparent_validation_and_remote_context() {
        let valid = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        assert_eq!(
            validate_trace_parent(valid).unwrap().as_deref(),
            Some(valid)
        );
        assert_eq!(validate_trace_parent("").unwrap(), None);
        let parent = parent_context(valid);
        let span = parent.span();
        assert_eq!(
            span.span_context().trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(
            span.span_context().span_id().to_string(),
            "00f067aa0ba902b7"
        );
        assert!(span.span_context().is_remote());
        for invalid in [
            "not-a-parent",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
        ] {
            assert_eq!(
                validate_trace_parent(invalid).unwrap_err().status,
                Status::InvalidArguments
            );
        }
    }

    #[test]
    fn exporter_names_and_option_precedence() {
        assert_eq!(
            Exporter::parse("zipkin").unwrap_err().status,
            Status::InvalidArguments
        );
        for name in ["none", "otlp", "console", "adbcfile"] {
            assert_eq!(Exporter::parse(name).unwrap().as_str(), name);
        }
        assert_eq!(resolve_exporter(None, None).unwrap(), Exporter::None);
        assert_eq!(resolve_exporter(None, Some("")).unwrap(), Exporter::None);
        assert_eq!(
            resolve_exporter(None, Some("console")).unwrap(),
            Exporter::Console
        );
        assert_eq!(
            resolve_exporter(Some(Exporter::AdbcFile), Some("invalid")).unwrap(),
            Exporter::AdbcFile
        );
        assert_eq!(
            resolve_exporter(Some(Exporter::None), Some("console")).unwrap(),
            Exporter::None
        );
        assert_eq!(
            resolve_exporter(None, Some("invalid")).unwrap_err().status,
            Status::InvalidArguments
        );
    }

    #[test]
    fn adbcfile_rotates_caps_and_does_not_prune_other_drivers() {
        let folder = temp_folder();
        let prefix: Arc<str> = Arc::from("apache.arrow.adbc.test-");
        let mut writer =
            RotatingFileWriter::new(folder.clone(), Arc::clone(&prefix), 40, 3).unwrap();
        // Distinct drivers may configure the same folder.
        let _other = RotatingFileWriter::new(
            folder.clone(),
            Arc::from("apache.arrow.adbc.test-extra-"),
            40,
            3,
        )
        .unwrap();
        let other_file =
            folder.join("apache.arrow.adbc.test-extra-2000-01-01-00-00-00.000000000.jsonl");
        fs::write(&other_file, b"other\n").unwrap();
        for _ in 0..8 {
            writer.write_line(b"{\"Name\":\"span\"}\n").unwrap();
        }
        writer.flush().unwrap();
        let files: Vec<_> = fs::read_dir(&folder)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(files.len(), 4);
        assert!(other_file.exists());
        assert!(
            files
                .iter()
                .filter(|path| path != &&other_file)
                .all(|path| {
                    path.file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .starts_with(prefix.as_ref())
                        && fs::metadata(path).unwrap().len() <= 40
                })
        );
        drop(writer);
        fs::remove_dir_all(folder).unwrap();
    }

    #[test]
    fn traced_exports_parent_attributes_error_and_filtered_child_spans() {
        let folder = temp_folder();
        let telemetry = Telemetry::new("test", "2.3.4", "test-system");
        // Compose with another layer to check app_layer works on any subscriber.
        let dispatch = tracing::Dispatch::new(
            Registry::default()
                .with(Identity::new())
                .with(telemetry.app_layer()),
        );
        assert!(!telemetry.is_enabled());
        telemetry
            .traced(&dispatch, "disabled", None, |span| {
                assert!(span.is_disabled());
                Ok(())
            })
            .unwrap();
        telemetry
            .configure(Some(Exporter::AdbcFile), Some(folder.to_str().unwrap()))
            .unwrap();
        telemetry
            .configure(Some(Exporter::AdbcFile), Some(folder.to_str().unwrap()))
            .unwrap();
        assert!(telemetry.is_enabled());
        let parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let result: Result<()> =
            telemetry.traced(&dispatch, "statement.ExecuteQuery", Some(parent), |span| {
                span.record("db.response.returned_rows", 7_i64);
                span.set_attribute("vendor.request.id", "request-42".to_owned());
                tracing::info_span!(target: "driverbase", "child", otel.name = "test.child")
                    .in_scope(|| {});
                tracing::info_span!(target: "sf_core", "filtered_core_child").in_scope(|| {});
                Err(Error::with_message_and_status(
                    "test failure",
                    Status::InvalidArguments,
                ))
            });
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
        telemetry.flush();
        let lines = read_spans(&folder);
        let root = lines
            .iter()
            .find(|line| line["Name"] == "statement.ExecuteQuery")
            .unwrap();
        let child = lines
            .iter()
            .find(|line| line["Name"] == "test.child")
            .unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(
            root["SpanContext"]["TraceID"],
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(root["Parent"]["SpanID"], "00f067aa0ba902b7");
        assert_eq!(child["Parent"]["SpanID"], root["SpanContext"]["SpanID"]);
        assert_eq!(
            child["SpanContext"]["TraceID"],
            root["SpanContext"]["TraceID"]
        );
        assert_eq!(
            root["InstrumentationScope"]["Name"],
            "apache.arrow.adbc.test"
        );
        assert_eq!(root["InstrumentationScope"]["Version"], "2.3.4");
        assert_eq!(root["Status"]["Code"], "Error");
        assert_eq!(root["Status"]["Description"], "test failure");
        assert_eq!(
            attribute(root, "db.system.name")["Value"]["Value"],
            "test-system"
        );
        assert_eq!(
            attribute(root, "db.operation.name")["Value"]["Value"],
            "statement.ExecuteQuery"
        );
        assert_eq!(
            attribute(root, "error.type")["Value"]["Value"],
            "Invalid Argument"
        );
        assert_eq!(
            attribute(root, "db.response.returned_rows")["Value"]["Value"],
            7
        );
        assert_eq!(
            attribute(root, "vendor.request.id")["Value"]["Value"],
            "request-42"
        );
        assert_eq!(
            root["Resource"]
                .as_array()
                .unwrap()
                .iter()
                .find(|attr| attr["Key"] == "service.name")
                .unwrap()["Value"]["Value"],
            "apache.arrow.adbc"
        );
        telemetry.configure(Some(Exporter::None), None).unwrap();
        assert!(!telemetry.is_enabled());
        telemetry
            .traced(&dispatch, "disabled-after", None, |span| {
                assert!(span.is_disabled());
                Ok(())
            })
            .unwrap();
        telemetry
            .configure(Some(Exporter::AdbcFile), Some(folder.to_str().unwrap()))
            .unwrap();
        telemetry
            .traced(&dispatch, "statement.GetInfo", None, |_| Ok(()))
            .unwrap();
        telemetry.flush();
        let lines = read_spans(&folder);
        assert_eq!(lines.len(), 3);
        let succeeded = lines
            .iter()
            .find(|line| line["Name"] == "statement.GetInfo")
            .unwrap();
        assert_eq!(succeeded["Status"]["Code"], "Ok");
        assert_eq!(succeeded["Parent"]["SpanID"], "0000000000000000");
        assert_eq!(
            succeeded["Parent"]["TraceID"],
            "00000000000000000000000000000000"
        );
        telemetry.shutdown();
        fs::remove_dir_all(folder).unwrap();
    }
}
