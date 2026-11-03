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
//!
//! The selected exporter gates recording; when enabled, sampling follows the
//! SDK's `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` configuration.
//! Console and file exporters write Go stdouttrace-compatible span JSON.
//! Exported spans carry the `apache.arrow.adbc` service name and any
//! `OTEL_RESOURCE_ATTRIBUTES`.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use adbc_core::error::{Error, Result, Status};
use chrono::{DateTime, SecondsFormat, Utc};
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::{
    Link, SamplingResult, Span as _, SpanContext, SpanId, SpanKind, Status as SpanStatus,
    TraceContextExt, TraceId,
};
use opentelemetry::{Context, InstrumentationScope, KeyValue, Value};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{
    BatchSpanProcessor, Config, Sampler, SdkTracerProvider, ShouldSample, Span as SdkSpan,
    SpanData, SpanExporter, SpanProcessor,
};
use serde_json::{Value as JsonValue, json};
use tracing_opentelemetry::{OpenTelemetryLayer, OpenTelemetrySpanExt};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::Layer;
use tracing_subscriber::registry::LookupSpan;

/// ADBC database, connection, or statement option carrying a W3C traceparent header.
pub const TRACE_PARENT: &str = "adbc.telemetry.trace_parent";
/// ADBC database option selecting the trace exporter.
pub const TRACES_EXPORTER: &str = "adbc.telemetry.traces_exporter";
/// ADBC database option selecting the trace file directory.
pub const TRACES_FOLDER_PATH: &str = "adbc.telemetry.traces_folder_path";
const MAX_FILE_BYTES: u64 = 1024 * 1024;
const MAX_FILES: usize = 100;
// Remote parent contexts cannot be recovered from SpanData after queue overflow.
// Evict them at this limit so dropped exports never retain contexts indefinitely.
const MAX_PARENT_CONTEXTS: usize = 2048;

/// The destination for ADBC spans.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exporter {
    None,
    /// OTLP over HTTP/protobuf, including HTTPS using Rustls with webpki roots.
    /// The HTTP exporter reads endpoint, headers, and timeout environment
    /// variables; other protocols are rejected.
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

    /// Parse a database exporter option; blank values are unset and allow
    /// [`Telemetry::configure`] to fall back to `OTEL_TRACES_EXPORTER`.
    pub fn parse_option(value: &str) -> Result<Option<Self>> {
        let value = value.trim();
        if value.is_empty() {
            Ok(None)
        } else {
            Self::parse(value).map(Some)
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
    TraceContextPropagator::new().extract_with_context(&Context::new(), &ParentHeader(value))
}

/// Validate a W3C traceparent, or clear the configured parent when blank.
pub fn validate_trace_parent(value: &str) -> Result<Option<String>> {
    let value = value.trim();
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
    configured: Mutex<ConfigurationState>,
    scope: InstrumentationScope,
    file_prefix: Arc<str>,
    db_system_name: Box<str>,
}

impl Telemetry {
    /// Construct an initially disabled tracer for an ADBC driver.
    /// `driver_name` is used as-is in the instrumentation scope and lowercased
    /// in JSONL filenames; `db_system_name` is written as `db.system.name`.
    pub fn new(driver_name: &str, driver_version: &str, db_system_name: &str) -> Self {
        let enabled = Arc::new(AtomicBool::new(false));
        let processor = SwitchableProcessor::default();
        let resource = Resource::builder()
            .with_service_name("apache.arrow.adbc")
            .build();
        let provider = SdkTracerProvider::builder()
            .with_resource(resource)
            .with_sampler(SwitchableSampler {
                enabled: Arc::clone(&enabled),
                delegate: Config::default().sampler,
            })
            .with_span_processor(processor.clone())
            .build();
        Self {
            provider,
            processor,
            enabled,
            configured: Mutex::new(ConfigurationState::Unconfigured),
            scope: InstrumentationScope::builder(format!("apache.arrow.adbc.{driver_name}"))
                .with_version(driver_version.to_owned())
                .with_schema_url(opentelemetry_semantic_conventions::SCHEMA_URL)
                .build(),
            file_prefix: Arc::from(format!("apache.arrow.adbc.{}-", driver_name.to_lowercase())),
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

    /// Set the exporter; explicit database options take precedence over the
    /// `OTEL_TRACES_EXPORTER` environment variable. Without either, tracing is
    /// disabled. `folder` is used only for [`Exporter::AdbcFile`]. The OTLP
    /// HTTP/protobuf exporter reads `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`,
    /// `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_HEADERS`,
    /// `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT`, and
    /// `OTEL_EXPORTER_OTLP_TIMEOUT`. The corresponding `*_PROTOCOL` variables
    /// must be `http/protobuf`; gRPC is not supported.
    /// Once [`Telemetry::shutdown`] is called, configuration cannot be changed.
    pub fn configure(&self, option: Option<Exporter>, folder: Option<&str>) -> Result<()> {
        let mut configured = self
            .configured
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if matches!(*configured, ConfigurationState::ShutDown) {
            return Err(telemetry_error(
                "telemetry has been shut down",
                Status::InvalidState,
            ));
        }
        let exporter = resolve_exporter(
            option,
            std::env::var("OTEL_TRACES_EXPORTER").ok().as_deref(),
        )?;
        if exporter == Exporter::Otlp {
            validate_otlp_protocol()?;
        }
        let folder = (exporter == Exporter::AdbcFile)
            .then(|| {
                folder
                    .filter(|path| !path.trim().is_empty())
                    .map(PathBuf::from)
            })
            .flatten();
        if matches!(&*configured, ConfigurationState::Active(previous, previous_folder) if *previous == exporter && *previous_folder == folder)
        {
            return Ok(());
        }
        let json_output = matches!(exporter, Exporter::Console | Exporter::AdbcFile);
        let processor = match exporter {
            Exporter::None => None,
            Exporter::Otlp => {
                use opentelemetry_otlp::WithHttpConfig;

                let timeout = [
                    opentelemetry_otlp::OTEL_EXPORTER_OTLP_TRACES_TIMEOUT,
                    opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT,
                ]
                .into_iter()
                .find_map(|name| std::env::var(name).ok()?.parse().ok())
                .map(Duration::from_millis)
                .unwrap_or(opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT);
                // Construct the blocking client off the caller's async runtime.
                let client = std::thread::Builder::new()
                    .spawn(move || {
                        reqwest::blocking::Client::builder()
                            .timeout(timeout)
                            .build()
                    })
                    .map_err(|error| telemetry_error(error, Status::IO))?
                    .join()
                    .map_err(|_| {
                        telemetry_error("HTTP client construction panicked", Status::Internal)
                    })?
                    .map_err(|error| telemetry_error(error, Status::InvalidArguments))?;
                let exporter = opentelemetry_otlp::SpanExporter::builder()
                    .with_http()
                    .with_http_client(client)
                    .build()
                    .map_err(|error| telemetry_error(error, Status::InvalidArguments))?;
                Some(BatchSpanProcessor::builder(exporter).build())
            }
            Exporter::Console => Some(
                BatchSpanProcessor::builder(JsonExporter::console(Arc::clone(
                    &self.processor.parents,
                )))
                .build(),
            ),
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
                    BatchSpanProcessor::builder(JsonExporter::file(
                        writer,
                        Arc::clone(&self.processor.parents),
                    ))
                    .build(),
                )
            }
        };
        // Stop sampling before swapping the old processor, then enable the
        // newly installed processor. The subscriber and tracing parents stay fixed.
        self.enabled.store(false, Ordering::Release);
        let previous = self.processor.replace(processor, json_output);
        self.enabled
            .store(exporter != Exporter::None, Ordering::Release);
        *configured = ConfigurationState::Active(exporter, folder);
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

    /// Flush and permanently shut down the SDK provider. Subsequent calls to
    /// [`Telemetry::configure`] return `InvalidState`.
    pub fn shutdown(&self) {
        let mut configured = self
            .configured
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if matches!(*configured, ConfigurationState::ShutDown) {
            return;
        }
        *configured = ConfigurationState::ShutDown;
        self.enabled.store(false, Ordering::Release);
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
                    tracing::error!(parent: &span, error = %error, exception.type = std::any::type_name_of_val(error));
                    span.set_status(SpanStatus::error(error.message.clone()));
                }
            }
            result
        })
    }
}

fn resolve_exporter(option: Option<Exporter>, environment: Option<&str>) -> Result<Exporter> {
    option.map_or_else(
        || {
            environment
                .map(Exporter::parse_option)
                .transpose()
                .map(|parsed| parsed.flatten().unwrap_or(Exporter::None))
        },
        Ok,
    )
}

fn validate_otlp_protocol() -> Result<()> {
    for name in [
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
    ] {
        if let Some(value) = std::env::var_os(name)
            && value.to_str() != Some("http/protobuf")
        {
            return Err(telemetry_error(
                format!("{name}={value:?}: only http/protobuf is supported (gRPC is unsupported)"),
                Status::InvalidArguments,
            ));
        }
    }
    Ok(())
}

fn telemetry_error(error: impl std::fmt::Display, status: Status) -> Error {
    Error::with_message_and_status(format!("failed to configure tracing: {error}"), status)
}

#[derive(Clone, Debug)]
struct SwitchableSampler {
    enabled: Arc<AtomicBool>,
    delegate: Box<dyn ShouldSample>,
}

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
        if self.enabled.load(Ordering::Acquire) {
            self.delegate.should_sample(
                parent_context,
                trace_id,
                name,
                span_kind,
                attributes,
                links,
            )
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

enum ConfigurationState {
    Unconfigured,
    Active(Exporter, Option<PathBuf>),
    ShutDown,
}

type ParentContexts = Arc<Mutex<HashMap<SpanId, SpanContext>>>;

#[derive(Default, Debug)]
struct ProcessorState {
    current: Option<BatchSpanProcessor>,
    resource: Option<Resource>,
    json_output: bool,
}

#[derive(Clone, Default, Debug)]
struct SwitchableProcessor {
    state: Arc<RwLock<ProcessorState>>,
    parents: ParentContexts,
}

impl SwitchableProcessor {
    fn replace(
        &self,
        mut next: Option<BatchSpanProcessor>,
        json_output: bool,
    ) -> Option<BatchSpanProcessor> {
        let mut state = self.state.write().unwrap_or_else(|err| err.into_inner());
        if let (Some(next), Some(resource)) = (next.as_mut(), state.resource.as_ref()) {
            next.set_resource(resource);
        }
        state.json_output = json_output;
        std::mem::replace(&mut state.current, next)
    }
}

impl SpanProcessor for SwitchableProcessor {
    fn on_start(&self, span: &mut SdkSpan, context: &Context) {
        let parent = context.span();
        if span.is_recording()
            && parent.span_context().is_valid()
            && parent.span_context().is_remote()
        {
            let mut parents = self.parents.lock().unwrap_or_else(|err| err.into_inner());
            if parents.len() >= MAX_PARENT_CONTEXTS {
                // A full batch queue can drop spans without calling the exporter.
                // Evict one remote parent rather than retain it for the process lifetime.
                if let Some(span_id) = parents.keys().next().copied() {
                    parents.remove(&span_id);
                }
            }
            parents.insert(span.span_context().span_id(), parent.span_context().clone());
        }
    }

    fn on_end(&self, span: SpanData) {
        let state = self.state.read().unwrap_or_else(|err| err.into_inner());
        if !state.json_output {
            self.parents
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .remove(&span.span_context.span_id());
        }
        if let Some(current) = state.current.as_ref() {
            current.on_end(span);
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.state
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .current
            .as_ref()
            .map_or(Ok(()), SpanProcessor::force_flush)
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.replace(None, false)
            .map_or(Ok(()), |current| current.shutdown_with_timeout(timeout))
    }

    fn set_resource(&mut self, resource: &Resource) {
        let mut state = self.state.write().unwrap_or_else(|err| err.into_inner());
        state.resource = Some(resource.clone());
        if let Some(current) = state.current.as_mut() {
            current.set_resource(resource);
        }
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
    parents: ParentContexts,
}

impl JsonExporter {
    fn console(parents: ParentContexts) -> Self {
        Self {
            output: JsonOutput::Stdout,
            resource: Resource::builder_empty().build(),
            parents,
        }
    }

    fn file(writer: RotatingFileWriter, parents: ParentContexts) -> Self {
        Self {
            output: JsonOutput::File(Mutex::new(writer)),
            resource: Resource::builder_empty().build(),
            parents,
        }
    }

    fn span_json(&self, span: &SpanData) -> JsonValue {
        let parent = self
            .parents
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .remove(&span.span_context.span_id());
        span_json(span, &self.resource, parent)
    }

    fn write_spans(&self, spans: &[SpanData]) -> io::Result<()> {
        // Every span in a failed batch must release its saved remote parent.
        let values: Vec<_> = spans.iter().map(|span| self.span_json(span)).collect();
        match &self.output {
            JsonOutput::Stdout => {
                let stdout = io::stdout();
                let mut writer = stdout.lock();
                for value in &values {
                    serde_json::to_writer(&mut writer, value)?;
                    writer.write_all(b"\n")?;
                }
                writer.flush()
            }
            JsonOutput::File(writer) => {
                let mut writer = writer.lock().unwrap_or_else(|err| err.into_inner());
                for value in &values {
                    let mut line = serde_json::to_vec(value)?;
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

    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.clone();
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

fn span_json(span: &SpanData, resource: &Resource, parent: Option<SpanContext>) -> JsonValue {
    // Root spans get an all-zero parent, as Go's stdouttrace writes.
    let parent = parent.unwrap_or_else(|| {
        if span.parent_span_id == SpanId::INVALID {
            SpanContext::empty_context()
        } else {
            SpanContext::new(
                span.span_context.trace_id(),
                span.parent_span_id,
                span.span_context.trace_flags(),
                false,
                span.span_context.trace_state().clone(),
            )
        }
    });
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
            SpanKind::Internal => 1,
            SpanKind::Server => 2,
            SpanKind::Client => 3,
            SpanKind::Producer => 4,
            SpanKind::Consumer => 5,
        },
        "StartTime": timestamp(span.start_time),
        "EndTime": timestamp(span.end_time),
        "Attributes": span.attributes.iter().map(json_attribute).collect::<Vec<_>>(),
        "Events": events,
        "Links": span.links.links.iter().map(|link| json!({
            "SpanContext": span_context_json(&link.span_context),
            "Attributes": link.attributes.iter().map(json_attribute).collect::<Vec<_>>(),
            "DroppedAttributeCount": link.dropped_attributes_count,
        })).collect::<Vec<_>>(),
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
    path: PathBuf,
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
            path: PathBuf::new(),
            bytes: 0,
            max_bytes,
            max_files,
        })
    }

    fn write_line(&mut self, line: &[u8]) -> io::Result<()> {
        if self.file.is_none() || self.bytes > 0 && self.bytes + line.len() as u64 > self.max_bytes
        {
            self.file.take();
            self.open_file(line.len())?;
        }
        if let Err(error) = self
            .file
            .as_mut()
            .expect("file just opened")
            .write_all(line)
        {
            self.discard_partial_line();
            return Err(error);
        }
        self.bytes += line.len() as u64;
        Ok(())
    }

    fn discard_partial_line(&mut self) {
        if self.file.take().is_some() {
            // Roll back an incomplete JSONL record to the last committed offset.
            // Truncate through a write handle: on Windows an append-only handle
            // lacks the access right that changing the file length requires.
            let _ = OpenOptions::new()
                .write(true)
                .open(&self.path)
                .and_then(|file| file.set_len(self.bytes));
        }
    }

    fn open_file(&mut self, line_len: usize) -> io::Result<()> {
        // Like the Go writer, reuse the latest non-full file for this driver.
        // Avoid appending a line that would exceed the strict file-size cap.
        if let Some(path) = self.trace_files()?.pop()
            && let Ok(file) = OpenOptions::new().append(true).open(&path)
            && let Ok(metadata) = file.metadata()
        {
            let bytes = metadata.len();
            if bytes < self.max_bytes
                && (bytes == 0 || bytes.saturating_add(line_len as u64) <= self.max_bytes)
            {
                self.file = Some(file);
                self.path = path;
                self.bytes = bytes;
                return Ok(());
            }
        }
        let mut timestamp = SystemTime::now();
        loop {
            let formatted = DateTime::<Utc>::from(timestamp).format("%Y-%m-%d-%H-%M-%S.%f");
            let path = self
                .folder
                .join(format!("{}{formatted}.jsonl", self.prefix));
            match OpenOptions::new().append(true).create_new(true).open(&path) {
                Ok(file) => {
                    self.file = Some(file);
                    self.bytes = 0;
                    // Pruning is maintenance, not a reason to lose an export.
                    let _ = self.prune(&path);
                    self.path = path;
                    return Ok(());
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    timestamp += Duration::from_nanos(1);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn trace_files(&self) -> io::Result<Vec<PathBuf>> {
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
        Ok(files)
    }

    fn prune(&self, current: &Path) -> io::Result<()> {
        let mut files = self.trace_files()?;
        while files.len() > self.max_files {
            let index = files.iter().position(|path| path != current).unwrap_or(0);
            match fs::remove_file(&files[index]) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            files.remove(index);
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
        env_dir("LOCALAPPDATA", std::env::var_os("LOCALAPPDATA"))
            .map(|path| path.join("ADBC/Traces"))
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
    env_dir("HOME", std::env::var_os("HOME"))
}

/// A base directory from environment variable `name`. An empty value counts
/// as unset, so traces are never written relative to the working directory.
fn env_dir(name: &str, value: Option<std::ffi::OsString>) -> Result<PathBuf> {
    value
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| telemetry_error(format!("{name} is unset or empty"), Status::InvalidState))
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
    use opentelemetry::trace::Tracer;
    use tracing_subscriber::layer::Identity;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::registry::Registry;

    fn temp_folder() -> PathBuf {
        std::env::temp_dir().join(format!("adbc-traces-test-{}", uuid::Uuid::new_v4()))
    }

    fn run_child(test: &str, environment: &[(&str, &str)]) {
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test])
            .env("DRIVERBASE_TELEMETRY_TEST_CHILD", "1")
            .env_remove("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
            .env_remove("OTEL_EXPORTER_OTLP_PROTOCOL")
            .envs(environment.iter().copied())
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "child test failed:\n{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
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
        assert_eq!(validate_trace_parent("  \t ").unwrap(), None);
        assert_eq!(
            validate_trace_parent(&format!("  {valid} \t"))
                .unwrap()
                .as_deref(),
            Some(valid)
        );
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
        let _active_span = parent.attach();
        assert_eq!(
            validate_trace_parent("not-a-parent").unwrap_err().status,
            Status::InvalidArguments
        );
        assert!(
            !parent_context("not-a-parent")
                .span()
                .span_context()
                .is_valid()
        );
    }

    #[test]
    fn sampler_honors_sdk_environment() {
        if std::env::var_os("DRIVERBASE_TELEMETRY_TEST_CHILD").is_some() {
            let telemetry = Telemetry::new("sampler", "1.0", "test");
            telemetry.enabled.store(true, Ordering::Release);
            let span = telemetry.tracer().start("sample");
            assert_eq!(
                span.is_recording(),
                std::env::var("DRIVERBASE_EXPECT_SAMPLED").unwrap() == "true"
            );
            drop(span);
            telemetry.shutdown();
        } else {
            for (sampler, argument, expected) in [
                ("always_on", "1.0", "true"),
                ("always_off", "1.0", "false"),
                ("traceidratio", "0.0", "false"),
                ("traceidratio", "1.0", "true"),
                ("parentbased_traceidratio", "0.0", "false"),
            ] {
                run_child(
                    "telemetry::tests::sampler_honors_sdk_environment",
                    &[
                        ("OTEL_TRACES_SAMPLER", sampler),
                        ("OTEL_TRACES_SAMPLER_ARG", argument),
                        ("DRIVERBASE_EXPECT_SAMPLED", expected),
                    ],
                );
            }
        }
    }

    #[test]
    fn otlp_configures_without_a_collector() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let telemetry = Telemetry::new("otlp", "1.0", "test");
            telemetry.configure(Some(Exporter::Otlp), None).unwrap();
            telemetry.shutdown();
        });
    }

    #[test]
    fn otlp_rejects_unsupported_protocol() {
        if std::env::var_os("DRIVERBASE_TELEMETRY_TEST_CHILD").is_some() {
            let telemetry = Telemetry::new("protocol", "1.0", "test");
            let result = telemetry.configure(Some(Exporter::Otlp), None);
            if std::env::var("DRIVERBASE_EXPECT_PROTOCOL_VALID").unwrap() == "true" {
                result.unwrap();
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.status, Status::InvalidArguments);
                assert!(error.message.contains("http/protobuf"));
                assert!(
                    error
                        .message
                        .contains(&std::env::var("DRIVERBASE_BAD_PROTOCOL_VAR").unwrap())
                );
            }
            telemetry.shutdown();
            return;
        }
        let test = "telemetry::tests::otlp_rejects_unsupported_protocol";
        run_child(
            test,
            &[
                ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
                ("DRIVERBASE_EXPECT_PROTOCOL_VALID", "false"),
                ("DRIVERBASE_BAD_PROTOCOL_VAR", "OTEL_EXPORTER_OTLP_PROTOCOL"),
            ],
        );
        run_child(
            test,
            &[
                ("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc"),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
                ("DRIVERBASE_EXPECT_PROTOCOL_VALID", "false"),
                (
                    "DRIVERBASE_BAD_PROTOCOL_VAR",
                    "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
                ),
            ],
        );
        run_child(
            test,
            &[
                ("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf"),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
                ("DRIVERBASE_EXPECT_PROTOCOL_VALID", "false"),
                ("DRIVERBASE_BAD_PROTOCOL_VAR", "OTEL_EXPORTER_OTLP_PROTOCOL"),
            ],
        );
        run_child(
            test,
            &[
                ("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf"),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
                ("DRIVERBASE_EXPECT_PROTOCOL_VALID", "true"),
            ],
        );
    }

    #[test]
    fn resource_is_exported_after_reconfiguration() {
        if std::env::var_os("DRIVERBASE_TELEMETRY_TEST_CHILD").is_none() {
            run_child(
                "telemetry::tests::resource_is_exported_after_reconfiguration",
                &[
                    ("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=staging"),
                    ("OTEL_TRACES_SAMPLER", "always_on"),
                ],
            );
            return;
        }

        let folder = temp_folder();
        let telemetry = Telemetry::new("resource", "1.0", "test");
        for _ in 0..2 {
            telemetry
                .configure(Some(Exporter::AdbcFile), Some(folder.to_str().unwrap()))
                .unwrap();
            telemetry.tracer().start("resource-span").end();
            telemetry.flush();
            telemetry.configure(Some(Exporter::None), None).unwrap();
        }
        let spans = read_spans(&folder);
        assert_eq!(spans.len(), 2);
        for span in &spans {
            let attributes = span["Resource"].as_array().unwrap();
            let resource_attribute = |key| {
                attributes
                    .iter()
                    .find(|attr| attr["Key"] == key)
                    .unwrap_or_else(|| panic!("missing resource attribute {key}"))["Value"]["Value"]
                    .clone()
            };
            assert_eq!(resource_attribute("service.name"), "apache.arrow.adbc");
            assert_eq!(resource_attribute("deployment.environment"), "staging");
        }
        telemetry.shutdown();
        fs::remove_dir_all(folder).unwrap();
    }

    #[test]
    fn json_spans_serialize_kinds_and_links() {
        let folder = temp_folder();
        let telemetry = Telemetry::new("kinds", "1.0", "test");
        telemetry
            .configure(Some(Exporter::AdbcFile), Some(folder.to_str().unwrap()))
            .unwrap();
        let tracer = telemetry.tracer();
        let linked = parent_context("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .span()
            .span_context()
            .clone();
        for (kind, number) in [
            (SpanKind::Internal, 1),
            (SpanKind::Server, 2),
            (SpanKind::Client, 3),
            (SpanKind::Producer, 4),
            (SpanKind::Consumer, 5),
        ] {
            let mut builder = tracer
                .span_builder(format!("kind-{number}"))
                .with_kind(kind);
            if number == 3 {
                builder = builder.with_links(vec![Link::new(
                    linked.clone(),
                    vec![KeyValue::new("link.type", "related")],
                    0,
                )]);
            }
            builder.start(&tracer).end();
        }
        telemetry.flush();
        let spans = read_spans(&folder);
        assert_eq!(spans.len(), 5);
        for number in 1..=5 {
            let span = spans
                .iter()
                .find(|span| span["Name"] == format!("kind-{number}"))
                .unwrap();
            assert_eq!(span["SpanKind"], number);
            if number == 3 {
                assert_eq!(span["Links"][0]["SpanContext"], span_context_json(&linked));
                assert_eq!(span["Links"][0]["Attributes"][0]["Key"], "link.type");
                assert_eq!(
                    span["Links"][0]["Attributes"][0]["Value"]["Value"],
                    "related"
                );
                assert_eq!(span["Links"][0]["DroppedAttributeCount"], 0);
            } else {
                assert_eq!(span["Links"], json!([]));
            }
        }
        telemetry.shutdown();
        fs::remove_dir_all(folder).unwrap();
    }

    #[test]
    fn failing_json_export_releases_remote_parents() {
        if std::env::var_os("DRIVERBASE_TELEMETRY_TEST_CHILD").is_none() {
            run_child(
                "telemetry::tests::failing_json_export_releases_remote_parents",
                &[("OTEL_TRACES_SAMPLER", "always_on")],
            );
            return;
        }
        let folder = temp_folder();
        let telemetry = Telemetry::new("failure", "1.0", "test");
        telemetry
            .configure(Some(Exporter::AdbcFile), Some(folder.to_str().unwrap()))
            .unwrap();
        fs::remove_dir_all(folder).unwrap();
        let remote = parent_context("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        let tracer = telemetry.tracer();
        for _ in 0..3 {
            for _ in 0..10 {
                tracer.start_with_context("failed-export", &remote).end();
            }
            telemetry.flush();
            assert!(telemetry.processor.parents.lock().unwrap().is_empty());
        }
        telemetry.shutdown();
    }

    #[test]
    fn remote_parent_contexts_are_bounded_without_exports() {
        if std::env::var_os("DRIVERBASE_TELEMETRY_TEST_CHILD").is_none() {
            run_child(
                "telemetry::tests::remote_parent_contexts_are_bounded_without_exports",
                &[("OTEL_TRACES_SAMPLER", "always_on")],
            );
            return;
        }
        let folder = temp_folder();
        let telemetry = Telemetry::new("overflow", "1.0", "test");
        telemetry
            .configure(Some(Exporter::AdbcFile), Some(folder.to_str().unwrap()))
            .unwrap();
        let remote = parent_context("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        let tracer = telemetry.tracer();
        let spans: Vec<_> = (0..MAX_PARENT_CONTEXTS + 32)
            .map(|_| tracer.start_with_context("pending", &remote))
            .collect();
        assert_eq!(
            telemetry.processor.parents.lock().unwrap().len(),
            MAX_PARENT_CONTEXTS
        );
        drop(spans);
        telemetry.shutdown();
        fs::remove_dir_all(folder).unwrap();
    }

    #[test]
    fn shutdown_disables_telemetry_and_rejects_configuration() {
        let folder = temp_folder();
        let telemetry = Telemetry::new("shutdown", "1.0", "test");
        telemetry
            .configure(Some(Exporter::AdbcFile), Some(folder.to_str().unwrap()))
            .unwrap();
        assert!(telemetry.is_enabled());
        telemetry.shutdown();
        assert!(!telemetry.is_enabled());
        for exporter in [Exporter::AdbcFile, Exporter::None] {
            assert_eq!(
                telemetry
                    .configure(Some(exporter), Some(folder.to_str().unwrap()))
                    .unwrap_err()
                    .status,
                Status::InvalidState
            );
        }
        telemetry.shutdown();
        fs::remove_dir_all(folder).unwrap();

        let unconfigured = Telemetry::new("unconfigured", "1.0", "test");
        unconfigured.shutdown();
        assert_eq!(
            unconfigured.configure(None, None).unwrap_err().status,
            Status::InvalidState
        );
    }

    #[test]
    fn file_prefix_is_lowercase_but_scope_preserves_driver_name() {
        let folder = temp_folder();
        let telemetry = Telemetry::new("MixedCase", "1.0", "test");
        assert_eq!(telemetry.scope.name(), "apache.arrow.adbc.MixedCase");
        telemetry
            .configure(Some(Exporter::AdbcFile), Some(folder.to_str().unwrap()))
            .unwrap();
        telemetry.tracer().start("mixed-case").end();
        telemetry.flush();
        let paths = fs::read_dir(&folder)
            .unwrap()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert!(
            paths[0]
                .file_name()
                .to_str()
                .unwrap()
                .starts_with("apache.arrow.adbc.mixedcase-")
        );
        assert_eq!(
            read_spans(&folder)[0]["InstrumentationScope"]["Name"],
            "apache.arrow.adbc.MixedCase"
        );
        telemetry.shutdown();
        fs::remove_dir_all(folder).unwrap();
    }

    #[test]
    fn remote_parent_survives_exporter_switch() {
        let folder = temp_folder();
        let telemetry = Telemetry::new("switch", "1.0", "test");
        telemetry
            .configure(Some(Exporter::AdbcFile), Some(folder.to_str().unwrap()))
            .unwrap();
        let remote = parent_context("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        let mut span = telemetry.tracer().start_with_context("switched", &remote);
        telemetry.configure(Some(Exporter::None), None).unwrap();
        telemetry
            .configure(Some(Exporter::AdbcFile), Some(folder.to_str().unwrap()))
            .unwrap();
        span.end();
        telemetry.flush();
        let spans = read_spans(&folder);
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0]["Parent"],
            span_context_json(remote.span().span_context())
        );
        telemetry.shutdown();
        fs::remove_dir_all(folder).unwrap();
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
        assert_eq!(Exporter::parse_option("").unwrap(), None);
        assert_eq!(Exporter::parse_option(" \t ").unwrap(), None);
        assert_eq!(
            Exporter::parse_option("  adbcfile \t").unwrap(),
            Some(Exporter::AdbcFile)
        );
        assert_eq!(
            Exporter::parse_option("invalid").unwrap_err().status,
            Status::InvalidArguments
        );
        assert_eq!(
            resolve_exporter(Exporter::parse_option("   ").unwrap(), Some("console")).unwrap(),
            Exporter::Console
        );
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
    fn adbcfile_reuses_latest_file_for_other_writers() {
        let folder = temp_folder();
        let prefix: Arc<str> = Arc::from("apache.arrow.adbc.shared-");
        let mut long_lived =
            RotatingFileWriter::new(folder.clone(), Arc::clone(&prefix), 1024, 2).unwrap();
        long_lived.write_line(b"{\"Name\":\"first\"}\n").unwrap();
        for index in 0..2 {
            let mut short_lived =
                RotatingFileWriter::new(folder.clone(), Arc::clone(&prefix), 1024, 2).unwrap();
            short_lived
                .write_line(format!("{{\"Name\":\"{index}\"}}\n").as_bytes())
                .unwrap();
        }
        long_lived.write_line(b"{\"Name\":\"last\"}\n").unwrap();
        long_lived.flush().unwrap();
        let files = long_lived.trace_files().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(fs::read_to_string(&files[0]).unwrap().lines().count(), 4);
        drop(long_lived);
        fs::remove_dir_all(folder).unwrap();
    }

    #[test]
    fn adbcfile_recovers_after_failed_write() {
        let folder = temp_folder();
        let mut writer = RotatingFileWriter::new(
            folder.clone(),
            Arc::from("apache.arrow.adbc.recover-"),
            1024,
            2,
        )
        .unwrap();
        let first = b"{\"Name\":\"first\"}\n";
        let last = b"{\"Name\":\"last\"}\n";
        writer.write_line(first).unwrap();
        let path = writer.trace_files().unwrap().pop().unwrap();
        // Simulate the bytes left by a partial write on a writable real file.
        writer
            .file
            .as_mut()
            .unwrap()
            .write_all(b"{\"Name\":\"incomplete")
            .unwrap();
        writer.discard_partial_line();
        assert!(writer.file.is_none());
        assert_eq!(fs::read(&path).unwrap(), first.to_vec());
        writer.file = Some(File::open(&path).unwrap());
        assert!(writer.write_line(b"{\"Name\":\"failed\"}\n").is_err());
        assert!(writer.file.is_none());
        assert_eq!(writer.bytes, first.len() as u64);
        writer.write_line(last).unwrap();
        writer.flush().unwrap();
        assert_eq!(
            fs::read(&path).unwrap(),
            [first.as_slice(), last.as_slice()].concat()
        );
        drop(writer);
        fs::remove_dir_all(folder).unwrap();
    }

    #[test]
    fn adbcfile_pruning_failure_does_not_lose_write() {
        let folder = temp_folder();
        let mut writer = RotatingFileWriter::new(
            folder.clone(),
            Arc::from("apache.arrow.adbc.prune-"),
            1024,
            1,
        )
        .unwrap();
        // A matching directory is unremovable with remove_file on all CI platforms.
        fs::create_dir(folder.join("apache.arrow.adbc.prune-2000-01-01-00-00-00.000000000.jsonl"))
            .unwrap();
        writer.write_line(b"{\"Name\":\"kept\"}\n").unwrap();
        writer.flush().unwrap();
        let files: Vec<_> = writer
            .trace_files()
            .unwrap()
            .into_iter()
            .filter(|path| path.is_file())
            .collect();
        assert_eq!(files.len(), 1);
        assert_eq!(
            fs::read_to_string(&files[0]).unwrap(),
            "{\"Name\":\"kept\"}\n"
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
        assert_eq!(root["Parent"]["Remote"], true);
        assert_eq!(root["Parent"]["TraceFlags"], "01");
        assert_eq!(child["Parent"]["SpanID"], root["SpanContext"]["SpanID"]);
        assert_eq!(child["Parent"]["Remote"], false);
        assert_eq!(
            child["Parent"]["TraceFlags"],
            root["SpanContext"]["TraceFlags"]
        );
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
        let event = &root["Events"][0];
        assert_eq!(event["Name"], "exception");
        let exception_attribute = |key| {
            event["Attributes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|attribute| attribute["Key"] == key)
                .unwrap_or_else(|| panic!("missing exception attribute {key}"))["Value"]["Value"]
                .clone()
        };
        assert_eq!(
            exception_attribute("exception.type"),
            std::any::type_name::<Error>()
        );
        assert!(
            exception_attribute("exception.message")
                .as_str()
                .unwrap()
                .contains("test failure")
        );
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

    #[test]
    fn empty_base_directory_variables_are_rejected() {
        for value in [None, Some("".into())] {
            let error = env_dir("HOME", value).unwrap_err();
            assert_eq!(error.status, Status::InvalidState);
            assert!(error.message.contains("HOME is unset or empty"));
        }
        assert_eq!(
            env_dir("HOME", Some("/home/u".into())).unwrap(),
            PathBuf::from("/home/u")
        );
    }
}
