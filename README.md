<!--
  Copyright (c) 2025 Columnar Technologies Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# driverbase-rs
Base framework and other libraries for developing ADBC drivers in Rust.

## Tracing

The optional `driverbase/telemetry` feature provides an OpenTelemetry tracing
layer for ADBC drivers. Tracing is disabled until the driver calls
`Telemetry::configure`, which selects an exporter from the database option
`adbc.telemetry.traces_exporter` or, when unset, `OTEL_TRACES_EXPORTER`.
Use `Exporter::parse_option` to treat a blank database option as unset.
Exporters are `none`, `otlp` (HTTP/protobuf, including HTTPS with Rustls and
webpki roots), `console`, and `adbcfile` (rotating JSONL files). Console and
file spans use the Go `stdouttrace` JSON shape. The database option
`adbc.telemetry.traces_folder_path` selects the `adbcfile` directory. The W3C
parent option `adbc.telemetry.trace_parent` can be set on a database,
connection, or statement. `Telemetry::shutdown` is final; configuring that
instance again returns `InvalidState`.

Sampling follows `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` whenever
tracing is enabled. `OTEL_RESOURCE_ATTRIBUTES` is included in exported spans,
alongside the `apache.arrow.adbc` service name. The OTLP HTTP exporter reads
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `OTEL_EXPORTER_OTLP_ENDPOINT`,
`OTEL_EXPORTER_OTLP_TRACES_HEADERS` / `OTEL_EXPORTER_OTLP_HEADERS`, and
`OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` / `OTEL_EXPORTER_OTLP_TIMEOUT`. If set,
`OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` / `OTEL_EXPORTER_OTLP_PROTOCOL` must be
`http/protobuf`; gRPC is not supported.
