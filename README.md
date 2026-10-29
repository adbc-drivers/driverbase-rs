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
layer for ADBC drivers. Tracing is disabled until an exporter is selected with
`Telemetry::configure` or `OTEL_TRACES_EXPORTER`. Exporters are `none`, `otlp`
(HTTP/protobuf, including HTTPS with Rustls and webpki roots), `console`, and
`adbcfile` (rotating JSONL files). Console and file spans use the Go
`stdouttrace` JSON shape.

Sampling follows `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` whenever
tracing is enabled. `OTEL_RESOURCE_ATTRIBUTES` is included in exported spans,
alongside the `apache.arrow.adbc` service name. OTLP endpoint, headers, and
timeout settings follow the standard `OTEL_EXPORTER_OTLP*` environment variables.
