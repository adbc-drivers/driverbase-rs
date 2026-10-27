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

## OpenTelemetry tracing

Enable the optional `telemetry` feature to use `driverbase::telemetry::Telemetry`.
Construct it with a driver name, version, and `db.system.name`, then add
`Telemetry::app_layer()` to the driver's tracing subscriber. This layer traces
only the `driverbase` target; compose any driver-specific layers separately
without filtering layers that need to inspect its parent span context.

`Telemetry::configure` selects `none`, `otlp` (HTTP/protobuf), `console`, or
`adbcfile`. An explicit exporter overrides `OTEL_TRACES_EXPORTER`; file traces
are rotating JSONL in Go stdouttrace format. Call `Telemetry::traced` around
ADBC operations with the dispatch and an optional validated W3C traceparent.
The callback can record `db.response.returned_rows` or use
`tracing_opentelemetry::OpenTelemetrySpanExt::set_attribute` for driver-specific
attributes.
