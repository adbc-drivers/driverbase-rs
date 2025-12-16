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

//! Helpers to manage query execution meeting ADBC specification.
//!
//! For example, the reader here handles binding multiple sets of parameters
//! and building result sets from row-oriented APIs.  It is not quite meant
//! for columnar- or Arrow-native APIs.

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::ArrowError;

/// A [RecordBatchReader] that also reports the number of rows affected.
pub trait RowsAffectedReader: RecordBatchReader + Send + 'static {
    fn rows_affected(&self) -> Option<i64>;
}

/// A [RecordBatchReader] intended to make it easier to adapt row-oriented
/// systems and support functionality like parameter binding.
pub struct RecordBatchReaderBase<
    P: ParamImpl<E>,
    T: RecordBatchReaderImpl<P, E>,
    E: super::error::ErrorHelper,
> {
    schema: arrow_schema::SchemaRef,
    options: ReaderOptions,
    params: Option<P>,
    inner: T,
    finished: bool,
    rows: usize,
    bytes: usize,
    rows_affected: Option<i64>,
    _marker: std::marker::PhantomData<E>,
}

pub struct ExecuteResult {
    // no schema -> no result set for query
    pub schema: Option<arrow_schema::Schema>,
    pub rows_affected: Option<i64>,
}

/// The driver-specific implementation of a record batch reader.
pub trait RecordBatchReaderImpl<P, E>: Send + 'static
where
    P: ParamImpl<E>,
    E: super::error::ErrorHelper,
{
    /// Execute the query with the given parameters (if present).
    fn execute(
        &mut self,
        params: Option<P::Params>,
    ) -> Result<Option<ExecuteResult>, super::error::Error<E>>;
    /// Append a row.  Return None if EOF, or Some(approx bytes appended).
    fn append(&mut self) -> Result<Option<usize>, super::error::Error<E>>;
    /// Flush the current batch, returning the columns.
    fn flush(&mut self) -> Result<Vec<arrow_array::ArrayRef>, super::error::Error<E>>;
}

/// How bind parameters are handled (either row by row, or batch by batch).
pub trait ParamImpl<E>: Send + 'static
where
    E: super::error::ErrorHelper,
{
    type Params: std::fmt::Debug + Send + 'static;

    fn next(&mut self) -> Result<Option<Self::Params>, super::error::Error<E>>;
}

/// Handle bind parameters in batches.
pub struct BatchedParamImpl<E: super::error::ErrorHelper> {
    bind_params: Box<dyn RecordBatchReader + Send>,
    _marker: std::marker::PhantomData<E>,
}

impl<E: super::error::ErrorHelper> BatchedParamImpl<E> {
    pub fn new(bind_params: Box<dyn RecordBatchReader + Send>) -> Self {
        Self {
            bind_params,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E> ParamImpl<E> for BatchedParamImpl<E>
where
    E: super::error::ErrorHelper,
{
    type Params = RecordBatch;

    fn next(&mut self) -> Result<Option<Self::Params>, super::error::Error<E>> {
        // TODO: we want to reslice
        match self.bind_params.next() {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(E::from_arrow(e).context("get next parameter batch")),
            None => Ok(None),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReaderOptions {
    batch_byte_limit: Option<usize>,
    batch_row_limit: usize,
}

impl ReaderOptions {
    pub fn with_batch_byte_limit(&mut self, limit: usize) {
        if limit == 0 {
            self.batch_byte_limit = None;
        } else {
            self.batch_byte_limit = Some(limit);
        }
    }

    pub fn with_batch_row_limit(&mut self, limit: usize) {
        if limit == 0 {
            self.batch_row_limit = usize::MAX;
        } else {
            self.batch_row_limit = limit;
        }
    }
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            batch_byte_limit: None,
            batch_row_limit: 4096,
        }
    }
}

impl<P, T, E> RecordBatchReaderBase<P, T, E>
where
    P: ParamImpl<E>,
    T: RecordBatchReaderImpl<P, E>,
    E: super::error::ErrorHelper,
{
    #[allow(clippy::new_ret_no_self, reason = "Don't expose the concrete type")]
    pub fn new(
        options: ReaderOptions,
        mut params: Option<P>,
        mut inner: T,
    ) -> Result<Box<dyn RowsAffectedReader + Send>, super::error::Error<E>> {
        let param_batch = params
            .as_mut()
            .map(|p| p.next())
            .transpose()?
            .and_then(|p| p);
        let exec_result = inner.execute(param_batch)?.ok_or_else(|| {
            E::internal(super::error::location!())
                .message("no result set available from RecordBatchReaderImpl")
        })?;

        let schema = exec_result.schema.unwrap_or(arrow_schema::Schema::empty());

        Ok(Box::new(Self {
            schema: Arc::new(schema),
            options,
            params,
            inner,
            finished: false,
            rows: 0,
            bytes: 0,
            rows_affected: exec_result.rows_affected,
            _marker: std::marker::PhantomData,
        }))
    }

    fn slurp_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.finished {
            return Ok(None);
        }

        loop {
            match self.inner.append() {
                Ok(Some(added_bytes)) => {
                    self.rows += 1;
                    self.bytes += added_bytes;

                    if self.rows >= self.options.batch_row_limit {
                        break;
                    } else if let Some(limit) = self.options.batch_byte_limit {
                        if self.bytes >= limit {
                            break;
                        }
                    }
                }
                Ok(None) => {
                    let param_batch = self
                        .params
                        .as_mut()
                        .map(|p| p.next())
                        .transpose()
                        .map_err(|e| {
                            ArrowError::ExternalError(Box::new(
                                e.context("get next parameter batch"),
                            ))
                        })?
                        .and_then(|p| p);

                    if let Some(params) = param_batch {
                        self.inner
                            .execute(Some(params))
                            .map_err(|e| {
                                ArrowError::ExternalError(Box::new(
                                    e.context("execute next parameter batch"),
                                ))
                            })?
                            .ok_or_else(|| {
                                ArrowError::ExternalError(Box::new(
                                    E::internal(super::error::location!())
                                        .message("get next result set")
                                        .to_adbc(),
                                ))
                            })?;
                        continue;
                    } else {
                        self.finished = true;
                        break;
                    }
                }
                Err(e) => {
                    self.finished = true;
                    return Err(arrow_schema::ArrowError::ExternalError(Box::new(e)));
                }
            }
        }

        if self.rows == 0 {
            self.finished = true;
            return Ok(None);
        }
        self.rows = 0;
        self.bytes = 0;

        let columns = self
            .inner
            .flush()
            .map_err(|e| arrow_schema::ArrowError::ExternalError(Box::new(e)))?;
        Ok(Some(RecordBatch::try_new(self.schema.clone(), columns)?))
    }
}

impl<P, T, E> RecordBatchReader for RecordBatchReaderBase<P, T, E>
where
    P: ParamImpl<E>,
    T: RecordBatchReaderImpl<P, E>,
    E: super::error::ErrorHelper,
{
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }
}

impl<P, T, E> Iterator for RecordBatchReaderBase<P, T, E>
where
    P: ParamImpl<E>,
    T: RecordBatchReaderImpl<P, E>,
    E: super::error::ErrorHelper,
{
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.slurp_batch() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl<P, T, E> RowsAffectedReader for RecordBatchReaderBase<P, T, E>
where
    P: ParamImpl<E>,
    T: RecordBatchReaderImpl<P, E>,
    E: super::error::ErrorHelper,
{
    fn rows_affected(&self) -> Option<i64> {
        self.rows_affected
    }
}
