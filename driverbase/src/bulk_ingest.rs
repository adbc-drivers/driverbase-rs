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

use super::error::ErrorHelper;

pub struct BulkIngestState<E: ErrorHelper> {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub mode: adbc_core::options::IngestMode,

    _marker: std::marker::PhantomData<E>,
}

impl<E> Default for BulkIngestState<E>
where
    E: ErrorHelper,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E> BulkIngestState<E>
where
    E: ErrorHelper,
{
    pub fn new() -> Self {
        Self {
            table: None,
            catalog: None,
            schema: None,
            mode: adbc_core::options::IngestMode::Create,

            _marker: std::marker::PhantomData,
        }
    }

    pub fn clear(&mut self) {
        self.table = None;
    }

    pub fn is_set(&self) -> bool {
        self.table.is_some()
    }

    pub fn set_option(
        &mut self,
        key: &adbc_core::options::OptionStatement,
        value: &adbc_core::options::OptionValue,
    ) -> Result<bool, super::error::Error<E>> {
        use adbc_core::constants;
        match key {
            adbc_core::options::OptionStatement::TargetTable => {
                if let adbc_core::options::OptionValue::String(table) = value {
                    self.table = Some(table.clone());
                    Ok(true)
                } else {
                    Err(E::invalid_argument().format(format_args!(
                        "invalid option {}={value:?}",
                        constants::ADBC_INGEST_OPTION_TARGET_TABLE
                    )))
                }
            }
            adbc_core::options::OptionStatement::TargetCatalog => {
                if let adbc_core::options::OptionValue::String(table) = value {
                    self.catalog = Some(table.clone());
                    Ok(true)
                } else {
                    Err(E::invalid_argument().format(format_args!(
                        "invalid option {}={value:?}",
                        constants::ADBC_INGEST_OPTION_TARGET_CATALOG
                    )))
                }
            }
            adbc_core::options::OptionStatement::TargetDbSchema => {
                if let adbc_core::options::OptionValue::String(table) = value {
                    self.schema = Some(table.clone());
                    Ok(true)
                } else {
                    Err(E::invalid_argument().format(format_args!(
                        "invalid option {}={value:?}",
                        constants::ADBC_INGEST_OPTION_TARGET_DB_SCHEMA
                    )))
                }
            }
            adbc_core::options::OptionStatement::IngestMode => {
                if let adbc_core::options::OptionValue::String(mode) = value {
                    self.mode = match mode.as_ref() {
                        constants::ADBC_INGEST_OPTION_MODE_CREATE => {
                            adbc_core::options::IngestMode::Create
                        }
                        constants::ADBC_INGEST_OPTION_MODE_APPEND => {
                            adbc_core::options::IngestMode::Append
                        }
                        constants::ADBC_INGEST_OPTION_MODE_REPLACE => {
                            adbc_core::options::IngestMode::Replace
                        }
                        constants::ADBC_INGEST_OPTION_MODE_CREATE_APPEND => {
                            adbc_core::options::IngestMode::CreateAppend
                        }
                        val => {
                            return Err(E::invalid_argument().format(format_args!(
                                "invalid option {}={val}",
                                constants::ADBC_INGEST_OPTION_MODE
                            )));
                        }
                    };
                    Ok(true)
                } else {
                    Err(E::invalid_argument().format(format_args!(
                        "invalid option {}={value:?}",
                        constants::ADBC_INGEST_OPTION_TARGET_TABLE
                    )))
                }
            }
            _ => Ok(false),
        }
    }
}
