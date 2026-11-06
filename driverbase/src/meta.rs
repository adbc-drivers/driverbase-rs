// Copyright (c) 2026 ADBC Drivers Contributors
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

// Helpers for implementing metadata collections.

use std::sync::Arc;

use arrow_array::RecordBatchReader;
use arrow_schema::Schema;

pub type Result<T, E> = std::result::Result<T, crate::error::Error<E>>;

pub type MetadataCollectionMap<Args, E> =
    std::collections::HashMap<&'static str, Box<dyn MetadataCollection<Args, E>>>;

/// A collection of the supported metadata collections (except for "meta").
pub struct SupportedMetadataCollections<Args, E> {
    collections: Arc<MetadataCollectionMap<Args, E>>,
}

impl<Args, E> Clone for SupportedMetadataCollections<Args, E> {
    fn clone(&self) -> Self {
        Self {
            collections: self.collections.clone(),
        }
    }
}

impl<Args, E> SupportedMetadataCollections<Args, E>
where
    Args: Send + 'static,
    E: crate::error::ErrorHelper,
{
    pub fn new(collections: MetadataCollectionMap<Args, E>) -> Self {
        Self {
            collections: Arc::new(collections),
        }
    }
}

/// Store metadata collection-related options on a Statement; manages setting
/// and validation of options.
pub struct MetadataCollectionState<Args, E> {
    supported_collections: SupportedMetadataCollections<Args, E>,
    collection: Option<Box<dyn MetadataCollectionQuery<Args, E> + Send>>,
}

impl<Args, E> MetadataCollectionState<Args, E>
where
    Args: Send + 'static,
    E: crate::error::ErrorHelper,
{
    pub fn new(supported_collections: SupportedMetadataCollections<Args, E>) -> Self {
        Self {
            supported_collections,
            collection: None,
        }
    }

    pub fn clear(&mut self) {
        self.collection = None;
    }

    pub fn is_set(&self) -> bool {
        self.collection.is_some()
    }

    pub fn take(&mut self) -> Option<Box<dyn MetadataCollectionQuery<Args, E> + Send>> {
        self.collection.take()
    }

    pub fn set_option(
        &mut self,
        key: &adbc_core::options::OptionStatement,
        value: &adbc_core::options::OptionValue,
    ) -> Result<bool, E> {
        if let adbc_core::options::OptionStatement::Other(k) = key {
            if k == "adbc.metadata.collection" {
                let v = E::option_as_string(key, value)?;
                if v == "meta" {
                    self.collection = Some(Box::new(MetaCollectionQuery {
                        supported_collections: self.supported_collections.clone(),
                    }));
                    Ok(true)
                } else if let Some(collection) = self.supported_collections.collections.get(v) {
                    self.collection = Some(collection.init());
                    Ok(true)
                } else {
                    Err(E::invalid_argument().format(format_args!(
                        "unknown collection adbc.metadata.collection={v:?}"
                    )))
                }
            } else if let Some(name) = k.strip_prefix("adbc.metadata.filter_pattern.") {
                let v = E::option_as_string(key, value)?;
                if let Some(collection) = &mut self.collection {
                    let filter = Filter::new_pattern(v.to_string()).map_err(|e| {
                        E::invalid_argument().format(format_args!(
                            "invalid regex pattern for filter {k}={v:?}: {e}"
                        ))
                    })?;
                    collection.add_filter(name, filter)?;
                    Ok(true)
                } else {
                    Err(E::invalid_argument()
                        .format(format_args!("filter {k} specified before collection")))
                }
            } else if let Some(name) = k.strip_prefix("adbc.metadata.filter.") {
                let v = E::option_as_string(key, value)?;
                if let Some(collection) = &mut self.collection {
                    collection.add_filter(name, Filter::Exact(v.to_string()))?;
                    Ok(true)
                } else {
                    Err(E::invalid_argument()
                        .format(format_args!("filter {k} specified before collection")))
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }
}

pub mod schema {
    use std::sync::{Arc, LazyLock};

    use arrow_schema::{DataType, Field, Schema};

    pub static META_FILTER: LazyLock<Schema> = LazyLock::new(|| {
        Schema::new(vec![
            Field::new("filter_name", DataType::Utf8, false),
            Field::new("filter_type", DataType::Int8, false),
            Field::new("filter_required", DataType::Boolean, false),
            Field::new("filter_remarks", DataType::Utf8, true),
        ])
    });

    /// The schema for the "meta" collection.
    pub static META: LazyLock<Schema> = LazyLock::new(|| {
        Schema::new(vec![
            Field::new("collection_name", DataType::Utf8, false),
            Field::new("collection_remarks", DataType::Utf8, true),
            // XXX: proposal uses JSON
            Field::new("collection_schema", DataType::Utf8, false),
            Field::new(
                "collection_filters",
                DataType::List(Arc::new(Field::new_list_field(
                    DataType::Struct(META_FILTER.fields.clone()),
                    true,
                ))),
                true,
            ),
        ])
    });

    /// The schema for the "catalogs" collection.
    pub static CATALOG: LazyLock<Schema> = LazyLock::new(|| {
        Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("catalog_remarks", DataType::Utf8, true),
        ])
    });

    /// The schema for the "schemas" collection.
    pub static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
        Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
            Field::new("db_schema_remarks", DataType::Utf8, true),
        ])
    });

    /// The schema for the "tables" collection.
    pub static TABLES: LazyLock<Schema> = LazyLock::new(|| {
        Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("table_definition", DataType::Utf8, true),
            Field::new("table_remarks", DataType::Utf8, true),
            // XXX: should be schema JSON
            Field::new("table_schema", DataType::Utf8, true),
        ])
    });
}

/// The implementation of a metadata collection; it records filters and
/// generates the result metadata.
#[async_trait::async_trait]
pub trait MetadataCollectionQuery<Args, E>
where
    E: crate::error::ErrorHelper,
{
    fn add_filter(&mut self, name: &str, filter: Filter) -> Result<(), E>;
    async fn execute(&self, args: Args) -> Result<Box<dyn RecordBatchReader + Send + 'static>, E>;
}

/// The description of a supported filter on a metadata collection. Used to
/// generate the "meta" collection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FilterDescription {
    pub name: String,
    pub filter_type_string: bool,
    pub filter_type_bytes: bool,
    pub filter_type_double: bool,
    pub filter_type_int: bool,
    pub required: bool,
    pub remarks: Option<String>,
    pub is_pattern: bool,
}

/// The metadata for a metadata collection.
pub trait MetadataCollection<Args, E>: Send + Sync {
    fn name(&self) -> &'static str;
    fn remarks(&self) -> Option<String> {
        None
    }
    fn schema(&self) -> Schema;
    fn supported_filters(&self) -> Vec<FilterDescription> {
        Vec::new()
    }
    fn init(&self) -> Box<dyn MetadataCollectionQuery<Args, E> + Send>;
}

/// A filter for a string field of a metadata collection.
#[derive(Clone, Debug)]
pub enum Filter {
    Pattern {
        pattern: String,
        compiled: regex_lite::Regex,
    },
    Exact(String),
}

impl PartialEq for Filter {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Filter::Pattern { pattern: p1, .. }, Filter::Pattern { pattern: p2, .. }) => p1 == p2,
            (Filter::Exact(e1), Filter::Exact(e2)) => e1 == e2,
            _ => false,
        }
    }
}

impl Filter {
    pub fn new_pattern(pattern: String) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let mut re = String::with_capacity(pattern.len());
        re.push('^');
        for chr in pattern.chars() {
            match chr {
                '%' => re.push_str(".*"),
                '_' => re.push('.'),
                // See is_meta_character in regex_lite
                '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^'
                | '$' | '#' | '&' | '-' | '~' => {
                    re.push('\\');
                    re.push(chr);
                }
                _ => re.push(chr),
            }
        }
        re.push('$');
        let compiled = regex_lite::Regex::new(&re)?;
        Ok(Filter::Pattern { pattern, compiled })
    }

    pub fn new_exact(exact: String) -> Self {
        Filter::Exact(exact)
    }

    pub fn matches(&self, value: &str) -> bool {
        match self {
            Filter::Pattern { compiled, .. } => compiled.is_match(value),
            Filter::Exact(exact) => exact == value,
        }
    }
}

/// The implementation of the "meta" collection.
struct MetaCollectionQuery<Args, E> {
    supported_collections: SupportedMetadataCollections<Args, E>,
}

#[async_trait::async_trait]
impl<Args: Send, E: crate::error::ErrorHelper> MetadataCollectionQuery<Args, E>
    for MetaCollectionQuery<Args, E>
{
    fn add_filter(&mut self, name: &str, _value: Filter) -> Result<(), E> {
        #[allow(
            clippy::match_single_binding,
            reason = "placeholder for future filter support"
        )]
        match name {
            _ => Err(E::invalid_argument().format(format_args!(
                "unsupported filter {name} for metadata collection 'meta'"
            ))),
        }
    }

    async fn execute(&self, _args: Args) -> Result<Box<dyn RecordBatchReader + Send + 'static>, E> {
        let mut names = arrow_array::builder::StringBuilder::new();
        let mut remarks = arrow_array::builder::StringBuilder::new();
        let mut schemas = arrow_array::builder::StringBuilder::new();
        let mut filters = {
            let filter_items = arrow_array::builder::StructBuilder::new(
                schema::META_FILTER.fields.clone(),
                vec![
                    Box::new(arrow_array::builder::StringBuilder::new()),
                    Box::new(arrow_array::builder::Int8Builder::new()),
                    Box::new(arrow_array::builder::BooleanBuilder::new()),
                    Box::new(arrow_array::builder::StringBuilder::new()),
                ],
            );

            arrow_array::builder::ListBuilder::new(filter_items)
        };

        names.append_value("meta");
        remarks.append_option(Some("Available metadata collections".to_string()));
        schemas.append_value(schema::META.to_string());
        filters.append_null();

        for (name, collection) in self.supported_collections.collections.iter() {
            names.append_value(name);
            remarks.append_option(collection.remarks());
            schemas.append_value(collection.schema().to_string());

            let supported_filters = collection.supported_filters();
            if supported_filters.is_empty() {
                filters.append_null();
            } else {
                for filter in supported_filters {
                    let name = format!(
                        "adbc.metadata.filter{}.{}",
                        if filter.is_pattern { "_pattern" } else { "" },
                        filter.name
                    );
                    filters
                        .values()
                        .field_builder::<arrow_array::builder::StringBuilder>(0)
                        .unwrap()
                        .append_value(&name);

                    let mut filter_type = 0;
                    if filter.filter_type_string {
                        filter_type |= 1;
                    }
                    if filter.filter_type_bytes {
                        filter_type |= 2;
                    }
                    if filter.filter_type_double {
                        filter_type |= 4;
                    }
                    if filter.filter_type_int {
                        filter_type |= 8;
                    }
                    filters
                        .values()
                        .field_builder::<arrow_array::builder::Int8Builder>(1)
                        .unwrap()
                        .append_value(filter_type);

                    filters
                        .values()
                        .field_builder::<arrow_array::builder::BooleanBuilder>(2)
                        .unwrap()
                        .append_value(filter.required);
                    filters
                        .values()
                        .field_builder::<arrow_array::builder::StringBuilder>(3)
                        .unwrap()
                        .append_option(filter.remarks.clone());

                    filters.values().append(true);
                }
                filters.append(true);
            }
        }

        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(schema::META.clone()),
            vec![
                Arc::new(names.finish()),
                Arc::new(remarks.finish()),
                Arc::new(schemas.finish()),
                Arc::new(filters.finish()),
            ],
        )?;

        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)], Arc::new(schema::META.clone()));

        Ok(Box::new(reader))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_filter_exact() {
        let filter = super::Filter::new_exact("test".to_string());
        assert!(filter.matches("test"));
        assert!(!filter.matches("test1"));
    }

    #[test]
    fn test_filter_pattern() {
        let filter = super::Filter::new_pattern("test%".to_string()).unwrap();
        assert!(filter.matches("test"));
        assert!(filter.matches("test1"));
        assert!(!filter.matches("atest"));

        let filter = super::Filter::new_pattern("test_".to_string()).unwrap();
        assert!(!filter.matches("test"));
        assert!(filter.matches("test1"));
        assert!(!filter.matches("atest"));

        let filter = super::Filter::new_pattern("%test%".to_string()).unwrap();
        assert!(filter.matches("test"));
        assert!(filter.matches("test1"));
        assert!(filter.matches("atest"));
        assert!(!filter.matches("at3st"));

        let filter = super::Filter::new_pattern("*".to_string()).unwrap();
        assert!(!filter.matches("test"));
        assert!(!filter.matches("test1"));
        assert!(!filter.matches("atest"));
        assert!(filter.matches("*"));
    }

    #[derive(Clone)]
    struct ErrorHelper;

    impl crate::error::ErrorHelper for ErrorHelper {
        const NAME: &'static str = "test";
    }

    #[test]
    fn test_state() {
        let mut state = super::MetadataCollectionState::<(), ErrorHelper>::new(
            super::SupportedMetadataCollections::new(std::collections::HashMap::new()),
        );

        assert!(!state.is_set());
        assert!(state.take().is_none());
    }

    #[test]
    fn test_state_no_filters_before_collection() {
        let mut state = super::MetadataCollectionState::<(), ErrorHelper>::new(
            super::SupportedMetadataCollections::new(std::collections::HashMap::new()),
        );

        let err = state
            .set_option(
                &adbc_core::options::OptionStatement::Other(
                    "adbc.metadata.filter_pattern.test".to_string(),
                ),
                &adbc_core::options::OptionValue::String("test".to_string()),
            )
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("filter adbc.metadata.filter_pattern.test specified before collection")
        );

        let err = state
            .set_option(
                &adbc_core::options::OptionStatement::Other(
                    "adbc.metadata.filter.test".to_string(),
                ),
                &adbc_core::options::OptionValue::String("test".to_string()),
            )
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("filter adbc.metadata.filter.test specified before collection")
        );
    }

    #[test]
    fn test_state_ignore_unrelated_options() {
        let mut state = super::MetadataCollectionState::<(), ErrorHelper>::new(
            super::SupportedMetadataCollections::new(std::collections::HashMap::new()),
        );
        let result = state.set_option(
            &adbc_core::options::OptionStatement::Other("unrelated.option".to_string()),
            &adbc_core::options::OptionValue::String("test".to_string()),
        );
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_state_capture_wrong_value_type() {
        let mut state = super::MetadataCollectionState::<(), ErrorHelper>::new(
            super::SupportedMetadataCollections::new(std::collections::HashMap::new()),
        );
        let err = state
            .set_option(
                &adbc_core::options::OptionStatement::Other("adbc.metadata.collection".to_string()),
                &adbc_core::options::OptionValue::Int(42),
            )
            .unwrap_err();
        assert!(err.to_adbc().status == adbc_core::error::Status::InvalidArguments);
    }
}
