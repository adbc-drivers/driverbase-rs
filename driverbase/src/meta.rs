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

    pub fn as_ref(&self) -> Option<&(dyn MetadataCollectionQuery<Args, E> + Send)> {
        self.collection.as_deref()
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
    use std::sync::LazyLock;

    use arrow_schema::{DataType, Field, Schema};

    /// The schema for the "meta" collection.
    pub static META: LazyLock<Schema> = LazyLock::new(|| {
        Schema::new(vec![
            Field::new("collection_name", DataType::Utf8, false),
            Field::new("collection_remarks", DataType::Utf8, true),
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

/// Get the Arrow schema representation of the supported filters.
pub fn get_filter_schema(filters: &[FilterDescription]) -> Schema {
    let mut fields = Vec::new();
    for filter in filters {
        let mut types = Vec::new();
        if filter.filter_type_string {
            types.push(arrow_schema::DataType::Utf8);
        }
        if filter.filter_type_bytes {
            types.push(arrow_schema::DataType::Binary);
        }
        if filter.filter_type_double {
            types.push(arrow_schema::DataType::Float64);
        }
        if filter.filter_type_int {
            types.push(arrow_schema::DataType::Int64);
        }

        let data_type = match <[_; 1]>::try_from(types) {
            Ok([ty]) => ty,
            Err(types) => arrow_schema::DataType::Union(
                arrow_schema::UnionFields::from_fields(
                    types
                        .into_iter()
                        .enumerate()
                        .map(|(i, t)| arrow_schema::Field::new(format!("field_{i}"), t, false)),
                ),
                arrow_schema::UnionMode::Sparse,
            ),
        };

        let name = format!(
            "adbc.metadata.filter{}.{}",
            if filter.is_pattern { "_pattern" } else { "" },
            filter.name
        );
        let mut field = arrow_schema::Field::new(name, data_type, !filter.required);
        if let Some(ref remarks) = filter.remarks {
            field.set_metadata(std::collections::HashMap::from([(
                "ADBC:metadata:filter:remarks".to_string(),
                remarks.clone(),
            )]));
        }

        fields.push(field);
    }
    Schema::new(fields)
}

/// The implementation of a metadata collection; it records filters and
/// generates the result metadata.
#[async_trait::async_trait]
pub trait MetadataCollectionQuery<Args, E>
where
    E: crate::error::ErrorHelper,
{
    fn add_filter(&mut self, name: &str, filter: Filter) -> Result<(), E>;
    fn schema(&self) -> Schema;
    fn supported_filters(&self) -> Vec<FilterDescription> {
        Vec::new()
    }
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

    fn schema(&self) -> Schema {
        schema::META.clone()
    }

    async fn execute(&self, _args: Args) -> Result<Box<dyn RecordBatchReader + Send + 'static>, E> {
        let mut names = arrow_array::builder::StringBuilder::new();
        let mut remarks = arrow_array::builder::StringBuilder::new();

        names.append_value("meta");
        remarks.append_option(Some("Available metadata collections".to_string()));

        for (name, collection) in self.supported_collections.collections.iter() {
            names.append_value(name);
            remarks.append_option(collection.remarks());
        }

        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(schema::META.clone()),
            vec![Arc::new(names.finish()), Arc::new(remarks.finish())],
        )?;

        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(batch)], Arc::new(schema::META.clone()));

        Ok(Box::new(reader))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use googletest::prelude::*;

    #[test]
    fn test_filter_exact() {
        let filter = Filter::new_exact("test".to_string());
        assert!(filter.matches("test"));
        assert!(!filter.matches("test1"));
    }

    #[test]
    fn test_filter_pattern() {
        let filter = Filter::new_pattern("test%".to_string()).unwrap();
        assert!(filter.matches("test"));
        assert!(filter.matches("test1"));
        assert!(!filter.matches("atest"));

        let filter = Filter::new_pattern("test_".to_string()).unwrap();
        assert!(!filter.matches("test"));
        assert!(filter.matches("test1"));
        assert!(!filter.matches("atest"));

        let filter = Filter::new_pattern("%test%".to_string()).unwrap();
        assert!(filter.matches("test"));
        assert!(filter.matches("test1"));
        assert!(filter.matches("atest"));
        assert!(!filter.matches("at3st"));

        let filter = Filter::new_pattern("*".to_string()).unwrap();
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
        let mut state = MetadataCollectionState::<(), ErrorHelper>::new(
            SupportedMetadataCollections::new(std::collections::HashMap::new()),
        );

        assert!(!state.is_set());
        assert!(state.take().is_none());
    }

    #[test]
    fn test_state_no_filters_before_collection() {
        let mut state = MetadataCollectionState::<(), ErrorHelper>::new(
            SupportedMetadataCollections::new(std::collections::HashMap::new()),
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
        let mut state = MetadataCollectionState::<(), ErrorHelper>::new(
            SupportedMetadataCollections::new(std::collections::HashMap::new()),
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
        let mut state = MetadataCollectionState::<(), ErrorHelper>::new(
            SupportedMetadataCollections::new(std::collections::HashMap::new()),
        );
        let err = state
            .set_option(
                &adbc_core::options::OptionStatement::Other("adbc.metadata.collection".to_string()),
                &adbc_core::options::OptionValue::Int(42),
            )
            .unwrap_err();
        assert_eq!(
            err.to_adbc().status,
            adbc_core::error::Status::InvalidArguments
        );
    }

    #[test]
    fn test_filter_schema() {
        let filters = vec![
            FilterDescription {
                name: "filter1".to_string(),
                filter_type_string: true,
                filter_type_bytes: false,
                filter_type_double: false,
                filter_type_int: false,
                required: true,
                remarks: Some("Filter 1".to_string()),
                is_pattern: false,
            },
            FilterDescription {
                name: "filter2".to_string(),
                filter_type_string: false,
                filter_type_bytes: true,
                filter_type_double: false,
                filter_type_int: false,
                required: false,
                remarks: None,
                is_pattern: true,
            },
            FilterDescription {
                name: "filter3".to_string(),
                filter_type_string: true,
                filter_type_bytes: true,
                filter_type_double: false,
                filter_type_int: false,
                required: false,
                remarks: None,
                is_pattern: false,
            },
        ];

        let schema = get_filter_schema(&filters);
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "adbc.metadata.filter.filter1");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert!(!schema.field(0).is_nullable());
        assert_that!(
            schema
                .field(0)
                .metadata()
                .get("ADBC:metadata:filter:remarks"),
            some(eq("Filter 1"))
        );

        assert_eq!(
            schema.field(1).name(),
            "adbc.metadata.filter_pattern.filter2"
        );
        assert_eq!(schema.field(1).data_type(), &DataType::Binary);
        assert!(schema.field(1).is_nullable());
        assert_that!(
            schema
                .field(1)
                .metadata()
                .get("ADBC:metadata:filter:remarks"),
            none()
        );

        assert_eq!(schema.field(2).name(), "adbc.metadata.filter.filter3");
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Union(
                arrow_schema::UnionFields::from_fields(vec![
                    Field::new("field_0", DataType::Utf8, false),
                    Field::new("field_1", DataType::Binary, false),
                ]),
                arrow_schema::UnionMode::Sparse,
            )
        );
        assert!(schema.field(2).is_nullable());
        assert_that!(
            schema
                .field(2)
                .metadata()
                .get("ADBC:metadata:filter:remarks"),
            none()
        );
    }
}
