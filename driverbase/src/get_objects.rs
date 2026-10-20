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

use std::{collections::VecDeque, sync::Arc};

use arrow_array::builder::{self, ArrayBuilder};
use arrow_schema::{DataType, Field, Fields, Schema};

#[derive(Clone, Debug)]
pub struct TableInfo {
    pub table_name: String,
    pub table_type: String,
}

#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub column_name: String,
    /// One-based position in the original table schema, before column filtering.
    /// Use `None` when the position is unknown.
    pub ordinal_position: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct TableAndColumnInfo {
    pub table: TableInfo,
    pub columns: Vec<ColumnInfo>,
}

pub trait GetObjectsImpl<E>: Send + 'static
where
    E: super::error::ErrorHelper,
{
    fn get_catalogs(&self, filter: Option<&str>) -> Result<Vec<String>, super::error::Error<E>>;
    fn get_db_schemas(
        &self,
        catalog: &str,
        filter: Option<&str>,
    ) -> Result<Vec<String>, super::error::Error<E>>;
    fn get_tables(
        &self,
        catalog: &str,
        db_schema: &str,
        table_filter: Option<&str>,
        table_type_filter: Option<&[String]>,
    ) -> Result<Vec<TableInfo>, super::error::Error<E>>;
    /// Return matching columns with their original table ordinal positions.
    /// Filtering must not renumber the columns in each table.
    fn get_columns(
        &self,
        catalog: &str,
        db_schema: &str,
        table_filter: Option<&str>,
        table_type_filter: Option<&[String]>,
        column_filter: Option<&str>,
    ) -> Result<Vec<TableAndColumnInfo>, super::error::Error<E>>;
}

static SCHEMA: std::sync::LazyLock<Schema> = std::sync::LazyLock::new(|| {
    let usage = Fields::from(vec![
        Field::new("fk_catalog", DataType::Utf8, true),
        Field::new("fk_db_schema", DataType::Utf8, true),
        Field::new("fk_table", DataType::Utf8, false),
        Field::new("fk_column_name", DataType::Utf8, false),
    ]);

    let constraint = Fields::from(vec![
        Field::new("constraint_name", DataType::Utf8, true),
        Field::new("constraint_type", DataType::Utf8, true),
        Field::new_list(
            "constraint_column_names",
            Field::new_list_field(DataType::Utf8, false),
            false,
        ),
        Field::new_list(
            "constraint_column_usage",
            Field::new_list_field(DataType::Struct(usage), false),
            false,
        ),
    ]);

    let column = Fields::from(vec![
        Field::new("column_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::Int32, true),
        Field::new("remarks", DataType::Utf8, true),
        Field::new("xdbc_data_type", DataType::Int16, true),
        Field::new("xdbc_type_name", DataType::Utf8, true),
        Field::new("xdbc_column_size", DataType::Int32, true),
        Field::new("xdbc_decimal_digits", DataType::Int16, true),
        Field::new("xdbc_num_prec_radix", DataType::Int16, true),
        Field::new("xdbc_nullable", DataType::Int16, true),
        Field::new("xdbc_column_def", DataType::Utf8, true),
        Field::new("xdbc_sql_data_type", DataType::Int16, true),
        Field::new("xdbc_datetime_sub", DataType::Int16, true),
        Field::new("xdbc_char_octet_length", DataType::Int32, true),
        Field::new("xdbc_is_nullable", DataType::Utf8, true),
        Field::new("xdbc_scope_catalog", DataType::Utf8, true),
        Field::new("xdbc_scope_schema", DataType::Utf8, true),
        Field::new("xdbc_scope_table", DataType::Utf8, true),
        Field::new("xdbc_is_autoincrement", DataType::Boolean, true),
        Field::new("xdbc_is_generatedcolumn", DataType::Boolean, true),
    ]);

    let table = Fields::from(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
        Field::new_list(
            "table_columns",
            Field::new_list_field(DataType::Struct(column), false),
            true,
        ),
        Field::new_list(
            "table_constraints",
            Field::new_list_field(DataType::Struct(constraint), false),
            true,
        ),
    ]);

    let db_schema = Fields::from(vec![
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new_list(
            "db_schema_tables",
            Field::new_list_field(DataType::Struct(table), false),
            true,
        ),
    ]);

    Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new_list(
            "catalog_db_schemas",
            Field::new_list_field(DataType::Struct(db_schema), false),
            true,
        ),
    ])
});

pub fn get_objects_schema() -> Schema {
    SCHEMA.clone()
}

pub fn get_objects<I, E>(
    inner: I,
    depth: adbc_core::options::ObjectDepth,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: Option<&str>,
    table_type: Option<Vec<&str>>,
    column_name: Option<&str>,
) -> Box<impl arrow_array::RecordBatchReader + Send + 'static>
where
    I: GetObjectsImpl<E>,
    E: super::error::ErrorHelper,
{
    Box::new(arrow_array::RecordBatchIterator::new(
        BatchIterator::new(GetObjectsBuilder::new(
            inner,
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        )),
        Arc::new(get_objects_schema()),
    ))
}

struct GetObjectsBuilder<I, E>
where
    I: GetObjectsImpl<E>,
    E: super::error::ErrorHelper,
{
    _marker: std::marker::PhantomData<E>,
    inner: I,

    depth: adbc_core::options::ObjectDepth,
    catalog_filter: Option<String>,
    db_schema_filter: Option<String>,
    table_name_filter: Option<String>,
    table_type_filter: Option<Vec<String>>,
    column_name_filter: Option<String>,
}

impl<I, E> GetObjectsBuilder<I, E>
where
    I: GetObjectsImpl<E>,
    E: super::error::ErrorHelper,
{
    fn new(
        inner: I,
        depth: adbc_core::options::ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Self {
        Self {
            _marker: std::marker::PhantomData,
            inner,

            depth,
            catalog_filter: catalog.map(|s| s.to_owned()),
            db_schema_filter: db_schema.map(|s| s.to_owned()),
            table_name_filter: table_name.map(|s| s.to_owned()),
            table_type_filter: table_type.map(|v| v.into_iter().map(|s| s.to_owned()).collect()),
            column_name_filter: column_name.map(|s| s.to_owned()),
        }
    }

    fn append_catalog(
        &mut self,
        catalog: &str,
        catalog_name: &mut arrow_array::builder::StringBuilder,
        catalog_db_schemas: &mut arrow_array::builder::ListBuilder<Box<dyn ArrayBuilder>>,
    ) -> Result<(), super::error::Error<E>> {
        catalog_name.append_value(catalog);

        // TODO(https://github.com/apache/arrow-adbc/issues/3716)
        if let adbc_core::options::ObjectDepth::Catalogs = self.depth {
            catalog_db_schemas.append_null();
        } else {
            self.append_db_schemas(catalog, catalog_db_schemas)?;
            catalog_db_schemas.append(true);
        }

        Ok(())
    }

    fn append_db_schemas(
        &self,
        catalog: &str,
        catalog_db_schemas: &mut arrow_array::builder::ListBuilder<Box<dyn ArrayBuilder>>,
    ) -> Result<(), super::error::Error<E>> {
        let schemas = self
            .inner
            .get_db_schemas(catalog, self.db_schema_filter.as_deref())?;

        let db_schema_item = catalog_db_schemas
            .values()
            .as_any_mut()
            .downcast_mut::<builder::StructBuilder>()
            .unwrap();
        for schema in schemas {
            {
                let db_schema_name = db_schema_item
                    .field_builder::<builder::StringBuilder>(0)
                    .unwrap();
                db_schema_name.append_value(&schema);
            }
            {
                let db_schema_tables = db_schema_item
                    .field_builder::<builder::ListBuilder<Box<dyn ArrayBuilder>>>(1)
                    .unwrap();
                // TODO(https://github.com/apache/arrow-adbc/issues/3716)
                if let adbc_core::options::ObjectDepth::Schemas = self.depth {
                    db_schema_tables.append_null();
                } else if let adbc_core::options::ObjectDepth::Tables = self.depth {
                    self.append_tables(catalog, schema.as_ref(), db_schema_tables)?;
                    db_schema_tables.append(true);
                } else {
                    self.append_tables_columns(catalog, schema.as_ref(), db_schema_tables)?;
                    db_schema_tables.append(true);
                }
            }

            db_schema_item.append(true);
        }
        Ok(())
    }

    fn append_tables(
        &self,
        catalog: &str,
        db_schema: &str,
        db_schema_tables: &mut arrow_array::builder::ListBuilder<Box<dyn ArrayBuilder>>,
    ) -> Result<(), super::error::Error<E>> {
        let table_item = db_schema_tables
            .values()
            .as_any_mut()
            .downcast_mut::<builder::StructBuilder>()
            .unwrap();

        for table in self.inner.get_tables(
            catalog,
            db_schema,
            self.table_name_filter.as_deref(),
            self.table_type_filter.as_deref(),
        )? {
            {
                let table_name = table_item
                    .field_builder::<builder::StringBuilder>(0)
                    .unwrap();
                table_name.append_value(&table.table_name);
            }
            {
                let table_type = table_item
                    .field_builder::<builder::StringBuilder>(1)
                    .unwrap();
                table_type.append_value(&table.table_type);
            }
            {
                let table_columns = table_item
                    .field_builder::<builder::ListBuilder<Box<dyn ArrayBuilder>>>(2)
                    .unwrap();
                table_columns.append_null();
            }
            {
                let table_constraints = table_item
                    .field_builder::<builder::ListBuilder<Box<dyn ArrayBuilder>>>(3)
                    .unwrap();
                table_constraints.append_null();
            }
            table_item.append(true);
        }

        Ok(())
    }

    fn append_tables_columns(
        &self,
        catalog: &str,
        db_schema: &str,
        db_schema_tables: &mut arrow_array::builder::ListBuilder<Box<dyn ArrayBuilder>>,
    ) -> Result<(), super::error::Error<E>> {
        let table_item = db_schema_tables
            .values()
            .as_any_mut()
            .downcast_mut::<builder::StructBuilder>()
            .unwrap();

        for table in self.inner.get_columns(
            catalog,
            db_schema,
            self.table_name_filter.as_deref(),
            self.table_type_filter.as_deref(),
            self.column_name_filter.as_deref(),
        )? {
            {
                let table_name = table_item
                    .field_builder::<builder::StringBuilder>(0)
                    .unwrap();
                table_name.append_value(&table.table.table_name);
            }
            {
                let table_type = table_item
                    .field_builder::<builder::StringBuilder>(1)
                    .unwrap();
                table_type.append_value(&table.table.table_type);
            }
            {
                let table_columns = table_item
                    .field_builder::<builder::ListBuilder<Box<dyn ArrayBuilder>>>(2)
                    .unwrap();

                for column in &table.columns {
                    let column_item = table_columns
                        .values()
                        .as_any_mut()
                        .downcast_mut::<builder::StructBuilder>()
                        .unwrap();

                    {
                        let column_name = column_item
                            .field_builder::<builder::StringBuilder>(0)
                            .unwrap();
                        column_name.append_value(&column.column_name);
                    }
                    {
                        let ordinal_position = column_item
                            .field_builder::<builder::Int32Builder>(1)
                            .unwrap();
                        ordinal_position.append_option(column.ordinal_position);
                    }
                    {
                        let remarks = column_item
                            .field_builder::<builder::StringBuilder>(2)
                            .unwrap();
                        remarks.append_null();
                    }
                    {
                        let xdbc_data_type = column_item
                            .field_builder::<builder::Int16Builder>(3)
                            .unwrap();
                        xdbc_data_type.append_null();
                    }
                    {
                        let xdbc_type_name = column_item
                            .field_builder::<builder::StringBuilder>(4)
                            .unwrap();
                        xdbc_type_name.append_null();
                    }
                    {
                        let xdbc_column_size = column_item
                            .field_builder::<builder::Int32Builder>(5)
                            .unwrap();
                        xdbc_column_size.append_null();
                    }
                    {
                        let xdbc_decimal_digits = column_item
                            .field_builder::<builder::Int16Builder>(6)
                            .unwrap();
                        xdbc_decimal_digits.append_null();
                    }
                    {
                        let xdbc_num_prec_radix = column_item
                            .field_builder::<builder::Int16Builder>(7)
                            .unwrap();
                        xdbc_num_prec_radix.append_null();
                    }
                    {
                        let xdbc_nullable = column_item
                            .field_builder::<builder::Int16Builder>(8)
                            .unwrap();
                        xdbc_nullable.append_null();
                    }
                    {
                        let xdbc_column_def = column_item
                            .field_builder::<builder::StringBuilder>(9)
                            .unwrap();
                        xdbc_column_def.append_null();
                    }
                    {
                        let xdbc_sql_data_type = column_item
                            .field_builder::<builder::Int16Builder>(10)
                            .unwrap();
                        xdbc_sql_data_type.append_null();
                    }
                    {
                        let xdbc_datetime_sub = column_item
                            .field_builder::<builder::Int16Builder>(11)
                            .unwrap();
                        xdbc_datetime_sub.append_null();
                    }
                    {
                        let xdbc_char_octet_length = column_item
                            .field_builder::<builder::Int32Builder>(12)
                            .unwrap();
                        xdbc_char_octet_length.append_null();
                    }
                    {
                        let xdbc_is_nullable = column_item
                            .field_builder::<builder::StringBuilder>(13)
                            .unwrap();
                        xdbc_is_nullable.append_null();
                    }
                    {
                        let xdbc_scope_catalog = column_item
                            .field_builder::<builder::StringBuilder>(14)
                            .unwrap();
                        xdbc_scope_catalog.append_null();
                    }
                    {
                        let xdbc_scope_schema = column_item
                            .field_builder::<builder::StringBuilder>(15)
                            .unwrap();
                        xdbc_scope_schema.append_null();
                    }
                    {
                        let xdbc_scope_table = column_item
                            .field_builder::<builder::StringBuilder>(16)
                            .unwrap();
                        xdbc_scope_table.append_null();
                    }
                    {
                        let xdbc_is_autoincrement = column_item
                            .field_builder::<builder::BooleanBuilder>(17)
                            .unwrap();
                        xdbc_is_autoincrement.append_null();
                    }
                    {
                        let xdbc_is_generatedcolumn = column_item
                            .field_builder::<builder::BooleanBuilder>(18)
                            .unwrap();
                        xdbc_is_generatedcolumn.append_null();
                    }
                    column_item.append(true);
                }

                table_columns.append(true);
            }
            {
                let table_constraints = table_item
                    .field_builder::<builder::ListBuilder<Box<dyn ArrayBuilder>>>(3)
                    .unwrap();
                table_constraints.append(true);
            }
            table_item.append(true);
        }

        Ok(())
    }
}

enum BuilderState {
    Uninit,
    ReadCatalogs(VecDeque<String>),
    Finished,
}

// Split out the builder state so that we can avoid mutable reborrows
struct BatchIterator<I, E>
where
    I: GetObjectsImpl<E>,
    E: super::error::ErrorHelper,
{
    state: BuilderState,
    catalog_name: builder::StringBuilder,
    catalog_db_schemas: Box<dyn builder::ArrayBuilder>,
    inner: GetObjectsBuilder<I, E>,
}

impl<I, E> BatchIterator<I, E>
where
    I: GetObjectsImpl<E>,
    E: super::error::ErrorHelper,
{
    fn new(inner: GetObjectsBuilder<I, E>) -> Self {
        let schema = get_objects_schema();
        let catalog_name = builder::StringBuilder::with_capacity(32, 65536);
        let catalog_db_schemas = builder::make_builder(schema.fields()[1].data_type(), 32);
        Self {
            state: BuilderState::Uninit,
            catalog_name,
            catalog_db_schemas,
            inner,
        }
    }
}

impl<I, E> Iterator for BatchIterator<I, E>
where
    I: GetObjectsImpl<E>,
    E: super::error::ErrorHelper,
{
    type Item = Result<arrow_array::RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            BuilderState::Uninit => match self
                .inner
                .inner
                .get_catalogs(self.inner.catalog_filter.as_deref())
            {
                Ok(catalogs) => {
                    self.state =
                        BuilderState::ReadCatalogs(catalogs.into_iter().collect::<VecDeque<_>>());
                    self.next()
                }
                Err(e) => {
                    self.state = BuilderState::Finished;
                    Some(Err(arrow_schema::ArrowError::ExternalError(Box::new(e))))
                }
            },
            BuilderState::ReadCatalogs(ref mut catalogs) => {
                if let Some(catalog) = catalogs.pop_front() {
                    let catalog_db_schemas = self
                        .catalog_db_schemas
                        .as_any_mut()
                        .downcast_mut::<builder::ListBuilder<Box<dyn ArrayBuilder>>>()
                        .unwrap();

                    match self.inner.append_catalog(
                        &catalog,
                        &mut self.catalog_name,
                        catalog_db_schemas,
                    ) {
                        Ok(()) => Some(arrow_array::RecordBatch::try_new(
                            Arc::new(get_objects_schema()),
                            vec![
                                Arc::new(self.catalog_name.finish()),
                                self.catalog_db_schemas.finish(),
                            ],
                        )),
                        Err(e) => {
                            self.state = BuilderState::Finished;
                            Some(Err(arrow_schema::ArrowError::ExternalError(Box::new(e))))
                        }
                    }
                } else {
                    self.state = BuilderState::Finished;
                    None
                }
            }
            BuilderState::Finished => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use adbc_core::options::ObjectDepth;
    use arrow_array::{Array, Int32Array, ListArray, StringArray, StructArray};

    use super::*;

    #[derive(Clone)]
    struct TestErrorHelper;

    impl crate::error::ErrorHelper for TestErrorHelper {
        const NAME: &'static str = "test";
    }

    type TestResult<T> = Result<T, crate::error::Error<TestErrorHelper>>;

    struct TestCatalog;

    impl GetObjectsImpl<TestErrorHelper> for TestCatalog {
        fn get_catalogs(&self, _filter: Option<&str>) -> TestResult<Vec<String>> {
            Ok(vec!["catalog".into()])
        }

        fn get_db_schemas(&self, _catalog: &str, _filter: Option<&str>) -> TestResult<Vec<String>> {
            Ok(vec!["schema".into()])
        }

        fn get_tables(
            &self,
            _catalog: &str,
            _db_schema: &str,
            _table_filter: Option<&str>,
            _table_type_filter: Option<&[String]>,
        ) -> TestResult<Vec<TableInfo>> {
            Ok(vec![TableInfo {
                table_name: "table".into(),
                table_type: "BASE TABLE".into(),
            }])
        }

        fn get_columns(
            &self,
            catalog: &str,
            db_schema: &str,
            table_filter: Option<&str>,
            table_type_filter: Option<&[String]>,
            column_filter: Option<&str>,
        ) -> TestResult<Vec<TableAndColumnInfo>> {
            // This fixture supports exact names and the test pattern "keep_%".
            let columns = [
                ("first", Some(1)),
                ("keep_second", Some(2)),
                ("third", Some(3)),
                ("keep_fourth", Some(4)),
                ("unknown", None),
            ]
            .into_iter()
            .filter(|(name, _)| match column_filter {
                None => true,
                Some("keep_%") => name.starts_with("keep_"),
                Some(filter) => *name == filter,
            })
            .map(|(name, ordinal_position)| ColumnInfo {
                column_name: name.into(),
                ordinal_position,
            })
            .collect();
            Ok(vec![TableAndColumnInfo {
                table: self
                    .get_tables(catalog, db_schema, table_filter, table_type_filter)?
                    .remove(0),
                columns,
            }])
        }
    }

    fn read_columns(filter: Option<&str>) -> Vec<(String, Option<i32>)> {
        let batches = get_objects(
            TestCatalog,
            ObjectDepth::All,
            None,
            None,
            None,
            None,
            filter,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.schema().as_ref(), &get_objects_schema());
        let schemas = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let schemas = schemas.as_any().downcast_ref::<StructArray>().unwrap();
        let tables = schemas
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let tables = tables.as_any().downcast_ref::<StructArray>().unwrap();
        let columns = tables
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let columns = columns.as_any().downcast_ref::<StructArray>().unwrap();
        let names = columns
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let positions = columns
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        names
            .iter()
            .zip(positions.iter())
            .map(|(name, position)| (name.unwrap().to_string(), position))
            .collect()
    }

    #[test]
    fn preserves_column_ordinals_after_filtering() {
        assert_eq!(
            read_columns(Some("keep_second")),
            vec![("keep_second".into(), Some(2))]
        );
        assert_eq!(
            read_columns(Some("keep_%")),
            vec![
                ("keep_second".into(), Some(2)),
                ("keep_fourth".into(), Some(4)),
            ]
        );
        assert!(read_columns(Some("missing")).is_empty());
    }

    #[test]
    fn preserves_unfiltered_and_unknown_column_ordinals() {
        assert_eq!(
            read_columns(None),
            vec![
                ("first".into(), Some(1)),
                ("keep_second".into(), Some(2)),
                ("third".into(), Some(3)),
                ("keep_fourth".into(), Some(4)),
                ("unknown".into(), None),
            ]
        );
        assert_eq!(
            read_columns(Some("unknown")),
            vec![("unknown".into(), None)]
        );
    }
}
