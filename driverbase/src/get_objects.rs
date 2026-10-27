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
use arrow_schema::Schema;

#[derive(Clone, Debug)]
pub struct TableInfo {
    pub table_name: String,
    pub table_type: String,
}

/// Metadata for a column returned by [`GetObjectsImpl::get_columns`].
///
/// Optional metadata maps to nullable Arrow fields; omit values using
/// `..Default::default()`. Adding these fields is a breaking change for
/// existing `ColumnInfo` struct literals: callers must add
/// `..Default::default()` (or populate every new field).
#[derive(Clone, Debug, Default)]
pub struct ColumnInfo {
    pub column_name: String,
    /// One-based position in the original table schema, before column filtering.
    /// Use `None` when the position is unknown.
    pub ordinal_position: Option<i32>,
    pub remarks: Option<String>,
    pub xdbc_data_type: Option<i16>,
    pub xdbc_type_name: Option<String>,
    pub xdbc_column_size: Option<i32>,
    pub xdbc_decimal_digits: Option<i16>,
    pub xdbc_num_prec_radix: Option<i16>,
    pub xdbc_nullable: Option<i16>,
    pub xdbc_column_def: Option<String>,
    pub xdbc_sql_data_type: Option<i16>,
    pub xdbc_datetime_sub: Option<i16>,
    pub xdbc_char_octet_length: Option<i32>,
    pub xdbc_is_nullable: Option<String>,
    pub xdbc_scope_catalog: Option<String>,
    pub xdbc_scope_schema: Option<String>,
    pub xdbc_scope_table: Option<String>,
    pub xdbc_is_autoincrement: Option<bool>,
    pub xdbc_is_generatedcolumn: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct TableAndColumnInfo {
    pub table: TableInfo,
    pub columns: Vec<ColumnInfo>,
}

/// A referenced foreign-key column in a table constraint.
#[derive(Clone, Debug)]
pub struct ConstraintColumnUsage {
    pub fk_catalog: Option<String>,
    pub fk_db_schema: Option<String>,
    pub fk_table: String,
    pub fk_column_name: String,
}

/// Metadata for a table constraint returned by [`GetObjectsImpl::get_constraints`].
#[derive(Clone, Debug)]
pub struct ConstraintInfo {
    pub constraint_name: Option<String>,
    /// Required by the ADBC GetObjects schema (e.g. `PRIMARY KEY` or `FOREIGN KEY`).
    pub constraint_type: String,
    /// This list is always present and its names are never null.
    pub constraint_column_names: Vec<String>,
    /// Referenced columns, if applicable; `None` yields a null list,
    /// while `Some(vec![])` yields a non-null empty list.
    pub constraint_column_usage: Option<Vec<ConstraintColumnUsage>>,
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
    /// Return constraints for a table at column/all depth.
    /// By default, no constraints are reported, so existing implementations
    /// need not implement this method.
    fn get_constraints(
        &self,
        _catalog: &str,
        _db_schema: &str,
        _table: &str,
    ) -> Result<Vec<ConstraintInfo>, super::error::Error<E>> {
        Ok(Vec::new())
    }
}

/// Return the canonical ADBC GetObjects schema, including nested field nullability.
pub fn get_objects_schema() -> Schema {
    adbc_core::schemas::GET_OBJECTS_SCHEMA.as_ref().clone()
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
                        remarks.append_option(column.remarks.as_deref());
                    }
                    {
                        let xdbc_data_type = column_item
                            .field_builder::<builder::Int16Builder>(3)
                            .unwrap();
                        xdbc_data_type.append_option(column.xdbc_data_type);
                    }
                    {
                        let xdbc_type_name = column_item
                            .field_builder::<builder::StringBuilder>(4)
                            .unwrap();
                        xdbc_type_name.append_option(column.xdbc_type_name.as_deref());
                    }
                    {
                        let xdbc_column_size = column_item
                            .field_builder::<builder::Int32Builder>(5)
                            .unwrap();
                        xdbc_column_size.append_option(column.xdbc_column_size);
                    }
                    {
                        let xdbc_decimal_digits = column_item
                            .field_builder::<builder::Int16Builder>(6)
                            .unwrap();
                        xdbc_decimal_digits.append_option(column.xdbc_decimal_digits);
                    }
                    {
                        let xdbc_num_prec_radix = column_item
                            .field_builder::<builder::Int16Builder>(7)
                            .unwrap();
                        xdbc_num_prec_radix.append_option(column.xdbc_num_prec_radix);
                    }
                    {
                        let xdbc_nullable = column_item
                            .field_builder::<builder::Int16Builder>(8)
                            .unwrap();
                        xdbc_nullable.append_option(column.xdbc_nullable);
                    }
                    {
                        let xdbc_column_def = column_item
                            .field_builder::<builder::StringBuilder>(9)
                            .unwrap();
                        xdbc_column_def.append_option(column.xdbc_column_def.as_deref());
                    }
                    {
                        let xdbc_sql_data_type = column_item
                            .field_builder::<builder::Int16Builder>(10)
                            .unwrap();
                        xdbc_sql_data_type.append_option(column.xdbc_sql_data_type);
                    }
                    {
                        let xdbc_datetime_sub = column_item
                            .field_builder::<builder::Int16Builder>(11)
                            .unwrap();
                        xdbc_datetime_sub.append_option(column.xdbc_datetime_sub);
                    }
                    {
                        let xdbc_char_octet_length = column_item
                            .field_builder::<builder::Int32Builder>(12)
                            .unwrap();
                        xdbc_char_octet_length.append_option(column.xdbc_char_octet_length);
                    }
                    {
                        let xdbc_is_nullable = column_item
                            .field_builder::<builder::StringBuilder>(13)
                            .unwrap();
                        xdbc_is_nullable.append_option(column.xdbc_is_nullable.as_deref());
                    }
                    {
                        let xdbc_scope_catalog = column_item
                            .field_builder::<builder::StringBuilder>(14)
                            .unwrap();
                        xdbc_scope_catalog.append_option(column.xdbc_scope_catalog.as_deref());
                    }
                    {
                        let xdbc_scope_schema = column_item
                            .field_builder::<builder::StringBuilder>(15)
                            .unwrap();
                        xdbc_scope_schema.append_option(column.xdbc_scope_schema.as_deref());
                    }
                    {
                        let xdbc_scope_table = column_item
                            .field_builder::<builder::StringBuilder>(16)
                            .unwrap();
                        xdbc_scope_table.append_option(column.xdbc_scope_table.as_deref());
                    }
                    {
                        let xdbc_is_autoincrement = column_item
                            .field_builder::<builder::BooleanBuilder>(17)
                            .unwrap();
                        xdbc_is_autoincrement.append_option(column.xdbc_is_autoincrement);
                    }
                    {
                        let xdbc_is_generatedcolumn = column_item
                            .field_builder::<builder::BooleanBuilder>(18)
                            .unwrap();
                        xdbc_is_generatedcolumn.append_option(column.xdbc_is_generatedcolumn);
                    }
                    column_item.append(true);
                }

                table_columns.append(true);
            }
            {
                let constraints =
                    self.inner
                        .get_constraints(catalog, db_schema, &table.table.table_name)?;
                let table_constraints = table_item
                    .field_builder::<builder::ListBuilder<Box<dyn ArrayBuilder>>>(3)
                    .unwrap();
                Self::append_constraints(table_constraints, &constraints);
            }
            table_item.append(true);
        }

        Ok(())
    }

    fn append_constraints(
        table_constraints: &mut builder::ListBuilder<Box<dyn ArrayBuilder>>,
        constraints: &[ConstraintInfo],
    ) {
        for constraint in constraints {
            let constraint_item = table_constraints
                .values()
                .as_any_mut()
                .downcast_mut::<builder::StructBuilder>()
                .unwrap();
            constraint_item
                .field_builder::<builder::StringBuilder>(0)
                .unwrap()
                .append_option(constraint.constraint_name.as_deref());
            constraint_item
                .field_builder::<builder::StringBuilder>(1)
                .unwrap()
                .append_value(&constraint.constraint_type);
            {
                let names = constraint_item
                    .field_builder::<builder::ListBuilder<Box<dyn ArrayBuilder>>>(2)
                    .unwrap();
                let values = names
                    .values()
                    .as_any_mut()
                    .downcast_mut::<builder::StringBuilder>()
                    .unwrap();
                for name in &constraint.constraint_column_names {
                    values.append_value(name);
                }
                names.append(true);
            }
            {
                let usage = constraint_item
                    .field_builder::<builder::ListBuilder<Box<dyn ArrayBuilder>>>(3)
                    .unwrap();
                if let Some(columns) = &constraint.constraint_column_usage {
                    for column in columns {
                        let usage_item = usage
                            .values()
                            .as_any_mut()
                            .downcast_mut::<builder::StructBuilder>()
                            .unwrap();
                        usage_item
                            .field_builder::<builder::StringBuilder>(0)
                            .unwrap()
                            .append_option(column.fk_catalog.as_deref());
                        usage_item
                            .field_builder::<builder::StringBuilder>(1)
                            .unwrap()
                            .append_option(column.fk_db_schema.as_deref());
                        usage_item
                            .field_builder::<builder::StringBuilder>(2)
                            .unwrap()
                            .append_value(&column.fk_table);
                        usage_item
                            .field_builder::<builder::StringBuilder>(3)
                            .unwrap()
                            .append_value(&column.fk_column_name);
                        usage_item.append(true);
                    }
                    usage.append(true);
                } else {
                    usage.append_null();
                }
            }
            constraint_item.append(true);
        }
        table_constraints.append(true);
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
    use arrow_array::{
        Array, BooleanArray, Int16Array, Int32Array, ListArray, StringArray, StructArray,
    };

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
                ..Default::default()
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

    struct RichCatalog {
        constraint_calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl GetObjectsImpl<TestErrorHelper> for RichCatalog {
        fn get_catalogs(&self, filter: Option<&str>) -> TestResult<Vec<String>> {
            TestCatalog.get_catalogs(filter)
        }

        fn get_db_schemas(&self, catalog: &str, filter: Option<&str>) -> TestResult<Vec<String>> {
            TestCatalog.get_db_schemas(catalog, filter)
        }

        fn get_tables(
            &self,
            catalog: &str,
            db_schema: &str,
            table_filter: Option<&str>,
            table_type_filter: Option<&[String]>,
        ) -> TestResult<Vec<TableInfo>> {
            TestCatalog.get_tables(catalog, db_schema, table_filter, table_type_filter)
        }

        fn get_columns(
            &self,
            catalog: &str,
            db_schema: &str,
            table_filter: Option<&str>,
            table_type_filter: Option<&[String]>,
            _column_filter: Option<&str>,
        ) -> TestResult<Vec<TableAndColumnInfo>> {
            let filled = ColumnInfo {
                column_name: "filled".into(),
                ordinal_position: Some(1),
                remarks: Some("a comment".into()),
                xdbc_data_type: Some(4),
                xdbc_type_name: Some("INTEGER".into()),
                xdbc_column_size: Some(10),
                xdbc_decimal_digits: Some(2),
                xdbc_num_prec_radix: Some(10),
                xdbc_nullable: Some(0),
                xdbc_column_def: Some("7".into()),
                xdbc_sql_data_type: Some(4),
                xdbc_datetime_sub: Some(3),
                xdbc_char_octet_length: Some(20),
                xdbc_is_nullable: Some("NO".into()),
                xdbc_scope_catalog: Some("scope_catalog".into()),
                xdbc_scope_schema: Some("scope_schema".into()),
                xdbc_scope_table: Some("scope_table".into()),
                xdbc_is_autoincrement: Some(false),
                xdbc_is_generatedcolumn: Some(true),
            };
            Ok(vec![TableAndColumnInfo {
                table: self
                    .get_tables(catalog, db_schema, table_filter, table_type_filter)?
                    .remove(0),
                columns: vec![
                    filled,
                    ColumnInfo {
                        column_name: "sparse".into(),
                        ..Default::default()
                    },
                ],
            }])
        }

        fn get_constraints(
            &self,
            catalog: &str,
            db_schema: &str,
            table: &str,
        ) -> TestResult<Vec<ConstraintInfo>> {
            assert_eq!((catalog, db_schema, table), ("catalog", "schema", "table"));
            self.constraint_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(vec![
                ConstraintInfo {
                    constraint_name: Some("pk_table".into()),
                    constraint_type: "PRIMARY KEY".into(),
                    constraint_column_names: vec!["filled".into()],
                    constraint_column_usage: None,
                },
                ConstraintInfo {
                    constraint_name: Some("fk_table".into()),
                    constraint_type: "FOREIGN KEY".into(),
                    constraint_column_names: vec!["filled".into(), "sparse".into()],
                    constraint_column_usage: Some(vec![
                        ConstraintColumnUsage {
                            fk_catalog: Some("ref_catalog".into()),
                            fk_db_schema: Some("ref_schema".into()),
                            fk_table: "parent".into(),
                            fk_column_name: "id".into(),
                        },
                        ConstraintColumnUsage {
                            fk_catalog: None,
                            fk_db_schema: None,
                            fk_table: "parent".into(),
                            fk_column_name: "other_id".into(),
                        },
                    ]),
                },
                ConstraintInfo {
                    constraint_name: None,
                    constraint_type: "CHECK".into(),
                    constraint_column_names: Vec::new(),
                    constraint_column_usage: Some(Vec::new()),
                },
            ])
        }
    }

    fn read_tables<I: GetObjectsImpl<TestErrorHelper>>(
        inner: I,
        depth: ObjectDepth,
        column_filter: Option<&str>,
    ) -> StructArray {
        let batches = get_objects(inner, depth, None, None, None, None, column_filter)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema().as_ref(), &get_objects_schema());
        let schemas = batches[0]
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
        tables
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone()
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

    #[test]
    fn get_objects_schema_matches_canonical_adbc_schema() {
        assert_eq!(
            &get_objects_schema(),
            adbc_core::schemas::GET_OBJECTS_SCHEMA.as_ref()
        );
    }

    #[test]
    fn emits_column_metadata_and_table_constraints_at_column_and_all_depth() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let constraint_calls = Arc::new(AtomicUsize::new(0));
        let tables = read_tables(
            RichCatalog {
                constraint_calls: constraint_calls.clone(),
            },
            ObjectDepth::Tables,
            None,
        );
        assert_eq!(constraint_calls.load(Ordering::SeqCst), 0);
        assert_eq!(tables.len(), 1);
        for index in [2, 3] {
            let list = tables
                .column(index)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert!(list.is_null(0));
        }

        let tables = read_tables(
            RichCatalog {
                constraint_calls: constraint_calls.clone(),
            },
            ObjectDepth::Columns,
            None,
        );
        assert_eq!(constraint_calls.load(Ordering::SeqCst), 1);
        let columns = tables
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(!columns.is_null(0));
        let columns = columns.value(0);
        let columns = columns.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns.null_count(), 0);
        let column_names = columns
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            column_names.iter().collect::<Vec<_>>(),
            vec![Some("filled"), Some("sparse")]
        );

        macro_rules! assert_column {
            ($index:expr, $array:ty, $value:expr) => {{
                let field = columns
                    .column($index)
                    .as_any()
                    .downcast_ref::<$array>()
                    .unwrap();
                assert_eq!(field.value(0), $value);
                assert!(field.is_null(1), "column field {} should be null", $index);
            }};
        }
        assert_column!(1, Int32Array, 1);
        assert_column!(2, StringArray, "a comment");
        assert_column!(3, Int16Array, 4);
        assert_column!(4, StringArray, "INTEGER");
        assert_column!(5, Int32Array, 10);
        assert_column!(6, Int16Array, 2);
        assert_column!(7, Int16Array, 10);
        assert_column!(8, Int16Array, 0);
        assert_column!(9, StringArray, "7");
        assert_column!(10, Int16Array, 4);
        assert_column!(11, Int16Array, 3);
        assert_column!(12, Int32Array, 20);
        assert_column!(13, StringArray, "NO");
        assert_column!(14, StringArray, "scope_catalog");
        assert_column!(15, StringArray, "scope_schema");
        assert_column!(16, StringArray, "scope_table");
        assert_column!(17, BooleanArray, false);
        assert_column!(18, BooleanArray, true);

        let constraints = tables
            .column(3)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(!constraints.is_null(0));
        let constraints = constraints.value(0);
        let constraints = constraints.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(constraints.len(), 3);
        assert_eq!(constraints.null_count(), 0);
        let names = constraints
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            names.iter().collect::<Vec<_>>(),
            vec![Some("pk_table"), Some("fk_table"), None]
        );
        let types = constraints
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            types.iter().collect::<Vec<_>>(),
            vec![Some("PRIMARY KEY"), Some("FOREIGN KEY"), Some("CHECK")]
        );
        assert_eq!(types.null_count(), 0);
        let constraint_columns = constraints
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(constraint_columns.null_count(), 0);
        for (index, expected) in [
            (0, vec![Some("filled")]),
            (1, vec![Some("filled"), Some("sparse")]),
            (2, vec![]),
        ] {
            let value = constraint_columns.value(index);
            let value = value.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(value.iter().collect::<Vec<_>>(), expected);
            assert_eq!(value.null_count(), 0);
        }

        let usage = constraints
            .column(3)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(usage.is_null(0));
        assert!(!usage.is_null(1));
        assert!(!usage.is_null(2));
        assert_eq!(usage.value(2).len(), 0);
        let usage_columns = usage.value(1);
        let usage_columns = usage_columns
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(usage_columns.len(), 2);
        assert_eq!(usage_columns.null_count(), 0);
        for (index, expected) in [
            (0, vec![Some("ref_catalog"), None]),
            (1, vec![Some("ref_schema"), None]),
            (2, vec![Some("parent"), Some("parent")]),
            (3, vec![Some("id"), Some("other_id")]),
        ] {
            let value = usage_columns
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(value.iter().collect::<Vec<_>>(), expected);
        }

        let all_tables = read_tables(
            RichCatalog {
                constraint_calls: constraint_calls.clone(),
            },
            ObjectDepth::All,
            None,
        );
        assert_eq!(constraint_calls.load(Ordering::SeqCst), 2);
        for index in [2, 3] {
            let list = all_tables
                .column(index)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert!(!list.is_null(0));
        }
    }

    #[test]
    fn absent_metadata_stays_null_but_default_constraints_are_an_empty_list() {
        let tables = read_tables(TestCatalog, ObjectDepth::All, None);
        let columns = tables
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(!columns.is_null(0));
        let columns = columns.value(0);
        let columns = columns.as_any().downcast_ref::<StructArray>().unwrap();
        for index in 2..columns.num_columns() {
            assert_eq!(columns.column(index).null_count(), columns.len());
        }
        let constraints = tables
            .column(3)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(!constraints.is_null(0));
        assert_eq!(constraints.value(0).len(), 0);

        let filtered_tables = read_tables(TestCatalog, ObjectDepth::Columns, Some("missing"));
        for index in [2, 3] {
            let list = filtered_tables
                .column(index)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert!(!list.is_null(0));
            assert_eq!(list.value(0).len(), 0);
        }
    }
}
