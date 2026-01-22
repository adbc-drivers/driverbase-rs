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

#![forbid(clippy::allow_attributes_without_reason)]

use std::sync::Arc;

pub mod bulk_ingest;
pub mod error;
pub mod get_objects;
pub mod record_reader;
pub mod util;

/// A helper to build the result for GetInfo.
pub struct InfoBuilder {
    info_name: arrow_array::builder::UInt32Builder,
    type_id: arrow_array::builder::Int8BufferBuilder,
    offset: arrow_array::builder::Int32BufferBuilder,
    string_value: arrow_array::builder::StringBuilder,
    bool_value: arrow_array::builder::BooleanBuilder,
    int64_value: arrow_array::builder::Int64Builder,
    int32_bitmask: arrow_array::builder::Int32Builder,
    string_list: arrow_array::builder::ListBuilder<arrow_array::builder::StringBuilder>,
    int32_to_int32_list_map: arrow_array::builder::MapBuilder<
        arrow_array::builder::Int32Builder,
        arrow_array::builder::ListBuilder<arrow_array::builder::Int32Builder>,
    >,
}

impl Default for InfoBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl InfoBuilder {
    const CODE_STRING: i8 = 0;
    const CODE_BOOL: i8 = 1;
    const CODE_INT64: i8 = 2;
    const CODE_INT32_BITMASK: i8 = 3;
    const CODE_STRING_LIST: i8 = 4;
    const CODE_INT32_TO_INT32_LIST_MAP: i8 = 5;

    pub fn new() -> Self {
        // XXX: the union builder in arrow-rs doesn't let you specify static type codes
        InfoBuilder {
            info_name: arrow_array::builder::UInt32Builder::new(),
            type_id: arrow_array::builder::Int8BufferBuilder::new(16),
            offset: arrow_array::builder::Int32BufferBuilder::new(16),
            string_value: arrow_array::builder::StringBuilder::new(),
            bool_value: arrow_array::builder::BooleanBuilder::new(),
            int64_value: arrow_array::builder::Int64Builder::new(),
            int32_bitmask: arrow_array::builder::Int32Builder::new(),
            string_list: arrow_array::builder::ListBuilder::new(
                arrow_array::builder::StringBuilder::new(),
            ),
            int32_to_int32_list_map: arrow_array::builder::MapBuilder::new(
                None,
                arrow_array::builder::Int32Builder::new(),
                arrow_array::builder::ListBuilder::new(arrow_array::builder::Int32Builder::new()),
            ),
        }
    }

    /// Add a string info value with the given key.
    pub fn add_string(&mut self, name: u32, value: impl AsRef<str>) {
        self.info_name.append_value(name);
        self.type_id.append(Self::CODE_STRING);
        self.offset
            .append(arrow_array::builder::ArrayBuilder::len(&self.string_value) as i32);
        self.string_value.append_value(value);
    }

    /// Finish building and get the result as an [arrow_array::RecordBatchReader].
    pub fn build(mut self) -> Box<dyn arrow_array::RecordBatchReader + Send> {
        let info_name = self.info_name.finish();
        // XXX: this is what arrow-rs's union builder does; but this copies. Why can't we move from the builder?
        let type_id = arrow_buffer::ScalarBuffer::from(self.type_id.as_slice().to_vec());
        let offset = arrow_buffer::ScalarBuffer::from(self.offset.as_slice().to_vec());
        let children: Vec<arrow_array::ArrayRef> = vec![
            Arc::new(self.string_value.finish()),
            Arc::new(self.bool_value.finish()),
            Arc::new(self.int64_value.finish()),
            Arc::new(self.int32_bitmask.finish()),
            Arc::new(self.string_list.finish()),
            Arc::new(self.int32_to_int32_list_map.finish()),
        ];

        let string_field =
            arrow_schema::Field::new("string_value", arrow_schema::DataType::Utf8, true);
        let bool_field =
            arrow_schema::Field::new("bool_value", arrow_schema::DataType::Boolean, true);
        let int64_field =
            arrow_schema::Field::new("int64_value", arrow_schema::DataType::Int64, true);
        let int32_bitmask_field =
            arrow_schema::Field::new("int32_bitmask", arrow_schema::DataType::Int32, true);
        let string_list_field = arrow_schema::Field::new(
            "string_list",
            arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Utf8,
                true,
            ))),
            true,
        );
        let int32_to_int32_list_map_field = arrow_schema::Field::new(
            "int32_to_int32_list_map",
            arrow_schema::DataType::Map(
                Arc::new(arrow_schema::Field::new(
                    "entries",
                    arrow_schema::DataType::Struct(arrow_schema::Fields::from(vec![
                        arrow_schema::Field::new("key", arrow_schema::DataType::Int32, false),
                        arrow_schema::Field::new(
                            "value",
                            arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new(
                                "item",
                                arrow_schema::DataType::Int32,
                                true,
                            ))),
                            true,
                        ),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        );

        let union_fields = arrow_schema::UnionFields::try_new(
            vec![
                Self::CODE_STRING,
                Self::CODE_BOOL,
                Self::CODE_INT64,
                Self::CODE_INT32_BITMASK,
                Self::CODE_STRING_LIST,
                Self::CODE_INT32_TO_INT32_LIST_MAP,
            ],
            vec![
                string_field,
                bool_field,
                int64_field,
                int32_bitmask_field,
                string_list_field,
                int32_to_int32_list_map_field,
            ],
        )
        .expect("failed to create union fields for InfoBuilder");

        let info_value = unsafe {
            arrow_array::UnionArray::new_unchecked(
                union_fields.clone(),
                type_id,
                Some(offset),
                children,
            )
        };

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("info_name", arrow_schema::DataType::UInt32, false),
            arrow_schema::Field::new(
                "info_value",
                arrow_schema::DataType::Union(union_fields, arrow_schema::UnionMode::Dense),
                false,
            ),
        ]));

        let batch = unsafe {
            arrow_array::RecordBatch::new_unchecked(
                schema.clone(),
                vec![Arc::new(info_name), Arc::new(info_value)],
                self.type_id.len(),
            )
        };

        Box::new(arrow_array::RecordBatchIterator::new(
            vec![batch].into_iter().map(Ok),
            schema,
        ))
    }
}
