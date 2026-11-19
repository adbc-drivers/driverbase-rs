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

// Helpers for filters in implementing metadata collections.

use arrow_array::Array;

use crate::error::ErrorHelper;

use super::Result;

/// A filter helper for a string field of a metadata collection.
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

/// A dynamic collection of provided filters.
pub type UntypedFilterSet = std::collections::HashMap<String, Filter>;

/// An iterator over a collection of untyped filter sets.
pub type FilterSetIterator<E> = dyn Iterator<Item = Result<UntypedFilterSet, E>>;

/// Iterate over filters provided by the user as Arrow data, parsing and
/// converting them to dynamic filter sets.
///
/// If the user did not provide filters, this will return an iterator of a
/// single empty filter set. If the user provided an empty record
/// batch/stream, this will return an empty iterator.
pub fn untyped_filter_sets<E: ErrorHelper>(
    filters: Option<Box<dyn arrow_array::RecordBatchReader + Send>>,
) -> Result<Box<FilterSetIterator<E>>, E> {
    if let Some(filters) = filters {
        let filter_iterator = ArrowFilterSetIterator::<E> {
            filters,
            current: None,
            _marker: std::marker::PhantomData,
        };
        Ok(Box::new(filter_iterator))
    } else {
        Ok(Box::new(EmptyFilterSetIterator::<E>::new()))
    }
}

struct EmptyFilterSetIterator<E> {
    consumed: bool,
    _marker: std::marker::PhantomData<E>,
}

impl<E> EmptyFilterSetIterator<E>
where
    E: ErrorHelper,
{
    fn new() -> Self {
        Self {
            consumed: false,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E> Iterator for EmptyFilterSetIterator<E>
where
    E: ErrorHelper,
{
    type Item = Result<UntypedFilterSet, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.consumed {
            None
        } else {
            self.consumed = true;
            Some(Ok(Default::default()))
        }
    }
}

// TODO: validate the filter schema up front and bind positionally?
struct ArrowFilterSetIterator<E> {
    filters: Box<dyn arrow_array::RecordBatchReader + Send>,
    current: Option<(arrow_array::RecordBatch, usize)>,
    _marker: std::marker::PhantomData<E>,
}

impl<E> Iterator for ArrowFilterSetIterator<E>
where
    E: ErrorHelper,
{
    type Item = Result<UntypedFilterSet, E>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((ref batch, index)) = self.current {
                if index >= batch.num_rows() {
                    self.current = None;
                } else {
                    break;
                }
            }
            if self.current.is_none() {
                match self.filters.next() {
                    Some(Ok(batch)) => {
                        self.current = Some((batch, 0));
                    }
                    Some(Err(e)) => return Some(Err(ErrorHelper::from_arrow(e))),
                    None => return None,
                }
            }
        }
        if let Some((batch, index)) = self.current.take() {
            let mut filters = UntypedFilterSet::default();
            for (i, field) in batch.schema().fields().iter().enumerate() {
                let column = batch.column(i);
                if column.is_null(index) {
                    continue;
                }
                let (name, exact) =
                    if let Some(name) = field.name().strip_prefix("adbc.metadata.filter.") {
                        (name, true)
                    } else if let Some(name) =
                        field.name().strip_prefix("adbc.metadata.filter_pattern.")
                    {
                        (name, false)
                    } else {
                        return Some(Err(ErrorHelper::invalid_argument()
                            .format(format_args!("unsupported filter field {}", field.name()))));
                    };

                let maybe_filter = if let Some(array) =
                    column.as_any().downcast_ref::<arrow_array::StringArray>()
                {
                    if !array.is_null(index) {
                        let value = array.value(index);
                        if exact {
                            Some(Filter::new_exact(value.to_string()))
                        } else {
                            match Filter::new_pattern(value.to_string()) {
                                Ok(filter) => Some(filter),
                                Err(e) => {
                                    return Some(Err(ErrorHelper::invalid_argument().format(
                                        format_args!("invalid pattern for filter {}: {}", name, e),
                                    )));
                                }
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    return Some(Err(ErrorHelper::invalid_argument().format(format_args!(
                        "unsupported filter type {:?} for field {}",
                        field.data_type(),
                        field.name()
                    ))));
                };

                if let Some(filter) = maybe_filter {
                    // TODO: error if filter already provided
                    filters.insert(name.to_string(), filter);
                }
            }
            self.current = Some((batch, index.saturating_add(1)));
            Some(Ok(filters))
        } else {
            // Should not happen
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
