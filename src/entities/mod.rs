pub mod mail;

use crate::FilterValue;
use std::collections::HashMap;
use unicase::UniCase;

/// A case-insensitive map of metadata properties which filter expressions
/// are evaluated against.
#[derive(Default, Clone, Debug)]
pub struct Metadata(HashMap<UniCase<&'static str>, FilterValue>);

impl Metadata {
    pub fn insert<V: Into<FilterValue>>(&mut self, key: &'static str, value: V) {
        self.0.insert(UniCase::new(key), value.into());
    }

    pub fn get(&self, key: &str) -> FilterValue {
        self.0
            .get(&UniCase::new(key))
            .cloned()
            .unwrap_or(FilterValue::Null)
    }
}
