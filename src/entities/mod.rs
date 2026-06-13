pub mod mail;

use crate::FilterValue;
use std::collections::HashMap;
use unicase::UniCase;

/// A case-insensitive map of metadata properties which filter expressions
/// are evaluated against. Values are stored (and returned) owned, so a
/// [`Metadata`] can be built once and queried freely.
#[derive(Default, Clone, Debug)]
pub struct Metadata(HashMap<UniCase<&'static str>, FilterValue<'static>>);

impl Metadata {
    pub fn insert<V: Into<FilterValue<'static>>>(&mut self, key: &'static str, value: V) {
        self.0.insert(UniCase::new(key), value.into());
    }

    pub fn get(&self, key: &str) -> FilterValue<'static> {
        self.0
            .get(&UniCase::new(key))
            .cloned()
            .unwrap_or(FilterValue::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_each_value_kind() {
        let mut meta = Metadata::default();
        meta.insert("flag", true);
        meta.insert("count", 3u32);
        meta.insert("name", "Alice".to_string());
        meta.insert("nothing", FilterValue::Null);
        meta.insert(
            "tags",
            vec![FilterValue::from("red".to_string()), FilterValue::from(7u8)],
        );

        assert_eq!(meta.get("flag"), FilterValue::Bool(true));
        assert_eq!(meta.get("count"), FilterValue::Number(3.0));
        assert_eq!(meta.get("name"), FilterValue::from("Alice"));
        assert_eq!(meta.get("nothing"), FilterValue::Null);
        assert_eq!(
            meta.get("tags"),
            FilterValue::Tuple(vec![FilterValue::from("red"), FilterValue::from(7u8)])
        );
    }

    #[test]
    fn get_is_case_insensitive_and_misses_are_null() {
        let mut meta = Metadata::default();
        meta.insert("Subject", "Quarterly invoice".to_string());

        assert_eq!(meta.get("subject"), FilterValue::from("Quarterly invoice"));
        assert_eq!(meta.get("SUBJECT"), FilterValue::from("Quarterly invoice"));
        assert_eq!(meta.get("missing"), FilterValue::Null);
    }
}
