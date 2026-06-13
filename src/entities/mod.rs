pub mod mail;

use crate::FilterValue;
use std::borrow::Cow;
use std::collections::HashMap;
use unicase::UniCase;

/// A case-insensitive map of metadata properties which filter expressions
/// are evaluated against.
///
/// Stored values own their data (`FilterValue<'static>`), so that a single
/// [`Metadata`] can be built once and queried repeatedly. [`Metadata::get`]
/// hands back a *borrowed* view of the stored value, which lets filter
/// evaluation avoid cloning the underlying strings.
#[derive(Default, Clone, Debug)]
pub struct Metadata(HashMap<UniCase<&'static str>, FilterValue<'static>>);

impl Metadata {
    pub fn insert<V: Into<FilterValue<'static>>>(&mut self, key: &'static str, value: V) {
        self.0.insert(UniCase::new(key), value.into());
    }

    pub fn get(&self, key: &str) -> FilterValue<'_> {
        // A hashed `get(&UniCase::new(key))` would tie the borrowed result to
        // the (short-lived) `key` rather than to `&self`, since the lookup
        // shrinks the map's `&'static str` keys to the query's lifetime. A
        // linear scan over the handful of metadata entries keeps the returned
        // borrow bound to `self`, which is what zero-alloc evaluation needs.
        let needle = UniCase::new(key);
        for (stored, value) in &self.0 {
            if *stored == needle {
                return borrow_value(value);
            }
        }
        FilterValue::Null
    }
}

/// Produces a borrowed view of a stored [`FilterValue`], reusing the owner's
/// string allocations rather than cloning them. Scalars are copied (they are
/// cheap), and tuples are rebuilt around borrowed elements.
fn borrow_value<'a>(value: &'a FilterValue<'static>) -> FilterValue<'a> {
    match value {
        FilterValue::Null => FilterValue::Null,
        FilterValue::Bool(b) => FilterValue::Bool(*b),
        FilterValue::Number(n) => FilterValue::Number(*n),
        FilterValue::String(s) => FilterValue::String(Cow::Borrowed(s.as_ref())),
        FilterValue::Tuple(items) => FilterValue::Tuple(items.iter().map(borrow_value).collect()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_borrows_each_value_kind() {
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
