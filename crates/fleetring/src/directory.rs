//! Directories: explicit rules that send keys to cells (ADR-0012).

use std::collections::BTreeMap;
use std::fmt;

use crate::CellId;

/// Explicit rules that send keys to cells: exact keys and key prefixes.
///
/// A directory is the rule half of a [`Cells`](crate::Cells) layer's selector, and it runs before
/// the selector ring. Use it for residency (every `eu:` key stays in the EU cell), for pinning
/// (one large tenant on a cell of its own), and for migrations in flight (a key stays in its old
/// cell until its move completes).
///
/// [`lookup`](Self::lookup) applies the rules in a fixed order:
///
/// 1. An exact rule for the key wins over every prefix rule.
/// 2. Otherwise, the prefix rule with the longest prefix of the key wins.
/// 3. Otherwise, no rule matches.
///
/// The empty prefix is a prefix of every key, so a rule for it, the *catch-all*, matches every
/// key that no other rule matches. Keys and prefixes are bytes, compared exactly as given: no
/// normalization or case folding, so `EU:acme` doesn't match the prefix `eu:`.
///
/// A directory keeps its rules sorted by bytes, so it depends only on its set of rules, never on
/// the order they were added in. A key or prefix has at most one rule of each kind: adding a
/// second rule for it replaces the first.
///
/// With the `serde` feature, a directory serializes as an object with two lists of rules,
/// `exact` and `prefix`, each sorted by bytes. An exact rule is an object with the fields `key`
/// and `cell`, and a prefix rule one with the fields `prefix` and `cell`; the catch-all is the
/// prefix rule whose prefix is `""`. Keys and prefixes are bytes, so each is written as a string
/// when its bytes are valid UTF-8, and otherwise as an object that holds them as lowercase hex
/// digits, two per byte: `{"hex": "ff00"}`. Deserializing reads either form for any rule. It
/// rejects malformed hex, unknown fields, and two rules of the same kind for the same bytes, in
/// whichever forms they are written: a directory carries no fingerprint of its own that would
/// catch a rule dropped or replaced on the way.
///
/// # Examples
///
/// ```
/// use fleetring::Directory;
///
/// let directory = Directory::new()
///     .prefix("eu:", "eu".parse()?)
///     .prefix("eu:vip:", "eu-vip".parse()?)
///     .exact("eu:vip:legacy", "eu".parse()?)
///     .catch_all("us".parse()?);
///
/// let cell = |key: &str| directory.lookup(key).map(|cell| cell.as_str());
/// assert_eq!(cell("eu:tenant:acme"), Some("eu"));
/// // The longest matching prefix wins.
/// assert_eq!(cell("eu:vip:globex"), Some("eu-vip"));
/// // An exact rule wins over every prefix.
/// assert_eq!(cell("eu:vip:legacy"), Some("eu"));
/// // Every other key falls through to the catch-all, including keys that differ only in case.
/// assert_eq!(cell("EU:tenant:acme"), Some("us"));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// With the `serde` feature, a directory round-trips through JSON, with non-UTF-8 bytes as hex:
///
/// ```
/// # use fleetring::Directory;
/// # #[cfg(feature = "serde")]
/// # {
/// let directory = Directory::new()
///     .exact([0xff, 0x00], "us".parse()?)
///     .prefix("eu:", "eu".parse()?)
///     .catch_all("us".parse()?);
/// let json = serde_json::to_string(&directory)?;
/// assert_eq!(
///     json,
///     concat!(
///         r#"{"exact":[{"key":{"hex":"ff00"},"cell":"us"}],"#,
///         r#""prefix":[{"prefix":"","cell":"us"},{"prefix":"eu:","cell":"eu"}]}"#,
///     )
/// );
/// let copy: Directory = serde_json::from_str(&json)?;
/// assert_eq!(copy, directory);
/// # }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Directory {
    exact: BTreeMap<Box<[u8]>, CellId>,
    prefixes: BTreeMap<Box<[u8]>, CellId>,
    /// The distinct lengths of the prefixes, longest first, so a lookup only tries the lengths
    /// that some rule has.
    prefix_lengths: Vec<usize>,
}

impl Directory {
    /// Creates a directory with no rules.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a rule that sends exactly `key` to `cell`, replacing any exact rule for `key`.
    ///
    /// An exact rule wins over every prefix rule, so it can pin one key inside a prefix that
    /// another rule sends elsewhere.
    #[must_use]
    pub fn exact(mut self, key: impl AsRef<[u8]>, cell: CellId) -> Self {
        self.exact.insert(key.as_ref().into(), cell);
        self
    }

    /// Adds a rule that sends every key starting with `prefix` to `cell`, replacing any prefix
    /// rule for `prefix`.
    ///
    /// When several prefix rules match a key, the longest prefix wins. The empty prefix matches
    /// every key; [`catch_all`](Self::catch_all) adds that rule.
    #[must_use]
    pub fn prefix(mut self, prefix: impl AsRef<[u8]>, cell: CellId) -> Self {
        let prefix = prefix.as_ref();
        // `prefix_lengths` is sorted longest first, so the search compares in reverse.
        let len = prefix.len();
        if let Err(position) = self.prefix_lengths.binary_search_by(|probe| len.cmp(probe)) {
            self.prefix_lengths.insert(position, len);
        }
        self.prefixes.insert(prefix.into(), cell);
        self
    }

    /// Adds the catch-all rule, which sends every key that no other rule matches to `cell`.
    ///
    /// This is the prefix rule for the empty prefix, so it replaces any earlier catch-all. A
    /// [`Cells`](crate::Cells) layer without a selector ring needs one, so that every key reaches
    /// a cell.
    #[must_use]
    pub fn catch_all(self, cell: CellId) -> Self {
        self.prefix(b"", cell)
    }

    /// Returns the cell the rules send `key` to, or `None` if no rule matches it.
    ///
    /// An exact rule for `key` wins; otherwise the longest matching prefix does. Takes
    /// O(p log n) time for n rules with p distinct prefix lengths.
    ///
    /// # Examples
    ///
    /// ```
    /// use fleetring::Directory;
    ///
    /// let directory = Directory::new().prefix("eu:", "eu".parse()?);
    /// assert_eq!(directory.lookup("eu:tenant:acme").map(|cell| cell.as_str()), Some("eu"));
    /// assert_eq!(directory.lookup("us:tenant:acme"), None);
    /// // Keys are bytes, so non-UTF-8 keys work too.
    /// assert_eq!(directory.lookup([0xff, 0xfe]), None);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn lookup(&self, key: impl AsRef<[u8]>) -> Option<&CellId> {
        let key = key.as_ref();
        if let Some(cell) = self.exact.get(key) {
            return Some(cell);
        }
        self.prefix_lengths
            .iter()
            .filter(|&&len| len <= key.len())
            .find_map(|&len| self.prefixes.get(&key[..len]))
    }

    /// Returns the exact rules as `(key, cell)` pairs, sorted by key, comparing bytes.
    pub fn exact_rules(&self) -> impl ExactSizeIterator<Item = (&[u8], &CellId)> {
        self.exact.iter().map(|(key, cell)| (&key[..], cell))
    }

    /// Returns the prefix rules as `(prefix, cell)` pairs, sorted by prefix, comparing bytes.
    ///
    /// The catch-all, if there is one, comes first: its prefix is empty.
    pub fn prefix_rules(&self) -> impl ExactSizeIterator<Item = (&[u8], &CellId)> {
        self.prefixes
            .iter()
            .map(|(prefix, cell)| (&prefix[..], cell))
    }

    /// Returns the number of rules, exact and prefix.
    #[must_use]
    pub fn len(&self) -> usize {
        self.exact.len() + self.prefixes.len()
    }

    /// Returns whether the directory has no rules.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.prefixes.is_empty()
    }

    /// Returns whether the directory has a catch-all rule, so that every key matches a rule.
    pub(crate) fn has_catch_all(&self) -> bool {
        self.prefixes.contains_key(&[][..])
    }

    /// Returns the cell of every rule: exact rules by key, then prefix rules by prefix.
    pub(crate) fn targets(&self) -> impl Iterator<Item = &CellId> {
        self.exact.values().chain(self.prefixes.values())
    }
}

impl fmt::Debug for Directory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Directory")
            .field("exact", &Rules(&self.exact))
            .field("prefix", &Rules(&self.prefixes))
            .finish()
    }
}

/// Debug-formats rules as a map from key or prefix to cell ID.
struct Rules<'a>(&'a BTreeMap<Box<[u8]>, CellId>);

impl fmt::Debug for Rules<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(
                self.0
                    .iter()
                    .map(|(bytes, cell)| (Bytes(bytes), cell.as_str())),
            )
            .finish()
    }
}

/// Debug-formats a key or prefix as a quoted string, escaping bytes that aren't printable ASCII.
struct Bytes<'a>(&'a [u8]);

impl fmt::Debug for Bytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\"", self.0.escape_ascii())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(id: &str) -> CellId {
        id.parse().expect("test cell ids are valid")
    }

    #[test]
    fn prefix_lengths_stay_distinct_and_longest_first() {
        let directory = Directory::new()
            .prefix("ab", cell("x"))
            .prefix("a", cell("x"))
            .prefix("abcd", cell("x"))
            .prefix("cd", cell("y"))
            .catch_all(cell("z"))
            .prefix("ab", cell("y"));
        assert_eq!(directory.prefix_lengths, [4, 2, 1, 0]);
        assert_eq!(directory.len(), 5);
    }

    #[test]
    fn the_longest_matching_prefix_wins_even_when_shorter_ones_are_added_later() {
        let directory = Directory::new()
            .prefix("tenant:big:", cell("big"))
            .prefix("tenant:", cell("small"));
        assert_eq!(directory.lookup("tenant:big:acme"), Some(&cell("big")));
        assert_eq!(directory.lookup("tenant:bi"), Some(&cell("small")));
        assert_eq!(directory.lookup("tenan"), None);
    }

    #[test]
    fn equality_ignores_insertion_order() {
        let forward = Directory::new()
            .exact("k", cell("a"))
            .prefix("p", cell("b"))
            .prefix("pq", cell("c"));
        let backward = Directory::new()
            .prefix("pq", cell("c"))
            .prefix("p", cell("b"))
            .exact("k", cell("a"));
        assert_eq!(forward, backward);
    }

    #[test]
    fn debug_shows_rules_as_escaped_strings() {
        let directory = Directory::new()
            .exact([0xff, b'a'], cell("x"))
            .catch_all(cell("y"));
        assert_eq!(
            format!("{directory:?}"),
            r#"Directory { exact: {"\xffa": "x"}, prefix: {"": "y"} }"#
        );
    }
}
