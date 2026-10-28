//! Deterministic key corpora.
//!
//! Each function lazily generates the first `n` keys of a fixed sequence, so a corpus is a prefix
//! of every larger one and holds only one key in memory at a time. The keys are byte-identical on
//! every platform: the checksum test below pins them, and CI runs it on s390x, i686, and wasm.

use std::fmt;
use std::iter;

use super::prng::SplitMix64;

/// Keys per corpus for quick checks, such as the checksum test.
pub(crate) const SMALL: usize = 10_000;

/// Keys per corpus for statistical tests in default `cargo test` runs.
pub(crate) const MEDIUM: usize = 100_000;

/// Keys per corpus for `#[ignore]`d statistical tests, which the release-mode CI job runs.
pub(crate) const LARGE: usize = 1_000_000;

/// The seed of [`agents`]. Seeds are arbitrary, but changing one changes its corpus.
const AGENTS_SEED: u64 = 1;

/// The seed of [`uuids`].
const UUIDS_SEED: u64 = 2;

/// The seed of [`binary`].
const BINARY_SEED: u64 = 3;

/// The first word of an agent name. Both word lists have 16 entries, a power of two, so every
/// word is equally likely.
const ADJECTIVES: [&str; 16] = [
    "amber", "brisk", "calm", "dusty", "eager", "fuzzy", "gentle", "hollow", "icy", "jolly",
    "keen", "lucky", "mellow", "nimble", "quiet", "rusty",
];

/// The second word of an agent name.
const NOUNS: [&str; 16] = [
    "badger", "comet", "falcon", "gecko", "heron", "ibis", "jaguar", "koala", "lynx", "marmot",
    "newt", "otter", "panda", "quokka", "raven", "walrus",
];

/// The longest binary key, in bytes.
const MAX_BINARY_LEN: usize = 256;

/// The version field of a UUID's first 64 bits.
const UUID_VERSION_MASK: u64 = 0xf000;

/// The version field's value in a random UUID (RFC 9562, version 4).
const UUID_VERSION_4: u64 = 0x4000;

/// The variant field of a UUID's last 64 bits.
const UUID_VARIANT_MASK: u64 = 0xc000_0000_0000_0000;

/// The variant field's value in an RFC 9562 UUID (`0b10`).
const UUID_VARIANT_RFC: u64 = 0x8000_0000_0000_0000;

/// A key corpus, for tests that run over every corpus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Corpus {
    /// See [`sequential`].
    Sequential,
    /// See [`agents`].
    Agents,
    /// See [`uuids`].
    Uuids,
    /// See [`binary`].
    Binary,
}

impl Corpus {
    /// Every corpus.
    pub(crate) const ALL: [Self; 4] = [Self::Sequential, Self::Agents, Self::Uuids, Self::Binary];

    /// Returns the corpus's first `n` keys.
    pub(crate) fn keys(self, n: usize) -> Box<dyn Iterator<Item = Vec<u8>>> {
        match self {
            Self::Sequential => Box::new(sequential(n)),
            Self::Agents => Box::new(agents(n)),
            Self::Uuids => Box::new(uuids(n)),
            Self::Binary => Box::new(binary(n)),
        }
    }
}

impl fmt::Display for Corpus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Sequential => "sequential",
            Self::Agents => "agents",
            Self::Uuids => "uuids",
            Self::Binary => "binary",
        })
    }
}

/// Tenant names that differ only in their trailing digits: `tenant:000000`, `tenant:000001`, …
pub(crate) fn sequential(n: usize) -> impl Iterator<Item = Vec<u8>> {
    (0..n).map(|i| format!("tenant:{i:06}").into_bytes())
}

/// Agent names such as `agent:brisk-otter-7`: two random words from small lists, then the index.
pub(crate) fn agents(n: usize) -> impl Iterator<Item = Vec<u8>> {
    let mut rng = SplitMix64::new(AGENTS_SEED);
    (0..n).map(move |i| {
        let adjective = choose(&mut rng, &ADJECTIVES);
        let noun = choose(&mut rng, &NOUNS);
        format!("agent:{adjective}-{noun}-{i}").into_bytes()
    })
}

/// Random version-4 UUIDs in lowercase 8-4-4-4-12 hex, such as
/// `1b4e28ba-2fa1-41d2-883f-0016d3cca427`.
pub(crate) fn uuids(n: usize) -> impl Iterator<Item = Vec<u8>> {
    let mut rng = SplitMix64::new(UUIDS_SEED);
    iter::repeat_with(move || {
        let high = (rng.next_u64() & !UUID_VERSION_MASK) | UUID_VERSION_4;
        let low = (rng.next_u64() & !UUID_VARIANT_MASK) | UUID_VARIANT_RFC;
        format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            high >> 32,
            (high >> 16) & 0xffff,
            high & 0xffff,
            low >> 48,
            low & 0xffff_ffff_ffff,
        )
        .into_bytes()
    })
    .take(n)
}

/// Random byte strings, mostly invalid UTF-8: an empty key, then keys of 1 to 256 bytes.
///
/// The lengths cover every XXH3 input-size class. Only the first key is empty: drawing lengths
/// from 0 would repeat the empty key once every 257 keys, and identical keys always land on the
/// same node, which skews balance tests on large corpora.
pub(crate) fn binary(n: usize) -> impl Iterator<Item = Vec<u8>> {
    let mut rng = SplitMix64::new(BINARY_SEED);
    let nonempty = iter::repeat_with(move || {
        let len = 1 + below(&mut rng, MAX_BINARY_LEN);
        let mut key = Vec::with_capacity(len.next_multiple_of(8));
        while key.len() < len {
            key.extend_from_slice(&rng.next_u64().to_le_bytes());
        }
        key.truncate(len);
        key
    });
    iter::once(Vec::new()).chain(nonempty).take(n)
}

/// Returns a draw from `0..bound`, exactly uniform when `bound` is a power of two, as every bound
/// here is.
fn below(rng: &mut SplitMix64, bound: usize) -> usize {
    let bound = u64::try_from(bound).expect("usize fits in u64");
    usize::try_from(rng.next_u64() % bound).expect("a draw below a usize bound fits in usize")
}

/// Returns a random word from `words`.
fn choose<'a>(rng: &mut SplitMix64, words: &[&'a str]) -> &'a str {
    words[below(rng, words.len())]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use fleetring::{StableHasher, Xxh3};

    use super::*;

    /// The digest of each corpus's first [`SMALL`] keys; see [`checksum`]. A corpus may change
    /// only on purpose: then paste the new value from the test's failure message.
    const CHECKSUMS: [(Corpus, u64); 4] = [
        (Corpus::Sequential, 0x8d0a1f588e046af3),
        (Corpus::Agents, 0x2a86b5a649795fe1),
        (Corpus::Uuids, 0x9afbf7d2faaa7b44),
        (Corpus::Binary, 0x9a6c67add988dcb0),
    ];

    /// An odd multiplier for [`checksum`]; any odd value works.
    const FOLD: u64 = 0x9e37_79b9_7f4a_7c15;

    /// Folds each key's XXH3-64 (seed 0) into one digest. The multiply makes every position count:
    /// rotate-xor alone would miss two keys swapped 64 places apart.
    fn checksum(keys: impl Iterator<Item = Vec<u8>>) -> u64 {
        keys.fold(0, |digest, key| {
            (digest ^ Xxh3.hash(&key, 0)).wrapping_mul(FOLD)
        })
    }

    #[test]
    fn checksums_are_platform_independent() {
        assert_eq!(CHECKSUMS.map(|(corpus, _)| corpus), Corpus::ALL);
        for (corpus, want) in CHECKSUMS {
            let got = checksum(corpus.keys(SMALL));
            assert_eq!(got, want, "{corpus} corpus checksum is {got:#018x}");
        }
    }

    #[test]
    fn smaller_corpora_are_prefixes_of_larger_ones() {
        for corpus in Corpus::ALL {
            let small: Vec<Vec<u8>> = corpus.keys(100).collect();
            let large: Vec<Vec<u8>> = corpus.keys(1_000).collect();
            assert_eq!((small.len(), large.len()), (100, 1_000), "{corpus}");
            assert_eq!(small, large[..100], "{corpus}");
            assert_eq!(corpus.keys(0).count(), 0, "{corpus}");
        }
    }

    #[test]
    fn keys_repeat_only_among_one_and_two_byte_binary_keys() {
        for corpus in Corpus::ALL {
            let mut seen = BTreeSet::new();
            for key in corpus.keys(SMALL) {
                if seen.contains(&key) {
                    assert!(
                        corpus == Corpus::Binary && key.len() <= 2,
                        "{corpus} corpus repeats {key:?}"
                    );
                } else {
                    seen.insert(key);
                }
            }
        }
    }

    #[test]
    fn sequential_keys_are_zero_padded_tenant_numbers() {
        let keys: Vec<Vec<u8>> = sequential(SMALL).collect();
        assert_eq!(keys[0], b"tenant:000000");
        assert_eq!(keys[1], b"tenant:000001");
        assert_eq!(keys[SMALL - 1], b"tenant:009999");
    }

    #[test]
    fn agent_keys_are_two_listed_words_and_the_index() {
        let mut adjectives = BTreeSet::new();
        let mut nouns = BTreeSet::new();
        for (i, key) in agents(1_000).enumerate() {
            let key = String::from_utf8(key).expect("agent keys are ASCII");
            let name = key
                .strip_prefix("agent:")
                .expect("agent keys start with agent:");
            let parts: Vec<&str> = name.split('-').collect();
            let [adjective, noun, index] = parts[..] else {
                panic!("{key:?} is not adjective-noun-index");
            };
            assert!(ADJECTIVES.contains(&adjective), "{key:?}");
            assert!(NOUNS.contains(&noun), "{key:?}");
            assert_eq!(index, i.to_string(), "{key:?}");
            adjectives.insert(adjective.to_owned());
            nouns.insert(noun.to_owned());
        }
        assert_eq!(
            (adjectives.len(), nouns.len()),
            (16, 16),
            "every word is drawn"
        );
    }

    #[test]
    fn uuid_keys_are_lowercase_version_4_uuids() {
        for key in uuids(1_000) {
            let key = String::from_utf8(key).expect("UUID keys are ASCII");
            let groups: Vec<&str> = key.split('-').collect();
            let lengths: Vec<usize> = groups.iter().map(|group| group.len()).collect();
            assert_eq!(lengths, [8, 4, 4, 4, 12], "{key}");
            assert!(
                groups.iter().all(|group| group
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))),
                "{key}"
            );
            assert!(groups[2].starts_with('4'), "version: {key}");
            assert!(
                groups[3].starts_with(['8', '9', 'a', 'b']),
                "variant: {key}"
            );
        }
    }

    #[test]
    fn binary_keys_are_empty_first_then_1_to_256_bytes() {
        let lengths: Vec<usize> = binary(SMALL).map(|key| key.len()).collect();
        assert_eq!(lengths[0], 0);
        assert!(
            lengths[1..]
                .iter()
                .all(|len| (1..=MAX_BINARY_LEN).contains(len))
        );
        assert!(lengths.contains(&1) && lengths.contains(&MAX_BINARY_LEN));
    }
}
