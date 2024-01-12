use icu::{
    casemap::CaseMapper,
    normalizer::{ComposingNormalizer, DecomposingNormalizer},
    properties::sets::diacritic,
    segmenter::WordSegmenter,
};
use std::fmt::Debug;
use writeable::Writeable;

thread_local! {
    static SEGMENTER: WordSegmenter = WordSegmenter::new_auto();
}

const CASEMAPPER: CaseMapper = CaseMapper::new();
const DECOMPOSER: DecomposingNormalizer = DecomposingNormalizer::new_nfd();
const RECOMPOSER: ComposingNormalizer = ComposingNormalizer::new_nfc();

pub(crate) fn normalizer_version() -> usize {
    0
}

pub(crate) fn normalize(input: &str) -> String {
    SEGMENTER.with(|segmenter| {
        let mut res = String::with_capacity(input.len());
        let mut last_brk = 0;
        let mut segments = segmenter.segment_str(input);
        let mut buf = String::new();
        // For each word
        while let Some(next_brk) = segments.next() {
            if segments.is_word_like() {
                // Fold case
                buf.clear();
                CASEMAPPER
                    .fold(&input[last_brk..next_brk])
                    .write_to(&mut buf)
                    .unwrap();
                // And remove diacritics
                res.extend(
                    RECOMPOSER.normalize_iter(
                        DECOMPOSER
                            .normalize_iter(buf.chars())
                            .filter(|c| !diacritic().contains(*c)),
                    ),
                );
                res.push(' ');
            }
            last_brk = next_brk;
        }
        res.pop(); // remove the last space if there was at least one word
        res
    })
}

#[derive(Clone, deepsize::DeepSizeOf, educe::Educe, serde::Deserialize, serde::Serialize)]
#[educe(Deref, Eq, Ord, PartialEq, PartialOrd)]
pub struct SearchableString {
    #[serde(rename = "_crdb-str")]
    #[educe(Deref)]
    value: String,

    #[serde(rename = "_crdb-normalized")]
    #[educe(Eq(ignore), Ord(ignore))]
    normalized: String,

    #[serde(rename = "_crdb-normalizer-version")]
    #[educe(Eq(ignore), Ord(ignore))]
    normalizer_version: usize,
}

impl SearchableString {
    pub fn new() -> SearchableString {
        SearchableString {
            value: String::new(),
            normalized: String::new(),
            normalizer_version: normalizer_version(),
        }
    }
}

impl<T: Into<String>> From<T> for SearchableString {
    fn from(value: T) -> SearchableString {
        let value: String = value.into();
        SearchableString {
            normalizer_version: normalizer_version(),
            normalized: normalize(&value),
            value,
        }
    }
}

impl Debug for SearchableString {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(fmt)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn basic_examples() {
        let tests = [
            ("Je   suis bien embêté !", "je suis bien embete"),
            (
                "Some 色々な言語の façon de faire un test :)",
                "some 色 々 な 言語 facon de faire un test",
            ),
            ("ば", "は"), // japanese diacritics too
            ("coupe-papier", "coupe papier"),
        ];
        for (before, after) in tests {
            assert_eq!(
                super::normalize(before),
                after,
                "normalization of {before:?} didn't match"
            );
        }
    }

    #[test]
    fn fuzz_normalizer() {
        bolero::check!().with_type().for_each(|s: &String| {
            super::normalize(s);
        });
    }
}
