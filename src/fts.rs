use icu::{
    casemap::CaseMapper,
    normalizer::{ComposingNormalizer, DecomposingNormalizer},
    properties::sets::diacritic,
    segmenter::WordSegmenter,
};
use writeable::Writeable;

thread_local! {
    static SEGMENTER: WordSegmenter = WordSegmenter::new_auto();
}

const CASEMAPPER: CaseMapper = CaseMapper::new();
const DECOMPOSER: DecomposingNormalizer = DecomposingNormalizer::new_nfd();
const RECOMPOSER: ComposingNormalizer = ComposingNormalizer::new_nfc();

pub fn normalize(input: &str) -> String {
    SEGMENTER.with(|segmenter| {
        let mut res = String::with_capacity(input.len());
        let mut last_brk = 0;
        let mut segments = segmenter.segment_str(input);
        let mut buf = String::new();
        // For each word
        while let Some(next_brk) = segments.next() {
            eprintln!("got word {:?}", &input[last_brk..next_brk]);
            if segments.is_word_like() {
                eprintln!(" -> keeping it");
                // Fold case
                buf.clear();
                CASEMAPPER
                    .fold(&input[last_brk..next_brk])
                    .write_to(&mut buf)
                    .unwrap();
                eprintln!(" -> casemapped {buf:?}");
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
