//! Idempotent apply contract helpers for at-least-once propagation.

use lethe_core::domain::Observation;
use std::fmt::Debug;

pub trait IdempotentFold<I> {
    type Output: Eq + Debug;

    fn apply(&mut self, input: &I);
    fn output(&self) -> Self::Output;
}

/// Marker contract for folds accepted by the persistent propagation runtime.
/// Implementations must be both commutative and idempotent.
pub trait CommutativeIdempotentObservationFold {
    fn apply(&mut self, observation: &Observation) -> Result<(), String>;
}

pub fn assert_at_least_once_idempotent<F, I>(mut fold: F, inputs: &[I])
where
    F: IdempotentFold<I>,
{
    for input in inputs {
        fold.apply(input);
    }
    let once = fold.output();

    for input in inputs {
        fold.apply(input);
    }
    let twice = fold.output();

    assert_eq!(once, twice, "idempotent fold changed after duplicate apply");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[derive(Default)]
    struct SeenSet {
        values: BTreeSet<String>,
    }

    impl IdempotentFold<String> for SeenSet {
        type Output = BTreeSet<String>;

        fn apply(&mut self, input: &String) {
            self.values.insert(input.clone());
        }

        fn output(&self) -> Self::Output {
            self.values.clone()
        }
    }

    #[test]
    fn conformance_helper_accepts_idempotent_fold() {
        let inputs = vec!["obs:1".to_owned(), "obs:2".to_owned()];

        assert_at_least_once_idempotent(SeenSet::default(), &inputs);
    }
}
