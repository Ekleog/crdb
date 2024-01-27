#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq)]
pub enum Importance {
    /// Only care about fetching the latest value once
    Once,

    /// Will want to re-use the value a few times, without a server round-trip each time
    ///
    /// However, whenever there is a client-side vacuum, this will force a server-round-trip next
    /// time the object is accessed
    Subscribe,

    /// Always keep this locally and up-to-date
    Lock,
}

impl PartialOrd for Importance {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;

        Some(match (self, other) {
            (Importance::Once, Importance::Once) => Ordering::Equal,
            (Importance::Once, _) => Ordering::Less,
            (Importance::Subscribe, Importance::Once) => Ordering::Greater,
            (Importance::Subscribe, Importance::Subscribe) => Ordering::Equal,
            (Importance::Subscribe, Importance::Lock) => Ordering::Less,
            (Importance::Lock, Importance::Lock) => Ordering::Equal,
            (Importance::Lock, _) => Ordering::Greater,
        })
    }
}
