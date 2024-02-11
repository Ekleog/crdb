#[cfg(feature = "client")]
use crate::db_trait::Lock;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Importance {
    /// Only care about fetching the latest value once
    Latest,

    /// Will want to re-use the value a few times, without a server round-trip each time
    ///
    /// However, whenever there is a client-side vacuum, this will force a server-round-trip next
    /// time the object is accessed
    Subscribe,

    /// Always keep this locally and up-to-date
    Lock,
}

#[cfg(feature = "client")]
impl Importance {
    pub(crate) fn to_object_lock(&self) -> Lock {
        if *self >= Importance::Lock {
            Lock::OBJECT
        } else {
            Lock::NONE
        }
    }

    pub(crate) fn to_query_lock(&self) -> Lock {
        if *self >= Importance::Lock {
            Lock::FOR_QUERIES
        } else {
            Lock::NONE
        }
    }

    pub(crate) fn to_subscribe(&self) -> bool {
        *self >= Importance::Subscribe
    }
}

impl Ord for Importance {
    fn cmp(&self, other: &Importance) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (self, other) {
            (Importance::Latest, Importance::Latest) => Ordering::Equal,
            (Importance::Latest, _) => Ordering::Less,
            (Importance::Subscribe, Importance::Latest) => Ordering::Greater,
            (Importance::Subscribe, Importance::Subscribe) => Ordering::Equal,
            (Importance::Subscribe, Importance::Lock) => Ordering::Less,
            (Importance::Lock, Importance::Lock) => Ordering::Equal,
            (Importance::Lock, _) => Ordering::Greater,
        }
    }
}

impl PartialOrd for Importance {
    fn partial_cmp(&self, other: &Importance) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
