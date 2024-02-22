use crate::{Object, ObjectId};
use std::marker::PhantomData;
use ulid::Ulid;

#[derive(Clone, Eq, PartialEq, educe::Educe, serde::Deserialize, serde::Serialize)]
#[educe(Debug(named_field = false), Ord, PartialOrd)]
pub struct DbPtr<T: Object> {
    #[educe(Debug(method = std::fmt::Display::fmt))]
    pub id: Ulid,
    #[educe(Debug(ignore))]
    _phantom: PhantomData<T>,
}

impl<T: Object> deepsize::DeepSizeOf for DbPtr<T> {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        0
    }
}

impl<T: Object> Copy for DbPtr<T> {}

impl<T: Object> DbPtr<T> {
    pub fn from(id: ObjectId) -> DbPtr<T> {
        DbPtr {
            id: id.0,
            _phantom: PhantomData,
        }
    }

    pub fn to_object_id(&self) -> ObjectId {
        ObjectId(self.id)
    }

    #[cfg(feature = "_tests")]
    pub fn from_string(s: &str) -> anyhow::Result<DbPtr<T>> {
        Ok(DbPtr {
            id: Ulid::from_string(s)?,
            _phantom: PhantomData,
        })
    }
}

#[cfg(feature = "_tests")]
impl<'a, T: Object> arbitrary::Arbitrary<'a> for DbPtr<T> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self {
            id: Ulid::from_bytes(u.arbitrary()?),
            _phantom: PhantomData,
        })
    }
}
