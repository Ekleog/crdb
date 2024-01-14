use crate::{BinPtr, EventId, ObjectId, SessionToken, Timestamp, TypeId};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("Missing binary pointers: {0:?}")]
    MissingBinaries(Vec<BinPtr>),

    #[error("{0:?} is outside the range of valid ULIDs")]
    InvalidTimestamp(Timestamp),

    #[error("{0:?} already exists with a different value set")]
    ObjectAlreadyExists(ObjectId),

    #[error("Event {0:?} already exists with a different value set")]
    EventAlreadyExists(EventId),

    #[error("{0:?} does not exist in database")]
    ObjectDoesNotExist(ObjectId),

    #[error("{0:?} does not exist in datapase")]
    TypeDoesNotExist(TypeId),

    #[error("{0:?} is not the hash of the provided value")]
    BinaryHashMismatch(BinPtr),

    #[error("Null byte in provided string")]
    NullByteInString,

    #[error("Invalid token: {0:?}")]
    InvalidToken(SessionToken),

    #[error("Invalid number provided")]
    InvalidNumber,

    #[error(
        "{event_id:?} is too early to be submitted on {object_id:?} created at {created_at:?}"
    )]
    EventTooEarly {
        event_id: EventId,
        object_id: ObjectId,
        created_at: EventId,
    },

    #[error("{object_id:?} has type {real_type_id:?} and not expected type {expected_type_id:?}")]
    WrongType {
        object_id: ObjectId,
        expected_type_id: TypeId,
        real_type_id: TypeId,
    },

    #[error(transparent)]
    Other(anyhow::Error),
}

pub trait ResultExt: Sized {
    type Ok;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<Self::Ok>;

    fn wrap_context(self, s: &str) -> Result<Self::Ok> {
        self.wrap_with_context(|| s.to_string())
    }
}

impl<T> ResultExt for Result<T> {
    type Ok = T;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<T> {
        match self {
            Err(Error::Other(e)) => Err(Error::Other(e.context(f()))),
            r => r,
        }
    }
}

impl<T> ResultExt for anyhow::Result<T> {
    type Ok = T;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<T> {
        match self {
            Err(e) => Err(Error::Other(e.context(f()))),
            Ok(r) => Ok(r),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> ResultExt for sqlx::Result<T> {
    type Ok = T;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<T> {
        match self {
            Err(sqlx::Error::Database(err)) => match err.code().as_ref().map(|c| &**c) {
                Some("22P05" | "22021") => Err(Error::NullByteInString),
                Some("22P03") => Err(Error::InvalidNumber),
                _ => Err(Error::Other(
                    anyhow::Error::from(sqlx::Error::Database(err)).context(f()),
                )),
            },
            Err(e) => Err(Error::Other(anyhow::Error::from(e).context(f()))),
            Ok(r) => Ok(r),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<T> ResultExt for std::result::Result<T, web_sys::DomException> {
    type Ok = T;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<T> {
        match self {
            Err(e) => Err(Error::Other(
                anyhow::anyhow!("{}: {}", e.name(), e.message()).context(f()),
            )),
            Ok(r) => Ok(r),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<T> ResultExt for std::result::Result<T, wasm_bindgen::JsValue> {
    type Ok = T;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<T> {
        use wasm_bindgen::JsCast;
        match self {
            Err(err) => {
                if err.has_type::<web_sys::DomException>() {
                    return Err(err.dyn_into::<web_sys::DomException>().unwrap())
                        .wrap_with_context(f);
                }
                Err(crate::Error::Other(
                    anyhow::anyhow!("error with unknown type: {err:?}").context(f()),
                ))
            }
            Ok(r) => Ok(r),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<T> ResultExt for std::result::Result<T, serde_wasm_bindgen::Error> {
    type Ok = T;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<T> {
        match self {
            Err(e) => Err(Error::Other(anyhow::anyhow!("{}", e).context(f()))),
            Ok(r) => Ok(r),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl<T> ResultExt for std::result::Result<T, indexed_db::Error<crate::Error>> {
    type Ok = T;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<T> {
        match self {
            Err(indexed_db::Error::User(e)) => Err(e).wrap_with_context(f),
            Err(e) => Err(Error::Other(anyhow::Error::from(e).context(f()))),
            Ok(r) => Ok(r),
        }
    }
}

impl<T> ResultExt for serde_json::Result<T> {
    type Ok = T;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<T> {
        match self {
            Err(e) => Err(Error::Other(anyhow::Error::from(e).context(f()))),
            Ok(r) => Ok(r),
        }
    }
}
