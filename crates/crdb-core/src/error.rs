use crate::{BinPtr, EventId, ObjectId, QueryId, SessionToken, TypeId};
use anyhow::anyhow;
use web_time::SystemTime;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("Missing binary pointers: {0:?}")]
    MissingBinaries(Vec<BinPtr>),

    #[error("{0:?} already exists with a different value set")]
    ObjectAlreadyExists(ObjectId),

    #[error("Event {0:?} already exists with a different value set")]
    EventAlreadyExists(EventId),

    #[error("{0:?} does not exist in database")]
    ObjectDoesNotExist(ObjectId),

    #[error("{0:?} does not exist in database")]
    QueryDoesNotExist(QueryId),

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

    #[error("Invalid time")]
    InvalidTime(SystemTime),

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

    #[error("Lost connection while request was in progress")]
    ConnectionLoss,

    #[error("Client violated the protocol")]
    ProtocolViolation,

    #[error("User is not allowed to perform this action")]
    Forbidden,

    #[error(transparent)]
    Other(anyhow::Error),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, thiserror::Error)]
#[non_exhaustive]
pub enum SerializableError {
    #[error("Missing binary pointers: {0:?}")]
    MissingBinaries(Vec<BinPtr>),

    #[error("{0:?} already exists with a different value set")]
    ObjectAlreadyExists(ObjectId),

    #[error("Event {0:?} already exists with a different value set")]
    EventAlreadyExists(EventId),

    #[error("{0:?} does not exist in database")]
    ObjectDoesNotExist(ObjectId),

    #[error("{0:?} does not exist in database")]
    QueryDoesNotExist(QueryId),

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

    #[error("Invalid time")]
    InvalidTime(SystemTime),

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

    #[error("Lost connection while request was in progress")]
    ConnectionLoss,

    #[error("Client violated the protocol")]
    ProtocolViolation,

    #[error("User is not allowed to perform this action")]
    Forbidden,

    #[error("Internal server error")]
    InternalServerError,
}

impl From<Error> for SerializableError {
    fn from(err: Error) -> SerializableError {
        match err {
            Error::MissingBinaries(e) => SerializableError::MissingBinaries(e),
            Error::ObjectAlreadyExists(e) => SerializableError::ObjectAlreadyExists(e),
            Error::EventAlreadyExists(e) => SerializableError::EventAlreadyExists(e),
            Error::ObjectDoesNotExist(e) => SerializableError::ObjectDoesNotExist(e),
            Error::QueryDoesNotExist(e) => SerializableError::QueryDoesNotExist(e),
            Error::TypeDoesNotExist(e) => SerializableError::TypeDoesNotExist(e),
            Error::BinaryHashMismatch(e) => SerializableError::BinaryHashMismatch(e),
            Error::NullByteInString => SerializableError::NullByteInString,
            Error::InvalidToken(e) => SerializableError::InvalidToken(e),
            Error::InvalidNumber => SerializableError::InvalidNumber,
            Error::InvalidTime(t) => SerializableError::InvalidTime(t),
            Error::EventTooEarly {
                event_id,
                object_id,
                created_at,
            } => SerializableError::EventTooEarly {
                event_id,
                object_id,
                created_at,
            },
            Error::WrongType {
                object_id,
                expected_type_id,
                real_type_id,
            } => SerializableError::WrongType {
                object_id,
                expected_type_id,
                real_type_id,
            },
            Error::ConnectionLoss => SerializableError::ConnectionLoss,
            Error::ProtocolViolation => SerializableError::ProtocolViolation,
            Error::Forbidden => SerializableError::Forbidden,
            Error::Other(err) => {
                tracing::error!(?err, "returning internal server error to client");
                SerializableError::InternalServerError
            }
        }
    }
}

impl From<SerializableError> for Error {
    fn from(err: SerializableError) -> Error {
        match err {
            SerializableError::MissingBinaries(e) => Error::MissingBinaries(e),
            SerializableError::ObjectAlreadyExists(e) => Error::ObjectAlreadyExists(e),
            SerializableError::EventAlreadyExists(e) => Error::EventAlreadyExists(e),
            SerializableError::ObjectDoesNotExist(e) => Error::ObjectDoesNotExist(e),
            SerializableError::QueryDoesNotExist(e) => Error::QueryDoesNotExist(e),
            SerializableError::TypeDoesNotExist(e) => Error::TypeDoesNotExist(e),
            SerializableError::BinaryHashMismatch(e) => Error::BinaryHashMismatch(e),
            SerializableError::NullByteInString => Error::NullByteInString,
            SerializableError::InvalidToken(e) => Error::InvalidToken(e),
            SerializableError::InvalidNumber => Error::InvalidNumber,
            SerializableError::InvalidTime(t) => Error::InvalidTime(t),
            SerializableError::EventTooEarly {
                event_id,
                object_id,
                created_at,
            } => Error::EventTooEarly {
                event_id,
                object_id,
                created_at,
            },
            SerializableError::WrongType {
                object_id,
                expected_type_id,
                real_type_id,
            } => Error::WrongType {
                object_id,
                expected_type_id,
                real_type_id,
            },
            SerializableError::ConnectionLoss => Error::ConnectionLoss,
            SerializableError::ProtocolViolation => Error::ProtocolViolation,
            SerializableError::Forbidden => Error::Forbidden,
            SerializableError::InternalServerError => {
                Error::Other(anyhow!("Internal server error"))
            }
        }
    }
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

#[cfg(any(feature = "sqlx-sqlite", feature = "server"))]
impl<T> ResultExt for sqlx::Result<T> {
    type Ok = T;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<T> {
        match self {
            Err(sqlx::Error::Database(err)) => match err.code().as_deref() {
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

#[cfg(feature = "indexed-db")]
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

#[cfg(feature = "indexed-db")]
impl<T> ResultExt for std::result::Result<T, web_sys::wasm_bindgen::JsValue> {
    type Ok = T;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<T> {
        use web_sys::wasm_bindgen::JsCast;
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

#[cfg(feature = "indexed-db")]
impl<T> ResultExt for std::result::Result<T, serde_wasm_bindgen::Error> {
    type Ok = T;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<T> {
        match self {
            Err(e) => Err(Error::Other(anyhow::anyhow!("{}", e).context(f()))),
            Ok(r) => Ok(r),
        }
    }
}

#[cfg(feature = "indexed-db")]
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

#[cfg(feature = "server")]
impl<T> ResultExt for std::result::Result<T, axum::Error> {
    type Ok = T;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<T> {
        match self {
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

impl<T> ResultExt for std::result::Result<T, SerializableError> {
    type Ok = T;

    fn wrap_with_context(self, f: impl FnOnce() -> String) -> Result<T> {
        match self {
            Ok(r) => Ok(r),
            Err(err) => Err(Error::from(err)).wrap_with_context(f),
        }
    }
}
