use std::time::SystemTime;
use ulid::Ulid;

#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[cfg_attr(feature = "_tests", derive(arbitrary::Arbitrary))]
pub struct Timestamp(u64); // Milliseconds since UNIX_EPOCH

impl Timestamp {
    pub fn now() -> Timestamp {
        Timestamp(
            u64::try_from(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            )
            .unwrap(),
        )
    }

    pub fn from_ms(v: u64) -> Timestamp {
        Timestamp(v)
    }

    pub fn max_for_ulid() -> Timestamp {
        Timestamp((1 << Ulid::TIME_BITS) - 1)
    }

    pub fn time_ms(&self) -> u64 {
        self.0
    }

    #[cfg(feature = "server")]
    pub fn from_i64_ms(v: i64) -> Timestamp {
        Timestamp(u64::try_from(v).expect("negative timestamp made its way in the database"))
    }

    pub fn time_ms_i(&self) -> crate::Result<i64> {
        i64::try_from(self.0).map_err(|_| crate::Error::InvalidTimestamp(*self))
    }
}

impl From<std::time::SystemTime> for Timestamp {
    fn from(t: std::time::SystemTime) -> Timestamp {
        // SystemTime.duration_since(UNIX_EPOCH) always returns a UTC number of seconds
        Timestamp(
            u64::try_from(
                t.duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            )
            .unwrap(),
        )
    }
}
