use anyhow::anyhow;
use web_time::SystemTime;

pub(crate) trait SystemTimeExt {
    fn ms_since_posix(&self) -> crate::Result<i64>;

    #[cfg(feature = "server")]
    fn from_ms_since_posix(ms: i64) -> crate::Result<SystemTime>;
}

impl SystemTimeExt for SystemTime {
    fn ms_since_posix(&self) -> crate::Result<i64> {
        self.duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| {
                crate::Error::Other(anyhow!("Failed computing duration since unix epoch"))
            })?
            .as_millis()
            .try_into()
            .map_err(|_| {
                crate::Error::Other(anyhow!(
                    "Failed converting duration into reasonably-bound milliseconds"
                ))
            })
    }

    #[cfg(feature = "server")]
    fn from_ms_since_posix(ms: i64) -> crate::Result<SystemTime> {
        use std::time::Duration;
        let ms = u64::try_from(ms).map_err(|_| {
            crate::Error::Other(anyhow!(
                "Cannot convert negative milliseconds into SystemTime"
            ))
        })?;
        Ok(SystemTime::UNIX_EPOCH + Duration::from_millis(ms as u64))
    }
}
