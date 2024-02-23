pub fn check_string(s: &str) -> crate::Result<()> {
    if s.contains('\0') {
        return Err(crate::Error::NullByteInString);
    }
    Ok(())
}

pub fn check_strings(v: &serde_json::Value) -> crate::Result<()> {
    match v {
        serde_json::Value::Null => (),
        serde_json::Value::Bool(_) => (),
        serde_json::Value::Number(_) => (),
        serde_json::Value::String(s) => check_string(s)?,
        serde_json::Value::Array(a) => {
            for v in a.iter() {
                check_strings(v)?;
            }
        }
        serde_json::Value::Object(m) => {
            for (k, v) in m.iter() {
                check_string(k)?;
                check_strings(v)?;
            }
        }
    }
    Ok(())
}
