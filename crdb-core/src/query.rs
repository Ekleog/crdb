use rust_decimal::Decimal;

use crate::fts;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum JsonPathItem {
    Key(String),

    /// Negative values count from the end
    // PostgreSQL throws an error if trying to use -> with a value beyond i32 range
    Id(i32),
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum Query {
    // Logic operators
    All(Vec<Query>),
    Any(Vec<Query>),
    Not(Box<Query>),

    // TODO(misc-low): this could be useful?
    // Any/all the values in the array at JsonPathItem must match Query
    // AnyIn(Vec<JsonPathItem>, Box<Query>),
    // AllIn(Vec<JsonPathItem>, Box<Query>),

    // JSON tests
    Eq(Vec<JsonPathItem>, serde_json::Value),

    // Integers
    Le(Vec<JsonPathItem>, Decimal),
    Lt(Vec<JsonPathItem>, Decimal),
    Ge(Vec<JsonPathItem>, Decimal),
    Gt(Vec<JsonPathItem>, Decimal),

    // Arrays and object containment
    Contains(Vec<JsonPathItem>, serde_json::Value),

    // Full text search
    ContainsStr(Vec<JsonPathItem>, String),
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for Query {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Query> {
        arbitrary_impl(u, 0)
    }
}

#[cfg(feature = "arbitrary")]
fn arbitrary_impl(u: &mut arbitrary::Unstructured<'_>, depth: usize) -> arbitrary::Result<Query> {
    if u.is_empty() || depth > 50 {
        // avoid stack overflow in arbitrary
        return Ok(Query::Eq(Vec::new(), serde_json::Value::Null));
    }
    let res = match u.arbitrary::<u8>()? % 10 {
        0 => Query::All({
            let mut v = Vec::new();
            u.arbitrary_loop(None, Some(50), |u| {
                v.push(arbitrary_impl(u, depth + 1)?);
                Ok(std::ops::ControlFlow::Continue(()))
            })?;
            v
        }),
        1 => Query::Any({
            let mut v = Vec::new();
            u.arbitrary_loop(None, Some(50), |u| {
                v.push(arbitrary_impl(u, depth + 1)?);
                Ok(std::ops::ControlFlow::Continue(()))
            })?;
            v
        }),
        2 => Query::Not(Box::new(arbitrary_impl(u, depth + 1)?)),
        3 => Query::Eq(
            u.arbitrary()?,
            u.arbitrary::<arbitrary_json::ArbitraryValue>()?.into(),
        ),
        4 => Query::Le(u.arbitrary()?, u.arbitrary()?),
        5 => Query::Lt(u.arbitrary()?, u.arbitrary()?),
        6 => Query::Ge(u.arbitrary()?, u.arbitrary()?),
        7 => Query::Gt(u.arbitrary()?, u.arbitrary()?),
        8 => Query::Contains(
            u.arbitrary()?,
            u.arbitrary::<arbitrary_json::ArbitraryValue>()?.into(),
        ),
        9 => Query::ContainsStr(u.arbitrary()?, u.arbitrary()?),
        _ => unimplemented!(),
    };
    Ok(res)
}

impl Query {
    pub fn check(&self) -> crate::Result<()> {
        match self {
            Query::All(v) => {
                for v in v {
                    v.check()?;
                }
            }
            Query::Any(v) => {
                for v in v {
                    v.check()?;
                }
            }
            Query::Not(v) => v.check()?,
            Query::Eq(p, v) => {
                Self::check_path(p)?;
                Self::check_value(v)?;
            }
            Query::Le(p, _) => Self::check_path(p)?,
            Query::Lt(p, _) => Self::check_path(p)?,
            Query::Ge(p, _) => Self::check_path(p)?,
            Query::Gt(p, _) => Self::check_path(p)?,
            Query::Contains(p, v) => {
                Self::check_path(p)?;
                Self::check_value(v)?;
            }
            Query::ContainsStr(p, s) => {
                Self::check_path(p)?;
                crate::check_string(s)?;
            }
        }

        Ok(())
    }

    fn check_value(v: &serde_json::Value) -> crate::Result<()> {
        match v {
            serde_json::Value::Null => (),
            serde_json::Value::Bool(_) => (),
            serde_json::Value::Number(_) => (),
            serde_json::Value::String(s) => crate::check_string(s)?,
            serde_json::Value::Array(v) => {
                for v in v.iter() {
                    Self::check_value(v)?;
                }
            }
            serde_json::Value::Object(m) => {
                for (k, v) in m.iter() {
                    crate::check_string(k)?;
                    Self::check_value(v)?;
                }
            }
        }
        Ok(())
    }

    fn check_path(p: &[JsonPathItem]) -> crate::Result<()> {
        p.iter().try_for_each(|i| match i {
            JsonPathItem::Id(_) => Ok(()),
            JsonPathItem::Key(k) => crate::check_string(k),
        })
    }

    pub fn matches<T: serde::Serialize>(&self, v: T) -> serde_json::Result<bool> {
        let json = serde_json::to_value(v)?;
        Ok(self.matches_json(&json))
    }

    pub fn matches_json(&self, v: &serde_json::Value) -> bool {
        match self {
            Query::All(q) => q.iter().all(|q| q.matches_json(v)),
            Query::Any(q) => q.iter().any(|q| q.matches_json(v)),
            Query::Not(q) => !q.matches_json(v),
            Query::Eq(p, to) => Self::deref(v, p)
                .map(|v| Self::compare_with_nums(v, to))
                .unwrap_or(false),
            Query::Le(p, to) => Self::deref_num(v, p).map(|n| n <= *to).unwrap_or(false),
            Query::Lt(p, to) => Self::deref_num(v, p).map(|n| n < *to).unwrap_or(false),
            Query::Ge(p, to) => Self::deref_num(v, p).map(|n| n >= *to).unwrap_or(false),
            Query::Gt(p, to) => Self::deref_num(v, p).map(|n| n > *to).unwrap_or(false),
            Query::Contains(p, pat) => {
                let Some(v) = Self::deref(v, p) else {
                    return false;
                };
                Self::contains(v, pat)
            }
            Query::ContainsStr(p, pat) => Self::deref(v, p)
                .and_then(|v| v.as_object())
                .and_then(|v| v.get("_crdb-normalized"))
                .and_then(|s| s.as_str())
                .map(|s| fts::matches(s, &fts::normalize(pat)))
                .unwrap_or(false),
        }
    }

    fn compare_with_nums(l: &serde_json::Value, r: &serde_json::Value) -> bool {
        use serde_json::Value::*;
        match (l, r) {
            (Null, Null) => true,
            (Bool(l), Bool(r)) => l == r,
            (l @ Number(_), r @ Number(_)) => {
                let normalized_l = serde_json::from_value::<Decimal>(l.clone());
                normalized_l.is_ok()
                    && normalized_l.ok() == serde_json::from_value::<Decimal>(r.clone()).ok()
            }
            (String(l), String(r)) => l == r,
            (Array(l), Array(r)) => {
                l.len() == r.len()
                    && l.iter()
                        .zip(r.iter())
                        .all(|(l, r)| Self::compare_with_nums(l, r))
            }
            (Object(l), Object(r)) => {
                l.len() == r.len()
                    && l.iter()
                        .zip(r.iter())
                        .all(|((lk, lv), (rk, rv))| lk == rk && Self::compare_with_nums(lv, rv))
            }
            _ => false,
        }
    }

    fn contains(v: &serde_json::Value, pat: &serde_json::Value) -> bool {
        use serde_json::Value::*;
        match (v, pat) {
            (Null, Null) => true,
            (Bool(l), Bool(r)) => l == r,
            (l @ Number(_), r @ Number(_)) => Self::compare_with_nums(l, r),
            (String(l), String(r)) => l == r,
            (Object(v), Object(pat)) => {
                for (key, pat) in pat.iter() {
                    if !v.get(key).map(|v| Self::contains(v, pat)).unwrap_or(false) {
                        return false;
                    }
                }
                true
            }
            (Array(v), Array(pat)) => {
                for pat in pat.iter() {
                    if !v.iter().any(|v| Self::contains(v, pat)) {
                        return false;
                    }
                }
                true
            }
            (Array(_), Object(_)) => false, // primitive containment doesn't work on objects
            (Array(v), pat) => v.iter().any(|v| Self::compare_with_nums(v, pat)), // but does work on primitives
            _ => false,
        }
    }

    fn deref_num(v: &serde_json::Value, path: &[JsonPathItem]) -> Option<Decimal> {
        use serde_json::Value;
        match Self::deref(v, path)? {
            Value::Number(n) => serde_json::from_value(Value::Number(n.clone())).ok(),
            _ => None,
        }
    }

    fn deref<'a>(v: &'a serde_json::Value, path: &[JsonPathItem]) -> Option<&'a serde_json::Value> {
        match path.first() {
            None => Some(v),
            Some(JsonPathItem::Key(k)) => match v.as_object() {
                None => None,
                Some(v) => v.get(k).and_then(|v| Self::deref(v, &path[1..])),
            },
            Some(JsonPathItem::Id(k)) if *k >= 0 => match v.as_array() {
                None => None,
                Some(v) => v.get(*k as usize).and_then(|v| Self::deref(v, &path[1..])),
            },
            Some(JsonPathItem::Id(k)) /* if *k < 0 */ => match v.as_array() {
                None => None,
                Some(v) => v
                    .len()
                    .checked_add_signed(isize::try_from(*k).unwrap())
                    .and_then(|i| v.get(i))
                    .and_then(|v| Self::deref(v, &path[1..])),
            },
        }
    }
}
