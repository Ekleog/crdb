use rust_decimal::Decimal;

#[derive(Debug)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum JsonPathItem {
    Key(String),

    /// Negative values count from the end
    // PostgreSQL throws an error if trying to use -> with a value beyond i32 range
    Id(i32),
}

#[derive(Debug)]
#[non_exhaustive]
pub enum Query {
    // Logic operators
    All(Vec<Query>),
    Any(Vec<Query>),
    Not(Box<Query>),

    // TODO: this could be useful?
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

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for Query {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Query> {
        arbitrary_impl(u, 0)
    }
}

#[cfg(test)]
fn arbitrary_impl<'a>(
    u: &mut arbitrary::Unstructured<'a>,
    depth: usize,
) -> arbitrary::Result<Query> {
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
        Ok(match self {
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
        })
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
        p.iter()
            .map(|i| match i {
                JsonPathItem::Id(_) => Ok(()),
                JsonPathItem::Key(k) => crate::check_string(k),
            })
            .collect()
    }

    pub fn matches<T: serde::Serialize>(&self, v: T) -> serde_json::Result<bool> {
        let json = serde_json::to_value(v)?;
        Ok(self.matches_impl(&json))
    }

    fn matches_impl(&self, v: &serde_json::Value) -> bool {
        match self {
            Query::All(q) => q.iter().all(|q| q.matches_impl(v)),
            Query::Any(q) => q.iter().any(|q| q.matches_impl(v)),
            Query::Not(q) => !q.matches_impl(v),
            Query::Eq(p, to) => Self::deref(v, p) == Some(to),
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
                .and_then(|s| s.as_str())
                .map(|s| s.contains(pat))
                .unwrap_or(false),
        }
    }

    fn contains(v: &serde_json::Value, pat: &serde_json::Value) -> bool {
        use serde_json::Value::*;
        match (v, pat) {
            (Null, Null) => true,
            (Bool(l), Bool(r)) => l == r,
            (Number(l), Number(r)) => l == r,
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
            (Array(v), pat) => v.iter().any(|v| v == pat), // but does work on primitives
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
        match path.get(0) {
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

    #[cfg(feature = "server")]
    pub(crate) fn where_clause(&self, first_idx: usize) -> String {
        let mut res = String::new();
        let mut bind_idx = first_idx;
        add_to_where_clause(&mut res, &mut bind_idx, self);
        res
    }

    #[cfg(feature = "server")]
    pub(crate) fn binds(&self) -> crate::Result<Vec<Bind<'_>>> {
        let mut res = Vec::new();
        add_to_binds(&mut res, self)?;
        Ok(res)
    }
}

#[cfg(feature = "server")]
fn add_to_where_clause(res: &mut String, bind_idx: &mut usize, query: &Query) {
    let mut initial_bind_idx = *bind_idx;
    match query {
        Query::All(v) => {
            res.push_str("TRUE");
            for q in v {
                res.push_str(" AND (");
                add_to_where_clause(&mut *res, &mut *bind_idx, q);
                res.push_str(")");
            }
        }
        Query::Any(v) => {
            res.push_str("FALSE");
            for q in v {
                res.push_str(" OR (");
                add_to_where_clause(&mut *res, &mut *bind_idx, q);
                res.push_str(")");
            }
        }
        Query::Not(q) => {
            res.push_str("NOT (");
            add_to_where_clause(&mut *res, &mut *bind_idx, q);
            res.push_str(")");
        }
        Query::Eq(path, _) => {
            res.push_str("snapshot");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            res.push_str(&format!(" = ${}", bind_idx));
            *bind_idx += 1;
        }
        Query::Le(path, _) => {
            res.push_str("CASE WHEN jsonb_typeof(snapshot");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            res.push_str(") = 'number' THEN (snapshot");
            add_path_to_clause(&mut *res, &mut initial_bind_idx, path);
            res.push_str(&format!(")::numeric <= ${} ELSE FALSE END", bind_idx));
            *bind_idx += 1;
        }
        Query::Lt(path, _) => {
            res.push_str("CASE WHEN jsonb_typeof(snapshot");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            res.push_str(") = 'number' THEN (snapshot");
            add_path_to_clause(&mut *res, &mut initial_bind_idx, path);
            res.push_str(&format!(")::numeric < ${} ELSE FALSE END", bind_idx));
            *bind_idx += 1;
        }
        Query::Ge(path, _) => {
            res.push_str("CASE WHEN jsonb_typeof(snapshot");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            res.push_str(") = 'number' THEN (snapshot");
            add_path_to_clause(&mut *res, &mut initial_bind_idx, path);
            res.push_str(&format!(")::numeric >= ${} ELSE FALSE END", bind_idx));
            *bind_idx += 1;
        }
        Query::Gt(path, _) => {
            res.push_str("CASE WHEN jsonb_typeof(snapshot");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            res.push_str(") = 'number' THEN (snapshot");
            add_path_to_clause(&mut *res, &mut initial_bind_idx, path);
            res.push_str(&format!(")::numeric > ${} ELSE FALSE END", bind_idx));
            *bind_idx += 1;
        }
        Query::Contains(path, _) => {
            res.push_str("snapshot");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            res.push_str(&format!(" @> ${}", bind_idx));
            *bind_idx += 1;
        }
        Query::ContainsStr(path, _) => {
            // TODO: Check normalizer_version and recompute at startup if not the right version
            // This will probably require adding a list of all the json paths to SearchableString's
            // in Object
            res.push_str("to_tsvector(snapshot");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            res.push_str(&format!(
                "->'_crdb-normalized') @@ phraseto_tsquery(${})",
                bind_idx
            ));
            *bind_idx += 1;
        }
    }
}

#[cfg(feature = "server")]
fn add_path_to_clause(res: &mut String, bind_idx: &mut usize, path: &[JsonPathItem]) {
    for _ in path {
        res.push_str(&format!("->${bind_idx}"));
        *bind_idx += 1;
    }
}

#[cfg(feature = "server")]
fn add_path_to_binds<'a>(res: &mut Vec<Bind<'a>>, path: &'a [JsonPathItem]) {
    for p in path {
        match p {
            JsonPathItem::Key(k) => res.push(Bind::Str(k)),
            JsonPathItem::Id(i) => res.push(Bind::I32(*i)),
        }
    }
}

#[cfg(feature = "server")]
pub(crate) enum Bind<'a> {
    Json(&'a serde_json::Value),
    Str(&'a str),
    String(String),
    Decimal(Decimal),
    I32(i32),
}

#[cfg(feature = "server")]
fn add_to_binds<'a>(res: &mut Vec<Bind<'a>>, query: &'a Query) -> crate::Result<()> {
    match query {
        Query::All(v) => {
            for q in v {
                add_to_binds(&mut *res, q)?;
            }
        }
        Query::Any(v) => {
            for q in v {
                add_to_binds(&mut *res, q)?;
            }
        }
        Query::Not(q) => {
            add_to_binds(&mut *res, q)?;
        }
        Query::Eq(p, v) => {
            add_path_to_binds(&mut *res, p);
            res.push(Bind::Json(v));
        }
        Query::Le(p, v) => {
            add_path_to_binds(&mut *res, p);
            res.push(Bind::Decimal(v.clone()));
        }
        Query::Lt(p, v) => {
            add_path_to_binds(&mut *res, p);
            res.push(Bind::Decimal(v.clone()));
        }
        Query::Ge(p, v) => {
            add_path_to_binds(&mut *res, p);
            res.push(Bind::Decimal(v.clone()));
        }
        Query::Gt(p, v) => {
            add_path_to_binds(&mut *res, p);
            res.push(Bind::Decimal(v.clone()));
        }
        Query::Contains(p, v) => {
            add_path_to_binds(&mut *res, p);
            res.push(Bind::Json(v));
        }
        Query::ContainsStr(p, v) => {
            add_path_to_binds(&mut *res, p);
            crate::check_string(&v)?;
            res.push(Bind::String(crate::fts::normalize(v)));
        }
    }
    Ok(())
}
