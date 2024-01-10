use bigdecimal::BigDecimal;

#[derive(Debug)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum JsonPathItem {
    Key(String),

    /// Negative values count from the end
    Id(isize),
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
    /// If the provided path does not exist, then this test will succeed
    Ne(Vec<JsonPathItem>, serde_json::Value),

    // Integers
    Le(Vec<JsonPathItem>, BigDecimal),
    Lt(Vec<JsonPathItem>, BigDecimal),
    Ge(Vec<JsonPathItem>, BigDecimal),
    Gt(Vec<JsonPathItem>, BigDecimal),

    // Arrays and object containment
    Contains(Vec<JsonPathItem>, serde_json::Value),

    // Full text search
    ContainsStr(Vec<JsonPathItem>, String),
}

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for Query {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Query> {
        Ok(match u.arbitrary::<u8>()? % 11 {
            0 => Query::All(
                u.arbitrary_iter()?
                    .collect::<arbitrary::Result<Vec<Query>>>()?,
            ),
            1 => Query::Any(
                u.arbitrary_iter()?
                    .collect::<arbitrary::Result<Vec<Query>>>()?,
            ),
            2 => Query::Not(u.arbitrary()?),
            3 => Query::Eq(
                u.arbitrary()?,
                u.arbitrary::<arbitrary_json::ArbitraryValue>()?.into(),
            ),
            4 => Query::Ne(
                u.arbitrary()?,
                u.arbitrary::<arbitrary_json::ArbitraryValue>()?.into(),
            ),
            5 => Query::Le(
                u.arbitrary()?,
                BigDecimal::new(u.arbitrary()?, u.arbitrary()?),
            ),
            6 => Query::Lt(
                u.arbitrary()?,
                BigDecimal::new(u.arbitrary()?, u.arbitrary()?),
            ),
            7 => Query::Ge(
                u.arbitrary()?,
                BigDecimal::new(u.arbitrary()?, u.arbitrary()?),
            ),
            8 => Query::Gt(
                u.arbitrary()?,
                BigDecimal::new(u.arbitrary()?, u.arbitrary()?),
            ),
            9 => Query::Contains(
                u.arbitrary()?,
                u.arbitrary::<arbitrary_json::ArbitraryValue>()?.into(),
            ),
            10 => Query::ContainsStr(u.arbitrary()?, u.arbitrary()?),
            _ => unimplemented!(),
        })
    }
}

impl Query {
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
            Query::Ne(p, to) => Self::deref(v, p) != Some(to),
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

    fn deref_num(v: &serde_json::Value, path: &[JsonPathItem]) -> Option<BigDecimal> {
        serde_json::from_value(Self::deref(v, path)?.clone()).ok()
    }

    fn deref<'a>(v: &'a serde_json::Value, path: &[JsonPathItem]) -> Option<&'a serde_json::Value> {
        match path.get(0) {
            None => None,
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
                    .checked_add_signed(*k)
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
    pub(crate) fn binds(&self) -> Vec<Bind<'_>> {
        let mut res = Vec::new();
        add_to_binds(&mut res, self);
        res
    }
}

#[cfg(feature = "server")]
fn add_to_where_clause(res: &mut String, bind_idx: &mut usize, query: &Query) {
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
            add_path_to_clause(&mut *res, path);
            res.push_str(&format!(" == ${}", bind_idx));
            *bind_idx += 1;
        }
        Query::Ne(path, _) => {
            res.push_str("snapshot");
            add_path_to_clause(&mut *res, path);
            res.push_str(&format!(" != ${}", bind_idx));
            *bind_idx += 1;
        }
        Query::Le(path, _) => {
            res.push_str("snapshot");
            add_path_to_clause(&mut *res, path);
            res.push_str(&format!("::numeric <= ${}", bind_idx));
            *bind_idx += 1;
        }
        Query::Lt(path, _) => {
            res.push_str("snapshot");
            add_path_to_clause(&mut *res, path);
            res.push_str(&format!("::numeric < ${}", bind_idx));
            *bind_idx += 1;
        }
        Query::Ge(path, _) => {
            res.push_str("snapshot");
            add_path_to_clause(&mut *res, path);
            res.push_str(&format!("::numeric >= ${}", bind_idx));
            *bind_idx += 1;
        }
        Query::Gt(path, _) => {
            res.push_str("snapshot");
            add_path_to_clause(&mut *res, path);
            res.push_str(&format!("::numeric > ${}", bind_idx));
            *bind_idx += 1;
        }
        Query::Contains(path, _) => {
            res.push_str("snapshot");
            add_path_to_clause(&mut *res, path);
            res.push_str(&format!(" @> ${}", bind_idx));
            *bind_idx += 1;
        }
        Query::ContainsStr(path, _) => {
            res.push_str("to_tsvector(snapshot");
            add_path_to_clause(&mut *res, path);
            res.push_str(&format!(") @@ phraseto_tsquery(${})", bind_idx));
            *bind_idx += 1;
        }
    }
}

#[cfg(feature = "server")]
fn add_path_to_clause(res: &mut String, path: &[JsonPathItem]) {
    for p in path {
        match p {
            JsonPathItem::Id(i) => {
                res.push_str(&format!("->{i}"));
            }
            JsonPathItem::Key(k) => {
                res.push_str(&format!("->'{k}'"));
            }
        }
    }
}

#[cfg(feature = "server")]
pub(crate) enum Bind<'a> {
    Json(&'a serde_json::Value),
    Str(&'a str),
    Decimal(BigDecimal),
}

#[cfg(feature = "server")]
fn add_to_binds<'a>(res: &mut Vec<Bind<'a>>, query: &'a Query) {
    match query {
        Query::All(v) => {
            for q in v {
                add_to_binds(&mut *res, q);
            }
        }
        Query::Any(v) => {
            for q in v {
                add_to_binds(&mut *res, q);
            }
        }
        Query::Not(q) => {
            add_to_binds(&mut *res, q);
        }
        Query::Eq(_, v) => {
            res.push(Bind::Json(v));
        }
        Query::Ne(_, v) => {
            res.push(Bind::Json(v));
        }
        Query::Le(_, v) => res.push(Bind::Decimal(v.clone())),
        Query::Lt(_, v) => res.push(Bind::Decimal(v.clone())),
        Query::Ge(_, v) => res.push(Bind::Decimal(v.clone())),
        Query::Gt(_, v) => res.push(Bind::Decimal(v.clone())),
        Query::Contains(_, v) => {
            res.push(Bind::Json(v));
        }
        Query::ContainsStr(_, v) => {
            res.push(Bind::Str(v));
        }
    }
}
