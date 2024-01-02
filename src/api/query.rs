#[derive(Debug)]
#[non_exhaustive]
pub enum JsonPathItem {
    Key(String),
    Id(isize),
}

#[derive(Debug)]
#[non_exhaustive]
pub enum JsonNumber {
    F64(f64),
    I64(i64),
}

impl JsonNumber {
    #[cfg(feature = "server")]
    fn to_bind(&self) -> Bind {
        match self {
            JsonNumber::F64(v) => Bind::F64(*v),
            JsonNumber::I64(v) => Bind::I64(*v),
        }
    }
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
    Ne(Vec<JsonPathItem>, serde_json::Value),

    // Integers
    Le(Vec<JsonPathItem>, JsonNumber),
    Lt(Vec<JsonPathItem>, JsonNumber),
    Ge(Vec<JsonPathItem>, JsonNumber),
    Gt(Vec<JsonPathItem>, JsonNumber),

    // Arrays and object subscripting
    Contains(Vec<JsonPathItem>, serde_json::Value),

    // Full text search
    ContainsStr(Vec<JsonPathItem>, String),
}

impl Query {
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
            res.push_str(&format!(" <= ${}", bind_idx));
            *bind_idx += 1;
        }
        Query::Lt(path, _) => {
            res.push_str("snapshot");
            add_path_to_clause(&mut *res, path);
            res.push_str(&format!(" < ${}", bind_idx));
            *bind_idx += 1;
        }
        Query::Ge(path, _) => {
            res.push_str("snapshot");
            add_path_to_clause(&mut *res, path);
            res.push_str(&format!(" >= ${}", bind_idx));
            *bind_idx += 1;
        }
        Query::Gt(path, _) => {
            res.push_str("snapshot");
            add_path_to_clause(&mut *res, path);
            res.push_str(&format!(" > ${}", bind_idx));
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
    I64(i64),
    F64(f64),
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
        Query::Le(_, v) => res.push(v.to_bind()),
        Query::Lt(_, v) => res.push(v.to_bind()),
        Query::Ge(_, v) => res.push(v.to_bind()),
        Query::Gt(_, v) => res.push(v.to_bind()),
        Query::Contains(_, v) => {
            res.push(Bind::Json(v));
        }
        Query::ContainsStr(_, v) => {
            res.push(Bind::Str(v));
        }
    }
}
