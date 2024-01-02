#[non_exhaustive]
pub enum JsonPathItem {
    Key(String),
    Id(usize),
}

#[non_exhaustive]
pub enum JsonNumber {
    F64(f64),
    I64(i64),
    U64(u64),
}

#[non_exhaustive]
pub enum Query {
    // Logic operators
    All(Vec<Query>),
    Any(Vec<Query>),
    Not(Box<Query>),

    // Any/all the values in the array at JsonPathItem must match Query
    AnyIn(Vec<JsonPathItem>, Box<Query>),
    AllIn(Vec<JsonPathItem>, Box<Query>),

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
