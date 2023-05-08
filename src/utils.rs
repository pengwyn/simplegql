use std::error::Error;
pub use itertools::Itertools;

pub use std::collections::HashMap;

pub use async_graphql::{
    self as gql,
    dynamic as gqld,
};

#[derive(Debug)]
pub struct SimpleGQLError {
    err: String
}

use std::fmt;
impl fmt::Display for SimpleGQLError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SimpleGQLError({})", self.err)
    }
}

impl Error for SimpleGQLError {}

impl From<String> for SimpleGQLError {
    fn from(x: String) -> Self {
        SimpleGQLError{ err: x }
    }
}

pub type ErrorAll = Box<dyn Error>;
pub type ResultAll<T> = core::result::Result<T, ErrorAll>;

pub type ObjectHashMap = HashMap<String, gql::Value>;


// TODO: Make this return owned values rather than accessors
pub fn hashmap_from_gql_obj<'a>(obj: &'a gqld::ObjectAccessor<'a>) -> HashMap<String, gql::Value> {
    obj
        .iter()
        .map(|(k,v)| (k.as_str().to_owned(), value_accessor_to_value(v)))
        .collect()
}

pub fn value_accessor_to_value(v: gqld::ValueAccessor) -> gql::Value {
    if let Ok(x) = v.boolean() {
        return gql::Value::Boolean(x);
    } else if let Ok(x) = v.string() {
        return gql::Value::String(x.to_string());
    } else if let Ok(x) = v.enum_name() {
        return gql::Value::Enum(gql::Name::new(x.clone()));
    } else if let Ok(x) = v.i64() {
        return gql::Value::Number(gql::Number::from(x));
    } else if let Ok(x) = v.f64() {
        return gql::Value::Number(gql::Number::from_f64(x).unwrap());
    } else if v.is_null() {
        return gql::Value::Null;
    } else if let Ok(x) = v.list() {
        return gql::Value::List(x.iter()
                                .map(value_accessor_to_value)
                                // .map(|x| value_accessor_to_value(&x))
                                .collect());
    } else if let Ok(x) = v.object() {
        return gql::Value::Object(x.iter()
                                  .map(|(k,v)| (k.clone(), value_accessor_to_value(v)))
                                  .collect());
    }

    panic!("Shouldn't get here");
}