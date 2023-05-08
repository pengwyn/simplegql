use crate::handlers::*;
use crate::utils::*;
use crate::parsing;

use itertools::Itertools;
use std::collections::HashMap;

// type IDType = u64;
// const id_sql_kind = "INTEGER";
// const id_sql_column_def = "AUTOINCREMENT";
type IDType = String;
const id_sql_kind: &str = "TEXT";
const id_sql_column_def: &str = "DEFAULT (lower(hex(randomblob(16))))";
// pub fn id_to_sqlite(id: IDType) -> sqlite::Value {
//     sqlite::Value::Integer(id as i64)
// }
// pub fn sqlite_to_id(id: sqlite::Value) -> gql::Result<IDType> {
//     if let sqlite::Value::Integer(id) = id {
//         Ok(id as IDType)
//     } else {
//         Err("ID is not an integer")?
//     }
// }
pub fn id_to_sqlite(id: IDType) -> sqlite::Value {
    sqlite::Value::String(id)
}
pub fn sqlite_to_id(id: sqlite::Value) -> gql::Result<IDType> {
    if let sqlite::Value::String(id) = id {
        Ok(id)
    } else {
        Err("ID is not an integer")?
    }
}
            

mod prepare;
pub mod handlers;
pub use handlers::{ObjectRefCached, HookFn};

// #[deriveClone]
pub struct SqliteDB {
    _conn_string: String,
    pub conn: sqlite::ConnectionWithFullMutex,
}

impl SqliteDB {

    pub fn new(conn_string: &str) -> ResultAll<SqliteDB> {
        let conn = sqlite::Connection::open_with_full_mutex(conn_string)?;
        conn.execute("PRAGMA foreign_keys = true;")?;
        let db = SqliteDB{
            _conn_string: conn_string.to_owned(),
            conn,
        };
        Ok(db)
    }

}
    

// fn _is_matching_type_gql(val: gql::Value, required: GQLValueType) -> bool {
//     match required {
//         // GQLValueType::Null => if let gql::Value::Null = val { return true },
//         GQLValueType::Float => if let gql::Value::Number(num) = val { return num.is_f64() },
//         GQLValueType::Integer => if let gql::Value::Number(num) = val { return num.is_i64() },
//         GQLValueType::String => if let gql::Value::String(_) = val { return true },
//         GQLValueType::Boolean => if let gql::Value::Boolean(_) = val { return true },
//         // GQLValueType::Binary => if let gql::Value::Binary(_) = val { return true },
//         // GQLValueType::Enum => if let gql::Value::Enum(_) = val { return true },
//         // GQLValueType::List => if let gql::Value::List(_) = val { return true },
//         // GQLValueType::Object => if let gql::Value::Object(_) = val { return true },
//         GQLValueType::NamedType(_) => panic!("Not yet"),
//         GQLValueType::Object(_) => panic!("Should never get here"),
//         GQLValueType::CustomScalar(_) => panic!("Should never get here"),
//         // This doesn't check the valid enum options
//         GQLValueType::Enum(_) => if let gql::Value::String(_) = val { return true },
//     }
//     return false;
// }

fn convert_sqlite_value(val: sqlite::Value, required: &GQLValueType) -> ResultAll<gql::Value> {
    match val {
        sqlite::Value::Binary(_vec) => {
            match required {
                // GQLValueType::Binary => {
                //     let bytes: bytes::Bytes = b.into();
                //     Value(bytes);
                // },
                _ => false
            }
        },
        sqlite::Value::Float(x) => {
            match required {
                GQLValueType::Float => return Ok(gql::Value::from(x)),
                _ => false,
            }
        }
        sqlite::Value::Integer(x) => {
            match required {
                GQLValueType::Boolean => return Ok(gql::Value::from(x != 0)),
                GQLValueType::Integer => return Ok(gql::Value::from(x)),
                _ => false
            }
        },
        sqlite::Value::String(x) => {
            match required {
                GQLValueType::String | GQLValueType::Enum(_) => return Ok(gql::Value::from(x)),
                _ => false,
            }
        },
        sqlite::Value::Null => { false },
    };
    return Err(format!("Can't convert a value to {required:?}"))?;
}

fn sqlite_type(kind: &GQLValueType, api: &parsing::APIDefinition) -> Option<String> {
    match kind {
        GQLValueType::Boolean => Some(String::from("INTEGER")),
        GQLValueType::Float => Some(String::from("REAL")),
        GQLValueType::Integer => Some(String::from("INTEGER")),
        GQLValueType::String => Some(String::from("TEXT")),
        GQLValueType::NamedType(_) => panic!("Shouldn't get here"),
        GQLValueType::Object(_) => Some(String::from("INTEGER")),
        GQLValueType::Enum(name) => Some(format!("TEXT REFERENCES ENUM_{name}(item)")),
        GQLValueType::CustomScalar(scalar) => {
            let scalar = api.scalars.get(scalar).expect("Missing scalar");
            sqlite_type(&scalar.backed_by, api)
        },
    }
}

// fn convert_gql_value(val_accessor: gqld::ValueAccessor, required: &GQLValueType) -> gql::Result<sqlite::Value> {
//     match required {
//         GQLValueType::Float => val_accessor.f64().map(|x| x.into()),
//         GQLValueType::Integer => val_accessor.i64().map(|x| x.into()),
//         GQLValueType::String => val_accessor.string().map(|x| x.into()),
//         GQLValueType::Boolean => val_accessor.boolean().map(|x| (x as i64).into()),
//         GQLValueType::NamedType(_) => panic!("Not yet"),
//         GQLValueType::Object(_) => panic!("Should never get here"),
//         GQLValueType::CustomScalar(_) => panic!("TODO"),
//         // TODO: This doesn't validate against the allowed enum options
//         GQLValueType::Enum(_) => val_accessor
//             .enum_name()
//             // .or_else(|_| val_accessor.string()
//                 // .or_else(|_| Err("Not a valid enum or string")?))
//             .map(|x| x.into()),
//     }
// }
fn convert_gql_value2(val: gql::Value, required: &GQLValueType) -> gql::Result<sqlite::Value> {
    if let gql::Value::Null = val {
        return Ok(sqlite::Value::Null);
    }
    match required {
        GQLValueType::Float => match val {
            gql::Value::Number(x) => Ok(x.as_f64().unwrap().into()),
            _ => Err("Need a float")?,
        },
        GQLValueType::Integer => match val {
            gql::Value::Number(x) => Ok(x.as_i64().unwrap().into()),
            _ => Err("Need an integer")?,
        },
        GQLValueType::String => match val {
            gql::Value::String(x) => Ok(x.into()),
            x => Err(format!("Need a string, got {:?} instead", x))?,
        },
        GQLValueType::Boolean => match val {
            gql::Value::Boolean(x) => Ok(sqlite::Value::Integer(if x { 1 } else { 0 })),
            _ => Err("Need a bool")?,
        },
        GQLValueType::NamedType(_) => panic!("Not yet"),
        GQLValueType::Object(_) => panic!("Should never get here"),
        GQLValueType::CustomScalar(_) => panic!("TODO"),
        // TODO: This doesn't validate against the allowed enum options
        GQLValueType::Enum(_) => match val {
            gql::Value::Enum(x) => Ok(x.to_string().into()),
            // We need to allow strings as inputs too, as that's the only way json variables can be presented.
            gql::Value::String(x) => Ok(x.into()),
            _ => Err("Need an enum")?,
        }
    }
}

fn edge_table_name(src_type: &str, field: &str, target_typ: &str) -> String {
    format!("EDGES_{src_type}_{field}_{target_typ}")
}

fn field_table_name(src_type: &str, field: &str) -> String {
    format!("FIELDS_{src_type}_{field}")
}

fn exe_statement(statement: &mut sqlite::Statement) -> gql::Result<()> {
    let res: sqlite::Result<Vec<_>> = statement
        .iter()
        .collect();
    res?;

    Ok(())
}