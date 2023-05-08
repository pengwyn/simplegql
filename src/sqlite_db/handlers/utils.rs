use super::*;

use crate::parsing;

pub fn field_is_basic_scalar(field: &parsing::Field) -> bool {
    !field.is_dynamic() && !field.is_list() && !field.is_object()
}

// pub fn read_id(row: &sqlite::Row) -> IDType {
//     let id: i64 = row.read("id");
//     let id = id as u64;
//     id
// }
pub fn read_id(row: &sqlite::Row) -> IDType {
    let id: &str = row.read("id");
    let id = id.to_string();
    id
}
    
    

pub fn find_obj_by_type(id: &IDType, typ: &'static parsing::Type, db: &SqliteDB) -> gql::Result<Option<ObjectRefCached>> {
    let obj = ObjectRefCached::new(typ, id.clone());
    let obj = obj.populate_row(&db)?;
    match obj.cached_query {
        None => Ok(None),
        _ => Ok(Some(obj))
    }
}
pub fn find_obj_by_type_name(id: &IDType, name: &str, api: &'static parsing::APIDefinition, db: &SqliteDB) -> gql::Result<Option<ObjectRefCached>> {
    let typ = api.types.get(name).unwrap();
    find_obj_by_type(id, typ, db)
}

impl ObjectRefCached {
    pub fn new(schema: &'static parsing::Type, id: IDType) -> ObjectRefCached {
        ObjectRefCached{
            obj: ObjectRef{schema,
                           id},
            cached_query: None,
        }
    }

    pub fn populate_row(self, db: &SqliteDB) -> gql::Result<ObjectRefCached> {
        if let Some(_) = self.cached_query {
            // println!("Using cached data");
            return Ok(self);
        }

        let mut statement = db.conn.prepare(
            format!("SELECT * FROM {} WHERE id = :id", self.obj.schema.name)
        ).map_err(|x| format!("SQL error: {x:?}"))?;
        statement.bind((":id", id_to_sqlite(self.obj.id.clone()))).map_err(|x| format!("SQL error: {x:?}"))?;

        let result = statement.iter()
            .map(|x| x.unwrap())
            .at_most_one()
            .unwrap_or_else(|_| panic!("SQL query for type should never find more than one type"));
            
        // println!("Result found: {:?}", result);
        match result {
            None => Ok(self),
            Some(row) => {
                let cached_query = CacheData::make(row, statement);
                Ok(ObjectRefCached{
                    cached_query: Some(cached_query),
                    ..self
                })
            }
        }
    }
}




pub fn create_obj(
    mut input_fields: ObjectHashMap,
    db: &'static SqliteDB,
    sch_obj: &'static parsing::Type,
    api: &'static parsing::APIDefinition,
) -> gql::Result<ObjectRefCached>
{
    let obj_ref = create_obj_basic_fields(&mut input_fields, db, sch_obj, api)?;

    // TODO: Cache the creation of the statement
    // Now add in any other links that were skipped above
    for field in &sch_obj.fields {
        if let Some(_) = field.dynamic_resolver {
            continue;
        }

        let gql_val = input_fields.remove(&field.name);
        let gql_field= match gql_val {
            Some(x) => x,
            _ => continue,
        };

        if let GQLValueType::Object(_) = &field.typ.kind {
            create_obj_links(field, gql_field, &obj_ref, db, sch_obj, api)?;
        } else if field.is_list() {
            create_obj_scalar_list(field, gql_field, &obj_ref, db, sch_obj, api)?;
        } else {
            return Err(format!("Shouldn't have got a '{}' in the inputs", field.name))?;
        }
    }

    if !input_fields.is_empty() {
        Err(format!("Extra fields present in input: {}", input_fields.keys().join(",")))?;
    }
        

    Ok(obj_ref)
}


pub fn create_obj_basic_fields(
    input_fields: &mut ObjectHashMap,
    db: &'static SqliteDB,
    sch_obj: &'static parsing::Type,
    _api: &'static parsing::APIDefinition,
) -> gql::Result<ObjectRefCached>
{
    let mut col_names = Vec::new();
    let mut col_val_ids = Vec::new();
    let mut col_vals = Vec::new();
    if let Some(gql_val) = input_fields.remove("id") {
        let sqlite_val = convert_gql_value(gql_val, &IDType::internal_type())?;
        col_names.push("\"id\"".into());
        col_val_ids.push(":id".into());
        col_vals.push(sqlite_val);
    }
    for (indx,field) in sch_obj.fields.iter().enumerate() {
        if let Some(_) = field.dynamic_resolver {
            continue;
        }
        // Worry about lists later
        if field.is_list() {
            continue;
        }
        if let GQLValueType::Object(_) = field.typ.kind {
            continue;
        }

        let gql_val = input_fields.remove(&field.name);
        let sqlite_val = match gql_val {
            None | Some(gql::Value::Null) => {
                if field.typ.is_nn {
                    return Err(gql::Error::new(format!("Non-nullable field '{}' not present", field.name)));
                }
                // TODO: Other defaults
                // sqlite::Value::Null

                // If null, don't add this in
                // Note: might have to change if we want to cache the statement itself.
                continue;
            },
            Some(gql_val_accessor) => convert_gql_value(gql_val_accessor, &field.typ.kind)?
        };

        // TODO lists
        // TODO object fields
            // GQLValueType::Enum(_) | GQLValueType::String => sqlite::Value::
        col_names.push(format!("\"{}\"", field.internal_name));
        let id = format!(":{}{}", field.internal_name, indx);
        col_val_ids.push(id);

        col_vals.push(sqlite_val);
    }
    let mut statement;
    if col_names.is_empty() {
        statement = db.conn.prepare(format!("INSERT INTO {} DEFAULT VALUES RETURNING *;", sch_obj.name))
            .map_err(|x| format!("SQL error: {x:?}"))?;
    } else {
        statement = db.conn.prepare(
            format!("INSERT INTO {} ({}) VALUES ({}) RETURNING *;",
                    sch_obj.name, col_names.iter().join(", "), col_val_ids.iter().join(", "))
        ).map_err(|x| format!("SQL error: {x:?}"))?;

        statement.bind_iter(zip(col_val_ids.iter().map(|x| x.as_str()), col_vals))?;
    }
    let result = statement.iter()
        .map(|x| x.unwrap())
        .exactly_one()
        .unwrap_or_else(|_| panic!("SQL query for type should never find more than one type"));

    let id = read_id(&result);
    let obj_ref = ObjectRefCached {
        obj: ObjectRef {
            id,
            schema: sch_obj,
        },
        cached_query: Some(CacheData::make(result, statement))
    };

    Ok(obj_ref)
}

pub fn create_obj_links(
    field: &parsing::Field,
    gql_field: gql::Value,
    obj_ref: &ObjectRefCached,
    db: &'static SqliteDB,
    sch_obj: &'static parsing::Type,
    api: &'static parsing::APIDefinition,
) -> gql::Result<()>
{
    let all_items = if field.is_list() {
        match gql_field {
            gql::Value::List(x) => x,
            _ => Err("Need a list as input!")?,
        }
    } else {
        vec![gql_field]
    };

    let target_name = match &field.typ.kind {
        GQLValueType::Object(x) => x,
        _ => panic!("Should have an object by the time we get here"),
    };

    let table_name;
    if let Some(src_field_name) = &field.incoming_source_name {
        table_name = edge_table_name(target_name, src_field_name, &sch_obj.name);
    } else {
        table_name = edge_table_name(&sch_obj.name, &field.internal_name, target_name);
    }

    let mut statement = db.conn.prepare(
        format!("INSERT INTO {} ({}) VALUES ({});",
                table_name,
                ["source", "target"].join(","),
                [":source", ":target"].join(","),
        )
    ).map_err(|x| format!("SQL error: {x:?}"))?;
    if field.is_incoming() {
        statement.bind((":target", id_to_sqlite(obj_ref.obj.id.clone())))?;
    } else {
        statement.bind((":source", id_to_sqlite(obj_ref.obj.id.clone())))?;
    }

    for item in all_items {
        let field_obj = match item {
            gql::Value::Null => {
                if field.typ.is_nn {
                    return Err(gql::Error::new(format!("Non-nullable field '{}' not present", field.name)));
                } else {
                    continue;
                }
            },
            gql::Value::Object(x) => x,
            _ => Err("Need an object")?,
        };
        // For now, only going to allow creating links to existing items
        let mut d: ObjectHashMap = field_obj
            .into_iter()
            .map(|(x,y)| (x.as_str().into(), y))
            .collect();
        let id_accessor = d.remove("id");
        let target_id: IDType;

        if let Some(id_accessor) = id_accessor {
            target_id = CanBeID::load_val(id_accessor)?;

            let check_obj = find_obj_by_type_name(&target_id, target_name, api, db)?;
            if let None = check_obj {
                return Err(format!("Cannot link to existing type '{target_name}' with id '{target_id}', it doesn't exist"))?;
            }

            if !d.is_empty() {
                return Err(format!("Cannot provide an id and other fields at the same time: {}", d.keys().join(",")))?;
            }
        } else {
            let input_fields = d;
            let target_type = api.types.get(target_name).unwrap();
            let target_ref = create_obj(input_fields, db, target_type, api)?;

            target_id = target_ref.id().clone();
        }


        if field.is_incoming() {
            statement.bind((":source", id_to_sqlite(target_id)))?;
        } else {
            statement.bind((":target", id_to_sqlite(target_id)))?;
        }

        let res: sqlite::Result<Vec<_>> = statement
            .iter()
            .collect();
        res?;

        statement.reset()?;
    }

    Ok(())
}

pub fn create_obj_scalar_list(
    field: &parsing::Field,
    gql_field: gql::Value,
    obj_ref: &ObjectRefCached,
    db: &'static SqliteDB,
    sch_obj: &'static parsing::Type,
    _api: &'static parsing::APIDefinition,
) -> gql::Result<()>
{
    let gql_list = match gql_field {
        gql::Value::List(x) => x,
        _ => Err("Not a list")?,
    };

    let table_name = field_table_name(&sch_obj.name, &field.name);

    let mut statement = db.conn.prepare(
        format!("INSERT INTO {table_name} ('source', 'value') VALUES (:source, :value);")
    ).map_err(|x| format!("SQL error: {x:?}"))?;
    statement.bind((":source", id_to_sqlite(obj_ref.obj.id.clone())))?;

    for gql_val in gql_list.into_iter() {
        let sqlite_val = match field.typ.kind {
            // Handle objects later
            GQLValueType::Object(_) => panic!("Shouldn't get here"),
            GQLValueType::NamedType(_) => panic!("Shouldn't get here"),
            _ => {
                convert_gql_value(gql_val, &field.typ.kind)?
            }
        };

        statement.bind((":value", sqlite_val))?;
        let res: sqlite::Result<Vec<_>> = statement
            .iter()
            .collect();
        res?;

        statement.reset()?;
    }

    Ok(())
}

pub fn sql_results_as_list_or_single(mut statement: sqlite::Statement, is_list: bool, target_type: &'static parsing::Type) -> gql::Result<Option<FieldResultSQL>> {
    let results = statement.iter()
        .map(|x| x.unwrap())
        .map(|row| {
            Ok::<_, gql::Error>(ObjectRefCached{
                obj: ObjectRef{
                    schema: target_type,
                    id: sqlite_to_id(row[0].clone())?,
                },
                cached_query: None,
            })})
        .try_collect()?;

    if is_list {
        return Ok(Some(FieldResult::RefList(results)))
    } else {
        let result = results.into_iter().at_most_one()
            .unwrap_or_else(|_| panic!("SQL query for single object should never find more than one"));
        let out = match result {
            None => Ok(None),
            Some(result) => {
                Ok(Some(FieldResult::Ref(result)))
            }
        };

        return out;
    }

}

pub fn sql_clear_object_non_scalar(
    field: &parsing::Field,
    obj_refs: &Vec<ObjectRefCached>,
    db: &'static SqliteDB,
    sch_obj: &'static parsing::Type,
    _api: &'static parsing::APIDefinition,
) -> gql::Result<()> {
    let table_name = if let GQLValueType::Object(target_name) = &field.typ.kind {
        if let Some(src_field_name) = &field.incoming_source_name {
            edge_table_name(target_name, src_field_name, &sch_obj.name)
        } else {
            edge_table_name(&sch_obj.name, &field.internal_name, target_name)
        }
    } else if field.is_list() {
        assert!(!field.is_incoming());
        field_table_name(&sch_obj.name, &field.internal_name)
    } else {
        panic!("Shouldn't get here");
    };

    let sql_ids: Vec<_> = obj_refs.iter().enumerate()
        .map(|(i,_)| format!(":source{i}"))
        .collect();

    let sql_id_str = sql_ids.join(",");

    let sql = if field.is_incoming() {
        format!("DELETE FROM {} WHERE \"target\" IN ({}) RETURNING *;", table_name, sql_id_str)
    } else {
        format!("DELETE FROM {} WHERE \"source\" IN ({}) RETURNING *;", table_name, sql_id_str)
    };

    let mut statement = db.conn.prepare(sql)
        .map_err(|x| format!("SQL error: {x:?}"))?;

    for (sql_id, obj) in zip(sql_ids, obj_refs) {
        statement.bind((sql_id.as_str(), id_to_sqlite(obj.id().clone())))?;
    }
    exe_statement(&mut statement)?;

    Ok(())
}