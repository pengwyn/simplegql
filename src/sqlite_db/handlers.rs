use super::*;

mod utils;
use utils::*;
pub use utils::read_id;

use crate::parsing::GQLIndexMap;

use std::iter::zip;
use gql::indexmap::IndexMap;

type FieldResultSQL = FieldResult<ObjectRefCached>;

type CacheMap = HashMap<String,sqlite::Value>;
#[derive(Debug, Clone)]
struct CacheData {
    map: CacheMap
}

impl CacheData {
    fn make(row: sqlite::Row, statement: sqlite::Statement) -> CacheData {
        let values: Vec<sqlite::Value> = row.into();
        let cols = statement.column_names();
        let map = Iterator::zip(
            cols.into_iter().map(|x| x.to_owned()),
            values.into_iter(),
        )
            .collect();
        CacheData{map}
    }

    pub fn as_map(&self) -> &CacheMap {
        &self.map
    }
}
#[derive(Debug, Clone)]
pub struct ObjectRefCached {
    obj: ObjectRef<IDType>,
    cached_query: Option<CacheData>,
}
impl SS for ObjectRefCached {}
impl IDTrait<IDType> for ObjectRefCached {
    fn id(&self) -> &IDType {
        &self.obj.id
    }
}


pub fn handler_field_scalar<'a>(
    db: &'static SqliteDB,
    sch_field: &'static parsing::Field,
    sch_type: &'static parsing::Type,
    obj_ref: &ObjectRefCached,
    ctx: gqld::ResolverContext<'a>,
) -> gql::Result<Option<FieldResultSQL>>
{
    // println!("Handling field {} on type {}", sch_field.name, sch_type.name);

    let obj = obj_ref.to_owned().populate_row(&db)?;
    let row = obj.cached_query
        .as_ref()
        .unwrap_or_else(|| panic!("Can't possibly get here without a valid row: {:#?}", obj_ref))
        .as_map();

    // println!("The cached data was {:?}", row);

    if let Some(_) = sch_field.dynamic_resolver {
        Err("TODO dynamic resolvers")?;
    }
    let field_val = &row[&sch_field.internal_name];
    if let sqlite::Value::Null = field_val {
        if sch_field.typ.is_nn {
            return Err("Null in DB when non-nullable expected")?;
        }
        return Ok(None);
    }

    let val = match &sch_field.typ.kind {
        GQLValueType::NamedType(_) => panic!("not here yet"),
        other => convert_sqlite_value(field_val.clone(), &other),
    }
    .map_err(|e| format!("Incorrect datatype '{:?}': {e}", field_val.kind()))?;

    Ok(Some(FieldResult::Value(val)))
}

pub fn handler_field_scalar_list<'a>(
    db: &'static SqliteDB,
    sch_field: &'static parsing::Field,
    sch_type: &'static parsing::Type,
    obj_ref: &ObjectRefCached,
    ctx: gqld::ResolverContext<'a>,
) -> gql::Result<Option<FieldResultSQL>>
{
    // println!("Handling field {} on type {}", sch_field.name, sch_type.name);

    let table_name = field_table_name(&sch_type.name, &sch_field.name);

    let mut statement = db.conn.prepare(
        format!("SELECT value FROM {table_name} WHERE source = :source")
    ).map_err(|x| format!("SQL error: {x:?}"))?;
    statement.bind((":source", id_to_sqlite(obj_ref.id().clone()))).map_err(|x| format!("SQL error: {x:?}"))?;

    let mut l = Vec::new();

    for row in statement.iter() {
        let row = row?;

        let sql_val = &row[0];
        let gql_val = convert_sqlite_value(sql_val.clone(), &sch_field.typ.kind)
            .map_err(|e| format!("Incorrect datatype '{:?}': {e}", sql_val.kind()))?;

        l.push(gql_val);
    }
    let gql_l = FieldResult::Value(gql::Value::from(l));

    Ok(Some(gql_l))
}

pub fn handler_field_object<'a>(
    db: &'static SqliteDB,
    sch_field: &'static parsing::Field,
    sch_type: &'static parsing::Type,
    sch_target_type: &'static parsing::Type,
    obj_ref: &ObjectRefCached,
) -> gql::Result<Option<FieldResultSQL>>
{
    // println!("Handling object field {} on type {} pointing at type {}", sch_field.name, sch_type.name, sch_target_type.name);

    let target_name = match &sch_field.typ.kind {
        GQLValueType::Object(target_name) => target_name,
        _ => panic!("Shouldn't get here"),
    };

    let table_name = edge_table_name(&sch_type.name, &sch_field.internal_name, target_name);
    let mut statement = db.conn.prepare(
        format!("SELECT target FROM {} WHERE source = :source", table_name)
    ).map_err(|x| format!("SQL error: {x:?}"))?;
    statement.bind((":source", id_to_sqlite(obj_ref.id().clone()))).map_err(|x| format!("SQL error: {x:?}"))?;

    sql_results_as_list_or_single(statement, sch_field.is_list(), sch_target_type)
}

pub fn handler_field_object_reverse<'a>(
    db: &'static SqliteDB,
    sch_field: &'static parsing::Field,
    sch_type: &'static parsing::Type,
    sch_target_type: &'static parsing::Type,
    src_field_name: &String,
    obj_ref: &ObjectRefCached,
) -> gql::Result<Option<FieldResultSQL>>
{
    // println!("Handling object field {} on type {} pointing at type {}", sch_field.name, sch_type.name, sch_target_type.name);

    let target_name = match &sch_field.typ.kind {
        GQLValueType::Object(target_name) => target_name,
        _ => panic!("Shouldn't get here"),
    };

    let table_name = edge_table_name(target_name, &src_field_name, &sch_type.name);
    let mut statement = db.conn.prepare(
        format!("SELECT source FROM {} WHERE target = :target", table_name)
    ).map_err(|x| format!("SQL error: {x:?}"))?;
    statement.bind((":target", id_to_sqlite(obj_ref.id().clone()))).map_err(|x| format!("SQL error: {x:?}"))?;

    sql_results_as_list_or_single(statement, sch_field.is_list(), sch_target_type)
}

pub fn handler_filter_list<'a>(
    db: &'static SqliteDB,
    sch_type: &'static parsing::Type,
    list: &[ObjectRefCached],
    filter: QueryFilter<IDType>,
    ctx: gqld::ResolverContext<'a>,
) -> gql::Result<Vec<ObjectRefCached>>
{
    // TODO
    Ok(list.to_owned())
}

pub fn handler_get_type<'a>(
    db: &'static SqliteDB,
    sch_obj: &'static parsing::Type,
    id: IDType,
    ctx: gqld::ResolverContext<'a>
) -> gql::Result<Option<ObjectRefCached>>
{
    println!("get{} for id={id}", sch_obj.name);
    find_obj_by_type(&id, sch_obj, db)
}

pub fn handler_query_type<'a>(
    db: &'static SqliteDB,
    sch_obj: &'static parsing::Type,
    filter: &QueryFilter<IDType>,
) -> gql::Result<Vec<ObjectRefCached>>
{
    println!("query{}", sch_obj.name);
    let mut sql = format!("SELECT * FROM {}", sch_obj.name);
    if let Some(ids) = &filter.ids {
        // This is bad, should be binding paramters, but will ignore that for now.
        let id_str = ids.iter()
            .map(|id| id.as_sql_literal())
            .join(",");
        sql += &format!("\nWHERE id IN ({id_str})");
    }
    if let Some(first) = &filter.first {
        sql += &format!("\nLIMIT {first}");
        if let Some(offset) = &filter.offset {
            sql += &format!("\nOFFSET {offset}");
        }
    }
    sql += ";";
    let mut statement = db.conn.prepare(sql)
        .map_err(|x| format!("SQL error: {x:?}"))?;

    let l: Vec<ObjectRefCached> = statement.iter()
        .map(|row| row.unwrap())
        .map(|row| {
            let id = read_id(&row);
            ObjectRefCached {
                obj: ObjectRef{
                    schema: sch_obj,
                    id,
                },
                cached_query: None,
            }
        })
        .collect();
    Ok(l)
}


pub fn handler_add_type<'a>(
    db: &'static SqliteDB,
    sch_obj: &'static parsing::Type,
    api: &'static parsing::APIDefinition,
    obj: gqld::ObjectAccessor,
    upsert: bool,
) -> gql::Result<ObjectRefCached>
{
    println!("add{}", sch_obj.name);
    let mut input_fields = hashmap_from_gql_obj(&obj);

    if upsert {
        let id = input_fields.remove("id").ok_or("Need an id for upsert")?;
        let id = <IDType as CanBeID>::load_val(id.clone())?;

        let obj_ref = find_obj_by_type(&id, sch_obj, db)?;
        if let Some(obj_ref) = obj_ref {
            handler_update_type(
                db,
                sch_obj,
                api,
                &QueryFilter {
                    ids: Some(vec![obj_ref.id().clone()]),
                    ..QueryFilter::default()
                },
                Some(input_fields),
                None
            )?;
            return Ok(obj_ref);
        }
        // Otherwise continue through to normal creation.
    }
    let obj_ref = create_obj(input_fields, db, sch_obj, api)?;

    Ok(obj_ref)
}

pub fn handler_update_type<'a>(
    db: &'static SqliteDB,
    sch_obj: &'static parsing::Type,
    api: &'static parsing::APIDefinition,
    filter: &QueryFilter<IDType>,
    mut arg_set: Option<ObjectHashMap>,
    mut arg_remove: Option<ObjectHashMap>,
) -> gql::Result<Vec<ObjectRefCached>> {
    let ids = match &filter.ids {
        Some(ids) => ids,
        None => panic!("Update needs ids")
    };

    println!("update{} with ids: {}", sch_obj.name, ids.join(","));

    let objs = handler_query_type(db, sch_obj, &filter)?;
    if objs.is_empty() {
        return Ok(vec![]);
    }

    if let Some(arg_set) = &mut arg_set {
        // First the basic fields

        let mut cols = Vec::new();
        for field in &sch_obj.fields {
            if !field_is_basic_scalar(field) {
                continue;
            }

            let gql_val = arg_set.remove(&field.name);
            let sqlite_val = match gql_val {
                None => continue,
                Some(gql::Value::Null) if field.typ.is_nn => {
                    return Err(gql::Error::new(format!("Non-nullable field '{}' trying to be set to null", field.name)));
                },
                Some(gql_val) => convert_gql_value2(gql_val, &field.typ.kind)?
            };

            let col_name = format!("\"{}\"", field.internal_name);
            let col_id = format!(":{}", field.internal_name);

            cols.push((col_name, col_id, sqlite_val));
        }
        let mut statement;
        if !cols.is_empty() {
            let assignments = cols.iter()
                .map(|(name, id, _)| format!("{name} = {id}"))
                .join(", ");
            let cond = format!("\"id\" IN ({})",
                               ids.iter().map(|x| x.as_sql_literal()).join(","));
            let sql = format!("UPDATE {} SET {} WHERE {};",
                              sch_obj.name,
                              assignments,
                              cond);
            statement = db.conn.prepare(sql)
                .map_err(|x| format!("SQL error: {x:?}"))?;

            statement.bind_iter(cols.iter()
                                .map(|(_,id,val)| (id.as_str(), val)))?;
            let result: sqlite::Result<Vec<_>> = statement.iter().collect();
            result?;
        }

        // TODO Then object links
        for field in &sch_obj.fields {
            if let Some(_) = field.dynamic_resolver {
                continue;
            }

            let gql_val = arg_set.remove(&field.name);
            let gql_field= match gql_val {
                Some(x) => x,
                _ => continue,
            };

            if let GQLValueType::Object(_) = &field.typ.kind {
                // Clear out anything from the table first
                sql_clear_object_non_scalar(field, &objs, db, sch_obj, api)?;
                for obj_ref in &objs {
                    create_obj_links(field, gql_field.clone(), &obj_ref, db, sch_obj, api)?;
                }
            } else if field.is_list() {
                sql_clear_object_non_scalar(field, &objs, db, sch_obj, api)?;
                for obj_ref in &objs {
                    create_obj_scalar_list(field, gql_field.clone(), &obj_ref, db, sch_obj, api)?;
                }
            } else {
                return Err(format!("Shouldn't have got a '{}' in the inputs", field.name))?;
            }
        }

        if !arg_set.is_empty() {
            let leftover = arg_set.keys().join(",");
            Err(format!("There were extra arguments leftover: {}", leftover))?;
        }
    }

    if let Some(arg_remove) = &mut arg_remove {
        // First the basic fields

        let mut cols = Vec::new();
        for field in &sch_obj.fields {
            if !field_is_basic_scalar(field) {
                continue;
            }

            let gql_val = arg_remove.remove(&field.name);
            match gql_val {
                None => continue,
                Some(gql::Value::Null) => (),
                Some(gql_val) => {
                    return Err(gql::Error::new(format!("Require null for remove field '{}'", field.name)));
                }
            };

            let col_name = format!("\"{}\"", field.internal_name);

            cols.push(col_name);
        }
        let mut statement;
        if !cols.is_empty() {
            let assignments = cols.iter()
                .map(|name| format!("{name} = NULL"))
                .join(", ");
            let cond = format!("\"id\" IN ({})",
                               ids.iter().map(|x| x.as_sql_literal()).join(","));
            let sql = format!("UPDATE {} SET {} WHERE {};",
                              sch_obj.name,
                              assignments,
                              cond);
            statement = db.conn.prepare(sql)
                .map_err(|x| format!("SQL error: {x:?}"))?;

            let result: sqlite::Result<Vec<_>> = statement.iter().collect();
            result?;
        }

        // TODO Then object links
        for field in &sch_obj.fields {
            if let Some(_) = field.dynamic_resolver {
                continue;
            }

            let gql_val = arg_remove.remove(&field.name);
            match gql_val {
                None => continue,
                Some(gql::Value::Null) => (),
                Some(gql_val) => {
                    return Err(gql::Error::new(format!("Require null for remove field '{}'", field.name)));
                }
            };

            if matches!(field.typ.kind, GQLValueType::Object(_)) || field.is_list() {
                sql_clear_object_non_scalar(field, &objs, db, sch_obj, api)?;
            } else {
                return Err(format!("Shouldn't have got a '{}' in the inputs", field.name))?;
            }
        }

        if !arg_remove.is_empty() {
            let leftover = arg_remove.keys().join(",");
            Err(format!("There were extra arguments leftover: {}", leftover))?;
        }
    }

    Ok(objs)
}

pub fn handler_transaction_start(db: &'static SqliteDB) -> gql::Result<()> {
    let sql = "BEGIN TRANSACTION;";
    println!("Running {sql}");

    let mut statement = db.conn.prepare(sql)
        .map_err(|x| format!("SQL error: {x:?}"))?;
    exe_statement(&mut statement)?;

    Ok(())
}

pub fn handler_transaction_stop(db: &'static SqliteDB, successful: bool) -> gql::Result<()> {
    let sql;
    if successful {
        sql = "COMMIT TRANSACTION;";
    } else {
        sql = "ROLLBACK TRANSACTION;";
    }
    println!("Running {sql}");

    let mut statement = db.conn.prepare(sql)
        .map_err(|x| format!("SQL error: {x:?}"))?;
    exe_statement(&mut statement)?;

    Ok(())
}

fn empty_func(
    db: &'static SqliteDB,
    sch_field: &'static parsing::Field,
    sch_typ: &'static parsing::Type,
    api: &'static parsing::APIDefinition,
    obj: &ObjectRefCached,
    hook_args: &GQLIndexMap,
    was_name: &str,
) -> gql::Result<Option<FieldResult<ObjectRefCached>>> {
    Err(gql::Error::from(format!("not allowing this: {}", was_name)))
}

// pub trait HookFnSH: Fn(&SqliteDB, &parsing::Field, &parsing::Type, &parsing::APIDefinition, &ObjectRefCached, gqld::ResolverContext) -> gql::Result<Option<FieldResult<ObjectRefCached>>> + Send + Sync + 'static {}

pub type HookFn = dyn Fn(&'static SqliteDB, &'static parsing::Field, &'static parsing::Type, &'static parsing::APIDefinition, &ObjectRefCached, &GQLIndexMap) -> gql::Result<Option<FieldResult<ObjectRefCached>>> + Send + Sync + 'static;

impl DBInterface for SqliteDB {
    type IDType = IDType;
    type Ref = ObjectRefCached;
    // type HookFn = Box<dyn HookFnSH>;
    type HookBox = Box<HookFn>;
    // type HTTPHookFn = Box<dyn Fn(&'static SqliteDB, &'static parsing::APIDefinition, String) -> gql::Result<String> + Send + Sync + 'static>;

    fn bind_handler_field(&'static self,
                          typ: &'static parsing::Type,
                          field: &'static parsing::Field,
                          conf: &'static crate::ServerConfig<Self>,
    ) -> ResultAll<&'static FieldHandlerFn<Self::Ref>>
    {
        let boxed: Box<FieldHandlerFn<Self::Ref>>;
        if let Some(dyn_resolver) = &field.dynamic_resolver {
            let hook = conf.hooks.get(&dyn_resolver.name)
                // .ok_or_else(|| format!("Unable to find hook '{}' in hooks list for dynamic resolver.", hook_name))?;
                .map(|x| x.as_ref())
                // .map(|x| x)
                .unwrap_or_else(|| {
                    println!("WARNING: Unable to find hook '{}' in hooks list for dynamic resolver.", dyn_resolver.name);
                    // I don't get why this is necessary. If somehow we could
                    // clone the closure out of the hooks map, then this would
                    // be fine, but as it is, if we add Clone anywhere near
                    // HookFn then there are a hundred problems. So instead,
                    // going to leak boxes everywhere just so we can have a
                    // reference to a function.
                    //
                    // Also I still don't know what the difference is between let x: T = Box:new and let x = Box::<T>::new.
                    let name_copy = dyn_resolver.name.clone();
                    let x: Self::HookBox = Box::new(
                        move |db, sch_field, sch_typ, api, obj, hook_args| empty_func(db, sch_field, sch_typ, api, obj, hook_args, &name_copy));
                    // let x = Box::<dyn HookFnSH>::new(&empty_func);
                    Box::leak(x)
                });
            boxed = Box::new(move |obj, ctx| {
                hook(self, field, typ, &conf.api, obj, &dyn_resolver.args)
            });
        } else if let GQLValueType::Object(target_name) = &field.typ.kind {
            let target_typ = conf.api.types.get(target_name).unwrap();
            if let Some(src_field_name) = &field.incoming_source_name {
                boxed = Box::new(move |obj, ctx| {
                    handler_field_object_reverse(self, field, typ, target_typ, &src_field_name, obj)
                });
            } else {
                boxed = Box::new(move |obj, ctx| {
                    handler_field_object(self, field, typ, target_typ, obj)
                });
            }
        } else if field.is_list() {
            boxed = Box::new(move |obj, ctx| {
                handler_field_scalar_list(self, field, typ, obj, ctx)
            });
        } else {
            boxed = Box::new(move |obj, ctx| {
                handler_field_scalar(self, field, typ, obj, ctx)
            });
        }
        Ok(Box::leak::<'static>(boxed))
    }

    fn bind_handler_filter_list(
        &'static self,
        typ: &'static parsing::Type,
        api: &'static parsing::APIDefinition
    ) -> &'static FilterListHandlerFn<Self::IDType, Self::Ref>
    {
        let boxed: Box<FilterListHandlerFn<Self::IDType, Self::Ref>> = Box::new(move |list, filter, ctx| {
            handler_filter_list(self, typ, list, filter, ctx)
        });
        Box::leak::<'static>(boxed)
    }

    fn bind_handler_get_type(&'static self, typ: &'static parsing::Type, _api: &'static parsing::APIDefinition) -> &'static GetTypeHandlerFn<IDType, ObjectRefCached> {
        let boxed: Box<GetTypeHandlerFn<Self::IDType, Self::Ref>> = Box::new(
            move |id, ctx| handler_get_type(self, typ, id, ctx)
        );
        Box::leak::<'static>(boxed)
    }

    fn bind_handler_query_type(&'static self, typ: &'static parsing::Type, _api: &'static parsing::APIDefinition) -> &'static QueryTypeHandlerFn<Self::IDType, Self::Ref> {
        let boxed: Box<QueryTypeHandlerFn<Self::IDType, Self::Ref>> = Box::new(move |filter, ctx| handler_query_type(self, typ, &filter));
        Box::leak::<'static>(boxed)
    }


    fn bind_handler_add_type(&'static self, typ: &'static parsing::Type, api: &'static parsing::APIDefinition) -> &'static AddTypeHandlerFn<Self::Ref> {
        let boxed: Box<AddTypeHandlerFn<Self::Ref>> = Box::new(move |obj, upsert, ctx| handler_add_type(self, typ, api, obj, upsert));
        Box::leak::<'static>(boxed)
    }

    fn bind_handler_update_type(&'static self, typ: &'static parsing::Type, api: &'static parsing::APIDefinition) -> &'static UpdateTypeHandlerFn<Self::IDType,Self::Ref> {
        let boxed: Box<UpdateTypeHandlerFn<Self::IDType,Self::Ref>> = Box::new(move |filter: QueryFilter<Self::IDType>, arg_set: Option<ObjectHashMap>, arg_remove: Option<ObjectHashMap>, ctx| handler_update_type(self, typ, api, &filter, arg_set, arg_remove));
        Box::leak::<'static>(boxed)
    }

    fn bind_handler_transaction_start(&'static self, api: &'static parsing::APIDefinition) -> &'static TransactionStartHandlerFn {
        let boxed: Box<TransactionStartHandlerFn> = Box::new(move || handler_transaction_start(self));
        Box::leak::<'static>(boxed)
    }

    fn bind_handler_transaction_stop(&'static self, api: &'static parsing::APIDefinition) -> &'static TransactionStopHandlerFn {
        let boxed: Box<TransactionStopHandlerFn> = Box::new(move |successful| handler_transaction_stop(self, successful));
        Box::leak::<'static>(boxed)
    }

}