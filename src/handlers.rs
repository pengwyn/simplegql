// This will be hardcoded for sqlite for now.

use gql::dynamic::ValueAccessor;

use crate::utils::*;

use crate::parsing;

pub enum FilterOp {
    Or,
    And,
    Equals(gql::Value),
}

#[derive(Clone, Debug)]
pub struct QueryFilter<ID: CanBeID> {
    pub ids: Option<Vec<ID>>,
    pub first: Option<i32>,
    pub offset: Option<i32>,
}
impl<ID: CanBeID> QueryFilter<ID> {
    pub fn default() -> QueryFilter<ID> {
        QueryFilter{
            ids: None,
            first: None,
            offset: None,
        }
    }
}

pub trait SS: std::fmt::Debug + Clone + Send + Sync + 'static {}
#[derive(Debug)]
pub enum FieldResult<RefType> {
    Value(gql::Value),
    Ref(RefType),
    RefList(Vec<RefType>),
}

// Seems like conflicts with T in that it could be gql::Value too.
// impl<RefType> From<RefType> for FieldResult<RefType> {
//     fn from(other: RefType) -> FieldResult<RefType> {FieldResult::Ref(other)}
// }
// impl<RefType> From<gql::Value> for FieldResult<RefType> {
//     fn from(other: gql::Value) -> FieldResult<RefType> {FieldResult::Value(other)}
// }
// impl<RefType> From<Vec<RefType>> for FieldResult<RefType> {
//     fn from(other: Vec<RefType>) -> FieldResult<RefType> {FieldResult::RefList(other)}
// }

pub type FieldHandlerFn<Ref> = dyn Fn(&Ref, gqld::ResolverContext) -> gql::Result<Option<FieldResult<Ref>>> + Send + Sync;
pub type FilterListHandlerFn<ID, Ref> = dyn Fn(&[Ref], QueryFilter<ID>, gqld::ResolverContext) -> gql::Result<Vec<Ref>> + Send + Sync;

pub type GetTypeHandlerFn<ID, Ref> = dyn Fn(ID, gqld::ResolverContext) -> gql::Result<Option<Ref>> + Send + Sync;
pub type QueryTypeHandlerFn<ID, Ref> = dyn Fn(QueryFilter<ID>, gqld::ResolverContext) -> gql::Result<Vec<Ref>> + Send + Sync;

pub type AddTypeHandlerFn<Ref> = dyn Fn(gqld::ObjectAccessor, bool, &gqld::ResolverContext) -> gql::Result<Ref> + Send + Sync;
pub type UpdateTypeHandlerFn<ID,Ref> = dyn Fn(QueryFilter<ID>, Option<ObjectHashMap>, Option<ObjectHashMap>, &gqld::ResolverContext) -> gql::Result<Vec<Ref>> + Send + Sync;

pub type TransactionStartHandlerFn = dyn Fn() -> gql::Result<()> + Send + Sync;
pub type TransactionStopHandlerFn = dyn Fn(bool) -> gql::Result<()> + Send + Sync;

pub trait IDTrait<IDType: CanBeID> {
    fn id(&self) -> &IDType;
}

pub trait DBInterface: Send + Sync + Sized {
    type IDType: CanBeID;
    type Ref: SS + IDTrait<Self::IDType>;
    // type HookFn: 'static;
    type HookBox: 'static;
    // type HTTPHookFn: 'static;

    fn bind_handler_field(&'static self, typ: &'static parsing::Type, field: &'static parsing::Field, conf: &'static crate::ServerConfig<Self>) -> ResultAll<&'static FieldHandlerFn<Self::Ref>>;
    fn bind_handler_filter_list(&'static self, typ: &'static parsing::Type, api: &'static parsing::APIDefinition) -> &'static FilterListHandlerFn<Self::IDType, Self::Ref>;

    fn bind_handler_get_type(&'static self, typ: &'static parsing::Type, api: &'static parsing::APIDefinition) -> &'static GetTypeHandlerFn<Self::IDType, Self::Ref>;
    fn bind_handler_query_type(&'static self, typ: &'static parsing::Type, api: &'static parsing::APIDefinition) -> &'static QueryTypeHandlerFn<Self::IDType, Self::Ref>;

    fn bind_handler_add_type(&'static self, typ: &'static parsing::Type, api: &'static parsing::APIDefinition) -> &'static AddTypeHandlerFn<Self::Ref>;
    fn bind_handler_update_type(&'static self, typ: &'static parsing::Type, api: &'static parsing::APIDefinition) -> &'static UpdateTypeHandlerFn<Self::IDType,Self::Ref>;

    fn bind_handler_transaction_start(&'static self, api: &'static parsing::APIDefinition) -> &'static TransactionStartHandlerFn;
    fn bind_handler_transaction_stop(&'static self, api: &'static parsing::APIDefinition) -> &'static TransactionStopHandlerFn;
}

#[derive(Debug, Clone)]
pub enum GQLValueType {
    // Null,
    Integer,
    Float,
    String,
    Boolean,
    // Binary,
    NamedType(String),
    Object(String),
    Enum(String),
    CustomScalar(String),
}

#[derive(Debug, Clone)]
pub struct GQLValueTypeAnnotated {
    pub kind: GQLValueType,
    pub is_list: bool,
    pub is_nn: bool,
    pub is_inner_nn: bool,
}

pub trait CanBeID: Sized {
    type Item;
    fn load(val: &gqld::ValueAccessor) -> gql::Result<Self>;
    fn load_val(val: gql::Value) -> gql::Result<Self>;
    fn as_typeref() -> &'static str;
    fn to_gql(&self) -> gql::Value;
    fn internal_type() -> GQLValueType;
    fn as_sql_literal(&self) -> String;
}
impl CanBeID for i64 {
    type Item = i64;
    fn load(val: &gqld::ValueAccessor) -> gql::Result<i64> { val.i64() }
    fn load_val(val: gql::Value) -> gql::Result<i64> {
        match val {
            gql::Value::Number(num) => num.as_i64().ok_or("Not a i64".into()),
            _ => Err("Not a i64")?,
        }
    }
    fn as_typeref() -> &'static str {
        gqld::TypeRef::INT
    }
    fn to_gql(&self) -> gql::Value {
        gql::Value::Number((*self).into())
    }
    fn internal_type() -> GQLValueType {
        GQLValueType::Integer
    }
    fn as_sql_literal(&self) -> String {
        format!("{}", self)
    }
}
impl CanBeID for u64 {
    type Item = u64;
    fn load(val: &gqld::ValueAccessor) -> gql::Result<u64> { val.u64() }
    fn load_val(val: gql::Value) -> gql::Result<u64> {
        match val {
            gql::Value::Number(num) => num.as_u64().ok_or("Not a u64".into()),
            _ => Err("Not a u64")?,
        }
    }
    fn as_typeref() -> &'static str {
        gqld::TypeRef::INT
    }
    fn to_gql(&self) -> gql::Value {
        gql::Value::Number((*self).into())
    }
    fn internal_type() -> GQLValueType {
        GQLValueType::Integer
    }
    fn as_sql_literal(&self) -> String {
        format!("{}", self)
    }
}
impl CanBeID for String {
    type Item = String;
    fn load(val: &gqld::ValueAccessor) -> gql::Result<String> {
        val.string().map(|x| x.to_string())
    }
    fn load_val(val: gql::Value) -> gql::Result<String> {
        match val {
            gql::Value::String(s) => Ok(s),
            _ => Err("Not a String")?,
        }
    }
    fn as_typeref() -> &'static str {
        gqld::TypeRef::STRING
    }
    fn to_gql(&self) -> gql::Value {
        gql::Value::String(self.clone())
    }
    fn internal_type() -> GQLValueType {
        GQLValueType::String
    }
    fn as_sql_literal(&self) -> String {
        format!("'{}'", self)
    }
}
    

// Treat the object ref as a value, that can reference an entry in the database,
// but may itself be out of date. So any use of this ref needs to be
// "lifetime-checked" but itself can live for as long as we want.
#[derive(Debug, Clone)]
pub struct ObjectRef<ID> {
    pub schema: &'static parsing::Type,
    pub id: ID,
}


// fn to_value(va: &gql::ValueAccessor) -> gql::Value {
//     va.
// }

impl GQLValueTypeAnnotated {
    fn as_typeref_with_given_string(&self, s: String) -> gqld::TypeRef {
        let typeref = match (self.is_list, self.is_nn, self.is_inner_nn) {
            (false, false, false) => gqld::TypeRef::named(s),
            (false, true, false) => gqld::TypeRef::named_nn(s),
            // (true, false, _) => panic!("Nullable lists have no use in this API"),
            (true, false, false) => gqld::TypeRef::named_list(s),
            (true, true, false) => gqld::TypeRef::named_nn_list(s),
            (true, true, true) => gqld::TypeRef::named_nn_list_nn(s),
            (true, false, true) => gqld::TypeRef::named_list_nn(s),
            _ => panic!("Not allowed")
        };

        typeref
    }
        
    pub fn as_typeref(&self) -> gqld::TypeRef {
        let s = match &self.kind {
            GQLValueType::Integer => gqld::TypeRef::INT,
            GQLValueType::Float => gqld::TypeRef::FLOAT,
            GQLValueType::String => gqld::TypeRef::STRING,
            GQLValueType::Boolean => gqld::TypeRef::BOOLEAN,
            GQLValueType::NamedType(_) => panic!("Shouldn't get here"),
            GQLValueType::Object(obj) => obj,
            GQLValueType::Enum(enm) => enm,
            GQLValueType::CustomScalar(scalar) => scalar,
        };

        self.as_typeref_with_given_string(s.to_string())
    }

    pub fn as_input_typeref(&self) -> gqld::TypeRef {
        let s = match &self.kind {
            GQLValueType::Integer => gqld::TypeRef::INT.to_string(),
            GQLValueType::Float => gqld::TypeRef::FLOAT.to_string(),
            GQLValueType::String => gqld::TypeRef::STRING.to_string(),
            GQLValueType::Boolean => gqld::TypeRef::BOOLEAN.to_string(),
            GQLValueType::NamedType(_) => panic!("Shouldn't get here"),
            GQLValueType::Object(obj) => format!("{}Ref", obj),
            GQLValueType::Enum(enm) => enm.clone(),
            GQLValueType::CustomScalar(scalar) => scalar.clone(),
        };

        let temp = GQLValueTypeAnnotated{
            is_nn: false,
            is_inner_nn: false,
            is_list: self.is_list,
            // Dummy
            kind: GQLValueType::Boolean,
        };

        temp.as_typeref_with_given_string(s)
    }

    pub fn as_nullable_typeref(&self) -> gqld::TypeRef {
        let temp_typeref = GQLValueTypeAnnotated{
            is_nn: false,
            is_inner_nn: false,
            ..self.clone()
        };

        temp_typeref.as_typeref()
    }
}


///////////////////////////////////////////////////////////////////////////////
//                                 Functions                                 //
///////////////////////////////////////////////////////////////////////////////

pub fn resolver_filter_list<'a, ID: CanBeID, Ref: SS>(ctx: gqld::ResolverContext<'a>, backend_fn: &'static FilterListHandlerFn<ID, Ref>) -> gqld::FieldFuture<'a> {
    gqld::FieldFuture::new(async move {
        // let v: Vec<gqld::FieldValue> = Vec::new();
        // Ok(Some(gqld::FieldValue::list(v)))
        let l: Result<Vec<_>, _> = ctx.parent_value.as_list().expect("Should have list")
            .iter()
            .map(|f| f.try_downcast_ref::<Ref>())
            .collect();
        let l = l?
            .into_iter()
                 .map(|x| gqld::FieldValue::owned_any(x.to_owned()));
        Ok(Some(gqld::FieldValue::list(l)))
    })
}

pub fn resolver_field<'a, Ref: SS>(ctx: gqld::ResolverContext<'a>, backend_fn: &'static FieldHandlerFn<Ref>) -> gqld::FieldFuture<'a> {
    gqld::FieldFuture::new(async move {
        let obj = ctx.parent_value
            .try_downcast_ref::<Ref>()
            .expect("Did not find parent object");
        let result = backend_fn(obj, ctx);
        // dbg!(&result);
        result.map(|x|
                   x.map(|x|
                         match x {
                             FieldResult::Value(v) => gqld::FieldValue::value(v),
                             FieldResult::Ref(r) => gqld::FieldValue::owned_any(r),
                             FieldResult::RefList(l) => gqld::FieldValue::list(
                                 l.into_iter().map(gqld::FieldValue::owned_any)
                             ),
                         }
                   )
        )
    })
}

pub fn resolver_id<'a, IDType: CanBeID, Ref: SS + IDTrait<IDType>>(ctx: gqld::ResolverContext<'a>) -> gqld::FieldFuture<'a> {
    gqld::FieldFuture::new(async move {
        let obj = ctx.parent_value
            .try_downcast_ref::<Ref>()
            .expect("Did not find parent object");
        Ok(Some(obj.id().to_gql()))
    })
}

pub fn resolver_get_type<'a, ID: CanBeID, Ref: SS>(ctx: gqld::ResolverContext<'a>, backend_fn: &'static GetTypeHandlerFn<ID, Ref>) -> gqld::FieldFuture<'a> {
    gqld::FieldFuture::new(async move {
        let id = ctx.args.try_get("id").expect("No id in args");
        let id: ID = CanBeID::load(&id).expect("Just failing for now");

        let obj = backend_fn(id, ctx)?;
        Ok(obj.map(|x| gqld::FieldValue::owned_any(x)))
    })
}
pub fn resolver_query_type<'a, ID: CanBeID, Ref: SS>(ctx: gqld::ResolverContext<'a>, backend_fn: &'static QueryTypeHandlerFn<ID, Ref>) -> gqld::FieldFuture<'a> {
    gqld::FieldFuture::new(async move {
        // TODO: some parsing
        let filter = parse_filter(&ctx.args)?;
        let out = backend_fn(filter, ctx)?;
        let conv = out.into_iter()
            .map(|x| gqld::FieldValue::owned_any(x));
        Ok(Some(gqld::FieldValue::list(conv)))
    })
}

pub fn resolver_add_type<'a, Ref: SS>(ctx: gqld::ResolverContext<'a>, backend_fn: &'static AddTypeHandlerFn<Ref>) -> gqld::FieldFuture<'a> {
    gqld::FieldFuture::new(async move {
        let input = ctx.args.try_get("input")?;//.expect("No input in args");
        let upsert = ctx.args.get("upsert")
            .map(|x| x.boolean())//.expect("Not a boolean"))
            .unwrap_or(Ok(false))?;

        let l = input.list()?;
        let refs: Result<Vec<Ref>, gql::Error> =
            l.iter()
            .map(|x| backend_fn(
                x.object().expect("Input wasn't an object"),
                upsert,
                &ctx)
            )
            .collect();
        let refs = refs?;
        let refs = refs.into_iter()
            .map(|r| gqld::FieldValue::owned_any(r));

        // TODO: Checks for upsert and ids
        // let id: ID = CanBeID::load(id).expect("Just failing for now");

        Ok(Some(gqld::FieldValue::list(refs)))
    })
}

pub fn resolver_update_type<'a, ID: CanBeID, Ref: SS>(ctx: gqld::ResolverContext<'a>, backend_fn: &'static UpdateTypeHandlerFn<ID,Ref>) -> gqld::FieldFuture<'a> {
    gqld::FieldFuture::new(async move {
        let input_value = ctx.args.get("input")
            .ok_or("'input' is a required argument")?;
        let input = input_value.object()
            .map_err(|_| "'input' must be an object")?;
        let filter = parse_filter(&input)?;

        let arg_set = input.get("set").map(|x| {
            let y = x.object()?;
            let z = hashmap_from_gql_obj(&y);
            Ok::<_, gql::Error>(z)
        }).transpose()?;
        let arg_remove = input.get("remove").map(|x| {
            Ok::<_, gql::Error>(hashmap_from_gql_obj(&x.object()?))
        }).transpose()?;

        let out = backend_fn(filter, arg_set, arg_remove, &ctx)?;
        let conv = out.into_iter()
            .map(|x| gqld::FieldValue::owned_any(x));
        Ok(Some(gqld::FieldValue::list(conv)))
    })
}


fn parse_filter<ID: CanBeID>(gql_val: &gqld::ObjectAccessor) -> gql::Result<QueryFilter<ID>> {
    let mut out_filter = QueryFilter::default();
    if let Some(filter) = gql_val.get("filter") {
        let filter = filter.object().map_err(|_| "Need an object for filter")?;
        if let Some(ids) = filter.get("id") {
            let ids: gql::Result<Vec<_>> = ids
                .list()
                .unwrap()
                .iter()
                .map(|x| <ID as CanBeID>::load(&x))
                .collect();
            let ids = ids?;
            out_filter.ids = Some(ids);
        }
    }
    if let Some(first) = gql_val.get("first") {
        out_filter.first = Some(first.i64()? as i32);
    }
    if let Some(offset) = gql_val.get("offset") {
        out_filter.offset = Some(offset.i64()? as i32);
    }
    Ok(out_filter)
}