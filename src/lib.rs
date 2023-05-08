use std::{
    net::{IpAddr, Ipv4Addr},
    collections::HashMap,
};

use gql::Executor;
use reqwest::StatusCode;
use utils::*;
use async_graphql::{
    http::{playground_source, GraphQLPlaygroundConfig},
};
use async_graphql_poem::*;
use poem::{*,
           listener::TcpListener,
           web::{Html, Data},
           http::HeaderMap,
           middleware::Cors,
};

pub mod utils;
pub use utils::{SimpleGQLError, ResultAll, ErrorAll};

pub mod parsing;
pub use parsing::{APIDefinition, parse_schema};

pub mod handlers;
pub use handlers::DBInterface;
use handlers::{
    CanBeID,
    resolver_filter_list,
    resolver_field,
    resolver_id,
    resolver_get_type,
    resolver_query_type,
    resolver_add_type,
    resolver_update_type, TransactionStartHandlerFn, TransactionStopHandlerFn,
};

pub mod sqlite_db;
pub use sqlite_db::SqliteDB;

#[handler]
async fn graphql_playground() -> impl IntoResponse {
    Html(playground_source(GraphQLPlaygroundConfig::new("/gql")))
}

pub type HTTPHookFn<T> = dyn Fn(&'static T, &'static parsing::APIDefinition, Option<&str>, &str) -> gql::Result<Response> + Send + Sync + 'static;
pub type HTTPHookBox<T> = Box<HTTPHookFn<T>>;
pub struct ServerConfig<T: DBInterface + 'static> {
    pub port: u16,
    pub bind_host: String,
    pub api: APIDefinition,
    pub db: T,
    pub enable_playground: bool,
    pub jwks_url: Option<String>,
    pub hooks: HashMap<String, T::HookBox>,
    pub http_hooks: HashMap<String, HTTPHookBox<T>>,
}

struct EndpointWrapper<'a, T: Send + Sync + 'static> {
    func: &'a HTTPHookFn<T>,
    db: &'static T,
    api: &'static parsing::APIDefinition,
    route: String,
}

#[async_trait::async_trait]
impl<'a, T: Send + Sync + 'static> Endpoint for EndpointWrapper<'a, T> {
    type Output = Response;

    async fn call(&self, mut req: Request) -> poem::Result<Response> {
        let body = req.take_body().into_string().await?;
        let query = req.original_uri().query();
        let output = (*self.func)(self.db, self.api, query, &body)
            .map_err(|e| {
                println!("Error while handling HTTP endpoint '{}': {:?}", self.route, e);
                poem::error::InternalServerError(SimpleGQLError::from("internal error".to_string()))
            });
        output
    }
}

    
// pub async fn run_server<DB: traits::DBInterfaceTrait>(conf: &ServerConfig<DB>) -> Result<(), SimpleGQLError> {
pub async fn run_server<T: DBInterface + 'static>(conf: &'static ServerConfig<T>) -> Result<(), SimpleGQLError> {
    let addr: IpAddr = if conf.bind_host == "localhost" {
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))
    } else {
        conf.bind_host.parse().map_err(|x| format!("Server address '{}' cannot be parsed: {x}", conf.bind_host))?
    };
    let server = Server::new(TcpListener::bind((addr, conf.port)));
    let app = setup_app(conf)
        .map_err(|x| format!("Failed to setup the app: {x}"))?;
    server.run(app).await.map_err(|x| format!("Running server encountered an error: {x}"))?;
    Ok(())
}

pub fn build_schema<T: DBInterface>(conf: &'static ServerConfig<T>) -> ResultAll<gqld::Schema> {
    let id_type_ref = gqld::TypeRef::named(T::IDType::as_typeref());
    let id_type_ref_nn = gqld::TypeRef::named_nn(T::IDType::as_typeref());
    let id_type_ref_list = gqld::TypeRef::named_nn_list(T::IDType::as_typeref());
    let mut schema = gqld::Schema::build("Query", Some("Mutation"), None);

    let mut query = gqld::Object::new("Query");
    let mut mutation = gqld::Object::new("Mutation");

    for (_, typ) in &conf.api.types {
        let mut gql_type = gqld::Object::new(&typ.name)
            .field(gqld::Field::new(
                "id",
                id_type_ref_nn.clone(),
                resolver_id::<T::IDType, T::Ref>,
            ));
        for field in &typ.fields {
            let handler = conf.db.bind_handler_field(&typ, &field, &conf)?;
            gql_type = gql_type.field(gqld::Field::new(
                field.name.clone(),
                field.typ.as_typeref(),
                move |ctx| resolver_field(ctx, handler),
            )
            .add_filter_arguments(&typ.name)
            )
        }

        schema = schema.register(gql_type);

        let bound_handler_get_type = conf.db.bind_handler_get_type(&typ, &conf.api);
        let bound_handler_query_type = conf.db.bind_handler_query_type(&typ, &conf.api);
        let bound_handler_filter_list = conf.db.bind_handler_filter_list(&typ, &conf.api);

        query = query
            .field(gqld::Field::new(
                format!("get{}", typ.name),
                gqld::TypeRef::named(typ.name.clone()),
                move |ctx| resolver_get_type(ctx, bound_handler_get_type),
            )
                   .argument(gqld::InputValue::new("id", id_type_ref_nn.clone()))
            )
            .field(gqld::Field::new(
                format!("query{}", typ.name),
                gqld::TypeRef::named_nn_list_nn(typ.name.clone()),
                move |ctx| resolver_query_type(ctx, bound_handler_query_type),
            )
            .add_filter_arguments(&typ.name)
            );

        let filter_type_name = format!("{}Filter", &typ.name);
        // Just hardcoded the id for now
        let gql_filter = gqld::InputObject::new(filter_type_name.clone())
            .field(gqld::InputValue::new("id", id_type_ref_list.clone()));
        schema = schema.register(gql_filter);

        let mutate_response_name = format!("Mutate{}Response", typ.name);
        let input_ref_name = format!("{}Ref", typ.name);
        let update_input_name = format!("Update{}Input", typ.name);
        // let add_input_name = format!("Add{}Input", typ.name);
        let bound_handler_add_type = conf.db.bind_handler_add_type(&typ, &conf.api);
        let bound_handler_update_type = conf.db.bind_handler_update_type(&typ, &conf.api);

        let gql_response = gqld::Object::new(mutate_response_name.clone())
            .field(gqld::Field::new(
                "count",
                gqld::TypeRef::named_nn(gqld::TypeRef::INT),
                extract_count,
            ))
            .field(gqld::Field::new(
                lowercase_name(&typ.name),
                gqld::TypeRef::named_nn_list_nn(typ.name.clone()),
                move |ctx| resolver_filter_list(ctx, bound_handler_filter_list),
            ))
            ;
        schema = schema.register(gql_response);

        let mut gql_input_ref = gqld::InputObject::new(input_ref_name.clone());
        gql_input_ref = gql_input_ref.field(gqld::InputValue::new("id", id_type_ref.clone()));
        for field in &typ.fields {
            if field.is_dynamic() {
                continue;
            }
            gql_input_ref = gql_input_ref.field(gqld::InputValue::new(
                field.name.clone(),
                field.typ.as_input_typeref(),
            ));
        }
        // dbg!(&gql_input_ref);
        schema = schema.register(gql_input_ref);

        let gql_update_input = gqld::InputObject::new(update_input_name.clone())
            .field(gqld::InputValue::new("filter", gqld::TypeRef::named_nn(filter_type_name.clone())))
            .field(gqld::InputValue::new("set", gqld::TypeRef::named(input_ref_name.clone())))
            .field(gqld::InputValue::new("remove", gqld::TypeRef::named(input_ref_name.clone())));
        schema = schema.register(gql_update_input);

        mutation = mutation
            .field(gqld::Field::new(
                format!("add{}", typ.name),
                gqld::TypeRef::named(mutate_response_name.clone()),
                move |ctx| resolver_add_type(ctx, bound_handler_add_type),
            )
                   .argument(gqld::InputValue::new("input", gqld::TypeRef::named_nn_list_nn(input_ref_name.clone())))
                   .argument(gqld::InputValue::new("upsert", gqld::TypeRef::named(gqld::TypeRef::BOOLEAN)))
            )
            .field(gqld::Field::new(
                format!("update{}", typ.name),
                gqld::TypeRef::named(mutate_response_name.clone()),
                move |ctx| resolver_update_type(ctx, bound_handler_update_type),
            )
                   .argument(gqld::InputValue::new("input", gqld::TypeRef::named_nn(update_input_name.clone())))
            );
    }

    for (_, enm) in &conf.api.enums {
        let mut gql_enum = gqld::Enum::new(&enm.name);
        for val in &enm.values {
            gql_enum = gql_enum.item(gqld::EnumItem::new(val));
        }

        schema = schema.register(gql_enum);
    }

    for (_, scalar) in &conf.api.scalars {
        let gql_scalar = gqld::Scalar::new(&scalar.name);

        schema = schema.register(gql_scalar);
    }

    // create the schema
    let schema = schema
        .register(query)
        .register(mutation)
        .finish()?;

    Ok(schema)
}

pub fn setup_app<T: DBInterface>(conf: &'static ServerConfig<T>) -> ResultAll<impl Endpoint> {

    let schema = build_schema(conf)?;

    // start the http server
    let mut app = Route::new()
        .at("/gql", post(gql_endpoint));
    if conf.enable_playground {
        app = app.at("/playground", get(graphql_playground));
        println!("GraphiQL: http://{}:{}/playground", conf.bind_host, conf.port);
    }

    for (route,hook) in &conf.http_hooks {
        app = app.at(route, EndpointWrapper{
            func: hook,
            db: &conf.db,
            api: &conf.api,
            route: route.clone(),
        });
    }
    let bound_transaction_start = conf.db.bind_handler_transaction_start(&conf.api);
    let bound_transaction_stop = conf.db.bind_handler_transaction_stop(&conf.api);

    let data_struct = DataStruct{
        transaction_start: bound_transaction_start,
        transaction_stop: bound_transaction_stop,
        schema,
        jwks_url: conf.jwks_url.clone(),
    };
    let app = app
        .with(Cors::new())
        .data(data_struct);
    Ok(app)
}

#[derive(Clone)]
struct DataStruct {
    pub transaction_start: &'static TransactionStartHandlerFn,
    pub transaction_stop: &'static TransactionStopHandlerFn,
    pub schema: gqld::Schema,
    pub jwks_url: Option<String>,
}

struct TransactionWrapper<'a> {
    data: &'a DataStruct,
    pub was_success: bool

}

impl<'a> TransactionWrapper<'a> {
    fn new(data: &DataStruct) -> TransactionWrapper {
        TransactionWrapper{data, was_success: false}
    }
}

impl<'a> Drop for TransactionWrapper<'a> {
    fn drop(&mut self) {
        if let Err(e) = (self.data.transaction_stop)(self.was_success) {
            eprintln!("Was an error while calling transaction_stop, ignoring: {:?}", e); 
        }
    }
}
    

use jsonwebtoken::{jwk, DecodingKey, decode, decode_header, Validation, Algorithm};
use serde::Deserialize;

#[derive(Debug,Deserialize)]
struct ExpectedClaims {
    pub permissions: Vec<String>,
}

use tokio::sync::{Mutex};
static GLOBAL_JWK: Mutex<Option<jwk::JwkSet>> = Mutex::const_new(None);
async fn get_jwks(url: &str) -> ResultAll<jwk::JwkSet> {
    let mut locked = GLOBAL_JWK.lock().await;
    if let Some(jwk) = locked.clone() {
        // println!("Reusing old jwks");
        return Ok(jwk);
    }

    println!("Fetching new jwks");

    let jwks_raw = reqwest::get(url)
        .await?
        .text()
        .await?;
    let jwks: jwk::JwkSet = serde_json::from_str(&jwks_raw).unwrap();

    *locked = Some(jwks.clone());

    Ok(jwks)
}

#[handler]
async fn gql_endpoint(
    // schema: Data<&gqld::Schema>,
    data_struct: Data<&DataStruct>,
    headers: &HeaderMap,
    // req: Option<GraphQLRequest>,
    req_batched: Option<GraphQLBatchRequest>,
    // body: String,
    // full: &Request,
) -> poem::Result<GraphQLBatchResponse> {
    let mut reqs = match req_batched {
        Some(x) => x.0,
        None => Err(poem::Error::from_string("blah", poem::http::StatusCode::FORBIDDEN))?,
    };
    // dbg!(&headers);
    let mut auth_token = headers.get("X-Auth-Token");
    if let Some(s) = auth_token {
        if s.is_empty() {
            auth_token = None;
        }
    }
    let auth_claims = if let Some(auth_token) = auth_token {
        let jwks_url = data_struct.0.jwks_url.as_ref().ok_or_else(|| {
            println!("Auth given with no way to validate it");
            poem::Error::from_status(StatusCode::INTERNAL_SERVER_ERROR)
        })?;

        let jwks = get_jwks(jwks_url).await
            .map_err(|e| {
                println!("Jwks error: {:?}", e);
                poem::Error::from_status(StatusCode::INTERNAL_SERVER_ERROR)}
            )?;

        let auth_token = auth_token.to_str()
            .map_err(|e| {
                println!("Auth token header is not a string: {:?}", e);
                poem::Error::from_status(StatusCode::INTERNAL_SERVER_ERROR)
            })?;

        // dbg!(&auth_token);
        let auth_token = auth_token.strip_prefix("Bearer ").ok_or_else(|| {
            println!("Auth token didn't start with Bearer: '{auth_token}'");
            poem::Error::from_status(StatusCode::INTERNAL_SERVER_ERROR)
        })?;

        let unvalidated = decode_header(&auth_token)
            .map_err(|e| { println!("Couldn't decode header: {:?}", e);
                           poem::Error::from_status(StatusCode::INTERNAL_SERVER_ERROR)
            })?;
        // dbg!(&unvalidated);
        let kid = match unvalidated.kid {
            None => { println!("No kid present in header");
                      return Err(poem::Error::from_status(StatusCode::INTERNAL_SERVER_ERROR));
            },
            Some(x) => x,
        };

        let jwk = jwks.find(&kid).ok_or_else(|| {
            println!("Kid is not present in header: {:?}", kid);
            poem::Error::from_status(StatusCode::INTERNAL_SERVER_ERROR)
        })?;
            
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&["asdf"]);
        let claims = decode::<ExpectedClaims>(
            auth_token,
            &DecodingKey::from_jwk(jwk)
                .map_err(|e| {
                    println!("Unable to prepare decoding key: {:?}", e);
                    poem::Error::from_status(StatusCode::INTERNAL_SERVER_ERROR)
                })?,
            &validation,
        )
            .map_err(|e| {
                println!("Unable to validate auth token: {:?}", e);
                poem::Error::from_status(StatusCode::INTERNAL_SERVER_ERROR)
            })?;

        // dbg!(&claims);

        Some(claims.claims)
    } else {
        None
    };
    // if let Some(token) = get_token_from_headers(headers) {
    //     req = req.data(token);
    // }
    // Ok(schema.execute(reqs).await.into())
    let mut is_mutation = false;
    for req in reqs.iter_mut() {
        if req_is_mutation(req).map_err(|_| poem::Error::from_string("Unexpected} error in parsing", poem::http::StatusCode::BAD_REQUEST))? {
            is_mutation = true;
            break;
        }
    }
    let mut transaction = None;
    if is_mutation {
        match auth_claims {
            None => {
                println!("Not authed while trying to mutate");
                return Err(poem::Error::from_status(StatusCode::UNAUTHORIZED));
            },
            Some(x) if !(x.permissions.contains(&"view:deleted".to_string()) || x.permissions.contains(&"add:images".to_string())) => {
                println!("view:deleted or add:images isn't in auth claims!");
                dbg!(&x.permissions);
                return Err(poem::Error::from_status(StatusCode::UNAUTHORIZED));
            },
            _ => (),
        };

        (data_struct.0.transaction_start)().unwrap();
        transaction = Some(TransactionWrapper::new(&data_struct));
    }

    // let all_query_strs = 
    //     reqs.iter()
    //     .map(|x| x.query.clone())
    //     .collect::<Vec<String>>()
    //     .join("\n\n");

    let batch_response = data_struct.0.schema.execute_batch(reqs).await;
    if is_mutation {
        if let Some(transaction) = &mut transaction {
            transaction.was_success = batch_response.is_ok();
        }
        // (data_struct.0.transaction_stop)(batch_response.is_ok()).unwrap();
    }
    if !batch_response.is_ok() {
        match &batch_response {
            gql::BatchResponse::Batch(v) => {
                v.iter()
                    .filter(|resp| resp.is_err())
                    .for_each(|resp| {
                        for err in &resp.errors {
                            dbg!(err);
                        }
                    });
            },
            gql::BatchResponse::Single(resp) => {
                for err in &resp.errors {
                    dbg!(err);
                }
            },
        };
    }
    Ok(batch_response.into())
}

fn req_is_mutation(req: &mut gql::Request) -> gql::Result<bool> {
    let query = req.parsed_query()?;
    let is_mutation = query.operations.iter()
        .any(|(_,op)| op.node.ty == gql::parser::types::OperationType::Mutation);

    Ok(is_mutation)
}


fn extract_count(ctx: gqld::ResolverContext) -> gqld::FieldFuture {
    gqld::FieldFuture::new(async move {
        let l = ctx.parent_value.try_to_list().unwrap();
        Ok(Some(gql::Value::from(l.len())))
    })
}
      
fn lowercase_name(name: &str) -> String {
    if name.len() == 0 {
        panic!("Empty string can't be lowercased");
    }
    if !name.is_ascii() {
        panic!("Ascii only allowed");
    }
    let mut s: Vec<u8> = name.bytes().collect();
    s[0] = s[0].to_ascii_lowercase();

    unsafe { String::from_utf8_unchecked(s) }
}

trait AddFilterArgs {
    fn add_filter_arguments(self, name: &str) -> Self;
}
impl AddFilterArgs for gqld::Field {
    fn add_filter_arguments(self, name: &str) -> gqld::Field {
        self
            .argument(gqld::InputValue::new("filter", gqld::TypeRef::named(format!("{}Filter", name))))
            .argument(gqld::InputValue::new("first", gqld::TypeRef::named(gqld::TypeRef::INT)))
            .argument(gqld::InputValue::new("offset", gqld::TypeRef::named(gqld::TypeRef::INT)))
        // .argument(gql::InputValue::new("order", gqld::TypeRef::named(format!("{}Order", typ.name)))),
    }
}
        