use simplegql::{
    ServerConfig,
    parse_schema,
    ResultAll,
    SqliteDB,
    APIDefinition,
};

use std::path::PathBuf;

use std::collections::HashMap;

use clap::{Parser, Args};
#[derive(Debug, Parser)]
#[command(name = "simplegql", about = "A GraphQL interface endpoint")]
struct Cli {
    #[arg(long)]
    schema: String,
    #[arg(long)]
    sqlite: Option<String>,
    #[arg(long, default_value_t = 5001)]
    port: u16,
    #[arg(long, default_value_t = String::from("localhost"))]
    bind: String,
    #[arg(long)]
    jwks: Option<String>,

    // For non-standard use in development or debugging
    #[arg(long, default_value_t = false)]
    migrate: bool,
    #[arg(long, default_value_t = false)]
    playground: bool,
}
    

#[tokio::main]
async fn main() -> ResultAll<()> {
    let cli = Cli::parse();

    let api = parse_schema(cli.schema.as_str())?;
    let sqlite_db = cli.sqlite.unwrap_or_else(|| {
        panic!("{}", "Need at least one kind of DB backend specified. Currently, only '--sqlite' is supported");
    });
    let db = load_db(PathBuf::from(sqlite_db), &api, cli.migrate)?;

    let hooks = HashMap::new();

    let http_hooks = HashMap::new();

    let box_conf = Box::new(ServerConfig{
        port: cli.port,
        bind_host: cli.bind.into(),
        api,
        db,
        enable_playground: cli.playground,
        jwks_url: cli.jwks,

        hooks,
        http_hooks,
    });
    let conf = Box::leak::<'static>(box_conf);

    simplegql::run_server(conf).await?;

    Ok(())
}

fn load_db(filename: PathBuf, api: &APIDefinition, allow_migration: bool) -> ResultAll<SqliteDB> {
    let db = SqliteDB::new(filename.to_str().unwrap())?;
    println!("Preparing tables");
    db.prepare_tables(api, allow_migration)?;
    Ok(db)
}

