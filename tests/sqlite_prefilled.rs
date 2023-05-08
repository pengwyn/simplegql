use itertools::Itertools;
use simplegql::{
    SqliteDB,
    ResultAll,
    ServerConfig,
    parsing::parse_schema_string,
};

use std::{
    collections::HashMap,
};

use serde_json::json;
use assert_json_diff::assert_json_eq;

use async_graphql as gql;

#[tokio::test]
async fn prefilled_db() -> ResultAll<()> {
    let db = SqliteDB::new(":memory:")?;

    let api = parse_schema_string(r###"
type Taxon {
    name: String!
    rank: Rank!
    observations: [Observation!]!
    description: String
}

enum Rank {
    Order,
    Family,
    Species,
}

type Observation {
    filename: String!
    parent: Taxon @incoming(src_field: "observations")
}"###
    )?;

    db.prepare_tables(&api, false)?;

    let query = "
INSERT INTO Taxon(id, name, rank, description) VALUES ('aaa', 'has observations', 'Order', 'has description');
INSERT INTO Taxon(id, name, rank) VALUES ('bbb', 'no observations', 'Family');
";
    db.conn.execute(query)?;

    let hooks = HashMap::new();
    let http_hooks = HashMap::new();

    let box_conf = Box::new(ServerConfig{
        port: 0,
        bind_host: String::from(""),
        api,
        db,
        enable_playground: false,
        jwks_url: None,

        hooks,
        http_hooks,
    });
    let conf = Box::leak::<'static>(box_conf);

    let schema = simplegql::build_schema(conf)?;

    let query = r###"
query {
    getTaxon(id: "aaa") {
        id
        name
        rank
        description
        observations {
            filename
            parent {
                id
            }
        }
    }
}
"###;
    let response = do_query(&schema, query).await?;

    assert_json_eq!(
        response,
        json!({
            "getTaxon": {
                "id": "aaa",
                "name": "has observations",
                "rank": "Order",
                "description": "has description",
                "observations": [],
            }
        })
    );

    let query = r###"
query {
    getTaxon(id: "bbb") {
        id
        name
        rank
        description
        observations {
            filename
            parent {
                id
            }
        }
    }
}
"###;
    let response = do_query(&schema, query).await?;

    assert_json_eq!(
        response,
        json!({
            "getTaxon": {
                "id": "bbb",
                "name": "no observations",
                "rank": "Family",
                "description": null,
                "observations": [],
            }
        })
    );

    let query = r###"
query {
    queryTaxon {
        id
    }
}
"###;
    let response = do_query(&schema, query).await?;
    let results = &response["queryTaxon"]
        .as_array().unwrap();

    assert!(results.len() == 2);
    assert!(results.iter().any(
        |x| *x == json!({"id": "aaa"})
    ));
    assert!(results.iter().any(
        |x| *x == json!({"id": "bbb"})
    ));

    let query = r###"
mutation {
    addTaxon(input: [{
        name: "newly added",
        rank: Species,
    }]) {
        count
        taxon {
            id
            name
            rank
            description
        }
    }

    updateTaxon(input: {
        filter: {id: ["aaa"]},
        set: {name: "changed",
              observations: [{filename: "new obs"}]},
        remove: {description: null},
    }) {
        count
        taxon {
            id
            name
            description
            observations {
                filename
                parent {
                    id
                }
            }
        }
    }
}
"###;
    let response = do_query(&schema, query).await?;
    let new_id = response["addTaxon"]["taxon"][0]["id"].as_str().unwrap();
    assert_json_eq!(
        response,
        json!({
            "addTaxon": {
                "count": 1,
                "taxon": [{
                "id": new_id,
                "name": "newly added",
                "rank": "Species",
                "description": null,
                }],
            },
            "updateTaxon": {
                "count": 1,
                "taxon": [{
                    "id": "aaa",
                    "name": "changed",
                    "description": null,
                    "observations": [{
                        "filename": "new obs",
                        "parent": {
                            "id": "aaa",
                        }
                    }]
                }]
            },
        })
    );

    Ok(())
}

async fn do_query(schema: &gql::dynamic::Schema, query: impl Into<String>) -> ResultAll<serde_json::Value> {
    let query = query.into();
    let response = schema.execute(&query).await;
    if !response.errors.is_empty() {
        eprintln!("\nError while executing: {query}");
        let err_cat = response.errors.iter()
            .map(|e| format!("'{}'", e.message))
            .collect::<Vec<_>>()
            .join("\n");
        eprintln!("Query failed: {}",  err_cat);
        Err("")?
    }

    Ok(response.data.into_json()?)
}