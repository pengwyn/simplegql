# SimpleGQL

This library allows a simplified GraphQL schema to be given and will run a
server with a backend store (currently only SQLite) and a set of automatically
generated resolvers for queries and mutations.

As an example, you can specify:

```graphql
type Taxon {
    name: String!
    description: String
    rank: Rank
    observations: [Observation!]!
}

enum Rank {
    Order
    Family
    Species
}

type Observation {
    filename: String!
    parent: Taxon @incoming(src_field: "observations")
}
```

and by running `cargo run -- --sqlite mydblocation.sqlite --schema
schema_above.graphql` a HTTP server will be started handling GraphQL requests at
`/gql` with a set of automatically generated resolvers including:

```graphql
Query {
    getTaxon(id: ID!): Taxon
    queryTaxon(filter: TaxonFilter, first: Int, offset: Int): [Taxon!]!

    getObservation(id: ID!): Observation
    queryObservation(filter: ObservationFilter, first: Int, offset: Int): [Observation!]!
}

Mutation {
    addTaxon(input: [TaxonInput!]!): TaxonMutationResponse
    updateTaxon(input: {filter: TaxonFilter, set: {...}, remove: {...}}): TaxonMutationResponse
    deleteTaxon(...): TaxonMutationResponse

    addObservation(input: [ObservationInput!]!): ObservationMutationResponse
    updateObservation(input: {filter: ObservationFilter, set: {...}, remove: {...}}): ObservationMutationResponse
    deleteObservation(...): ObservationMutationResponse
}
```

This allows for the following kinds of queries:

```graphql
query {
    getTaxon(id: "bbb") {
        id
        name
        rank
        description
        observations(first: 5) {
            filename
            parent {
                id
            }
        }
    }
}

query {
    queryTaxon(first: 10) {
        id
    }
}

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
}

mutation {
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
```

The API for the queries and mutation is heavily influenced by that of [Dgraph](https://dgraph.io/docs/graphql/).

## Backends

This crate is designed to support modular backend "drivers" for a given API. An implementation of a driver must include handlers for:
- accessing of an entity of a particular type, 
- accessing a filtered list of entities of a particular type, 
- accessing a field of an entity,
- adding a new entity of a given type,
- updating entities of a given type with a filter,
- starting a transaction,
- completing (with or without failure) a transaction

Currently, only my first attempt at a crude SQLite backend is implemented.

## Command line arguments

```
Usage: simplegql [OPTIONS] --schema <SCHEMA>

Options:
      --schema <SCHEMA>
      --sqlite <SQLITE>
      --port <PORT>      [default: 5001]
      --bind <BIND>      [default: localhost]
      --jwks <JWKS>
      --migrate
      --playground
  -h, --help             Print help
```

Some options in more detail:

- `--jwks` provides a URL to require authentication for mutations. This will
  also be for per-entity queries in the futrue. The authentication must be given
  in the `X-Auth-Header` HTTP header as `Bearer <JWK>` in the mutation request.
- `--migrate` allows the database to be overwritten with an updated schema. By
  default, the backend will only create new unconflicting entity types/edge
  types. This flag is required if you change the schema as it can cause data
  loss if you are not careful.
- `--playground` starts a GraphIQL playground at `/playground`.

## Directives

### @incoming

`@incoming(src_field: String)`

Indicates the given field is the counterpart of another outgoing (regular) field.

### @unique

`@unique`

### @dynamic - Dynamic resolvers

`@dynamic(hook: String!, args: Object!)`

Adding hooks is possible for custom resolvers of given fields. For example:

```graphql
type Taxon {
    name: String!
    children: [Taxon!]!
    parent: Taxon @incoming
    allParents: [Taxon!]! @dynamic(hook: "allParents": args: {})
}
```

These Taxon entites could be connected in a tree-like style. The `allParents`
hook can then provide the complete ancestory of a given entity. This must be
implemented in custom code as it is arbitrarily recursive (GraphQL does not
allow for recursive queries).

Currently, to include a hook like this, you must build your own crate using
`simplegql` as a dependency. The hooks are then made available with a HashMap to
the server configuration. The hooks must be written for a given backend.

In the future, these hooks should be allowed to defined in a different
high-level language such as Node.js. The hooks will also be able to be defined
in a backend-agnostic form or specific to a particular backend.

In addition there is support for separate HTTP endpoints.

# TODO list

Short-term TODOs:
- Auth audience and JWK url read from input file 
- Switching between ID and UUID for sqlite backend from configuration.
- Backend-agnostic custom resolvers
- Proper auth handling per entity instead of blanket lock on mutations
- Introduce proper error and result types
- Update JWKS before expiry.

Long-term TODOs:
- Resolvers in other languages through bindings
- Avoid (or at least wrap an interface around) the Box::leak for the initalised
  configuration. The purpose is to allow a program to start/stop a server
  without leaking memory. Must avoid unnecessary Arc+Mutex on every resolver
  call.
- Implement the filter feature fully using backend-specific option and a
  fallback to post-query filtering.
- Examples for the different directives and hooks.

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE2.0](LICENSE-APACHE2.0) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
