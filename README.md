# GraphQL-Beehive
-----------------------------------

A graphQL library for constructing graphQL services with PostgreSQL and Apollo-server. Uses Schema directives to assist in setting up tables and basic queries.


## Getting started

### Tag your schema

```graphql
    schema @beehive(schema_name: "beehive_tests") {
        query: Query
        mutation: Mutation
    }
```

The `beehive` directive is used on the schema to tell Beehive what PostgreSQL schema to use. Beehive will create the schema if it does not exist.

### Tables

```graphql
    type Thing @beehiveTable(table_name: "things") {
        thing_id: ID! @beehiveIndex(primary: true)
        name: String
        related: [RelatedThing!] @beehiveRelation(target_type_name: "RelatedThing", target_field_name: "thing")
        dimensions: [Float!]
    }
```

The `beehiveTable` directive is used on the types to configure the tables to use for each type. Types that do not have a directive will not be handled correctly by Beehive.

NOTE: Tagging input types in unnecessary, a mutation direction will handle those cases.

#### Linking things

```graphql
        related: [RelatedThing!] @beehiveRelation(target_type_name: "RelatedThing", target_field_name: "thing")
```

The `beehiveRelation` directive adds a resolver to the field that does one of two things. If the field is a list then the resolver queries on the `target_field_name` on the target table with the id of the object in context. If the field is not a list then it is expected that the field will have the UUID of the related object in the value in context, it loads that one object from the target table.


### Queries

#### Listing Objects

```graphql
    type Query {
        things(page: PaginationInput): ThingList! @beehiveList(target_type_name: "Thing")
    }
```
The `beehiveList` directive attaches a list resolver to the query. You need to set the type name so Beehive can discover the correct table to load from. The `PaginationInput` input has parameters for doing pagination and sorting. `PageInfo` is returned along side the data array in the response.

```graphql
    # the input for any request that would return a list of object
    input PaginationInput {
        max: Int
        cursor: String
    }

    # the output of a list of objects, where `Thing` is the type of object you expect
    type ThingList {
        data: [Thing!]!
        page_info: PageInfo
    }

    type PageInfo {
        total: Int
        count: Int
        max: Int
        cursor: String
    }
```
#### Getting an Object

```graphql
        getThing(thing_id: String!): Thing @beehiveGet(target_type_name: "Thing")
```

The `beehiveGet` directive associates a get resolver with the query. You need to set the type name so Beehive can discover the correct table to load from.

### Mutations

### Creating an object

```graphql
        newThing(thing: ThingInput): Thing! @beehiveCreate(target_type_name: "Thing")
```

The `beehiveCreate` directive attaches an insert resolver to the mutation. You need to set the type name so Beehive can discover the correct table to insert into.


### TODOs

- More tests, coverage is adequate but could be better, especially for failure cases
- `PaginationInput` is not yet implemented
- `PageInfo` is not yet implemented
- `@beehiveCreate` does not handle nested objects well at this point
- Update mutations are not yet supported
- Delete mutations are not yet supported
- Better logic in the database provisioning, right now it just blindly does a "create if not exists", but maybe needs to be more resilient?

## Contributing

Got ideas? Create an issue and/or create a pull request.


#### The MIT License (MIT)

Copyright (c) 2018 Wildflower Foundation

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.