const {
  makeExecutableSchema,
  mergeSchemas,
} = require('graphql-tools');
const {BeehiveDirectives, BeehiveTypeDefs} = require("../queen")



const logger = { log: e => console.log(e) }
  

exports.schema = makeExecutableSchema({
  typeDefs: [
    BeehiveTypeDefs,
`

    interface AnyThing @beehiveTable(table_name: "things", pk_column: "thing_id") {
        thing_id: ID!
    }

    type Thing implements AnyThing @beehiveTable(table_name: "things", pk_column: "thing_id") {
        thing_id: ID!
        name: String
        related: [RelatedThing!] @beehiveRelation(target_type_name: "RelatedThing", target_field_name: "thing")
        dimensions: [Float!]
    }

    input ThingInput {
        name: String
        dimensions: [Float!]
    }

    type RelatedThing @beehiveTable(table_name: "rel_things") {
        rel_thing_id: ID!
        name: String
        thing: Thing @beehiveRelation(target_type_name: "Thing")
        subject: String
    }

    input RelatedThingInput {
        name: String
        thing: ID!
        subject: String
    }

    type ThingList {
        data: [Thing!]!
        page_info: PageInfo!
    }

    type RelatedThingsList {
        data: [RelatedThing!]!
        page_info: PageInfo
    }

    type Query {
        things(page: PaginationInput): ThingList! @beehiveList(target_type_name: "Thing")
        getThing(thing_id: String!): Thing @beehiveGet(target_type_name: "Thing")
        relatedThings(page: PaginationInput): RelatedThingsList! @beehiveList(target_type_name: "RelatedThing")
    }

    type Mutation {
        newThing(thing: ThingInput): Thing! @beehiveCreate(target_type_name: "Thing")
        newRelatedThing(relatedThing: RelatedThingInput): RelatedThing! @beehiveCreate(target_type_name: "RelatedThing")
    }

    schema @beehive(schema_name: "beehive_tests") {
        query: Query
        mutation: Mutation
    }

`
  ],
  schemaDirectives: BeehiveDirectives,
  logger: logger,
})

