const gqldate = require("graphql-iso-date");

const {BeehiveDirectives, BeehiveTypeDefs} = require("./types")
const pgsql = require("./pgsql")


exports.BeehiveResolvers = {
    Datetime: gqldate.GraphQLDateTime,
}

exports.BeehiveDirectives = BeehiveDirectives
exports.BeehiveTypeDefs = BeehiveTypeDefs
exports.ensureDatabase = pgsql.ensureDatabase

exports.hivePg = pgsql
