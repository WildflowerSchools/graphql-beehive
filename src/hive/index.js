const gqldate = require("graphql-iso-date");

const { Pool } = require('pg')
const pool = new Pool()
const {BeehiveDirectives, BeehiveTypeDefs} = require("./types")
const pgsql = require("./pgsql")

var graphS3

try {
    graphS3 = require("@wildflowerschools/graphql-s3-directive")
} catch(err) {
    console.log(err)
    // not a problem, unless you want to use the s3-directive
}


exports.BeehiveResolvers = {
    Datetime: gqldate.GraphQLDateTime,
}

if (graphS3) {
    exports.BeehiveResolvers = Object.assign(exports.BeehiveResolvers, graphS3.resolvers)
}



exports.BeehiveDirectives = BeehiveDirectives
exports.BeehiveTypeDefs = BeehiveTypeDefs
exports.ensureDatabase = pgsql.ensureDatabase

exports.hivePg = pgsql


const BeehivePlugin = {
  requestDidStart(requestContext) {
    return {
      parsingDidStart(requestContext) {
        console.log('Parsing started!')
      },
      async executionDidStart(requestContext) {
        console.log('Executing request!')
        requestContext.context.client = async function() {
          if(!requestContext.context.__client) {
            requestContext.context.__client = await pool.connect()
            await requestContext.context.__client.query('BEGIN')
          }
          return requestContext.context.__client
        }
      },
      async didEncounterErrors(requestContext) {
        console.log('ERROR!')
        console.log(requestContext.errors)
      },
      async willSendResponse(requestContext) {
        console.log('Response started!')
        if(requestContext.context.__client) {
          return new Promise(async function(resolve, reject) {
            console.log("committing to db")
            try {
              await requestContext.context.__client.query('COMMIT')
              await requestContext.context.__client.release()
            } catch(e) {
              console.log(e)
            }
            resolve();
          })
        }
      }
    }
  },
};

exports.BeehivePlugin = BeehivePlugin;
