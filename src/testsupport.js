const { Pool } = require('pg')
const pool = new Pool()

const { BeehiveResolvers } = require("./hive")


exports.cleanPostgres = async function() {
    try {
        var schema_name = "beehive_tests"
        var tables = await pool.query(`SELECT table_name FROM information_schema.tables WHERE table_schema='${schema_name}'`)
        for (var table of tables.rows) {
            await pool.query(`TRUNCATE TABLE ${schema_name}.${table.table_name} CASCADE`)
        }
    } catch(err) {
        console.log(err)
    }
}


exports.doUnitTest = async function(tst, tableList) {
    return new Promise(async function(resolve, reject) {
        var err
        await exports.cleanPostgres();
        try {
            await tst()
        } catch(e) {
            err = e
        }
        await exports.cleanPostgres();
        if(err) {
            reject(err)
        } else {
            resolve()
        }
    });
}

exports.server = async function(schema) {
    const express = require("express");
    const { ApolloServer } = require('apollo-server-express');
    const voyager = require('graphql-voyager/middleware');
    const { ensureDatabase } = require('./hive');


    await (async () => {
        console.log("checking database")
        try {
            await ensureDatabase(schema)
            console.log("database checked")
        } catch (e) {
            console.log(e)
        }
    })();

    const server = new ApolloServer({
        schema,
        formatError: error => {
            console.log(error);
            return error;
        },
        formatResponse: response => {
            console.log(response);
            return response;
        },
    });

    const app = express();

    app.use('/voyager', voyager.express({ endpointUrl: server.graphqlPath }));

    server.applyMiddleware({ app });

    return app.listen({ port: 4000 }, () =>
      console.log(`🚀 Server ready at http://localhost:4000${server.graphqlPath}`)
    )

}