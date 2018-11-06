const { Pool } = require('pg')
const pool = new Pool()


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
