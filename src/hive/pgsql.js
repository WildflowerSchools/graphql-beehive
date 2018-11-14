const uuidv4 = require('uuid/v4')
const { Pool } = require('pg')
const pool = new Pool()


function applySystem(row) {
    var obj = row.data
    obj.system = {
        created: row.created,
        last_modified: row.last_modified,
        type_name: row.type_name,
    }
    return obj
}

exports.applySystem = applySystem


exports.ensureDatabase = async function(schema) {
    const client = await pool.connect()
    try {
        await client.query('BEGIN')
        await client.query(`CREATE SCHEMA IF NOT EXISTS ${schema._beehive.schema_name}`)
        console.log(`schema '${schema._beehive.schema_name}' should exist now`)

        console.log(schema._beehive.tables)

        for (var type of Object.keys(schema._beehive.tables)) {
            const table = schema._beehive.tables[type]
            console.log(`checking for '${table.table_name}' table`)
            await client.query(`CREATE TABLE IF NOT EXISTS ${schema._beehive.schema_name}.${table.table_name} (${table.pk_column} UUID PRIMARY KEY, data JSONB, created timestamp DEFAULT current_timestamp, last_modified timestamp, type_name varchar(128))`)
            console.log(`table '${table.table_name}' should exist now`)
            await client.query(`CREATE INDEX IF NOT EXISTS ${schema._beehive.schema_name}_${table.table_name}_jsonbgin ON ${schema._beehive.schema_name}.${table.table_name} USING gin (data)`)
            console.log(`jsonb index '${table.table_name}' should exist now`)
        }

        await client.query('COMMIT')

    } catch (e) {
        console.log("something failed, rolling back")
        await client.query('ROLLBACK')
        throw e
    } finally {
        client.release
        console.log("all done")
    }
    var tables = await pool.query(`SELECT table_name FROM information_schema.tables WHERE table_schema='${schema._beehive.schema_name}'`)
    for (var table of tables.rows) {
        console.log(table)
    }
}



exports.insertType = async function(schema, table_config, input) {
    const pk_column = table_config.pk_column
    const target_type_name = table_config.type.name
    var forDB = {}

    if(!(pk_column in input)) {
        // pk_column not in input, so we set it to a uuidv4
        forDB[pk_column] = uuidv4()
    }

    for (var field_name of Object.keys(table_config.type._fields)) {
        if(field_name in input) {
            forDB[field_name] = input[field_name]
        }
    }

    const thing_id = forDB[pk_column]

    const client = await pool.connect()
    try {
        await client.query('BEGIN')
        await client.query(`INSERT INTO ${schema._beehive.schema_name}.${table_config.table_name} (${pk_column}, data, type_name) VALUES ($1, $2, $3)`, [
                           thing_id,
                           forDB,
                           target_type_name,
                           ])
        await client.query('COMMIT')
    } catch (e) {
        console.log("something failed")
        await client.query('ROLLBACK')
        throw e
    } finally {
        client.release()
    }
    var things = await pool.query(`SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${pk_column} = $1`, [thing_id])
    return applySystem(things.rows[0])
}


exports.listType = async function(schema, table_config, pageInfo) {
    var things = await pool.query(`SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name}`)
    var rows = []
    for(var row of things.rows) {
        rows.push(applySystem(row))
    }
    return rows
}


exports.getItem = async function(schema, table_config, pk) {
    var things = await pool.query(`SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${table_config.pk_column} = $1`, [pk])

    if(things.rows.length) {
        return applySystem(things.rows[0])
    }
    return null
}


exports.getRelatedItems = async function(schema, table_config, target_field_name, value) {
    var query = `SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE data @> '{"${target_field_name}":  "${value}"}'`
    var things = await pool.query(query)
    var rows = []
    for(var row of things.rows) {
        rows.push(applySystem(row))
    }
    return rows
}
