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

        console.log('ensuring the beehive_system_global_lookups table exists')
        await client.query(`CREATE TABLE IF NOT EXISTS ${schema._beehive.schema_name}.beehive_system_global_lookups (obj_uid UUID PRIMARY KEY, type_name varchar(128))`)

        console.log(schema._beehive.tables)

        for (var type of Object.keys(schema._beehive.tables)) {
            const table = schema._beehive.tables[type]
            console.log(`checking for '${table.table_name}' table`)
            await client.query(`CREATE TABLE IF NOT EXISTS ${schema._beehive.schema_name}.${table.table_name} (${table.pk_column} UUID PRIMARY KEY, data JSONB, created timestamp DEFAULT current_timestamp, last_modified timestamp, type_name varchar(128))`)
            console.log(`table '${table.table_name}' should exist now`)
            await client.query(`CREATE INDEX IF NOT EXISTS ${schema._beehive.schema_name}_${table.table_name}_jsonbgin ON ${schema._beehive.schema_name}.${table.table_name} USING gin (data)`)
            console.log(`jsonb index '${table.table_name}' should exist now`)
        }

        for(var index of schema._beehive.indexes) {
            const table = schema._beehive.tables[index.target_type_name]
            await client.query(`CREATE INDEX IF NOT EXISTS ${schema._beehive.schema_name}_${table.table_name}_btree_${index.field.name} ON ${schema._beehive.schema_name}.${table.table_name} USING BTREE ((data->'index.field.name'))`)
            console.log(`btree index '${table.table_name}' for '${index.field.name}' should exist now`)
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


async function setGlobalLookup(schema, table_config, uid) {
    const client = await pool.connect()
    const target_type_name = table_config.type.name
    try {
        await client.query('BEGIN')
        await client.query(`INSERT INTO ${schema._beehive.schema_name}.beehive_system_global_lookups (obj_uid, type_name)
                                VALUES ($1, $2)
                                ON CONFLICT (obj_uid)
                                    DO NOTHING`, [
                           uid,
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
}

exports.inferType = async function(schema, uid) {
    var things = await pool.query(`SELECT type_name FROM ${schema._beehive.schema_name}.beehive_system_global_lookups WHERE obj_uid = $1`, [uid])

    if(things.rows.length) {
        return things.rows[0].type_name
    }
    return null
}



exports.insertType = async function(schema, table_config, input) {
    const client = await pool.connect()
    const pk_column = table_config.pk_column
    const target_type_name = table_config.type.name
    var pk = input[pk_column]

    if(!(pk_column in input)) {
        // pk_column not in input, so we set it to a uuidv4
        pk = uuidv4()
    }

    try {
        const target_type_name = table_config.type.name
        var forDB = {}

        forDB[pk_column] = pk

        for (var field_name of Object.keys(table_config.type._fields)) {
            if(field_name in input) {
                forDB[field_name] = input[field_name]
            }
        }

        await client.query('BEGIN')

        if(table_config.table_type == "assignment" && table_config.exclusive) {
            let where = renderQuery({
                    operator: "AND",
                    children: [
                        {
                            field: table_config.assigned_field,
                            operator: "EQ",
                            value: forDB[table_config.assigned_field],
                        },
                        {
                            operator: "OR",
                            children: [
                                {field: "end", operator: "ISNULL"},
                                {field: "end", operator: "GT", value: forDB.start},
                                {field: "start", operator: "GT", value: forDB.start},
                            ],
                        },
                    ]
                })
            // console.log(where)
            await client.query(`UPDATE ${schema._beehive.schema_name}.${table_config.table_name} 
                                    SET data = data || '{"${table_config.end_field_name}": "${forDB.start}"}',
                                    last_modified = CURRENT_TIMESTAMP WHERE ${where}`)
        }

        await client.query(`INSERT INTO ${schema._beehive.schema_name}.${table_config.table_name} (${pk_column}, data, type_name)
                                VALUES ($1, $2, $3)
                                ON CONFLICT (${pk_column})
                                    DO UPDATE
                                        SET data = $2`, [
                           pk,
                           forDB,
                           target_type_name,
                           ])
        await client.query('COMMIT')
        await setGlobalLookup(schema, table_config, pk)
    } catch (e) {
        console.log("something failed")
        await client.query('ROLLBACK')
        throw e
    } finally {
        client.release()
    }
    var things = await pool.query(`SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${pk_column} = $1`, [pk])
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


exports.getRelatedItems = async function(schema, table_config, target_field_name, value, pageInfo) {
    var query = `SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE data @> '{"${target_field_name}":  "${value}"}'`
    var things = await pool.query(query)
    var rows = []
    for(var row of things.rows) {
        rows.push(applySystem(row))
    }
    return rows
}


const opMap = {
    EQ: "=",
    NE: "<>",
    LIKE: "LIKE",
    RE: "=",
    IN: "IN",
    LT: "<",
    GT: ">",
    LTE: "<=",
    GTE: ">=",
}


function renderQuery(query) {
    if(["EQ", "NE", "LIKE", "RE", "IN", "LT", "GT", "LTE", "GTE"].includes(query.operator)) {
        // simple query with no child-elements
        // TODO - add support for numeric values
        return `data->>'${query.field}' ${opMap[query.operator]} '${query.value}'`
    } else if(query.operator == "ISNULL") {
        return `(NOT(data ? '${query.field}') OR (data ? '${query.field}') is NULL)`
    } else if(query.operator == "NOTNULL") {
        return `((data ? '${query.field}') AND (data ? '${query.field}') <> NULL)`
    } else {
        // console.log(query)
        // a boolean expression with children
        var childrenSQL = []
        for(var child of query.children) {
            childrenSQL.push(renderQuery(child))
        }
        const joinBit = ` ${query.operator} `
        return `(${childrenSQL.join(joinBit)})`
    }
}

exports.queryType = async function(schema, table_config, query, pageInfo) {
    var sql = `SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${renderQuery(query)}`
    var explained = await pool.query(`EXPLAIN ${sql}`)
    var things = await pool.query(sql)
    var rows = []
    for(var row of things.rows) {
        rows.push(applySystem(row))
    }
    return rows
}

exports.simpleQueryType = async function(schema, table_config, query, pageInfo) {
    // console.log(query)
    var sql = `SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE data @> '${JSON.stringify(query)}'`
    // console.log(sql)
    var explained = await pool.query(`EXPLAIN ${sql}`)
    // console.log(explained)
    var things = await pool.query(sql)
    var rows = []
    for(var row of things.rows) {
        rows.push(applySystem(row))
    }
    // console.log(rows)
    return rows
}


exports.putType = async function(schema, table_config, pk, input) {
    const client = await pool.connect()
    const pk_column = table_config.pk_column
    try {
        const target_type_name = table_config.type.name
        var forDB = {}

        forDB[pk_column] = pk

        for (var field_name of Object.keys(table_config.type._fields)) {
            if(field_name in input) {
                forDB[field_name] = input[field_name]
            }
        }

        await client.query('BEGIN')
        await client.query(`UPDATE ${schema._beehive.schema_name}.${table_config.table_name} 
                                SET data = $2,
                                type_name = $3,
                                last_modified = CURRENT_TIMESTAMP WHERE ${pk_column} = $1`, [
                           pk,
                           forDB,
                           target_type_name,
                           ])
        await client.query('COMMIT')
        await setGlobalLookup(schema, table_config, pk)
    } catch (e) {
        console.log("something failed")
        await client.query('ROLLBACK')
        throw e
    } finally {
        client.release()
    }
    var things = await pool.query(`SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${pk_column} = $1`, [pk])
    return applySystem(things.rows[0])
}


exports.patchType = async function(schema, table_config, pk, input) {
    var current = await exports.getItem(schema, table_config, pk)
    if(!current) {
        throw Error(`Object of type ${table_config.type.name} with primary key ${pk} not found`)
    }
    for (var field_name of Object.keys(input)) {
        current[field_name] = input[field_name]
    }
    return exports.putType(schema, table_config, pk, current)
}

