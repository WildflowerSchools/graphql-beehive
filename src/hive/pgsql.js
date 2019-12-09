const uuidv4 = require('uuid/v4')
const { Pool } = require('pg')
const pool = new Pool()
const util = require('util')
const cassandraMAP = require("cassandra-map");


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


function mapType(schema, field) {
    var isList = false
    var fType = "UUID"
    var current = field.astNode
    do {
        if(current.kind == "ListType") {
            isList = true
        }
        if(current.kind == "NamedType") {
            switch(current.name.value) {
                case "String":
                    if(isList) {
                        return "varchar(256)[]"
                    } else {
                        return "varchar(256)"
                    }
                    break
                case "Int":
                    if(isList) {
                        return "bigint[]"
                    } else {
                        return "bigint"
                    }
                    break
                case "Float":
                    if(isList) {
                        return "double precision[]"
                    } else {
                        return "double precision"
                    }
                    break
                case "Boolean":
                    if(isList) {
                        return "boolean[]"
                    } else {
                        return "boolean"
                    }
                    break
                case "Datetime":
                    if(isList) {
                        return "timestamp[]"
                    } else {
                        return "timestamp"
                    }
                    break
                default:
                    var astNode = schema._typeMap[current.name.value].astNode
                    if(astNode && astNode.kind && astNode.kind == "EnumTypeDefinition") {
                        if(isList) {
                            return "varchar(256)[]"
                        } else {
                            return "varchar(256)"
                        }
                    } else {
                        if(isList) {
                            return "UUID[]"
                        } else {
                            return "UUID"
                        }
                    }
            }
        }
        current = current.type
    } while (true);
}

exports.ensureDatabase = async function(schema) {
    const client = await pool.connect()
    try {
        await client.query('BEGIN')
        await client.query(`CREATE SCHEMA IF NOT EXISTS ${schema._beehive.schema_name}`)
        console.log(`schema '${schema._beehive.schema_name}' should exist now`)

        console.log('ensuring the beehive_system_global_lookups table exists')
        await client.query(`CREATE TABLE IF NOT EXISTS ${schema._beehive.schema_name}.beehive_system_global_lookups (obj_uid UUID PRIMARY KEY, type_name varchar(128))`)

        console.log(schema._beehive.tables)

        for(var type of Object.keys(schema._beehive.tables)) {
            const table = schema._beehive.tables[type]
            console.log(`checking for '${table.table_name}' table`)
            if(table.table_type == "jsonb") {
                await client.query(`CREATE TABLE IF NOT EXISTS ${schema._beehive.schema_name}.${table.table_name} (${table.pk_column} UUID PRIMARY KEY, data JSONB, created timestamp DEFAULT current_timestamp, last_modified timestamp, type_name varchar(128))`)
                console.log(`table '${table.table_name}' should exist now`)
                await client.query(`CREATE INDEX IF NOT EXISTS ${schema._beehive.schema_name}_${table.table_name}_jsonbgin ON ${schema._beehive.schema_name}.${table.table_name} USING gin (data)`)
                console.log(`jsonb index '${table.table_name}' should exist now`)
            } else if(table.table_type == "native") {
                if(table.partition) {
                    await client.query(`CREATE TABLE IF NOT EXISTS ${schema._beehive.schema_name}.${table.table_name} (${table.pk_column} UUID PRIMARY KEY, data JSONB, created timestamp DEFAULT current_timestamp, last_modified timestamp, type_name varchar(128), ${table.partition} ${mapType(schema, table.type._fields[table.partition])}) PARTITION BY range (${table.partition})`)
                } else {
                    await client.query(`CREATE TABLE IF NOT EXISTS ${schema._beehive.schema_name}.${table.table_name} (${table.pk_column} UUID PRIMARY KEY, data JSONB, created timestamp DEFAULT current_timestamp, last_modified timestamp, type_name varchar(128))`)
                }
                console.log(`table '${table.table_name}' should exist now`)
                for(var name of Object.keys(table.type._fields)) {
                    if(name != table.pk_column && name != "system" && !table.native_exclude.includes(name)) {
                        var field = table.type._fields[name]
                        await client.query(`ALTER TABLE IF EXISTS ${schema._beehive.schema_name}.${table.table_name} ADD COLUMN IF NOT EXISTS ${name} ${mapType(schema, field)}`)
                        console.log(`table '${table.table_name}' native column '${name}' should exist now`)
                    }
                }
                if(table.indexes && table.indexes.length > 0) {
                    for(var index of table.indexes) {
                        await client.query(`CREATE INDEX IF NOT EXISTS beehive_${table.table_name}__${index.name} ON ${schema._beehive.schema_name}.${table.table_name} USING ${index.type} (${index.columns.join(', ')})`)
                    }
                }
            } else {
                console.log(`unknown tabe type '${table.table_type}', '${table.table_name}' not created`)
            }
        }

        for(var index of schema._beehive.indexes) {
            const table = schema._beehive.tables[index.target_type_name]
            await client.query(`CREATE INDEX IF NOT EXISTS ${schema._beehive.schema_name}_${table.table_name}_btree_${index.field.name} ON ${schema._beehive.schema_name}.${table.table_name} USING BTREE ((data->'${index.field.name}'))`)
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

        if(table_config.is_assignment && table_config.exclusive) {
            const start = forDB[table_config.start_field_name] = new Date(forDB[table_config.start_field_name]).toISOString()
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
                                {field: table_config.end_field_name, operator: "ISNULL"},
                                {field: table_config.end_field_name, operator: "GT", value: start},
                                {field: table_config.start_field_name, operator: "GT", value: start},
                            ],
                        },
                    ]
                }, table_config, schema)

            await client.query(`UPDATE ${schema._beehive.schema_name}.${table_config.table_name}
                                    SET data = data || '{"${table_config.end_field_name}": "${start}"}',
                                    last_modified = CURRENT_TIMESTAMP WHERE ${where}`)
        }

        if(table_config.table_type == "native") {
            // need to count the columns and update the insert to include all the columns
            var keys = Object.keys(forDB)
            if(keys.indexOf("system") >= 0) keys.splice(keys.indexOf("system"), 1)
            if(keys.indexOf(pk_column) >= 0) keys.splice(keys.indexOf(pk_column), 1)
            for(var k of table_config.native_exclude) {
                if(keys.indexOf(k) >= 0) keys.splice(keys.indexOf(k), 1)
            }
            var vars = [...Array(4 + keys.length).keys()]
            vars.shift()
            var fields = [pk_column, "data", "type_name"]
            var values = [pk, forDB, target_type_name]
            for(var key of keys) {
                if(!table_config.native_exclude.includes(key)) {
                    fields.push(key)
                    values.push(forDB[key])
                }
            }
            await client.query(`INSERT INTO ${schema._beehive.schema_name}.${table_config.table_name} (${fields.join(', ')})
                                    VALUES ($${ vars.join(', $')})
                                    ON CONFLICT (${pk_column})
                                        DO UPDATE
                                            SET ${doSets(fields, 1)}, last_modified = CURRENT_TIMESTAMP`, values)
        } else {
            await client.query(`INSERT INTO ${schema._beehive.schema_name}.${table_config.table_name} (${pk_column}, data, type_name)
                                    VALUES ($1, $2, $3)
                                    ON CONFLICT (${pk_column})
                                        DO UPDATE
                                            SET data = $2`, [
                               pk,
                               forDB,
                               target_type_name,
                               ])
        }
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
    var things = await pool.query(renderPageInfo(`SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name}`, pageInfo, table_config, schema))
    var rows = []
    for(var row of things.rows) {
        rows.push(applySystem(row))
    }
    return {data: rows, page_info: getPageInfoResult(pageInfo, rows.length)}
}


exports.getItem = async function(schema, table_config, pk) {
    var things = await pool.query(`SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${table_config.pk_column} = $1`, [pk])

    if(things.rows.length) {
        return applySystem(things.rows[0])
    } else {
        console.log(`looking in ${schema._beehive.schema_name}.${table_config.table_name} for ${table_config.pk_column} = ${pk} but didn't find it`)
        console.log(things)
    }
    return null
}


exports.getRelatedItems = async function(schema, table_config, target_field_name, value, pageInfo, explain_only) {
    return exports.getRelatedItemsFiltered(schema, table_config, target_field_name, value, null, pageInfo, explain_only)
}


exports.deleteRelations = async function(schema, table_config, target_field_name, value) {
    var things = await pool.query(`DELETE FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE data @> '{"${target_field_name}":  "${value}"}'`)

    if(things.rowCount) {
        return things.rowCount
    } else {
        console.log(`looking in ${schema._beehive.schema_name}.${table_config.table_name} to delete ${target_field_name} = ${value} but didn't find it`)
        console.log(things)
    }
    return null
}


exports.getRelatedItemsFiltered = async function(schema, table_config, target_field_name, value, query, pageInfo, explain_only) {
    if(table_config.table_type == "native") {
        var where = `${target_field_name} = ${encodeValue(schema, table_config, target_field_name, query.value, query.values)} ${query ? "AND" : "" }`
    } else {
        var where = `data @> '{"${target_field_name}":  "${value}"}' ${query ? "AND" : "" }`
    }
    var sql_query = `SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${where} ${renderQuery(query, table_config, schema)}`
    sql_query = renderPageInfo(sql_query, pageInfo, table_config, schema)
    if(explain_only) {
        return await pool.query("EXPLAIN " + sql_query)
    }
    var things = await pool.query(sql_query)
    var rows = []
    for(var row of things.rows) {
        rows.push(applySystem(row))
    }
    return rows
}

function renderPageInfo(query, pageInfo, table_config, schema) {
    var result = query
    if(pageInfo) {
        if(pageInfo.sort) {
            var sorts = []
            for(var sort of pageInfo.sort) {
                if(table_config.table_type == "native") {
                    // TODO - determine if this should only happen if the field is indexed
                    sorts.push(`${sort.field} ${sort.direction ? sort.direction : 'ASC'}`)
                } else {
                    sorts.push(`data->>'${sort.field}' ${sort.direction ? sort.direction : 'ASC'}`)
                }
            }
            result += ` ORDER BY ${sorts.join(", ")}`
        } else {
            // order by creation date by default, we do this so things are predictable with pagination if no sorting is specified
            result += ` ORDER BY created ASC`
        }
        if(table_config.table_type != "native") {
            result = `WITH temp as (${result}) SELECT * FROM temp`
        }
        // set a default max to 20 and an upper limit to the max at 1000 to prevent too much data from being loaded
        if(pageInfo.max && pageInfo.max <= 100) {
            result = `${result} LIMIT ${pageInfo.max}`
        } else if(pageInfo.max && pageInfo.max > 100) {
            result = `${result} LIMIT 1000`
        } else {
            result = `${result} LIMIT 20`
        }
        if(pageInfo.cursor) {
            result += ` ${decodeCursor(pageInfo.cursor)}`
        }
    }
    return result
}


function decodeCursor(cursor) {
    let buff = Buffer.from(cursor, 'base64')
    return buff.toString('ascii')
}


function encodeCursor(offset) {
    let buff = Buffer.from(`OFFSET ${offset}`, 'ascii')
    return buff.toString('base64')
}


function getPageInfoResult(pageInfo, count, total) {
    var nextOffset = 0
    if(pageInfo && pageInfo.cursor) {
        var poff = Number(decodeCursor(pageInfo.cursor).substring(7))
        nextOffset += poff
    }
    nextOffset += count
    const result = {
        total: total,
        count: count,
        max: pageInfo ? pageInfo.max : null,
        sort: pageInfo ? pageInfo.sort : null,
        cursor: encodeCursor(nextOffset),
    }
    return result
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
    CONTAINS: "@>",
    CONTAIN_BY: "<@",
}

function encodeValue(schema, table_config, name, value, values) {
    var field = table_config.type._fields[name]
    var col_type = mapType(schema, field)
    if(values) {
        return `ARRAY${cassandraMAP.stringify(values)}::varchar[]`
    }
    if(col_type.startsWith("varchar") || col_type.startsWith("UUID") || col_type == "timestamp") {
        return `'${value}'`
    } else {
        return value
    }
}


function renderQuery(query, table_config, schema) {
    if(query) {
        if(["EQ", "NE", "LIKE", "RE", "IN", "LT", "GT", "LTE", "GTE"].includes(query.operator)) {
            if(table_config.table_type == "native") {
                return `${query.field} ${opMap[query.operator]} ${encodeValue(schema, table_config, query.field, query.value, query.values)}`
            } else {
                if(query.field.indexOf(".") >= 0) {
                    return `data #>>'{${query.field.split(".").join(",")}}' ${opMap[query.operator]} '${query.value}'`
                }
                return `data->>'${query.field}' ${opMap[query.operator]} ${encodeValue(schema, table_config, query.field, query.value, query.values)}`
            }
        } else if(["CONTAINS", "CONTAIN_BY"].includes(query.operator)) {
            if(table_config.table_type == "native") {
                return `${query.field} ${opMap[query.operator]} ${encodeValue(schema, table_config, query.field, query.value, query.values)}`
            } else {
                var box = {}
                if(query.values) {
                    box[query.field] =  query.values
                } else {
                    box[query.field] =  query.value
                }
                return `data::jsonb ${opMap[query.operator]} '${JSON.stringify(box)}'`
            }
        } else if(query.operator == "ISNULL") {
            if(table_config.table_type == "native") {
                return `${query.field} IS NULL`
            } else {
                return `(NOT(data ? '${query.field}') OR (data ? '${query.field}') is NULL)`
            }
        } else if(query.operator == "NOTNULL") {
            if(table_config.table_type == "native") {
                return `${query.field} <> NULL`
            } else {
                return `((data ? '${query.field}') AND (data ? '${query.field}') <> NULL)`
            }
        } else {
            var childrenSQL = []
            for(var child of query.children) {
                childrenSQL.push(renderQuery(child, table_config, schema))
            }
            const joinBit = ` ${query.operator} `
            return `(${childrenSQL.join(joinBit)})`
        }
    }
    return ""
}

exports.queryType = async function(schema, table_config, query, pageInfo, explain_only) {
    var sql = `SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name}  ${query ? "WHERE" : "" } ${renderQuery(query, table_config, schema)}`
    sql = renderPageInfo(sql, pageInfo, table_config, schema)
    if(process.env.ENVIRONMENT == 'local') {
        console.log(sql)
    }
    if(explain_only) {
        return await pool.query("EXPLAIN " + sql)
    }
    var things = await pool.query(sql)
    var rows = []
    for(var row of things.rows) {
        rows.push(applySystem(row))
    }
    return {data: rows, page_info: getPageInfoResult(pageInfo, rows.length)}
}

function jsonToNativeWhere(query) {
    var wheres = []
    for (var field_name of Object.keys(query)) {
        wheres.push(`${field_name} = '${query[field_name]}'`)
    }
    return wheres.join(" AND ")
}

exports.simpleQueryType = async function(schema, table_config, query, pageInfo) {
    if(table_config.table_type == "native") {
        var sql = renderPageInfo(`SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${jsonToNativeWhere(query)}`, pageInfo, table_config, schema)
    } else {
        var sql = renderPageInfo(`SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE data @> '${JSON.stringify(query)}'`, pageInfo, table_config, schema)
    }
    var things = await pool.query(sql)
    var rows = []
    for(var row of things.rows) {
        rows.push(applySystem(row))
    }
    return {data: rows, page_info: getPageInfoResult(pageInfo, rows.length)}
}


function doSets(fields, offset) {
    var sets = []
    for(var i in fields) {
        sets.push(`${fields[i]} = $${(Number(i) + offset)}`)
    }
    return sets.join(", ")
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
        if(table_config.table_type == "jsonb") {
            await client.query(`UPDATE ${schema._beehive.schema_name}.${table_config.table_name}
                                    SET data = $2,
                                    type_name = $3,
                                    last_modified = CURRENT_TIMESTAMP WHERE ${pk_column} = $1`, [
                               pk,
                               forDB,
                               target_type_name,
                               ])
        } else if(table_config.table_type == "native") {
            var keys = Object.keys(forDB)
            if(keys.indexOf("system") >= 0) keys.splice(keys.indexOf("system"), 1)
            if(keys.indexOf(pk_column) >= 0) keys.splice(keys.indexOf(pk_column), 1)
            for(var k of table_config.native_exclude) {
                if(keys.indexOf(k) >= 0) keys.splice(keys.indexOf(k), 1)
            }
            var fields = ["data"]
            var values = [pk, forDB]
            for(var key of keys) {
                if(!table_config.native_exclude.includes(key)) {
                    fields.push(key)
                    values.push(forDB[key])
                }
            }
            
            var sql = `UPDATE ${schema._beehive.schema_name}.${table_config.table_name} SET ${doSets(fields, 2)}, last_modified = CURRENT_TIMESTAMP WHERE ${pk_column} = $1`
            console.log(sql)
            await client.query(sql, values)
        }
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


exports.appendToListField = async function(schema, table_config, object_id, target_field, input) {
    var current = await exports.getItem(schema, table_config, object_id)
    if(!current) {
        throw Error(`Object of type ${table_config.type.name} with primary key ${object_id} not found`)
    }
    if(typeof input == "string") {
        input = [input]
    }
    var vector = current[target_field]
    if(vector==null) vector = []
    input = input.filter(function(el) { return !vector.includes(el) })
    current[target_field] = vector.concat(input)
    return exports.putType(schema, table_config, object_id, current)
}


exports.deleteFromListField = async function(schema, table_config, object_id, target_field, input) {
    var current = await exports.getItem(schema, table_config, object_id)
    if(!current) {
        throw Error(`Object of type ${table_config.type.name} with primary key ${object_id} not found`)
    }
    if(typeof input == "string") {
        input = [input]
    }
    if(current[target_field]==null) current[target_field] = []
    current[target_field] = current[target_field].filter(function(el) { return !input.includes(el) })
    return exports.putType(schema, table_config, object_id, current)
}


exports.deleteType = async function(schema, table_config, pk) {
    var things = await pool.query(`DELETE FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${table_config.pk_column} = $1`, [pk])

    if(things.rowCount) {
        return things.rowCount
    } else {
        console.log(`looking in ${schema._beehive.schema_name}.${table_config.table_name} to delete ${table_config.pk_column} = ${pk} but didn't find it`)
        console.log(things)
    }
    return null
}
