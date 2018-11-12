const uuidv4 = require('uuid/v4')
const {SchemaDirectiveVisitor} = require('graphql-tools')
const { Pool } = require('pg')
const pool = new Pool()
const dateFormat = require('dateformat')


process.env.check_db_schema = "true"


exports.BeehiveTypeDefs = `
    directive @beehive (schema_name: String) on SCHEMA

    directive @beehiveTable (table_name: String, pk_column: String, resolve_type_field: String) on OBJECT | INTERFACE

    directive @beehiveCreate(target_type_name: String!) on FIELD_DEFINITION

    directive @beehiveList(target_type_name: String!) on FIELD_DEFINITION

    directive @beehiveRelation(target_type_name: String!, target_field_name: String) on FIELD_DEFINITION

    directive @beehiveUTCDate on FIELD_DEFINITION

    directive @beehiveQuery(
            target_type_name: String!
        ) on FIELD_DEFINITION

    directive @beehiveGet(
            target_type_name: String!
        ) on FIELD_DEFINITION

    input PaginationInput {
        max: Int
        cursor: String
    }

    type PageInfo {
        total: Int
        count: Int
        max: Int
        cursor: String
    }

    type System {
        type_name: String!
        created: String! @beehiveUTCDate
        last_modified: String @beehiveUTCDate
    }

    type _beehive_helper_ {
        system: System!
    }

`

function findIdField(obj) {
    for (var field_name of Object.keys(obj._fields)) {
        if (obj._fields[field_name].type == "ID!") {
            return field_name
        }
    }
    return "id"
}

function applySystem(row) {
    var obj = row.data
    obj.system = {
        created: row.created,
        last_modified: row.last_modified,
        type_name: row.type_name,
    }
    return obj
}



class BeehiveDirective extends SchemaDirectiveVisitor {

    visitObject(type) {
        var table_config = {
            type: type,
            table_name: type.name,
            pk_column: findIdField(type),
        }

        if(this.args.pk_column) {
            table_config["pk_column"] = this.args.pk_column
        }

        if(this.args.table_name) {
            table_config["table_name"] = this.args.table_name
        }

        type._fields.system = this.schema._typeMap._beehive_helper_._fields.system

        this.schema._beehive.tables[type.name] = table_config
        this.schema._beehive.lctypemap[type.name.toLowerCase()] = type.name
    }

    visitInterface(type) {
        // visit the object to get all the benefits of table creation
        this.visitObject(type)

        // beehive is brought into scope for the resolveType function
        const _beehive = this.schema._beehive
        // field to use to do a type resolution
        const resolve_type_field = this.args.resolve_type_field
        type.resolveType = async function(obj, context, info) {
            // this is the actual resolver
            // if the resolve_type_field is set then we do a lookup on the object
            // and the lctypemap to determine the actual type
            if(resolve_type_field) {
                var resolvedType = obj[resolve_type_field]
                resolvedType = _beehive.lctypemap[resolvedType.toLowerCase()]
                return resolvedType
            }
            // otherwise, we just return the type name, it most cases this doesn't help since it will always be equal to the interface name
            // TODO: maybe that statement isn't true, maybe if the target type on the create is the actual type and not the interface then it
            //   will actually be able to resolve to the correct type but still share a table.
            return obj.system.type_name
        }
    }

    visitSchema(schema) {
        schema._beehive = {
            schema_name: this.args.schema_name ? this.args.schema_name : "beehive",
            tables: [],
            lctypemap: [],
        }
    }

}


class BeehiveMutationDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        const inputName = target_type_name.charAt(0).toLowerCase() + target_type_name.slice(1)
        const schema = this.schema
        const table_config = this.schema._beehive.tables[target_type_name]
        if(!table_config) {
            throw Error(`Table definition (${target_type_name}) not forund by beehive.`)
        }
        const pk_column = table_config.pk_column
        
        field.resolve = async function (obj, args, context, info) {
            const thing_id = uuidv4()
            const input = args[inputName]
            if(!input) {
                throw Error(`Input not found as expected (${inputName}) by beehive.`)
            }
            
            var forDB = {}

            if(!(pk_column in input)) {
                // pk_column not in input, so we set it to a uuidv4
                forDB[pk_column] = thing_id
            }

            for (var field_name of Object.keys(table_config.type._fields)) {
                if(field_name in input) {
                    forDB[field_name] = input[field_name]
                }
            }

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
        };
    }

}


class BeehiveListDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        const schema = this.schema

        field.resolve = async function (obj, args, context, info) {
            const table_config = schema._beehive.tables[target_type_name]
            var things = await pool.query(`SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name}`)

            var rows = []
            for(var row of things.rows) {
                rows.push(applySystem(row))
            }
            return {data: rows}
        }
    }

}

class BeehiveQueryDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        const schema = this.schema

        field.resolve = async function (obj, args, context, info) {
            // TODO - need to construct a query for this somehow
            // const table_config = schema._beehive.tables[target_type_name]
            // var things = await pool.query(`SELECT data FROM ${schema._beehive.schema_name}.${table_config.table_name}`)

            var rows = []
            // for(var row of things.rows) {
            //     rows.push(row.data)
            // }
            return {data: rows}
        }
    }

}

class BeehiveGetDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        const schema = this.schema

        field.resolve = async function (obj, args, context, info) {
            const table_config = schema._beehive.tables[target_type_name]
            var things = await pool.query(`SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${table_config.pk_column} = $1`, [args[table_config.pk_column]])

            if(things.rows.length) {
                return applySystem(things.rows[0])
            } else {
                return null
            }
        }
    }

}

class BeehiveRelationDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        const this_object_type = details.objectType
        const schema = this.schema
        const field_name = field.name
        const target_field_name = this.args.target_field_name


        const isListType = Reflect.has(field.type, "ofType")

        field.resolve = async function (obj, args, context, info) {
            console.log(`loading a relation ${target_type_name}`)
            const table_config = schema._beehive.tables[target_type_name]
            if(isListType) {
                const local_table_config = schema._beehive.tables[this_object_type]
                var query = `SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE data @> '{"${target_field_name}":  "${obj[local_table_config.pk_column]}"}'`
                var things = await pool.query(query)
                var rows = []
                for(var row of things.rows) {
                    rows.push(applySystem(row))
                }
                return rows
            } else {
                var things = await pool.query(`SELECT created, last_modified, data, type_name FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${table_config.pk_column} = $1`, [obj[field_name]])
                if(things.rows.length) {
                    return applySystem(things.rows[0])
                }
                return null
            }
        }
    }

}


class beehiveUTCDateDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const field_name = field.name

        field.resolve = async function (obj, args, context, info) {
            return dateFormat(obj[field_name], "isoUtcDateTime")
        }
    }

}




exports.BeehiveDirective = BeehiveDirective
exports.BeehiveMutationDirective = BeehiveMutationDirective

exports.BeehiveDirectives = {
    beehive: BeehiveDirective,
    beehiveTable: BeehiveDirective,
    beehiveCreate: BeehiveMutationDirective,
    beehiveList: BeehiveListDirective,
    beehiveQuery: BeehiveQueryDirective,
    beehiveGet: BeehiveGetDirective,
    beehiveRelation: BeehiveRelationDirective,
    beehiveUTCDate: beehiveUTCDateDirective
};

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


