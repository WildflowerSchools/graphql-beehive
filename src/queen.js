const uuidv4 = require('uuid/v4');
const {
    SchemaDirectiveVisitor,
} = require('graphql-tools');
const { Pool } = require('pg')
const pool = new Pool()

process.env.check_db_schema = "true"


exports.BeehiveTypeDefs = `
    directive @beehive (schema_name: String) on SCHEMA

    directive @beehiveTable (table_name: String, pk_column: String) on OBJECT

    directive @beehiveCreate(target_type_name: String!) on FIELD_DEFINITION

    directive @beehiveList(target_type_name: String!) on FIELD_DEFINITION

    directive @beehiveRelation(target_type_name: String!, target_field_name: String) on FIELD_DEFINITION

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

`

function findIdField(obj) {
    for (var field_name of Object.keys(obj._fields)) {
        if (obj._fields[field_name].type == "ID!") {
            return field_name
        }
    }
    return "id"
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

        this.schema._beehive.tables[type.name] = table_config
    }

    visitSchema(schema) {
        schema._beehive = {
            schema_name: this.args.schema_name ? this.args.schema_name : "beehive",
            tables: [],
        }
    }

}


class BeehiveMutationDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        const inputName = target_type_name.charAt(0).toLowerCase() + target_type_name.slice(1)
        const schema = this.schema
        const table_config = this.schema._beehive.tables[target_type_name]
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
                // const ft = table_config.type._fields[field_name].type
                // TODO - lookup to see if a field is a scalar or not
                if(field_name in input) {
                    forDB[field_name] = input[field_name]
                }
            }

            const client = await pool.connect()
            try {
                await client.query('BEGIN')
                await client.query(`INSERT INTO ${schema._beehive.schema_name}.${table_config.table_name} (${pk_column}, data) VALUES ($1, $2)`, [
                                   thing_id,
                                   forDB
                                   ])
                await client.query('COMMIT')
            } catch (e) {
                console.log("something failed")
                await client.query('ROLLBACK')
                throw e
            } finally {
                client.release()
            }
            var things = await pool.query(`SELECT data FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${pk_column} = $1`, [thing_id])
            return things.rows[0].data
        };
    }

}


class BeehiveListDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        const schema = this.schema

        field.resolve = async function (obj, args, context, info) {
            const table_config = schema._beehive.tables[target_type_name]
            var things = await pool.query(`SELECT data FROM ${schema._beehive.schema_name}.${table_config.table_name}`)

            var rows = []
            for(var row of things.rows) {
                rows.push(row.data)
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
            var things = await pool.query(`SELECT data FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${table_config.pk_column} = $1`, [args[table_config.pk_column]])

            if(things.rows.length) {
                return things.rows[0].data
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
                var query = `SELECT data FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE data @> '{"${target_field_name}":  "${obj[local_table_config.pk_column]}"}'`
                var things = await pool.query(query)
                var rows = []
                for(var row of things.rows) {
                    rows.push(row.data)
                }
                return rows
            } else {
                var things = await pool.query(`SELECT data FROM ${schema._beehive.schema_name}.${table_config.table_name} WHERE ${table_config.pk_column} = $1`, [obj[field_name]])
                if(things.rows.length) {
                    return things.rows[0].data
                }
                return null
            }
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
            await client.query(`CREATE TABLE IF NOT EXISTS ${schema._beehive.schema_name}.${table.table_name} (${table.pk_column} UUID PRIMARY KEY, data JSONB)`)
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


