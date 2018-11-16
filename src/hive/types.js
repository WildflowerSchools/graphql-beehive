const {SchemaDirectiveVisitor} = require('graphql-tools')
const {insertType, listType, getItem, getRelatedItems, putType, patchType, queryType, simpleQueryType} = require("./pgsql")


exports.BeehiveTypeDefs = `
    # ISO formated Date Timestamp
    scalar Datetime

    directive @beehive (schema_name: String) on SCHEMA

    directive @beehiveTable (table_name: String, pk_column: String, resolve_type_field: String) on OBJECT | INTERFACE

    directive @beehiveIndexed(target_type_name: String!) on FIELD_DEFINITION

    directive @beehiveCreate(target_type_name: String!) on FIELD_DEFINITION
    
    directive @beehiveUpdate(target_type_name: String!) on FIELD_DEFINITION
    
    directive @beehiveReplace(target_type_name: String!) on FIELD_DEFINITION

    directive @beehiveList(target_type_name: String!) on FIELD_DEFINITION
    
    directive @beehiveRelation(target_type_name: String!, target_field_name: String) on FIELD_DEFINITION

    directive @beehiveUnion on UNION

    directive @beehiveQuery(
            target_type_name: String!
        ) on FIELD_DEFINITION

    directive @beehiveSimpleQuery(
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
        created: Datetime!
        last_modified: Datetime
    }

    type _beehive_helper_ {
        system: System!
    }

    enum Operator {
        OR
        AND
        NOT
        EQ
        NE
        LIKE
        RE
        IN
    }

    input QueryExpression {
        field: String
        operator: Operator!
        value: String
        children: [QueryExpression!]
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
            pk_column: this.args.pk_column,
        }

        if(!this.args.pk_column) {
            table_config["pk_column"] = findIdField(type)
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
            indexes: [],
        }
    }

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        var index_config = {
            target_type_name: target_type_name,
            field: field,
        }
        this.schema._beehive.indexes.push(index_config)
    }

}


class BeehiveCreateDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        const inputName = target_type_name.charAt(0).toLowerCase() + target_type_name.slice(1)
        const schema = this.schema
        const table_config = this.schema._beehive.tables[target_type_name]
        if(!table_config) {
            throw Error(`Table definition (${target_type_name}) not forund by beehive.`)
        }
        
        field.resolve = async function (obj, args, context, info) {
            const input = args[inputName]
            if(!input) {
                throw Error(`Input not found as expected (${inputName}) by beehive.`)
            }
            
            return insertType(schema, table_config, input)
        }
    }

}

class BeehiveReplaceDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        const inputName = target_type_name.charAt(0).toLowerCase() + target_type_name.slice(1)
        const schema = this.schema

        field.resolve = async function (obj, args, context, info) {
            const table_config = schema._beehive.tables[target_type_name]
            return putType(schema, table_config, args[table_config.pk_column], args[inputName])
        }
    }

}

class BeehiveUpdateDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        const inputName = target_type_name.charAt(0).toLowerCase() + target_type_name.slice(1)
        const schema = this.schema

        field.resolve = async function (obj, args, context, info) {
            const table_config = schema._beehive.tables[target_type_name]
            return patchType(schema, table_config, args[table_config.pk_column], args[inputName])
        }
    }

}


class BeehiveListDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        const schema = this.schema

        field.resolve = async function (obj, args, context, info) {
            const table_config = schema._beehive.tables[target_type_name]
            return {data: listType(schema, table_config, args.page)}
        }
    }

}

class BeehiveQueryDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        const schema = this.schema

        field.resolve = async function (obj, args, context, info) {
            const table_config = schema._beehive.tables[target_type_name]
            var rows = queryType(schema, table_config, args.query, args.page)
            return {data: rows}
        }
    }

}

class BeehiveSimpleQueryDirective extends SchemaDirectiveVisitor {

    visitFieldDefinition(field, details) {
        const target_type_name = this.args.target_type_name
        const schema = this.schema

        field.resolve = async function (obj, args, context, info) {
            const table_config = schema._beehive.tables[target_type_name]
            var query = {}

            for (var field_name of Object.keys(args)) {
                if(field_name != "page") {
                    query[field_name] = args[field_name]
                }
            }

            var rows = simpleQueryType(schema, table_config, query, args.page)
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
            return getItem(schema, table_config, args[table_config.pk_column])
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
                return getRelatedItems(schema, table_config, target_field_name, obj[local_table_config.pk_column])
            } else {
                return getItem(schema, table_config, obj[field_name])
            }
        }
    }

}


class BeehiveUnionDirective extends SchemaDirectiveVisitor {
    visitUnion(union) {
        union.resolveType = async function(obj, context, info) {
            // This doesn't actually work.
            return obj.system.type_name
        } 
    }
}


exports.BeehiveDirectives = {
    beehive: BeehiveDirective,
    beehiveTable: BeehiveDirective,
    beehiveCreate: BeehiveCreateDirective,
    beehiveList: BeehiveListDirective,
    beehiveQuery: BeehiveQueryDirective,
    beehiveSimpleQuery: BeehiveSimpleQueryDirective,
    beehiveGet: BeehiveGetDirective,
    beehiveRelation: BeehiveRelationDirective,
    beehiveUnion: BeehiveUnionDirective,
    beehiveReplace: BeehiveReplaceDirective,
    beehiveUpdate: BeehiveUpdateDirective,
    beehiveIndexed: BeehiveDirective,
};