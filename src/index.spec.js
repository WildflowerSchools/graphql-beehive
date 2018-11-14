const run = require('docker-run')
const { request } = require('graphql-request')
const expect = require('chai').expect
const { Pool } = require('pg')
const pool = new Pool()
const {server} = require("./testsupport")


const uri = "http://localhost:4000/graphql"

process.env.PGPASSWORD = "iamaninsecurepassword"
process.env.PGUSER = "beehive_user"
process.env.PGDATABASE = "beehive-tests-integrated"
process.env.PGHOST = "localhost"
process.env.PGPORT = "5432"


var dbContainer



before(async function() {
    // helper for waiting for things
    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    // check for postgreSQL to be running
    function status() {
        return pool.query("SELECT 'hello world'").then(data => {
          return true
        }).catch(err => {
          return false})
    }

    // do the deed, get postgreSQL running
    dbContainer = await (async function() {
        console.log('starting postgres')
        var isUp = await status()
        if (isUp) {
            console.log('postgres is already up')
            return true
        }
        var child = run('postgres:10.4', {
          remove: true,
          env: {
              POSTGRES_PASSWORD: "iamaninsecurepassword",
              POSTGRES_USER: "beehive_user",
              POSTGRES_DB: "beehive-tests-integrated",
          },
          ports: {
            5432: 5432
          }
        })
        // wait for it to come up
        for (let i=0; i < 4; i++) {
            var ok = await status()
            if (ok) {
                return child
            }
            await sleep(3000)
        }

        // if it hasn't come up destroy it
        child.destroy()
        throw Error("postgres didn't start")
    })()

})


after(async function(){ 
    console.log("shutting down postgres and express")
    dbContainer.destroy()
})



describe('Beehive test suite', function(){


    var expressApp

    before(async function() {
        // setup an apollo-server-express app and run it
        const { schema } = require("./schema");
        expressApp = await server(schema)
    })

    after(async function() {
        expressApp.close()
    })

    describe('things', function() {
        it('creates a thing', async function() {
            var query = `
                    mutation {
                      newThing(thing: {name: "thing"}) {
                        thing_id
                        name
                        system {
                            created
                        }
                      }
                    }
                `
            var thing = await request(uri, query)
            console.log(thing)
            expect(thing).to.not.equal(null)
            expect(thing.newThing.thing_id).to.not.equal(null)
            expect(thing.newThing.name).to.equal("thing")
            expect(thing.newThing.system.created).to.not.equal(null)
        })

        it('replaces a thing', async function() {
            var query = `
                    mutation {
                      newThing(thing: {name: "tobereplaced"}) {
                        thing_id
                        name
                        system {
                            created
                        }
                      }
                    }
                `
            var thing = await request(uri, query)
            expect(thing.newThing.name).to.equal("tobereplaced")
            query = `
                    mutation {
                        replaceThing(thing_id: "${thing.newThing.thing_id}", thing: {name: "replaced"}) {
                            thing_id
                            name
                            system {
                                created
                                last_modified
                            }
                        }
                    }
                `
            var replacedthing = await request(uri, query)
            expect(replacedthing.replaceThing.name).to.equal("replaced")
            expect(replacedthing.replaceThing.thing_id).to.equal(thing.newThing.thing_id)
        })

        it('patch a related thing', async function() {
            var query = `
                    mutation {
                      newRelatedThing(relatedThing: {name: "munster", subject: "director"}) {
                            rel_thing_id
                            name
                            subject
                        }
                    }
                `
            var thing = await request(uri, query)
            expect(thing.newRelatedThing.name).to.equal("munster")
            expect(thing.newRelatedThing.subject).to.equal("director")
            query = `
                    mutation {
                        updateRelatedThing(rel_thing_id: "${thing.newRelatedThing.rel_thing_id}", relatedThing: {subject: "futurama"}) {
                            rel_thing_id
                            name
                            subject
                        }
                    }
                `
            thing = await request(uri, query)
            expect(thing.updateRelatedThing.name).to.equal("munster")
            expect(thing.updateRelatedThing.subject).to.equal("futurama")
        })

        it('list things', async function() {
            var query = `
                    query {
                      things {
                        data {
                            ... on Thing {
                                thing_id
                                name
                            }
                        }
                      }
                    }
                `
            var things = await request(uri, query)
            console.log(things)
            expect(things).to.not.equal(null)
            expect(things.things.data).to.not.equal(null)
            expect(things.things.data[0].thing_id).to.not.equal(null)
            expect(things.things.data[0].name).to.equal("thing")
        })

        it('create a related thing', async function() {
            var query = `
                    mutation {
                      newThing(thing: {name: "thing"}) {
                        thing_id
                      }
                    }
                `
            var thing = await request(uri, query)
            query = `
                    mutation {
                      newRelatedThing(relatedThing: {name: "thing", subject: "director", thing: "${thing.newThing.thing_id}"}) {
                            rel_thing_id
                            name
                            subject
                            thing {
                                thing_id
                            }
                            start
                        }
                    }
                `
            console.log(query)
            var rel_thing = await request(uri, query)
            console.log(rel_thing)
            expect(rel_thing).to.not.equal(null)
            expect(rel_thing.newRelatedThing.rel_thing_id).to.not.equal(null)
            expect(rel_thing.newRelatedThing.name).to.equal("thing")
            expect(rel_thing.newRelatedThing.subject).to.equal("director")
            expect(rel_thing.newRelatedThing.thing.thing_id).to.equal(thing.newThing.thing_id)
        })

        it('get a thing', async function() {
            var query = `
                    mutation {
                        newThing(thing: {name: "monster-rock"}) {
                            thing_id
                        }
                    }
                `
            var thing = await request(uri, query)
            query = `
                    query {
                        getThing(thing_id: "${thing.newThing.thing_id}") {
                            thing_id
                            name
                        }
                    }
                `
            console.log(query)
            var getThing = await request(uri, query)
            console.log(getThing)
            expect(getThing.getThing).to.not.equal(null)
            expect(getThing.getThing.thing_id).to.equal(thing.newThing.thing_id)
            expect(getThing.getThing.name).to.equal("monster-rock")
        })

        it('list relations', async function() {
            var query = `
                    mutation {
                      newThing(thing: {name: "multiple-related"}) {
                        thing_id
                      }
                    }
                `
            var thing = await request(uri, query)
            query = `
                    mutation {
                      first: newRelatedThing(relatedThing: {name: "related 1", thing: "${thing.newThing.thing_id}"}) {
                            rel_thing_id
                        }
                      second: newRelatedThing(relatedThing: {name: "related 2", thing: "${thing.newThing.thing_id}"}) {
                            rel_thing_id
                        }
                    }
                `
            console.log(query)
            var related = await request(uri, query)
            console.log(related)
            expect(related).to.not.equal(null)
            expect(related.first.rel_thing_id).to.not.equal(null)
            expect(related.second.rel_thing_id).to.not.equal(null)

            query = `
                    query {
                        updatedThing: getThing(thing_id: "${thing.newThing.thing_id}") {
                            related {
                                rel_thing_id
                            }
                        }
                    }
                `
            console.log(query)
            var getThing = await request(uri, query)
            console.log(getThing)
            expect(getThing.updatedThing.related).to.not.equal(null)
            expect(getThing.updatedThing.related.length).to.equal(2)
        })

    })

})






describe('Beehive test suite', function(){

    describe('exceptions', function() {
        it('should fail', async function() {

            expect(function() { require("./schema/no_schema")}).to.throw(TypeError)
        })


    })

})








