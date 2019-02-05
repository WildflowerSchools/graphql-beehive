const run = require('docker-run')
const { request } = require('graphql-request')
const expect = require('chai').expect
const { Pool } = require('pg')
const pool = new Pool()
const {server} = require("./testsupport")
const ServerMock = require("mock-http-server");


const uri = "http://localhost:4423/graphql"

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
                await sleep(10000)
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



describe('Beehive general suite', function(){


    var expressApp
    var mockserver = new ServerMock({ host: "localhost", port: 1111 })

    before(async function() {
        // setup an apollo-server-express app and run it
        const { schema } = require("../src/schema");
        expressApp = await server(schema)
    })

    after(async function() {
        expressApp.close()
    })

    beforeEach(async function() {
        mockserver.start(function(){})
        mockserver.on({
            method: 'POST',
            path: '/streams/fakenews',
            reply: {
                status:  200,
                headers: { "content-type": "application/json" },
                body:    JSON.stringify({ status: "ok" })
            }
        });
    })


    afterEach(async function() {
        mockserver.stop(function(){})
    })

    describe('things', function() {

        it('assignments', async function() {
            const query = `
                    mutation {
                      holder1: holder(holder: {name: "holder1"}) {
                            holder_id
                        }
                      holder2: holder(holder: {name: "holder2"}) {
                            holder_id
                        }
                      held1: held(held: {name: "highlander1"}) {
                            held_id
                      }
                      held2: held(held: {name: "highlander2"}) {
                            held_id
                      }
                    }
                `
            var results = await request(uri, query)
            expect(results.held1.held_id).to.not.equal(null)
            expect(results.held2.held_id).to.not.equal(null)
            expect(results.holder1.holder_id).to.not.equal(null)
            expect(results.holder2.holder_id).to.not.equal(null)
            var assign_1 = `
                    mutation {
                        assignment1: assignment(assignment: {assigned: "${results.held1.held_id}", holder: "${results.holder1.holder_id}", start: "${new Date().toISOString()}"}) {
                            assignment_id
                        }
                        assignment2: assignment(assignment: {assigned: "${results.held2.held_id}", holder: "${results.holder1.holder_id}", start: "${new Date().toISOString()}"}) {
                            assignment_id
                        }
                    }
                `
            var results_assign_1 = await request(uri, assign_1)
            console.log("================== ASSIGNMENTS ==============================")
            console.log(results_assign_1)
            expect(results_assign_1.assignment1.assignment_id).to.not.equal(null)
            expect(results_assign_1.assignment2.assignment_id).to.not.equal(null)
            var verifyQuery = `
                query {
                  getAssignments {
                    data {
                      assignment_id
                      assigned {
                        held_id
                      }
                      holder {
                        holder_id
                      }
                      start
                      end
                    }
                  }
                }
            `
            console.log("================== VERIFICATION SET 1 =======================")
            var verificationSet = await request(uri, verifyQuery)
            console.log(verificationSet)
            expect(verificationSet.getAssignments.data.length).to.equal(2)
            var assign_2 = `
                    mutation {
                        assignment1: assignment(assignment: {assigned: "${results.held1.held_id}", holder: "${results.holder2.holder_id}", start: "${new Date().toISOString()}"}) {
                            assignment_id
                        }
                    }
                `
            var results_assign_2 = await request(uri, assign_2)
            console.log("================== VERIFICATION SET 2 =======================")
            verificationSet = await request(uri, verifyQuery)
            console.log(verificationSet)
            expect(verificationSet.getAssignments.data.length).to.equal(3)
            for(var assignment of verificationSet.getAssignments.data) {
                if(assignment.assignment_id == results_assign_2.assignment1.assignment_id) {
                    expect(assignment.end).to.equal(null)
                    expect(assignment.assigned.held_id).to.equal(results.held1.held_id)
                    expect(assignment.holder.holder_id).to.equal(results.holder2.holder_id)
                } else if(assignment.assigned.held_id == results.held1.held_id) {
                    expect(assignment.end).to.not.equal(null)
                    expect(assignment.assigned.held_id).to.equal(results.held1.held_id)
                    expect(assignment.holder.holder_id).to.equal(results.holder1.holder_id)
                } else if(assignment.assigned.held_id == results.held1.held_id) {
                    expect(assignment.end).to.equal(null)
                    expect(assignment.assigned.held_id).to.equal(results.held2.held_id)
                    expect(assignment.holder.holder_id).to.equal(results.holder1.holder_id)
                }
            }
        })

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
            // console.log(thing)
            expect(thing).to.not.equal(null)
            expect(thing.newThing.thing_id).to.not.equal(null)
            expect(thing.newThing.name).to.equal("thing")
            expect(thing.newThing.system.created).to.not.equal(null)
            expect(mockserver.requests().length).to.not.equal(0)
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
            // console.log(things)
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
            // console.log(query)
            var rel_thing = await request(uri, query)
            // console.log(rel_thing)
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
            // console.log(query)
            var getThing = await request(uri, query)
            // console.log(getThing)
            expect(getThing.getThing).to.not.equal(null)
            expect(getThing.getThing.thing_id).to.equal(thing.newThing.thing_id)
            expect(getThing.getThing.name).to.equal("monster-rock")
        })


        it('findThings', async function() {
            var query = `
                    mutation {
                        first: newThing(thing: {name: "granola-bar"}) {
                            thing_id
                        }

                        second: newThing(thing: {name: "granola-bars"}) {
                            thing_id
                        }
                    }
                `
            var things = await request(uri, query)
            query = `
                    query {
                        findThings(query: {field: "name", operator: EQ, value: "granola-bars"}) {
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
            expect(things.findThings).to.not.equal(null)
            expect(things.findThings.data.length).to.equal(1)
            expect(things.findThings.data[0].thing_id).to.not.equal(null)
            expect(things.findThings.data[0].name).to.equal("granola-bars")
        })


        it('matchThings', async function() {
            // matchThings(
            //     name: String,
            //     material: String,
            //     type: TypeOfThing,
            //     page: PaginationInput): ThingList @beehiveSimpleQuery(target_type_name: "Thing")
            var query = `
                    mutation {
                        first: newThing(thing: {name: "brownie-bar"}) {
                            thing_id
                        }

                        second: newThing(thing: {name: "brownie-bars"}) {
                            thing_id
                        }
                    }
                `
            var things = await request(uri, query)
            query = `
                    query {
                        matchThings(name: "brownie-bar") {
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
            expect(things.matchThings).to.not.equal(null)
            expect(things.matchThings.data.length).to.equal(1)
            expect(things.matchThings.data[0].thing_id).to.not.equal(null)
            expect(things.matchThings.data[0].name).to.equal("brownie-bar")
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
            // console.log(query)
            var related = await request(uri, query)
            // console.log(related)
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
            // console.log(query)
            var getThing = await request(uri, query)
            // console.log(getThing)
            expect(getThing.updatedThing.related).to.not.equal(null)
            expect(getThing.updatedThing.related.length).to.equal(2)
        })

    })

})






describe('Beehive no schema test', function(){

    describe('exceptions', function() {
        it('should fail', async function() {

            expect(function() { require("../src/schema/no_schema")}).to.throw(TypeError)
        })


    })

})








