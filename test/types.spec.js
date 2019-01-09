const expect = require('chai').expect
const {findIdField} = require('../src/hive/types')


describe('Beehive test suite', function(){

    describe('findIdField', function() {
        it('find ID field in a type', async function() {
            var field = findIdField({
                _fields: {
                    jim: {
                        type: "pants"
                    },
                    id8: {
                        type: "ID!"
                    },
                    idiot: {
                        type: "ID"
                    }
                }
            })
            expect(field).to.equal("id8")
        })

        it('find ID field in a type not existing', async function() {
            var field = findIdField({
                _fields: {
                    jim: {
                        type: "pants"
                    },
                    idiot: {
                        type: "ID"
                    }
                }
            })
            expect(field).to.equal("id")
        })


    })

})


