// make sure the beehive_stream kinesis stream is created

process.env.BEEHIVE_ENABLE_EVENTS = "yes"

var drones = require("./src/hive/drones")
var evt = new drones.Event("horfrost", "Thing", "2319", "CREATE")
drones.sendEvent(evt)
