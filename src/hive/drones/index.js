const AWS = require('aws-sdk')

const BEEHIVE_STREAM = process.env.BEEHIVE_STREAM ? process.env.BEEHIVE_STREAM : "beehive_stream"
const BEEHIVE_PARTITION_KEY = process.env.BEEHIVE_PARTITION_KEY ? process.env.BEEHIVE_PARTITION_KEY : "beehive_partition_key"

var client
if (process.env.BEEHIVE_MOCK_STREAM == "yes") {
    client = new AWS.Kinesis({endpoint: "http://localhost:4567"});
} else {
    client = new AWS.Kinesis();
}


class Event {
    constructor(topic, type, id, activity, ts, stream_name, partition_key) {
        this.topic = topic
        this.type = type
        this.id = id
        this.activity = activity
        this.ts = ts ? ts : new Date().toISOString()
        this.stream_name = stream_name ? stream_name : BEEHIVE_STREAM
        this.partition_key = partition_key ? partition_key : BEEHIVE_PARTITION_KEY
    }

    json() {
        return {
            "topic": this.topic,
            "type": this.type,
            "id": this.id,
            "activity": this.activity,
            "ts": this.ts,
        }
    }
}


exports.sendEvent = async function(event) {
    console.log(event);
    const DEBUG = process.env.DEBUG == "yes"
    client.putRecord({
        PartitionKey: event.partition_key,
        StreamName: event.stream_name,
        Data: JSON.stringify(event.json())
    }, function(err, data) {
        if (err) {
            if (DEBUG) {
                console.error(err.stack);
            }
        } else {
            if (DEBUG) {
                console.log(data)
            }
        }
    });
}

exports.Event = Event
