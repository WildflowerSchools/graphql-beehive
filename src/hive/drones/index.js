const axios = require("axios")

const AWS = require('aws-sdk')
var client = new AWS.Kinesis();

const BEEHIVE_STREAM = process.env.BEEHIVE_STREAM ? process.env.BEEHIVE_STREAM : "beehive_stream"
const BEEHIVE_PARTITION_KEY = process.env.BEEHIVE_PARTITION_KEY ? process.env.BEEHIVE_PARTITION_KEY : "beehive_partition_key"


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
    client.putRecord({
        PartitionKey: event.partition_key,
        StreamName: event.stream_name,
        Data: JSON.stringify(event.json())
    }, function(err, data) {
        if (err) {
            console.log(err, err.stack);
        } else {
            console.log(data);
        }
    });
}

exports.Event = Event
