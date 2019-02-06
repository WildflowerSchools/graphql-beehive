const axios = require("axios")

const PROTON_PACK_STREAM_URI = process.env.PROTON_PACK_STREAM_URI

class Event {
    constructor(topic, type, id, activity, ts) {
        this.topic = topic
        this.type = type
        this.id = id
        this.activity = activity
        this.ts = ts ? ts : new Date().toISOString()
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
    var response = await axios.post(PROTON_PACK_STREAM_URI, event.json())
    return response
}

exports.Event = Event
