'use strict';

const { Kafka } = require('kafkajs');
const msg = process.argv[2];

async function producer() {
    try {
        const kafka = new Kafka({
            "clientId": 'kafkaJs_1', //any string 
            "brokers": ["127.0.0.1:9092"] //can give multiple brokers with comma separated
        });
        const producer = kafka.producer();
        console.log('Connecting.....');
        await producer.connect();
        console.log('Connected');

        //A-M = 0, N-Z = 1
        const partition = msg[0] < "N" ? 0 : 1;

        const result = await producer.send({
            "topic": "Users",
            "messages": [{
                "value": msg,
                "partition": partition
            }]
        })
        console.log('Created successfully ' + JSON.stringify(result));
        await producer.disconnect();
    } catch(exception) {
        console.log(`Internal server error ${exception}`);
    } finally {
        process.exit(0);
    }
}

producer();