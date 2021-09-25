'use strict';

const { Kafka } = require('kafkajs');

async function consumer() {
    try {
        const kafka = new Kafka({
            "clientId": 'kafkaJs_1', //any string 
            "brokers": ["127.0.0.1:9092"] //can give multiple brokers with comma separated
        });
    
        const consumer = kafka.consumer({"groupId": "kafkajs_consumer"});
        console.log('Connecting.....');
        await consumer.connect();
        console.log('Connected');

        //A-M = 0, N-Z = 1
        await consumer.subscribe({
            "topic": "Users",
            "fromBeginning": true
        });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
                console.log(`- ${prefix} ${message.key}#${message.value}`)
            }
        })
        //await consumer.disconnect();
    } catch(exception) {
        console.log(`Internal server error ${exception}`);
    } finally {
        //process.exit(0);
    }
}

consumer();