'use strict';

const { Kafka } = require('kafkajs');

async function createTopic() {
    try {
        const kafka = new Kafka({
            "clientId": 'kafkaJs_1', //any string 
            "brokers": ["127.0.0.1:9092"] //can give multiple brokers with comma separated
        });
        const admin = kafka.admin();
        console.log('Connecting.....');
        await admin.connect();
        console.log('Connected');
        await admin.createTopics({
            topics: [{
                "topic": "Users",
                "numPartitions": 2
            }] // can send multiple topics
        });
        console.log('Created successfully');
        await admin.disconnect();
    } catch(exception) {
        console.log(`Internal server error ${exception}`);
    } finally {
        process.exit(0);
    }
}

createTopic();