import { Kafka, logLevel } from 'kafkajs';

export const kafka = new Kafka({
    clientId: 'ecommerce-app',
    brokers: ['localhost:9094'],
    retry: {
        initialRetryTime: 100,
        retries: 8,
    },
    logLevel: logLevel.INFO,
});
