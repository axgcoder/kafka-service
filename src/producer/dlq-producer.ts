import { kafka } from '../config/kafka';
import { TOPICS } from '../interfaces/events';

interface DLQMessage {
    originalTopic: string;
    originalMessage: string;
    error: string;
    serviceName: string;
    timestamp: string;
    partition: number;
    offset: string;
}

const producer = kafka.producer();
let isConnected = false;

async function ensureConnection() {
    if (!isConnected) {
        await producer.connect();
        isConnected = true;
        console.log('[DLQ Producer] Connected');
    }
}

export async function sendToDLQ(message: DLQMessage): Promise<void> {
    await ensureConnection();

    console.log(`[DLQ Producer] Sending failed message to DLQ from ${message.serviceName}`);

    await producer.send({
        topic: TOPICS.DLQ,
        messages: [
            {
                value: JSON.stringify(message),
            },
        ],
    });

    console.log(`[DLQ Producer] Message sent to DLQ successfully`);
}
