import { kafka } from '../config/kafka';
import { TOPICS, DLQMessage } from '../interfaces/events';

export class DLQConsumer {
    private consumer = kafka.consumer({ groupId: 'dlq-monitor-group' });

    async connect() {
        await this.consumer.connect();
        console.log('DLQConsumer connected - Monitoring failed messages');
        await this.consumer.subscribe({ topic: TOPICS.DLQ, fromBeginning: true });

        await this.consumer.run({
            eachMessage: async ({ message }) => {
                if (message.value) {
                    try {
                        const dlqMessage: DLQMessage = JSON.parse(message.value.toString());
                        console.log('\n========== DLQ MESSAGE RECEIVED ==========');
                        console.log(`Service: ${dlqMessage.serviceName}`);
                        console.log(`Original Topic: ${dlqMessage.originalTopic}`);
                        console.log(`Error: ${dlqMessage.error}`);
                        console.log(`Timestamp: ${dlqMessage.timestamp}`);
                        console.log(`Partition: ${dlqMessage.partition}, Offset: ${dlqMessage.offset}`);
                        console.log('Original Message:', dlqMessage.originalMessage);
                        console.log('===========================================\n');
                    } catch (error) {
                        console.error('[DLQConsumer] Error parsing DLQ message:', error);
                    }
                }
            },
        });
    }
}
