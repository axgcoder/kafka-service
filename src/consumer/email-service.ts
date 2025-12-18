import { kafka } from '../config/kafka';
import { TOPICS, PaymentSuccessEvent, PaymentFailedEvent } from '../interfaces/events';
import { sendToDLQ } from '../producer/dlq-producer';

export class EmailService {
    private consumer = kafka.consumer({ groupId: 'email-group' });

    async connect() {
        await this.consumer.connect();
        console.log('EmailService Consumer connected');
        // Listen to BOTH Success and Failure topics from Payment Service
        await this.consumer.subscribe({ topic: TOPICS.PAYMENT_SUCCESS, fromBeginning: true });
        await this.consumer.subscribe({ topic: TOPICS.PAYMENT_FAILED, fromBeginning: true });

        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                if (message.value) {
                    try {
                        if (topic === TOPICS.PAYMENT_SUCCESS) {
                            const event: PaymentSuccessEvent = JSON.parse(message.value.toString());
                            console.log(
                                `[EmailService] Preparing confirmation email for ${event.email}`,
                            );
                            await new Promise((resolve) => setTimeout(resolve, 800));
                            console.log(
                                `[EmailService] Order Confirmed: Email sent to ${event.email} for order ${event.order_id}`,
                            );
                        } else if (topic === TOPICS.PAYMENT_FAILED) {
                            const event: PaymentFailedEvent = JSON.parse(message.value.toString());
                            console.log(
                                `[EmailService] Preparing failure notification for ${event.email}`,
                            );
                            await new Promise((resolve) => setTimeout(resolve, 800));
                            console.log(
                                `[EmailService] Order Failed: Failure email sent to ${event.email} for order ${event.order_id}. Reason: ${event.reason}`,
                            );
                        }
                    } catch (error) {
                        console.error('[EmailService] Error processing message:', error);
                        await sendToDLQ({
                            originalTopic: topic,
                            originalMessage: message.value.toString(),
                            error: error instanceof Error ? error.message : String(error),
                            serviceName: 'EmailService',
                            timestamp: new Date().toISOString(),
                            partition,
                            offset: message.offset,
                        });
                    }
                }
            },
        });
    }
}
