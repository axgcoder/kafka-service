import { kafka } from '../config/kafka';
import { TOPICS, OrderPlacedEvent, PaymentSuccessEvent } from '../interfaces/events';

export class PaymentService {
    private consumer = kafka.consumer({ groupId: 'payment-group' });
    private producer = kafka.producer();

    async connect() {
        await this.consumer.connect();
        await this.producer.connect();

        console.log('PaymentService connected (Consumer & Producer)');
        await this.consumer.subscribe({ topic: TOPICS.ORDER_PLACED, fromBeginning: true });

        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                if (message.value) {
                    try {
                        const event: OrderPlacedEvent = JSON.parse(message.value.toString());
                        console.log(
                            `[PaymentService] Processing payment for order ${event.order_id}. Amount: ${event.total}`,
                        );

                        // Simulate processing time
                        await new Promise((resolve) => setTimeout(resolve, 500));

                        // Simulate 80% success rate
                        const isSuccess = Math.random() < 0.8;

                        if (isSuccess) {
                            console.log(
                                `[PaymentService] Payment successful for order ${event.order_id}`,
                            );
                            const successEvent: PaymentSuccessEvent = {
                                order_id: event.order_id,
                                email: event.email,
                                timestamp: new Date().toISOString(),
                            };

                            await this.producer.send({
                                topic: TOPICS.PAYMENT_SUCCESS,
                                messages: [{ value: JSON.stringify(successEvent) }],
                            });
                        } else {
                            console.log(
                                `[PaymentService] Payment FAILED for order ${event.order_id}`,
                            );
                            await this.producer.send({
                                topic: TOPICS.PAYMENT_FAILED,
                                messages: [
                                    {
                                        value: JSON.stringify({
                                            order_id: event.order_id,
                                            email: event.email,
                                            reason: 'Insufficient funds',
                                            timestamp: new Date().toISOString(),
                                        }),
                                    },
                                ],
                            });
                        }
                    } catch (error) {
                        console.error('[PaymentService] Error processing message:', error);
                    }
                }
            },
        });
    }
}
