import { kafka } from '../config/kafka';
import { OrderPlacedEvent, TOPICS } from '../interfaces/events';

export class OrderService {
    private producer = kafka.producer();

    constructor() {}

    async connect() {
        await this.producer.connect();
        console.log('OrderService Producer connected');
    }

    async disconnect() {
        await this.producer.disconnect();
        console.log('OrderService Producer disconnected');
    }

    async placeOrder(orderData: Omit<OrderPlacedEvent, 'timestamp'>) {
        const event: OrderPlacedEvent = {
            ...orderData,
            timestamp: new Date().toISOString(),
        };

        try {
            await this.producer.send({
                topic: TOPICS.ORDER_PLACED,
                messages: [
                    {
                        key: event.order_id,
                        value: JSON.stringify(event),
                    },
                ],
            });
            console.log(`[OrderService] Order placed and event published: ${event.order_id}`);
            return event.order_id;
        } catch (error) {
            console.error('[OrderService] Error publishing order event:', error);
            throw error;
        }
    }
}
