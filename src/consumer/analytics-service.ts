import { kafka } from '../config/kafka';
import { TOPICS, OrderPlacedEvent } from '../interfaces/events';

export class AnalyticsService {
    private consumer = kafka.consumer({ groupId: 'analytics-group' });

    async connect() {
        await this.consumer.connect();
        console.log('AnalyticsService Consumer connected');
        await this.consumer.subscribe({ topic: TOPICS.ORDER_PLACED, fromBeginning: true });

        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                if (message.value) {
                    const event: OrderPlacedEvent = JSON.parse(message.value.toString());
                    console.log(
                        `[AnalyticsService] Tracking order ${event.order_id} - Total: $${event.total}`,
                    );
                    // Simulate processing time
                    await new Promise((resolve) => setTimeout(resolve, 100));
                    console.log(`[AnalyticsService] Order ${event.order_id} tracked`);
                }
            },
        });
    }
}
