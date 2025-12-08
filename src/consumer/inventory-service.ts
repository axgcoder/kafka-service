import { kafka } from '../config/kafka';
import { TOPICS, OrderPlacedEvent } from '../interfaces/events';

export class InventoryService {
    private consumer = kafka.consumer({ groupId: 'inventory-group' });

    async connect() {
        await this.consumer.connect();
        console.log('InventoryService Consumer connected');
        await this.consumer.subscribe({ topic: TOPICS.ORDER_PLACED, fromBeginning: true });

        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                if (message.value) {
                    try {
                        const event: OrderPlacedEvent = JSON.parse(message.value.toString());
                        console.log(
                            `[InventoryService] Reserving details for order ${event.order_id}. Items: ${event.items.length}`,
                        );
                        // Simulate processing time
                        await new Promise((resolve) => setTimeout(resolve, 300));
                        console.log(
                            `[InventoryService] Items reserved for order ${event.order_id}`,
                        );
                    } catch (error) {
                        console.error('[InventoryService] Error processing message:', error);
                    }
                }
            },
        });
    }
}
