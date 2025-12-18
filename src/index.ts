import { OrderService } from './producer/order-service';
import { PaymentService } from './consumer/payment-service';
import { InventoryService } from './consumer/inventory-service';
import { EmailService } from './consumer/email-service';
import { AnalyticsService } from './consumer/analytics-service';
import { DLQConsumer } from './consumer/dlq-consumer';

import { createTopics } from './create-topics';

async function main() {
    console.log('Starting E-commerce Event Driven Architecture Demo...');

    // Ensure topics exist
    await createTopics();

    // Initialize Services
    const orderService = new OrderService();
    const paymentService = new PaymentService();
    const inventoryService = new InventoryService();
    const emailService = new EmailService();
    const analyticsService = new AnalyticsService();
    const dlqConsumer = new DLQConsumer();

    // Start Consumers
    await Promise.all([
        paymentService.connect(),
        inventoryService.connect(),
        emailService.connect(),
        analyticsService.connect(),
        dlqConsumer.connect(),
    ]);

    // Start Producer
    await orderService.connect();

    // Simulate placing an order
    console.log('\n--- Simulate placing an order ---');
    const orderData = {
        order_id: 'ORD-' + Math.floor(Math.random() * 10000),
        customer_id: 'CUST-001',
        email: 'customer@example.com',
        items: [
            { productId: 'PROD-1', quantity: 1, price: 100 },
            { productId: 'PROD-2', quantity: 2, price: 50 },
        ],
        total: 200,
    };

    // Wait for consumers to fully join their groups
    console.log('Waiting for consumer groups to stabilize...');
    await new Promise((resolve) => setTimeout(resolve, 5000));

    await orderService.placeOrder(orderData);

    // Keep the process alive to allow consumers to process events
    console.log('\nWaiting for consumers to process events (Press Ctrl+C to exit)...');

    // Handle graceful shutdown
    const shutdown = async () => {
        console.log('\nShutting down...');
        await orderService.disconnect();
        process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
}

main().catch(console.error);
