// Kafka Topic Names
export const TOPICS = {
    ORDER_PLACED: 'order.placed',
    PAYMENT_SUCCESS: 'payment.success',
    PAYMENT_FAILED: 'payment.failed',
    DLQ: 'dead-letter-queue',
} as const;

// Event Interfaces
export interface OrderPlacedEvent {
    order_id: string;
    email: string;
    total: number;
    items: Array<{
        productId: string;
        quantity: number;
        price: number;
    }>;
    timestamp: string;
}

export interface PaymentSuccessEvent {
    order_id: string;
    email: string;
    timestamp: string;
}

export interface PaymentFailedEvent {
    order_id: string;
    email: string;
    reason: string;
    timestamp: string;
}

export interface DLQMessage {
    originalTopic: string;
    originalMessage: string;
    error: string;
    serviceName: string;
    timestamp: string;
    partition: number;
    offset: string;
}
