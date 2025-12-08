export interface OrderItem {
    productId: string;
    quantity: number;
    price: number;
}

export interface OrderPlacedEvent {
    order_id: string;
    customer_id: string;
    email: string;
    items: OrderItem[];
    total: number;
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

export const TOPICS = {
    ORDER_PLACED: 'order-placed',
    PAYMENT_SUCCESS: 'payment-success',
    PAYMENT_FAILED: 'payment-failed',
};
