# Event-Driven Architecture Demo with Kafka

This project demonstrates an **Event-Driven Architecture (EDA)** for a typical e-commerce scenario using **Apache Kafka** and **TypeScript**.

It replicates a scenario where placing an order triggers multiple independent downstream actions (payments, inventory, email, analytics) via asynchronous events. It specifically implements the **Saga Pattern** (Choreography) to manage distributed consistency between Payment and Email services.

## Architecture Overview

1.  **Order Service (Producer)**: Receives an order request and publishes an immutable `OrderPlaced` event.
2.  **Kafka (Event Bus)**: Acts as the central nervous system, storing events durably.
3.  **Consumers & Saga Flow**:
    *   **Analytics & Inventory Service**: Listen to `OrderPlaced` purely for tracking/reservation.
    *   **Payment Service**: Listens to `OrderPlaced`, processes payment, and then emits **`PaymentSuccess`** or **`PaymentFailed`**.
    *   **Email Service**: Listens to **`PaymentSuccess`** (to send confirmation) or **`PaymentFailed`** (to send failure notification).

## How It Works

### 1. Topic Creation
When the application starts, it explicitly creates the necessary Kafka topics (`order-placed`, `payment-success`, `payment-failed`) to ensure the infrastructure is ready before any service tries to connect. This prevents "Unknown Topic" errors during startup.

### 2. The Saga Pattern (Choreography)
Dependencies are managed through events rather than direct calls:
*   **Order Placed**: The `OrderService` simply announces "Order #123 Placed". It *does not call* the Payment service.
*   **Decoupled Payment**: The `PaymentService` reacts to this event. It determines if the payment succeeds or fails (simulated with 80% success rate) and publishes the result as a new event.
*   **Conditional Email**: The `EmailService` does *not* listen to the initial order event. It waits for the **Payment** result. This guarantees that we never send a "Success" email for a failed order.

### 3. Kafka Configuration (KRaft Mode)
The project is configured to use Kafka in **KRaft mode** (without Zookeeper) for a simpler and more stable local development environment.
*   **Admin Client**: Used to programmatically create topics.
*   **Consumer Groups**: Each service runs in its own consumer group (e.g., `payment-group`, `inventory-group`). This ensures that *every* service gets a copy of the event (Fan-out pattern).

### 4. Dead Letter Queue (DLQ)
Failed messages are automatically sent to a **Dead Letter Queue** instead of being lost or blocking the consumer.

*   **DLQ Producer**: A shared utility (`dlq-producer.ts`) sends failed messages to the `dead-letter-queue` topic.
*   **DLQ Consumer**: Monitors the DLQ and logs failed messages with full context (original topic, error, service name, partition, offset).
*   **Error Context**: Each DLQ message includes the original message content so it can be investigated or replayed later.

This pattern ensures:
- No message loss during processing failures
- Consumers keep processing without blocking
- Failed messages are captured for debugging and potential retry

## Prerequisites

*   [Docker](https://www.docker.com/) & Docker Compose
*   [Node.js](https://nodejs.org/) (v16+)

## Setup & Run

1.  **Install Dependencies**
    ```bash
    npm install
    ```

2.  **Start Infrastructure**
    Start Kafka using Docker Compose.
    ```bash
    docker-compose up -d
    ```
    *Note: The Kafka broker is configured to listen on `localhost:9094`.*

3.  **Run the Demo**
    Run the main script. This will start all consumers, simulate placing an order, and log the processing flow.
    ```bash
    npx ts-node src/index.ts
    ```

4.  **Stop Infrastructure**
    When finished, you can stop the containers:
    ```bash
    docker-compose down
    ```

## Project Structure

*   `src/index.ts`: Main entry point. Initializes topics and starts all services.
*   `src/producer/`: Contains the OrderService and DLQ producer utility.
*   `src/consumer/`: Contains the consumer services (Payment, Inventory, Email, Analytics, DLQ).
*   `src/create-topics.ts`: Script to ensure Kafka topics exist.
*   `src/config/`: Kafka client configuration.
*   `src/interfaces/`: Shared TypeScript interfaces for events and DLQ messages.
*   `docker-compose.yml`: Infrastructure definition (KRaft mode).

## Troubleshooting

### "Consumers not receiving events" / Stuck Process
If the process starts but services like `InventoryService` or `EmailService` never log any activity, you may have **zombie processes** from a previous run holding onto the consumer group assignments.
**Fix**:
1. Find any running node processes: `ps aux | grep node` or `lsof -i :9094`
2. Kill them: `kill -9 <PID>`

## Sample Output
When running successfully, you should see a flow similar to this:
```text
[OrderService] Order placed and event published: ORD-3241
[AnalyticsService] Tracking order ORD-3241 - Total: $200
[PaymentService] Processing payment... Success
[InventoryService] Reserving details for order ORD-3241
[EmailService] Order Confirmed: Email sent to customer@example.com
```
