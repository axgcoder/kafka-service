import { kafka } from './config/kafka';
import { TOPICS } from './interfaces/events';

export async function createTopics() {
    const admin = kafka.admin();
    await admin.connect();

    console.log('Admin connected, checking topics...');

    const existingTopics = await admin.listTopics();
    const topicsToCreate = Object.values(TOPICS).filter((t) => !existingTopics.includes(t));

    if (topicsToCreate.length > 0) {
        console.log(`Creating topics: ${topicsToCreate.join(', ')}`);
        await admin.createTopics({
            topics: topicsToCreate.map((topic) => ({
                topic,
                numPartitions: 1,
                replicationFactor: 1,
            })),
        });
        console.log('Topics created successfully');
    } else {
        console.log('All topics already exist');
    }

    await admin.disconnect();
}
