const { kafka } = require('./client');

const group = process.argv[2] || 'default-group'; // Fallback to 'default-group' if not provided

if (!group) {
    throw new Error("Consumer groupId must be a non-empty string. Please provide a valid groupId.");
}

async function init() {
    const consumer = kafka.consumer({ groupId: group });
    await consumer.connect();
    await consumer.subscribe({ topic: "rider-updates", fromBeginning: true }); // Corrected to 'topic'

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`[${group}: ${topic}]: PART: ${partition}: ${message.value.toString()}`);
        },
    });

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
        console.log("Disconnecting Consumer...");
        await consumer.disconnect();
        console.log("Consumer Disconnected Successfully");
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log("Disconnecting Consumer...");
        await consumer.disconnect();
        console.log("Consumer Disconnected Successfully");
        process.exit(0);
    });
}

init();
