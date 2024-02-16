const express = require("express");
const amqp = require("amqplib");
const app = express();
app.use(express.json());
const PORT = process.env.PORT || 4002;
const AMQP_URL = process.env.AMQP_URL || "amqp://localhost:5672";
let channel, connection;

async function connectQueue() {
    try {
        connection = await amqp.connect(AMQP_URL);
        channel = await connection.createChannel();
        
        // Declare queues to consume messages
        const queuesExchangeOne = ['queue1', 'queue2', 'queue3'];
        const queuesExchangeTwo = ['queue6', 'queue5', 'queue4'];
        
        // Assert queues for Exchange One
        await Promise.all(queuesExchangeOne.map(async (queue) => {
            await channel.assertQueue(queue, { durable: true });
            console.log(`Consumer connected to queue: ${queue} for Exchange One`);
            // Consume messages from the queue
            channel.consume(queue, (msg) => {
                if (msg !== null) {
                    console.log(`Received message from queue ${queue} for Exchange One:`, msg.content.toString());
                    // Acknowledge the message to remove it from the queue
                    channel.ack(msg);
                }
            });
        }));

        // Assert queues for Exchange Two
        await Promise.all(queuesExchangeTwo.map(async (queue) => {
            await channel.assertQueue(queue, { durable: true });
            console.log(`Consumer connected to queue: ${queue} for Exchange Two`);
            // Consume messages from the queue
            channel.consume(queue, (msg) => {
                if (msg !== null) {
                    console.log(`Received message from queue ${queue} for Exchange Two:`, msg.content.toString());
                    // Acknowledge the message to remove it from the queue
                    channel.ack(msg);
                }
            });
        }));
    } catch (error) {
        console.error('Failed to connect to AMQP', error);
        // Reconnect logic or other error handling here
    }
}

connectQueue().catch(console.error);

app.listen(PORT, () => {
    console.log(`Server running at port ${PORT}`);
});
