// consumer/receive
const express = require("express");
const amqp = require("amqplib");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 4002;
const AMQP_URL = process.env.AMQP_URL || "amqp://localhost:5672";
const queue = process.env.TASK_QUEUE || 'task_queue';

let channel, connection;

async function connectQueue() {
    try {
        connection = await amqp.connect(AMQP_URL);
        channel = await connection.createChannel();
        await channel.assertQueue(queue, { durable: true });

        channel.consume(queue, message => {
            // Process message
            processMessage(message);
            channel.ack(message);
        }, { noAck: false });
    } catch (error) {
        console.error('Failed to connect to AMQP', error);
        // Reconnect logic or other error handling here
    }
}

function processMessage(data) {
    const now = new Date();
    const istTime = now.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    const content = Buffer.from(data.content).toString();

    console.log(`${istTime}: Data received - ${content}`);
}

connectQueue().catch(console.error);

app.listen(PORT, () => {
    console.log(`Server running at port ${PORT}`);
});
