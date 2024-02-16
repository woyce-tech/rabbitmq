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
        console.log(`Connected to AMQP, consuming ${queue} queue`);
    } catch (error) {
        console.error('Failed to connect to AMQP', error);
        setTimeout(connectQueue, 5000); // Retry connection after 5 seconds
    }
}
function processMessage(data) {
    const now = new Date();
    const istTime = now.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    const content = Buffer.from(data.content).toString();
    // Your message processing logic here
    console.log(`${istTime}: Data received - ${content}`);
    // Further processing can be added here
}
process.on('exit', () => {
    if (connection) {
        connection.close();
        console.log('Closing AMQP connection');
    }
});
process.on('SIGINT', () => process.exit()); // Catch CTRL+C
process.on('SIGTERM', () => process.exit()); // Catches terminate signals
connectQueue().catch(console.error);
app.listen(PORT, () => {
    console.log(`Server running at port ${PORT}`);
});
