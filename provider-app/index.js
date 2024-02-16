const express = require("express");
const app = express();
const PORT = process.env.PORT || 4001;
app.use(express.json());
const amqp = require("amqplib");
const AMQP_URL = process.env.AMQP_URL || "amqp://localhost:5672";
let channel, connection;
async function connectQueue() {
    try {
        connection = await amqp.connect(AMQP_URL);
        channel = await connection.createChannel();
        // Declare Exchanges
        await channel.assertExchange('Exchange_One', 'direct', { durable: true });
        await channel.assertExchange('Exchange_Two', 'direct', { durable: true });
        // Declare queues and bind them to Exchange One
        const queuesExchangeOne = ['queue1', 'queue2', 'queue3'];
        await Promise.all(queuesExchangeOne.map(async (queue) => {
            await channel.assertQueue(queue, { durable: true });
            await channel.bindQueue(queue, 'Exchange_One', ''); // Use an appropriate routing key if needed
        }));
        // Declare queues and bind them to Exchange Two
        const queuesExchangeTwo = ['queue4', 'queue5', 'queue6'];
        await Promise.all(queuesExchangeTwo.map(async (queue) => {
            await channel.assertQueue(queue, { durable: true });
            await channel.bindQueue(queue, 'Exchange_Two', ''); // Use an appropriate routing key if needed
        }));
        console.log('Exchanges and Queues are set up and bound.');
    } catch (error) {
        console.error('Error in connectQueue:', error);
        throw error;
    }
}
connectQueue().catch(console.error);
const sendDataToExchanges = async (data) => {
    if (!channel) {
        throw new Error('Cannot send data: channel not initialized');
    }
    const bufferData = Buffer.from(JSON.stringify(data));
    try {
        // Publish to Exchange One - replace '' with the appropriate routing key if needed
        await channel.publish('Exchange_One', '', bufferData, { persistent: true });
        // Publish to Exchange Two - replace '' with the appropriate routing key if needed
        await channel.publish('Exchange_Two', '', bufferData, { persistent: true });
        console.log('Data sent to both exchanges.');
    } catch (error) {
        console.error('Error while publishing:', error);
        throw error;
    }
};
app.get("/send-msg", async (req, res) => {
    const now = new Date();
    const istTime = now.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    const data = {
        title: "Six of Crows",
        time: istTime
    };
    try {
        await sendDataToExchanges(data);
        console.log(`A message is sent to both exchanges at ${istTime}`);
        res.send(`Message Sent to Exchanges: ${istTime}`);
    } catch (error) {
        console.error(error);
        res.status(500).send('Failed to send message to exchanges.');
    }
});

app.listen(PORT, () => console.log(`Server running at port ${PORT}`));
