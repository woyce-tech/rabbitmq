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
        const queuesExchangeOne = [
            { name: 'queue1', routingKey: 'key1' },
            { name: 'queue2', routingKey: 'key2' },
            { name: 'queue3', routingKey: 'key3' }
        ];
        await Promise.all(queuesExchangeOne.map(async ({ name, routingKey }) => {
            await channel.assertQueue(name, { durable: true });
            await channel.bindQueue(name, 'Exchange_One', routingKey);
        }));
        // Declare queues and bind them to Exchange Two
        const queuesExchangeTwo = [
            { name: 'queue4', routingKey: 'key4' },
            { name: 'queue5', routingKey: 'key5' },
            { name: 'queue6', routingKey: 'key6' }
        ];
        await Promise.all(queuesExchangeTwo.map(async ({ name, routingKey }) => {
            await channel.assertQueue(name, { durable: true });
            await channel.bindQueue(name, 'Exchange_Two', routingKey);
        }));
        console.log('Exchanges, Queues, and Bindings are set up.');
    } catch (error) {
        console.error('Error in connectQueue:', error);
        throw error;
    }
}
connectQueue().catch(console.error);
const sendDataToExchanges = async (data, routingKeyOne, routingKeyTwo) => {
    if (!channel) {
        throw new Error('Cannot send data: channel not initialized');
    }
    const bufferData = Buffer.from(JSON.stringify(data));
    try {
        // Publish to Exchange One
        await channel.publish('Exchange_One', routingKeyOne, bufferData, { persistent: true });
        // Publish to Exchange Two
        await channel.publish('Exchange_Two', routingKeyTwo, bufferData, { persistent: true });
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
        // Specify routing keys for each exchange
        await sendDataToExchanges(data, 'key1', 'key4');
        console.log(`A message is sent to both exchanges at ${istTime}`);
        res.send(`Message Sent to Exchanges: ${istTime}`);
    } catch (error) {
        console.error(error);
        res.status(500).send('Failed to send message to exchanges.');
    }
});

app.listen(PORT, () => console.log(`Server running at port ${PORT}`));
