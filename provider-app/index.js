// publisher/send
const express = require("express");
const app = express();
const PORT = process.env.PORT || 4001;
app.use(express.json());
const amqp = require("amqplib");

const AMQP_URL = process.env.AMQP_URL || "amqp://localhost:5672";
const queue = 'task_queue';
let channel, connection;

async function connectQueue() {
    connection = await amqp.connect(AMQP_URL);
    channel = await connection.createChannel();
    await channel.assertQueue(queue, { durable: true });
}

connectQueue().catch(console.error);

const sendData = async (data) => {
    if (!channel) {
        throw new Error('Cannot send data: channel not initialized');
    }
    await channel.sendToQueue(queue, Buffer.from(JSON.stringify(data)), { persistent: true });
};

app.get("/send-msg", async (req, res) => {
    const now = new Date();
    const istTime = now.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    
    const data = {
        title: "Six of Crows",
        time: istTime
    };
    
    try {
        await sendData(data);
        console.log(`A message is sent to queue at ${istTime}`);
        res.send(`Message Sent: ${istTime}`);
    } catch (error) {
        console.error(error);
        res.status(500).send('Failed to send message.');
    }
});

app.listen(PORT, () => console.log(`Server running at port ${PORT}`));
