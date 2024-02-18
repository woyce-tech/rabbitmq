from flask import Flask, jsonify
import pika
import os
from datetime import datetime
import pytz
import json

app = Flask(__name__)
PORT = int(os.environ.get('PORT', 4001))
AMQP_URL = os.environ.get('AMQP_URL', 'amqp://guest:guest@localhost:5672/')

PARAMS = [  # Consolidating exchange and queue information to reduce redundancy
    ('Exchange_One', 'direct', [
        {'name': 'queue1', 'routing_key': 'key1'},
        {'name': 'queue2', 'routing_key': 'key2'},
        {'name': 'queue3', 'routing_key': 'key3'}
    ]),
    ('Exchange_Two', 'direct', [
        {'name': 'queue4', 'routing_key': 'key4'},
        {'name': 'queue5', 'routing_key': 'key5'},
        {'name': 'queue6', 'routing_key': 'key6'}
    ])
]


def setup_exchanges_queues(channel):
    for exchange, exchange_type, queues in PARAMS:
        channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)
        for q_info in queues:
            channel.queue_declare(queue=q_info['name'], durable=True)
            channel.queue_bind(queue=q_info['name'], exchange=exchange, routing_key=q_info['routing_key'])


def get_channel():
    connection_params = pika.URLParameters(AMQP_URL)
    connection = pika.BlockingConnection(connection_params)
    return connection.channel()


channel = get_channel()  # Initialize the channel at the start
setup_exchanges_queues(channel)  # Setup exchanges and queues on startup
channel.confirm_delivery()  # Enable publisher confirms


# Define callback function for acknowledgments
def on_delivery_confirmation(method_frame):
    if method_frame.method.NAME == 'Basic.Ack':
        print(" [x] Message delivered successfully.")
    else:
        print(" [!] Message delivery failed.")


# channel.add_callback(on_delivery_confirmation)  # Set up a callback for acknowledgments


@app.route('/send-msg', methods=['GET'])
def send_msg():
    ist_timezone = pytz.timezone('Asia/Kolkata')
    ist_time = datetime.now(tz=ist_timezone).strftime('%Y-%m-%d %H:%M:%S')

    data = {
        'title': "Six of Crows",
        'time': ist_time
    }
    buffer_data = json.dumps(data).encode()

    try:
        # Using a helper function to send messages
        send_data(channel, buffer_data, 'Exchange_One', 'key1')
        send_data(channel, buffer_data, 'Exchange_Two', 'key4')
        app.logger.info(f'Message sent to both exchanges at {ist_time}')
        return jsonify({'message': f'Message Sent to Exchanges: {ist_time}'}), 200
    except Exception as error:
        app.logger.error(error)
        return jsonify({'error': 'Failed to send message to exchanges.'}), 500


def send_data(channel, data, exchange, routing_key):
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=data,
        properties=pika.BasicProperties(delivery_mode=2)
    )


if __name__ == '__main__':
    app.run(port=PORT, debug=os.environ.get('DEBUG', 'False').lower() in ('true', '1', 't'))
