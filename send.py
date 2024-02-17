from flask import Flask, jsonify
import pika
import os
from datetime import datetime
import pytz
import json

app = Flask(__name__)
PORT = int(os.environ.get('PORT', 4001))
AMQP_URL = os.environ.get('AMQP_URL', 'amqp://localhost:5672')

connection_params = pika.URLParameters(AMQP_URL)
connection = None
channel = None

def connect_queue():
    global connection, channel
    try:
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()

        # Declare Exchanges
        channel.exchange_declare(exchange='Exchange_One', exchange_type='direct', durable=True)
        channel.exchange_declare(exchange='Exchange_Two', exchange_type='direct', durable=True)

        # Declare queues and bind them to Exchange One
        queues_exchange_one = [
            {'name': 'queue1', 'routing_key': 'key1'},
            {'name': 'queue2', 'routing_key': 'key2'},
            {'name': 'queue3', 'routing_key': 'key3'}
        ]
        for q in queues_exchange_one:
            channel.queue_declare(queue=q['name'], durable=True)
            channel.queue_bind(queue=q['name'], exchange='Exchange_One', routing_key=q['routing_key'])

        # Declare queues and bind them to Exchange Two
        queues_exchange_two = [
            {'name': 'queue4', 'routing_key': 'key4'},
            {'name': 'queue5', 'routing_key': 'key5'},
            {'name': 'queue6', 'routing_key': 'key6'}
        ]
        for q in queues_exchange_two:
            channel.queue_declare(queue=q['name'], durable=True)
            channel.queue_bind(queue=q['name'], exchange='Exchange_Two', routing_key=q['routing_key'])

        print('Exchanges, Queues, and Bindings are set up.')
    except Exception as error:
        print('Error in connectQueue:', error)
        raise error

# Connect to the queue on startup
connect_queue()

def send_data_to_exchanges(data, routing_key_one, routing_key_two):
    if not channel:
        raise Exception('Cannot send data: channel not initialized')
    
    buffer_data = json.dumps(data).encode()
    
    # Publish to Exchange One
    channel.basic_publish(exchange='Exchange_One', routing_key=routing_key_one, body=buffer_data, properties=pika.BasicProperties(delivery_mode=2))
    
    # Publish to Exchange Two
    channel.basic_publish(exchange='Exchange_Two', routing_key=routing_key_two, body=buffer_data, properties=pika.BasicProperties(delivery_mode=2))
    
    print('Data sent to both exchanges.')

@app.route('/send-msg', methods=['GET'])
def send_msg():
    ist_timezone = pytz.timezone('Asia/Kolkata')
    now = datetime.now(tz=ist_timezone)
    ist_time = now.strftime('%Y-%m-%d %H:%M:%S')
    
    data = {
        'title': "Six of Crows",
        'time': ist_time
    }
    
    try:
        send_data_to_exchanges(data, 'key1', 'key4')
        print(f'A message is sent to both exchanges at {ist_time}')
        return jsonify({'message': f'Message Sent to Exchanges: {ist_time}'}), 200
    except Exception as error:
        print(error)
        return jsonify({'error': 'Failed to send message to exchanges.'}), 500

if __name__ == '__main__':
    app.run(port=PORT, debug=True)
