import boto3
import json
from kafka import KafkaConsumer

# Initialize AWS Firehose client
firehose_client = boto3.client('firehose')

# Function to send data to Kinesis Firehose
def send_to_firehose(firehose_stream_name, data):
    response = firehose_client.put_record(
        DeliveryStreamName=firehose_stream_name,
        Record={
            'Data': json.dumps(data) + '\n'
        }
    )
    return response

# Function to parse Kafka message from key-value string to dictionary
def parse_kafka_message(kafka_message):
    data = {}
    fields = kafka_message.split(', ')
    
    for field in fields:
        key, value = field.split(': ')
        data[key] = value
    
    if 'ItemCount' in data:
        data['ItemCount'] = int(data['ItemCount'])
    if 'GameTick' in data:
        data['GameTick'] = int(data['GameTick'])
    
    return data


consumer = KafkaConsumer(
    'YOUR_TOPIC',  # Kafka topic
    bootstrap_servers='YOUR_KAFKA_IP:9092',  # Kafka server
    enable_auto_commit=True,
)

firehose_stream_name = 'YOUR_FIREHOSE_STREAM_NAME'

# Consume messages from Kafka and send them to Firehose
for message in consumer:
    # Decode the message received from Kafka
    kafka_message = message.value.decode('utf-8')

    # Print the received message for debugging
    print(f"Received message: {kafka_message}")
    
    # Parse the Kafka message from string format to a dictionary
    try:
        kafka_data = parse_kafka_message(kafka_message)
    except Exception as e:
        print(f"Error parsing message: {e}")
        continue
    
    # Send the parsed data to Kinesis Firehose
    firehose_response = send_to_firehose(firehose_stream_name, kafka_data)