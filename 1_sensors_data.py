from kafka import KafkaProducer
from configs import kafka_config, prefix
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
topic_name = f'{prefix}_building_sensors_4'

sensor_id = str(uuid.uuid4())
sensor_type = random.choice(['temperature', 'humidity'])

value_range = [20, 50] if sensor_type == 'temperature' else [10, 90]
for i in range(10):
    try:
        data = {
            "timestamp": time.time(),  # Часова мітка
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "value": random.randint(*value_range)
        }
        producer.send(topic_name, value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {i} sent to topic '{topic_name}' successfully.")
        print(data)
        time.sleep(5)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()

