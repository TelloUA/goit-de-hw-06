from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config, prefix
import json

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='sensors'   # Ідентифікатор групи споживачів
)

topic_name = f'{prefix}_general_alerts'

# Підписка на топіки
consumer.subscribe([topic_name])

print(f"Subscribed to topics: {topic_name}")

try:
    for message in consumer:
        message_value = message.value

        print("Full message content:", message_value)
        
        print("Window Start:", message_value.get('window_start', 'N/A'))
        print("Window End:", message_value.get('window_end', 'N/A'))
        print("Temperature Avg:", message_value.get('temperature_avg', 'N/A'))
        print("Humidity Avg:", message_value.get('humidity_avg', 'N/A'))
        print("Code:", message_value.get('code', 'N/A'))
        print("Message:", message_value.get('message', 'N/A'))
        print("-----")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer
