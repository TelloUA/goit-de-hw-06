from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, prefix

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

num_partitions = 1
replication_factor = 1

for topic in ['general_alerts', 'building_sensors', 'building_sensors_4']:
    topic_name = f'{prefix}_{topic}'

    new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

[print(topic) for topic in admin_client.list_topics() if prefix in topic]

admin_client.close()

