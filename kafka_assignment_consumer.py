import threading
import pandas as pd
import json
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka import TopicPartition,Consumer
from concurrent.futures import ThreadPoolExecutor

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'VDGKT22VE2X765XB',
    'sasl.password': '0NP2EBHvffUFoBtguP9QifgT24PrFq5NzBthGwlnmWJSB2z8nVeEL7oY2PJSokcf',
     'group.id': '2'
    # 'auto.offset.reset': 'earliest'
}

def kafka_process(msg,offset):

    columns = ['key','value']
    df = pd.DataFrame(columns = columns)

    # Continually read messages from Kafka
    try:
        while True:
            msg = consumer.poll(2.0)

            if msg is None:
                break
            if msg.error():
                print('Consumer error: {}'.format(msg.error()))
                continue

            key = str(msg.key())
            value = [msg.value()]

            new_row = pd.DataFrame({'key':key,'value':value})
            df = pd.concat([df,new_row],ignore_index=True)
            print('record added to df with key {} and value {}'.format(msg.key(),msg.value()))
            consumer.commit(offset=offset)

        #print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("consumer closed")

    with open('c:/users/digha/documents/python scripts/new_json.json','a') as file:
        df.to_json(file,orient='records',lines=True)
        print('df into json')

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-l622j.us-east-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('GNEQ6SUQCNPAWUUM', 'SGHFgMPb2U5HbuE51NjZlge8SeFw6cTegw2NGPet7qHbBYF+fmgSz6TTjkxoT4Vy')
})

# Fetch the latest Avro schema for the value
subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)


consumer =Consumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    # 'key.deserializer': key_deserializer,
    # 'value.deserializer': avro_deserializer,
    # 'value_deserializer':lambda m: m.decode('utf-8'),
    'group.id': kafka_config['group.id'],
    # 'auto.offset.reset': kafka_config['auto.offset.reset'],
    'enable.auto.commit': False
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

partitions = [TopicPartition('product_updates', partition) for partition in consumer.partitions_for_topic('product_updates')]
consumer.assign(partitions)

# Subscribe to the 'retail_data' topic
#consumer.subscribe(['product_updates'])

for message in consumer:
    topic_partition = TopicPartition('product_updates',message.partition)
    offset = (message.offset + 1, None)
    topic_offset = {topic_partition:offset}
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.submit(kafka_process,message,topic_offset)