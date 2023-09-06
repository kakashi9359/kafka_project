import threading
import pandas as pd
import json
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer




# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': '',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '',
    'sasl.password': '',
     'group.id': '2'
    # 'auto.offset.reset': 'earliest'
}





# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-l622j.us-east-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('', '')
})

# Fetch the latest Avro schema for the value
subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
     'group.id': kafka_config['group.id']
    # 'auto.offset.reset': kafka_config['auto.offset.reset'],
    #'enable.auto.commit': True
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})




# Subscribe to the 'retail_data' topic
consumer.subscribe(['product_updates'])

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
       

        #print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("consumer closed")


columns2 = ['id','name','category','price','last_updated']

required_df = pd.DataFrame(columns=columns2)

for index,row in df.iterrows():
    new_row = {'id':row['value']['id'],'name':row['value']['name'],'category':row['value']['category'],'price':row['value']['price'],'last_updated':row['value']['last_updated']}
    required_df.loc[len(required_df)] = new_row




#data transformation
#category column to uppercase
required_df['category'] = required_df['category'].str.upper()

#applying discount for cricket goods because of WC 2023 in India !
discount = 0.2
required_df.loc[required_df['category']=='CRICKET',['price']]=required_df['price']*(1-discount)


with open('c:/users/digha/documents/python scripts/new_json.json','a') as file:
    required_df.to_json(file,orient='records',lines=True)
    print('df into json')


