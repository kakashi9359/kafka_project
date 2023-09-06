import mysql.connector
from sqlalchemy import create_engine
import pymysql
from datetime import datetime, timedelta

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd

def delivery_report(err,msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'VDGKT22VE2X765XB',
    'sasl.password': '0NP2EBHvffUFoBtguP9QifgT24PrFq5NzBthGwlnmWJSB2z8nVeEL7oY2PJSokcf'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-l622j.us-east-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('GNEQ6SUQCNPAWUUM', 'SGHFgMPb2U5HbuE51NjZlge8SeFw6cTegw2NGPet7qHbBYF+fmgSz6TTjkxoT4Vy')
})

# Fetch the latest Avro schema for the value
subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
# key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str='{"type": "string"}')
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})

#connect with mysql database to fetch data in pandas dataframe
# MySQL connection settings
db_settings = {
    'host': 'localhost',
    'port':'3306',
    'user': 'root',
    'password': 'ILoveChess1e4',
    'db': 'noob_db'
}

url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(db_settings["user"], db_settings["password"], db_settings["host"], db_settings["port"], db_settings["db"])
engine = create_engine(url,echo=True)
connection = engine.connect()

file = open('c:/users/digha/documents/python scripts/last_processed_timestamp.txt','r')
last_read_time = str(file.read())
print(last_read_time)

sql_query = 'SELECT * FROM product WHERE last_updated > %s'
params = (last_read_time,)

df = pd.read_sql(sql_query, connection, params=params)
new_last_processed_time = df['last_updated'].max()


with open('c:/users/digha/documents/python scripts/last_processed_timestamp.txt','w') as file:
    file.write(str(new_last_processed_time))

df.last_updated = df.last_updated.apply(str)

# Iterate over DataFrame rows and produce to Kafka
for index, row in df.iterrows():
    # Create a dictionary from the row values
    value = row.to_dict()
    # Produce to Kafka
    producer.produce(topic='product_updates', key=str(id), value=value, on_delivery=delivery_report)
    producer.flush()

print("Data successfully published to Kafka")



