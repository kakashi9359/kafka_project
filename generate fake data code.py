from faker import Faker
from faker.providers import DynamicProvider
import random
import mysql.connector
from datetime import datetime, timedelta

# MySQL connection settings
db_settings = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'db': 'noob_db'
}


sports_gear_provider = DynamicProvider(provider_name='sports_gear',elements=['tshirt','track','cap','shorts','jersey','sneakers'])
sports_name_provider = DynamicProvider(provider_name='sports_name',elements=['baseball','cricket','f1','soccer','basketball'])
fake = Faker()
fake.add_provider(sports_gear_provider)
fake.add_provider(sports_name_provider)

connection = mysql.connector.connect(**db_settings)
cursor = connection.cursor()

for _ in range(1000):
    id = fake.unique.random_int(min=1,max=2000)
    name = fake.sports_gear().capitalize()
    category = fake.sports_name().capitalize()
    price = round(random.uniform(10, 1000), 2)
    last_updated = datetime.now() - timedelta(minutes=random.randint(0, 365))
    
    insert_query = f"""
    INSERT INTO product (id,name, category, price, last_updated)
    VALUES ('{id}','{name}', '{category}', {price}, '{last_updated}')
    """
    
    cursor.execute(insert_query)
    connection.commit()

cursor.close()
connection.close()

print("Data insertion completed.")
