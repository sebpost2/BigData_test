from confluent_kafka import Producer
import json
import time
import random
from faker import Faker

# Initialize Faker for generating fake data
fake = Faker()

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}

# Kafka producer function
def produce():
    producer = Producer(conf)
    try:
        while True:
            # Produce some dummy data to Kafka topic "Proyecto"
            netflix_data = {
                'userid': fake.uuid4(),
                'user_location': random.choice(['Nepal', 'USA', 'India', 'China', 'Belgium', 'Canada', 'Switzerland']),
                'channelid': fake.random_int(min=1, max=50),
                'genre': random.choice(['thriller', 'comedy', 'romcom', 'fiction']),
                'lastactive': fake.date_time_between(start_date='-10m', end_date='now').isoformat(),
                'title': fake.name(),
                'watchfrequency': fake.random_int(min=1, max=10),
                'etags': fake.uuid4()
            }
            producer.produce('Proyecto', value=json.dumps(netflix_data).encode('utf-8'))
            print(f"Produced message: {netflix_data}")
            time.sleep(1)
            producer.flush()

    except KeyboardInterrupt:
        pass

    finally:
        # Close the producer
        producer.flush()
        producer.close()

# Call the producer function
if __name__ == '__main__':
    produce()