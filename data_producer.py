import time
import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def produce_data():
    while True:
        data = {
            'sensor_id': random.randint(1, 10),
            'temperature': round(random.uniform(20.0, 30.0), 2),
            'timestamp': int(time.time())
        }
        producer.send('sensor_data', value=data)
        time.sleep(1)

if __name__ == "__main__":
    produce_data()
