import json
import os

import pymongo
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017/"
)
mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client["cars_db"]
collection = db["cars"]

consumer = KafkaConsumer(
    "divar_cars",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(
        v.decode("utf-8")
    ),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="divar_cars_group",
)
for message in consumer:
    car = message.value
    print(car)
    try:
        collection.replaceOne(
            {"_id": car["_id"]},
            car,
            upsert=True
        )
        consumer.commit()
    except Exception as e:
        print(f"Error processing car: {e}")
        raise