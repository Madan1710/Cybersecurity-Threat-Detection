from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_csv("/Users/madankumar/Documents/SEM 2/Data_Engineering_2/Exam/cybersecurity_logs_dataset.csv")

for _, row in df.iterrows():
    producer.send("cyber_logs", row.to_dict())
    print("Sent:", row["user_id"])
    time.sleep(0.1)

