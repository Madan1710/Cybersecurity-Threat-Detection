from kafka import KafkaConsumer
import json
from collections import defaultdict

consumer = KafkaConsumer(
    "cyber_logs",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

failed_logins = defaultdict(int)

print("Listening for threats...")

for message in consumer:
    data = message.value
    
    user = data["user_id"]
    status = data["login_status"]
    requests = data["request_count"]
    
    # Brute force detection
    if status == "fail":
        failed_logins[user] += 1
        
        if failed_logins[user] > 5:
            print(f"⚠️ ALERT: Multiple failed logins from {user}")
    
    # Request spike detection
    if requests > 40:
        print(f"⚠️ ALERT: High request spike from {user}")

