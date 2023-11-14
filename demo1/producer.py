import json
import time

from kafka import KafkaProducer

# Tạo một nhà sản xuất Kafka
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# Gửi dữ liệu đến Kafka
while True:
    # Tạo một dữ liệu theo dõi hoạt động
    data = {
        "timestamp": time.time(),
        "user_id": "123456789",
    }
    producer.send("nguyenthanhan", json.dumps(data).encode())

    producer.flush()

    print("sent")

    time.sleep(1)
