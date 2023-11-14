import json
import time

from kafka import KafkaConsumer

# Tạo một người tiêu dùng Kafka
consumer = KafkaConsumer("nguyenthanhan", bootstrap_servers="localhost:9092")

# Đọc dữ liệu từ Kafka
for message in consumer:
    data = json.loads(message.value.decode())
    print(data)