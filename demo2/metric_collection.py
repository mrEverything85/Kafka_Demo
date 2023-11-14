import psutil
import time
import json

from kafka import KafkaProducer

# Tạo producer Kafka
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# Thu thập dữ liệu hệ thống
while True:
    # CPU usage
    cpu_usage = psutil.cpu_percent(interval=1)

    # Memory usage
    memory_usage = psutil.virtual_memory().percent

    # Disk usage
    disk_usage = psutil.disk_usage('/').percent

    # Network usage
    network_usage = psutil.net_io_counters().bytes_recv / 1024 ** 2

    # Gửi dữ liệu đến Kafka qua json
    metrics = {
        "cpu_usage": cpu_usage,
        "memory_usage": memory_usage,
        "disk_usage": disk_usage,
        "network_usage": network_usage
    }
    producer.send("metrics", json.dumps(metrics).encode())
    print("sending data")
    time.sleep(1)
