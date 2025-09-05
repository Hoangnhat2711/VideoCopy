import json
from confluent_kafka import Producer
import socket
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("kafka_producer")

class KafkaManager:
    """Manages the Confluent Kafka Producer lifecycle."""
    def __init__(self, app_logger_callback=None):
        self.producer = None
        self.producer_config = {}
        self.app_logger = app_logger_callback or (lambda msg, level: log.info(f"[{level}] {msg}"))

    def configure_producer(self, bootstrap_servers, **kwargs):
        """Configures or re-configures the Kafka producer."""
        if not bootstrap_servers or not isinstance(bootstrap_servers, str) or not bootstrap_servers.strip():
            msg = "Địa chỉ Kafka server không hợp lệ hoặc bị bỏ trống."
            self.app_logger(msg, "ERROR")
            return False, msg

        new_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': socket.gethostname(),
            'retries': 3,
            'retry.backoff.ms': 1000,
            **kwargs
        }

        if new_config != self.producer_config:
            self.close_producer()
            self.app_logger("Đang cấu hình Kafka producer...", "INFO")
            try:
                # Test connectivity by creating a temporary producer
                temp_producer = Producer(new_config)
                # List topics with a timeout to verify the connection
                temp_producer.list_topics(timeout=10)
                
                # If the above commands succeed, we create the final producer
                self.producer = Producer(new_config)
                self.producer_config = new_config
                
                self.app_logger("Kết nối tới Kafka producer thành công.", "SUCCESS")
                return True, "Kết nối tới Kafka producer thành công."
            except Exception as e:
                self.producer = None
                self.producer_config = {}
                msg = f"Không thể kết nối tới Kafka server tại '{bootstrap_servers}': {e}"
                self.app_logger(msg, "ERROR")
                return False, msg
        
        return True, "Kafka producer đã được cấu hình."

    def delivery_report(self, err, msg):
        """Callback for message delivery reports."""
        if err is not None:
            self.app_logger(f"Gửi tin nhắn tới topic '{msg.topic()}' thất bại: {err}", "ERROR")
        else:
            self.app_logger(f"Gửi tin nhắn tới topic '{msg.topic()}' thành công (partition {msg.partition()}).", "SUCCESS")

    def send_message(self, topic, message_data):
        """Sends a JSON message to the specified Kafka topic."""
        if not self.producer:
            return False, "Kafka producer chưa được cấu hình."
        
        if not topic or not topic.strip():
            return False, "Chủ đề (topic) Kafka không được để trống."

        self.app_logger(f"Đang gửi tin nhắn tới topic '{topic}'...", "INFO")
        try:
            # The produce call is asynchronous. The delivery_report callback will be triggered.
            self.producer.produce(
                topic,
                value=json.dumps(message_data, ensure_ascii=False).encode('utf-8'),
                callback=self.delivery_report
            )
            # Flush waits for message delivery and triggers the callback.
            # A timeout is provided to avoid blocking indefinitely.
            remaining_messages = self.producer.flush(10) # 10 second timeout
            if remaining_messages > 0:
                 self.app_logger(f"Vẫn còn {remaining_messages} tin nhắn trong hàng đợi sau khi flush.", "WARN")
            
            # For this app, we consider it "sent" if produce() doesn't raise an immediate error.
            return True, "Đã gửi tin nhắn vào hàng đợi thành công."

        except BufferError:
            self.producer.flush() # Flush the buffer and retry
            self.producer.produce(
                topic,
                value=json.dumps(message_data, ensure_ascii=False).encode('utf-8'),
                callback=self.delivery_report
            )
            return True, "Hàng đợi đầy, đã flush và gửi lại tin nhắn."
        except Exception as e:
            msg = f"Lỗi không mong muốn khi gửi tin nhắn: {e}"
            self.app_logger(msg, "ERROR")
            return False, msg

    def close_producer(self):
        """Closes the Kafka producer connection."""
        if self.producer:
            self.app_logger("Đang đóng kết nối Kafka producer...", "INFO")
            self.producer.flush(10) # Wait up to 10s for messages to be sent
            self.producer = None
            self.producer_config = {}
            self.app_logger("Đã đóng kết nối Kafka producer.", "INFO")

# Example usage:
if __name__ == '__main__':
    kafka_manager = KafkaManager()
    bootstrap_servers = 'localhost:9092'
    success, msg = kafka_manager.configure_producer(bootstrap_servers)
    print(msg)

    if success:
        topic = 'video_copy_events'
        message = {
            'event': 'test_message_confluent',
            'files': ['/path/to/video1.mp4', '/path/to/video2.mov']
        }
        success, msg = kafka_manager.send_message(topic, message)
        print(msg)

    kafka_manager.close_producer()
