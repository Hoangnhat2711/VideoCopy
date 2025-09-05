import json
from confluent_kafka import Producer
import socket
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("kafka_producer")

class KafkaManager:
    """Quản lý vòng đời của Confluent Kafka Producer."""
    def __init__(self, app_logger_callback=None):
        self.producer = None
        self.producer_config = {}
        self.app_logger = app_logger_callback or (lambda msg, level: log.info(f"[{level}] {msg}"))

    def configure_producer(self, bootstrap_servers, **kwargs):
        """Cấu hình hoặc cấu hình lại Kafka producer."""
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
            self.app_logger("Đang cấu hình Kafka producer...", "INFO")
            try:
                # Producer được tạo ở đây. Các vấn đề về kết nối sẽ được xử lý
                # bất đồng bộ và được báo cáo bởi callback gửi tin khi có tin nhắn được sản xuất.
                self.producer = Producer(new_config)
                self.producer_config = new_config
                
                self.app_logger("Đã cấu hình Kafka producer thành công.", "SUCCESS")
                return True, "Đã cấu hình Kafka producer thành công."
            except Exception as e:
                self.producer = None
                self.producer_config = {}
                msg = f"Lỗi cấu hình Kafka producer: {e}"
                self.app_logger(msg, "ERROR")
                return False, msg
        
        return True, "Kafka producer đã được cấu hình."

    def delivery_report(self, err, msg):
        """Callback cho các báo cáo về việc gửi tin nhắn."""
        if err is not None:
            self.app_logger(f"Gửi tin nhắn tới topic '{msg.topic()}' thất bại: {err}", "ERROR")
        else:
            self.app_logger(f"Gửi tin nhắn tới topic '{msg.topic()}' thành công (partition {msg.partition()}).", "SUCCESS")

    def send_message(self, topic, message_data):
        """Gửi một tin nhắn JSON đến topic Kafka được chỉ định."""
        if not self.producer:
            return False, "Kafka producer chưa được cấu hình."
        
        if not topic or not topic.strip():
            return False, "Chủ đề (topic) Kafka không được để trống."

        self.app_logger(f"Đang gửi tin nhắn tới topic '{topic}'...", "INFO")
        try:
            # Lệnh produce là bất đồng bộ. Callback delivery_report sẽ được kích hoạt.
            self.producer.produce(
                topic,
                value=json.dumps(message_data, ensure_ascii=False).encode('utf-8'),
                callback=self.delivery_report
            )
            # Flush sẽ đợi tin nhắn được gửi và kích hoạt callback.
            # Một khoảng thời gian chờ được cung cấp để tránh bị chặn vô thời hạn.
            remaining_messages = self.producer.flush(10) # Thời gian chờ 10 giây
            if remaining_messages > 0:
                 self.app_logger(f"Vẫn còn {remaining_messages} tin nhắn trong hàng đợi sau khi flush.", "WARN")
            
            # Đối với ứng dụng này, chúng ta coi là "đã gửi" nếu produce() không gây ra lỗi ngay lập tức.
            return True, "Đã gửi tin nhắn vào hàng đợi thành công."

        except BufferError:
            self.producer.flush() # Xả bộ đệm và thử lại
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

# Ví dụ sử dụng:
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
