import json
from kafka import KafkaConsumer

# --- Cấu hình ---
# Phải trùng với cấu hình trong ứng dụng của bạn
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'video_copy_events'

print(f"Đang lắng nghe trên topic '{KAFKA_TOPIC}' tại server {KAFKA_BOOTSTRAP_SERVERS}...")
print("Chờ tin nhắn mới... (Nhấn Ctrl+C để thoát)")

try:
    # Khởi tạo Consumer
    # - auto_offset_reset='earliest': Bắt đầu đọc từ tin nhắn cũ nhất nếu consumer mới được khởi động
    # - value_deserializer: Tự động chuyển đổi tin nhắn từ dạng byte JSON về đối tượng Python (dictionary)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Vòng lặp vô tận để chờ tin nhắn mới
    for message in consumer:
        # message.value chứa nội dung tin nhắn đã được giải mã
        video_data = message.value
        
        print("\n--- [ĐÃ NHẬN] Tin nhắn mới! ---")
        print(f"Thời gian gửi: {video_data.get('timestamp')}")
        
        copied_videos = video_data.get('copied_videos', [])
        print(f"Số lượng video đã sao chép: {len(copied_videos)}")
        
        # In ra các đường dẫn để xem thử
        for i, path in enumerate(copied_videos):
            print(f"  - {path}")
            
        print("---------------------------------")


except Exception as e:
    print(f"\nĐã xảy ra lỗi: {e}")
