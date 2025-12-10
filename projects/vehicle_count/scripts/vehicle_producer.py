import cv2, base64, json, time, sys, os
from kafka import KafkaProducer

KAFKA_BROKER = "kafka:9092"
TOPIC = "vehicle-stream"
FPS_LIMIT = 5  # frame/s gửi lên Kafka

def encode_frame(frame):
    _, buffer = cv2.imencode(".jpg", frame)
    return base64.b64encode(buffer).decode("utf-8")

def stream_video(video_path, producer, camera_id):
    if not os.path.exists(video_path):
        print(f"[{camera_id}] ❌ File not found: {video_path}")
        sys.exit(1)

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"[{camera_id}] ⚠️ Cannot open video: {video_path}")
        sys.exit(1)

    fps_interval = 1 / FPS_LIMIT
    frame_count = 0
    print(f"[{camera_id}] 🎥 Start streaming {os.path.basename(video_path)}")

    try:
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                print(f"[{camera_id}] ✅ Finished ({frame_count} frames).")
                break

            frame_b64 = encode_frame(frame)
            message = {
                "camera_id": camera_id,
                "timestamp": time.time(),
                "video_name": os.path.basename(video_path),
                "frame_id": frame_count,
                "frame_data": frame_b64
            }

            producer.send(TOPIC, value=message)
            frame_count += 1
            if frame_count % 20 == 0:
                print(f"[{camera_id}] 📤 Sent frame_id: {frame_count}, frame_data: ...")
            time.sleep(fps_interval)

        producer.flush()
        print(f"[{camera_id}] 🏁 Done sending {frame_count} frames to Kafka.")
    except Exception as e:
        print(f"[{camera_id}] ❌ Error: {e}")
    finally:
        cap.release()

def main():
    if len(sys.argv) != 3:
        print("Usage: python vehicle_producer.py <camera_id> <video_path>")
        sys.exit(1)
    camera_id, video_path = sys.argv[1], sys.argv[2]
    print(f"[{camera_id}] 🔌 Connecting to Kafka at {KAFKA_BROKER} ...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3
    )
    stream_video(video_path, producer, camera_id)
    print(f"[{camera_id}] 🏁 Done sending stream.")

if __name__ == "__main__":
    main()
