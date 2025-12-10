import os, time, json
import pandas as pd
from kafka import KafkaProducer

# ====== Config ======
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("ABSA_TOPIC", "absa-reviews")
INPUT_CSV = os.getenv("ABSA_INPUT", "/tmp/absa/test_data.csv")

def main():
    print(f"[Producer] 🚀 Starting Kafka Producer")
    print(f"[Producer] Broker: {KAFKA_BROKER}")
    print(f"[Producer] Topic:  {TOPIC}")
    print(f"[Producer] Input:  {INPUT_CSV}")

    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"[Producer] ❌ Not found: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)
    if "Review" not in df.columns:
        raise ValueError("[Producer] ❌ CSV must contain 'Review' column")

    print(f"[Producer] ✅ Loaded {len(df)} rows from CSV")

    # ====== Init Kafka Producer ======
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=50,
        retries=3
    )
    print("[Producer] 🔗 Connected to Kafka broker")

    # ====== Send messages ======
    print(f"[Producer] 🚚 Sending {len(df)} reviews to topic '{TOPIC}' ...\n")
    for i, row in df.iterrows():
        msg = {"review": str(row["Review"]).strip()}
        producer.send(TOPIC, value=msg)
        print(f"[Producer] → ({i+1}/{len(df)}) Sent: {msg['review'][:80]}...")

        if (i + 1) % 10 == 0:
            print(f"[Producer] 🔄 Flushing batch up to row {i+1}")
            producer.flush()
        time.sleep(0.5)

    producer.flush()
    print("\n[Producer] ✅ All messages flushed successfully")
    print("[Producer] ✨ Finished sending all reviews.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[Producer] ❌ Error: {e}")
