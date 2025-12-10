#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Financial Fraud – Single Test Producer
Gửi đúng 1 record hardcode lên Kafka để test Spark consumer.
"""

import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [TestProducer] %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------------
# Kafka Config
# -----------------------------
BOOTSTRAP_SERVERS = [
    "kafka1:29092",
    "kafka2:29093",
    "kafka3:29094",
]

TOPIC = "financial_fraud"


def connect_kafka():
    """Kết nối Kafka (retry 10 lần)."""
    for i in range(10):
        try:
            logger.info(f"Connecting Kafka... attempt {i+1}/10")
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info("✅ Connected Kafka.")
            return producer
        except NoBrokersAvailable:
            logger.warning("Kafka not yet available... retry in 2s")
            time.sleep(2)

    raise RuntimeError("❌ Cannot connect Kafka after retries.")


def main():
    producer = connect_kafka()

    # -----------------------------
    # Hard-coded test record
    # -----------------------------
    record = {
        "fraud_bool": 0,
        "income": 42000,
        "name_email_similarity": 0.15,
        "prev_address_months_count": 10,
        "current_address_months_count": 12,
        "customer_age": 35,
        "days_since_request": 3,
        "intended_balcon_amount": 1500,
        "payment_type": "DEBIT",
        "zip_count_4w": 3,
        "velocity_6h": 0.5,
        "velocity_24h": 1.2,
        "velocity_4w": 8.3,
        "bank_branch_count_8w": 2,
        "date_of_birth_distinct_emails_4w": 1,
        "employment_status": "FULL_TIME",
        "credit_risk_score": 0.22,
        "email_is_free": 1,
        "housing_status": "RENT",
        "phone_home_valid": 1,
        "phone_mobile_valid": 1,
        "bank_months_count": 36,
        "has_other_cards": 0,
        "proposed_credit_limit": 3000,
        "foreign_request": 0,
        "source": "MOBILE",
        "session_length_in_minutes": 8,
        "device_os": "iOS",
        "keep_alive_session": 1,
        "device_distinct_emails_8w": 2,
        "device_fraud_count": 0,
        "month": 9,
        "source_id": "TEST_SOURCE",
        "event_time": time.time(),
    }

    logger.info(f"Sending 1 test record to Kafka topic '{TOPIC}' ...")

    producer.send(TOPIC, value=record)
    producer.flush()

    logger.info("🎉 Test record sent successfully!")


if __name__ == "__main__":
    main()
