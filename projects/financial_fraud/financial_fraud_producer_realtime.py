#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Producer] %(message)s",
)
logger = logging.getLogger("fraud-producer")

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka1:29092,kafka2:29093,kafka3:29094"
).split(",")

TOPIC = "financial_fraud"


def connect_kafka(max_retries=10):
    for i in range(max_retries):
        try:
            logger.info(f"Kafka connecting ({i+1}/{max_retries}) to {BOOTSTRAP_SERVERS}")
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info("✅ Kafka connected.")
            return producer
        except NoBrokersAvailable:
            logger.warning("Kafka unavailable, retry in 2s...")
            time.sleep(2)

    raise RuntimeError("❌ Kafka unreachable")


# ================================================
# HARDCODE TEST RECORD (đúng schema fraud model)
# ================================================
def generate_test_record():
    now = time.time()

    record = {
        "fraud_bool": 0,
        "income": 65000,
        "name_email_similarity": 0.12,
        "prev_address_months_count": 18,
        "current_address_months_count": 5,
        "customer_age": 29,
        "days_since_request": 3,
        "intended_balcon_amount": 1200,
        "payment_type": "CARD",
        "zip_count_4w": 2,
        "velocity_6h": 0,
        "velocity_24h": 1,
        "velocity_4w": 3,
        "bank_branch_count_8w": 1,
        "date_of_birth_distinct_emails_4w": 0,
        "employment_status": "EMPLOYED",
        "credit_risk_score": 0.78,
        "email_is_free": 1,
        "housing_status": "RENT",
        "phone_home_valid": 1,
        "phone_mobile_valid": 1,
        "bank_months_count": 12,
        "has_other_cards": 0,
        "proposed_credit_limit": 3000,
        "foreign_request": 0,
        "source": "WEB",
        "session_length_in_minutes": 4.5,
        "device_os": "Android",
        "keep_alive_session": 1,
        "device_distinct_emails_8w": 1,
        "device_fraud_count": 0,
        "month": 11,
        # metadata
        "event_time": now,
        "source_id": "REALTIME_TEST",
    }

    return record


def main():
    producer = connect_kafka()

    record = generate_test_record()

    producer.send(TOPIC, value=record)
    producer.flush()

    logger.info("📤 Sent 1 fraud test event → topic '%s'", TOPIC)


if __name__ == "__main__":
    main()
