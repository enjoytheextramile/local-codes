# producer_lambda_enterprise.py
# Robust Producer Lambda for enterprise e-commerce pipeline
# Splits API event into two Kafka topics: customer_details and payment_details
# Maintains ordering per customer, retries, and idempotence

import json
import os
import time
from kafka import KafkaProducer, KafkaError

# Kafka topics for enterprise separation
CUSTOMER_TOPIC = os.environ.get("CUSTOMER_TOPIC", "customer_details")
PAYMENT_TOPIC = os.environ.get("PAYMENT_TOPIC", "payment_details")
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092")

# Kafka producer with retries and idempotence
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks='all',
    retries=5,
    enable_idempotence=True
)

def send_to_kafka(topic, key_str, value, max_attempts=3):
    key = key_str.encode("utf-8")
    attempt = 0
    while attempt < max_attempts:
        try:
            future = producer.send(topic, key=key, value=value)
            result = future.get(timeout=10)
            return True
        except KafkaError as e:
            print(f"Send failed for event {value.get('event_id')} to {topic}, attempt {attempt+1}: {e}")
            attempt += 1
            time.sleep(0.5)
    return False

def lambda_handler(event, context):
    """
    Receives API Gateway event, splits into customer and payment parts,
    sends to separate Kafka topics with robust handling.
    """
    try:
        body = json.loads(event.get("body", "{}"))

        # Ensure event_id and timestamp
        if "event_id" not in body:
            import uuid
            body["event_id"] = str(uuid.uuid4())
        if "event_time" not in body:
            from datetime import datetime, timezone
            body["event_time"] = datetime.now(timezone.utc).isoformat()

        # Send customer details to customer_details topic
        customer_event = {
            "order_id": body["order_id"],
            "customer_id": body["customer_id"],
            "customer_details": body.get("customer_details", {}),
            "event_time": body["event_time"],
            "event_id": body["event_id"]
        }
        success_customer = send_to_kafka(CUSTOMER_TOPIC, body["customer_id"], customer_event)

        # Send payment details to payment_details topic
        payment_event = {
            "order_id": body["order_id"],
            "customer_id": body["customer_id"],
            "payment_details": body.get("payment_details", {}),
            "event_time": body["event_time"],
            "event_id": body["event_id"]
        }
        success_payment = send_to_kafka(PAYMENT_TOPIC, body["customer_id"], payment_event)

        if success_customer and success_payment:
            return {"statusCode": 200, "body": json.dumps({"message": "Events sent to Kafka"})}
        else:
            return {"statusCode": 500, "body": json.dumps({"message": "Failed to send one or more events"})}

    except Exception as e:
        return {"statusCode": 400, "body": json.dumps({"message": f"Bad request or error: {e}"})}
