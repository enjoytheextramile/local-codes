# client_send_enterprise.py
# Simulates e-commerce order events for enterprise pipeline.
# Splits events into two logical parts: customer details and payment details.
# Supports hot keys, duplicates, late/out-of-order events, and traffic bursts.

import requests
import uuid
import random
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

URL = "https://9zyzol4mt0.execute-api.us-east-1.amazonaws.com/test1/"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def past_iso(seconds_back):
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_back)).isoformat()

def generate_order_event(i, hot_key=False, duplicate_event_id=None, late_seconds=None, out_of_order=False):
    """
    Generate a single order event with both:
    - Customer details
    - Payment details
    """
    customer_id = "HOT_CUSTOMER" if hot_key else f"CUST_{random.randint(1, 20)}"
    event_id = duplicate_event_id if duplicate_event_id else str(uuid.uuid4())

    event_time = past_iso(late_seconds) if late_seconds else now_iso()
    if out_of_order:
        jitter = random.randint(-60, 60)
        event_time = (datetime.now(timezone.utc) + timedelta(seconds=jitter)).isoformat()

    event = {
        "order_id": f"ORD{i+1}",
        "customer_id": customer_id,
        "customer_details": {
            "name": f"Customer {i+1}",
            "email": f"user{i+1}@example.com"
        },
        "payment_details": {
            "payment_id": str(uuid.uuid4()),
            "amount": random.randint(100, 500),
            "method": random.choice(["card", "upi", "wallet"]),
            "status": random.choice(["confirmed", "pending", "failed"])
        },
        "event_time": event_time,
        "event_id": event_id
    }
    return event

def send_request(event):
    """Send a single event to API Gateway"""
    try:
        r = requests.post(URL, json=event, timeout=5)
        return r.status_code
    except Exception as e:
        print("Request failed:", e)
        return None

def run_test(total_events=50, workers=10, hot_key_ratio=0.3,
             duplicate_ratio=0.2, late_ratio=0.1, out_of_order_ratio=0.1):
    """Send events in bulk with advanced scenarios"""
    duplicate_pool = []
    events = []

    for i in range(total_events):
        hot_key = random.random() < hot_key_ratio
        use_duplicate = duplicate_pool and (random.random() < duplicate_ratio)
        duplicate_id = random.choice(duplicate_pool) if use_duplicate else None
        late_seconds = random.randint(60, 86400) if random.random() < late_ratio else None
        out_of_order = random.random() < out_of_order_ratio

        event = generate_order_event(
            i,
            hot_key=hot_key,
            duplicate_event_id=duplicate_id,
            late_seconds=late_seconds,
            out_of_order=out_of_order
        )
        duplicate_pool.append(event["event_id"])
        events.append(event)

    # Send events concurrently
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(send_request, events))

    # Report
    success = sum(1 for r in results if r and 200 <= r < 300)
    failed = total_events - success
    print(f"Success: {success}, Failed: {failed}")

if __name__ == "__main__":
    run_test(
        total_events=50,
        workers=10,
        hot_key_ratio=0.3,
        duplicate_ratio=0.2,
        late_ratio=0.1,
        out_of_order_ratio=0.1
    )
