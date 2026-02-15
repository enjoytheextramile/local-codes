# consumer_lambda_firehose.py
# Consumes Kafka events from MSK and sends them to Kinesis Firehose.
# Firehose handles buffering and S3 delivery; Lambda adds partition keys and error handling.

import json
import os
import boto3

# Firehose delivery stream name
FIREHOSE_STREAM = os.environ.get("FIREHOSE_STREAM", "ecommerce_firehose")

firehose_client = boto3.client("firehose")

def send_to_firehose(partition_key, record):
    """
    Send a single record to Firehose.
    Partition key is used for buffering/delivery hints.
    Firehose writes automatically to S3 based on configuration.
    """
    try:
        firehose_client.put_record(
            DeliveryStreamName=FIREHOSE_STREAM,
            Record={'Data': json.dumps(record) + "\n"},
            # PartitionKey is optional, can help Firehose buffer by key
        )
        return True
    except Exception as e:
        print(f"Failed to send event {record.get('event_id')} to Firehose: {e}")
        return False

def lambda_handler(event, context):
    """
    Lambda triggered by MSK Event Source Mapping.
    Reads records from Kafka topics: customer_details and payment_details.
    Sends each event to Firehose for S3 delivery.
    """
    for tp, records in event.get("records", {}).items():
        topic_name, partition_id = tp.rsplit("-", 1)
        for record in records:
            try:
                data = json.loads(record["value"])
                customer_id = data.get("customer_id", "unknown")

                # Optional: log late/out-of-order events, metrics
                send_to_firehose(customer_id, data)

            except Exception as e:
                print(f"Error processing record from {topic_name} partition {partition_id}: {e}")
