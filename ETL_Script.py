import boto3
import json
import hashlib
import psycopg2
from datetime import datetime
import logging
from contextlib import contextmanager

# Configure logging to help with debugging and tracking application behavior
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration settings for SQS and PostgreSQL
QUEUE_URL = 'http://localhost:4566/000000000000/login-queue'
DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5433
}
SQS_CONFIG = {
    'endpoint_url': 'http://localhost:4566',
    'region_name': 'us-east-1'
}

# Initialize the SQS client to interact with LocalStack's SQS
sqs = boto3.client('sqs', **SQS_CONFIG)

@contextmanager
def get_db_connection():
    """Context manager to handle PostgreSQL database connections."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

def get_messages_from_queue():
    """
    Fetch messages from the SQS queue.
    This function retrieves messages from the specified SQS queue.
    """
    try:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5
        )
        logging.info("SQS Response: %s", response)
        return response.get('Messages', [])
    except Exception as e:
        logging.error("Error receiving messages from queue: %s", e)
        return []

def mask_pii(data):
    """
    Mask PII fields and return masked data.
    This function hashes the 'ip' and 'device_id' fields to protect PII.
    """
    data['masked_ip'] = hashlib.sha256(data['ip'].encode()).hexdigest()
    data['masked_device_id'] = hashlib.sha256(data['device_id'].encode()).hexdigest()
    del data['ip']
    del data['device_id']
    return data

def transform_data(message):
    """
    Transform the raw message into a structured format suitable for the database.
    This function processes the message body, masks PII, and formats the data.
    """
    data = json.loads(message['Body'])
    masked_data = mask_pii(data)
    
    create_date = datetime.strptime(masked_data['create_date'], "%Y-%m-%d").date()
    
    return (
        masked_data['user_id'],
        masked_data['device_type'],
        masked_data['masked_ip'],
        masked_data['masked_device_id'],
        masked_data['locale'],
        int(masked_data['app_version']),
        create_date
    )

def write_to_postgres(data):
    """
    Insert transformed data into PostgreSQL.
    This function inserts the processed data into the 'user_logins' table.
    """
    insert_query = """
    INSERT INTO user_logins (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(insert_query, data)
                conn.commit()
                logging.info("Data inserted successfully into user_logins table")
    except Exception as e:
        logging.error("Error inserting data into PostgreSQL: %s", e)

def main():
    """
    Main function to process SQS messages and insert data into PostgreSQL.
    This function orchestrates the entire flow from fetching messages to inserting data.
    """
    messages = get_messages_from_queue()
    if not messages:
        logging.info("No messages received.")
        return

    for message in messages:
        data = transform_data(message)
        write_to_postgres(data)
        try:
            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )
            logging.info("Message deleted successfully from the queue")
        except Exception as e:
            logging.error("Error deleting message from queue: %s", e)

if __name__ == '__main__':
    main()
