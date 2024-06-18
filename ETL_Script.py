import boto3
import json
import hashlib
import psycopg2
from datetime import datetime
import logging
from contextlib import contextmanager

# Configure logging to log only errors
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants for SQS and PostgreSQL configurations
SQS_QUEUE_URL = 'http://localhost:4566/000000000000/login-queue'
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

# Function to hash PII data
def mask_value(value):
    """Hashes the provided value using SHA-256."""
    return hashlib.sha256(value.encode()).hexdigest()

# Function to flatten JSON data for database insertion
def flatten_json(data):
    """Flattens the JSON data for easier insertion into the database."""
    version_string = str(data.get('app_version', '0.0.0'))
    try:
        version_parts = version_string.split('.')
        app_version = int(''.join(version_parts))
    except ValueError:
        app_version = 0  # Default value if parsing fails

    return {
        'user_id': data.get('user_id', ''),
        'device_type': data.get('device_type', ''),
        'masked_ip': mask_value(data.get('ip', '')),
        'masked_device_id': mask_value(data.get('device_id', '')),
        'locale': data.get('locale', ''),
        'app_version': app_version,
        'create_date': data.get('create_date', '2024-08-16')  # Default date
    }

# Context manager for PostgreSQL database connections
@contextmanager
def get_db_connection():
    """Context manager to handle PostgreSQL database connections."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        yield conn
    except psycopg2.OperationalError as e:
        logging.error(f"Error connecting to the database: {e}")
        raise
    finally:
        if conn:
            conn.close()

# Function to ensure the user_logins table exists in the database
def create_table_if_not_exists():
    """Ensures the user_logins table exists in the database."""
    create_table_query = """
        CREATE TABLE IF NOT EXISTS user_logins (
            user_id VARCHAR(128),
            device_type VARCHAR(32),
            masked_ip VARCHAR(256),
            masked_device_id VARCHAR(256),
            locale VARCHAR(32),
            app_version INT,
            create_date DATE
        )
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                conn.commit()
    except psycopg2.Error as e:
        logging.error(f"Error creating table: {e}")
        raise

# Function to insert data into the user_logins table
def write_to_postgres(flat_data):
    """Inserts flattened data into the PostgreSQL user_logins table."""
    insert_query = """
        INSERT INTO user_logins (user_id, device_type, masked_ip, masked_device_id, locale, app_version, create_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    data_tuple = (
        flat_data['user_id'], flat_data['device_type'], flat_data['masked_ip'],
        flat_data['masked_device_id'], flat_data['locale'], flat_data['app_version'], flat_data['create_date']
    )
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(insert_query, data_tuple)
                conn.commit()
    except psycopg2.Error as e:
        logging.error(f"Error inserting into database: {e}")
        raise

# Function to ensure the SQS queue exists
def create_sqs_queue_if_not_exists():
    """Ensures the SQS queue exists."""
    sqs = boto3.client('sqs', **SQS_CONFIG)
    try:
        sqs.get_queue_url(QueueName='login-queue')
    except sqs.exceptions.QueueDoesNotExist:
        try:
            sqs.create_queue(QueueName='login-queue')
        except Exception as e:
            logging.error(f"Error creating queue: {e}")
            raise

# Function to delete a message from the SQS queue
def delete_message_from_queue(receipt_handle):
    """Deletes a message from the SQS queue using the provided receipt handle."""
    sqs = boto3.client('sqs', **SQS_CONFIG)
    try:
        sqs.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=receipt_handle
        )
    except boto3.exceptions.Boto3Error as e:
        logging.error(f"Error deleting message from queue: {e}")
        raise

# Function to receive messages from the SQS queue
def get_messages_from_queue():
    """Receives messages from the SQS queue."""
    sqs = boto3.client('sqs', **SQS_CONFIG)
    try:
        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=0  # Non-blocking check
        )
        return response.get('Messages', [])
    except Exception as e:
        logging.error(f"Error receiving messages from queue: {e}")
        return []

# Main function to process messages and insert data into the database
def main():
    """Main function to process messages from SQS and insert them into PostgreSQL."""
    create_sqs_queue_if_not_exists()
    create_table_if_not_exists()

    while True:
        messages = get_messages_from_queue()

        if not messages:
            print("No more messages to process.")
            break

        for message in messages:
            try:
                data = json.loads(message['Body'])
                flat_data = flatten_json(data)
                write_to_postgres(flat_data)
                delete_message_from_queue(message['ReceiptHandle'])
            except json.JSONDecodeError as je:
                logging.error(f"JSON decode error: {je}")
            except Exception as e:
                logging.error(f"Error processing message: {e}")

if __name__ == '__main__':
    main()
