# fetch-rewards-data-engineering

## Overview
### This project is designed to demonstrate data processing capabilities using AWS services simulated locally via LocalStack. The script reads messages from an SQS queue, masks PII data, and stores the transformed data in a PostgreSQL database. This setup is particularly useful for data engineering tasks that require handling sensitive information securely and efficiently.


## Project decisions has been added in a separate file named Project Decisions

Step 1: Clone the Repository
```
git clone https://github.com/ksmehta2/fetch-rewards-data-engineering.git
```

Step 2: Build and Run Docker Containers
```
docker-compose up -d
```

Step 3: Install Python Dependencies
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Step 4: Ensure Services are Properly Initialized by Running Docker Compose in Foreground
```
docker-compose up
```
Step 5: Run the ETL Script to Create the Queue.
This is to ensure the queue is created before sending messages.

```
python ETL_Script.py
```
Step 6: Send Sample Messages to AWS Queue.
After the queue has been created, send sample messages to the SQS queue.
```
awslocal sqs send-message --queue-url http://localhost:4566/000000000000/login-queue --message-body '{
  "user_id": "67890",
  "device_type": "tablet",
  "ip": "192.168.0.2",
  "device_id": "device456",
  "locale": "en_UK",
  "app_version": 2,
  "create_date": "2023-02-01"
}'

awslocal sqs send-message --queue-url http://localhost:4566/000000000000/login-queue --message-body '{
  "user_id": "54321",
  "device_type": "desktop",
  "ip": "192.168.0.3",
  "device_id": "device789",
  "locale": "fr_FR",
  "app_version": 3,
  "create_date": "2023-03-01"
}'
```

Step 7: Run the ETL Script.
Run the ETL script again to process the messages and insert them into PostgreSQL.
```
python ETL_Script.py
```

Step 8: Verify data in PostgreSQL.
```
psql -d postgres -U postgres -p 5433 -h localhost -W
```

When prompted, enter the password for the PostgreSQL user, which is 'postgres'.

After logging into PostgreSQL, run:
```
SELECT * FROM user_logins;
```

