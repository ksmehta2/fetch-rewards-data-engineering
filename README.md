# fetch-rewards-data-engineering
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

Step 4: Initialize AWS LocalStack
```
docker pull fetchdocker/data-takehome-postgres
docker pull fetchdocker/data-takehome-localstack
```
Step 5:Send Sample Messages to AWS Queue 
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

Step 6: Run the ETL Script
```
python ETL_Script.py
```

Step 7: Verify data in PostgreSQL
```
psql -d postgres -U postgres -p 5433 -h localhost -W
SELECT * FROM user_logins;
```

