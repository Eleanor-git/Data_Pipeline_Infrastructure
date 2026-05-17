# Data Pipeline (Prefect + Docker)

This project is a data pipeline infrastructure built with Prefect and Docker.
It includes a Prefect server and a worker that executes ETL flows.

## Architecture

- Prefect Server: workflow orchestration + Monitoring UI
- Prefect Worker: executes flows
- Data Pipeline: ETL logic and workflow are written in Python

## Architecture Diagram
```
                    +----------------------+
                    |   E-commerce App     |
                    | (Orders / Customers) |
                    +----------+-----------+
                               |
                               | Upload Raw Data
                               v
                    +----------------------+
                    |     AWS S3 Bucket    |
                    |      raw-data/       |
                    +----------+-----------+
                               |
                               | Polling
                               v
                    +----------------------+
                    |     Prefect Server   |
                    |  Flow Orchestration  |
                    +----------+-----------+
                               |
                 +-------------+-------------+
                 |                           |
                 | Execute Flow              |
                 v                           |
          +-------------+                    |
          | Prefect     |                    |
          | Worker      |                    |
          +------+------+                    |
                 |                           |
                 | Run Python ETL            |
                 v                           |
       +----------------------+              |
       | Python Transformation|              |
       |  Data Preprocessing  |              |
       |  (prefect_flows.py)  |              |
       +----------+-----------+              |
                  |                          |
                  | Upload Cleaned Data      |
                  v                          |
        +-----------------------+            |
        | AWS S3 Bucket         |            |
        | processed-data/       |            |
        +-----------------------+            |
                                             |
                                             v
                                 +-------------------+
                                 | Logging/Monitoring|
                                 | Prefect UI        |
                                 +-------------------+
```

## Data Flow
```
Raw Customer Data
        ↓
S3 Raw Bucket (data/)
        ↓
Prefect Flow Trigger
        ↓
Python Transformation Script (prefect_flows.py) 
        ↓
Processed Dataset
        ↓
S3 Processed Bucket(processed-data/)
```

## Project Structure
```
├── .env                              # AWS credentials (should be created)
├── data/                             # Sample datasets
├── data-pipeline/                    # Main pipeline code
│   ├── requirements.txt              # Python dependencies
│   ├── docker-compose.yml            # Container orchestration
│   ├── src/                          # Source code
│   │   └── s3_uploader.py            # Upload data to AWS S3 (Data Ingestion)
│   │   └── simple_data_processing.py # ETL
│   │   └── orchestration/            # Workflow management
│   │       └── prefect_flows.py      # Prefect orchestration
│   ├── config/                       # Configuration files
│   │   └──prefect.yaml               # Prefect configuration
│   ├── infrastructure/               # Container infrastructure
│   │   └── docker/
│   │       └── Dockerfile            # Container build instructions
│   └── docker-compose.yml            # Docker compose
└── Readme.md
```
## Set up Environment
Create a `.env` file if needed:
```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=...
AWS_S3_BUCKET_NAME=...
```
Prefect Env setup:

```PREFECT_API_URL=http://prefect-server:4200/api```

## How to Run

Make sure Docker and Docker Compose are installed.

Run the entire system:

```bash
docker-compose -f data_pipeline/docker-compose.yml up --build -d
docker exec -it data_pipeline-data-pipeline-1 python data_pipeline/src/orchestration/prefect_flows.py
```
```
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect worker start --pool project-pool
```
under `data_pipeline/config`
```
prefect deploy --all 
```

Resource:

This project is inspired and adapted from [Navid's The Data Engineer Bootcamp](https://www.udemy.com/course/data-engineering-bootcamp/learn/lecture/52642221?start=15#questions)
