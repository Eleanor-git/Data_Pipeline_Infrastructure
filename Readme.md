# Data Pipeline (Prefect + Docker)

This project is a data pipeline infrastructure built with Prefect and Docker.
It includes a Prefect server and a worker that executes ETL flows.

## Architecture

- Prefect Server: workflow orchestration + UI
- Prefect Worker: executes flows
- Data Pipeline: ETL logic written in Python

## How to Run

Make sure Docker and Docker Compose are installed.

Run the entire system:

```bash
docker-compose -f data_pipeline/docker-compose.yml up --build -d
docker exec -it data_pipeline-data-pipeline-1 python data_pipeline/src/orchestration/prefect_flows.py