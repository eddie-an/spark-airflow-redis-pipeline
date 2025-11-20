#!/bin/bash

set -e

airflow db migrate

airflow users create \
    --username admin \
    --password admin \
    --firstname admin \
    --lastname user \
    --role Admin \
    --email admin@example.com || true

airflow webserver -p 8080 &

airflow scheduler
