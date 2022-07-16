#!/bin/bash
set -e

docker-compose up airflow-init \
    && docker-compose up -d