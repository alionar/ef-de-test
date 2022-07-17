## DB
### Deploy DB containers
```
cd soal_1/docker/db \
    && docker-compose up -d
```
### Reinitiate DB & DWH containers (remove db data)
```
docker-compose down -v && docker-compose up -d 
```

### Shut down DB containers
```
docker-compose down
```

## Airflow
### Initiate airflow containers
```
docker-compose up airflow-init \
    && docker-compose up -d
```

### Default Airflow user
```
username: airflow
password: airflow
```

### Export DAG Graph image
```
docker exec -it airflow-airflow-webserver-1 /bin/bash

airflow dags show dwh_pipeline --save dwh_pipeline.png && mv dwh_pipeline.png dags/dwh_pipeline.png
```