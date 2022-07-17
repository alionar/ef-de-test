# Soal_1

## Answers
1. Airflow DAG: [dwh_pipeline.py](https://raw.github.com/alionar/ef-de-test/master/soal_1/dags/dwh_pipeline.py)
2. DWH Tables: [ddl](https://raw.github.com/alionar/ef-de-test/master/soal_1/docs/sql/dwh_ddl.sql)
    - `dim_date`
    - `dim_customer`
    - `fact_order_accumulating`

## Answer for Optional Question #3
[dbdiagram link](https://dbdiagram.io/d/62d39949cc1bc14cc5d1563c)

### Explanation
Because 

## Results
### DAG graph: `dwh_pipeline`
![operator_legend](https://raw.github.com/alionar/ef-de-test/master/soal_1/docs/imgs/dwh_pipeline_dag_graph_legend.png?raw=true)

![dwh_pipeline](https://raw.github.com/alionar/ef-de-test/master/soal_1/docs/imgs/dwh_pipeline_dag_graph.png?raw=true)

### Datawarehouse tables
#### `dim_date`
![img](https://raw.github.com/alionar/ef-de-test/master/soal_1/docs/imgs/dwh_table_dim_date.png?raw=true)
#### `dim_customer`
![img](https://raw.github.com/alionar/ef-de-test/master/soal_1/docs/imgs/dwh_table_dim_customer.png?raw=true)
#### `fact_order_accumulating`
![img](https://raw.github.com/alionar/ef-de-test/master/soal_1/docs/imgs/dwh_table_fact_order_accumulating.png?raw=true)

