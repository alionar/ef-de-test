from datetime import datetime, timedelta
from urllib.parse import urlparse
from airflow import DAG
from airflow.operators.generic_transfer import GenericTransfer
from airflow.operators.dummy import DummyOperator
from airflow.operators.sql import SQLCheckOperator, BranchSQLOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
from helper import soal_1 as s

default_args = {
    'owner': 'aulia',
    'email': ['lionar.id13@gmail.com'],
    'start_date': datetime(2022, 7, 16),
    'retry_delay': timedelta(minutes=10),
    'retries': 1
}

dag = DAG(
    dag_id='dwh_pipeline',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
    tags=['dwh']
)

db_source = s.db_airflow_conn_id('source')
dwh = s.db_airflow_conn_id('dwh')

# Task Groups
# g_dwh_raw_schema = TaskGroup(group_id='raw_schema')
# g_dwh_raw_tables = TaskGroup(group_id='raw_tables')
# g_dwh_raw_load = TaskGroup(group_id='raw_load')
# g_dwh_raw_load_check = TaskGroup(group_id='raw_load_check')

# g_dwh_schema = TaskGroup(group_id='dwh_schema')
# g_dwh_tf_tables = TaskGroup(group_id='dwh_transform_tables')
# g_dwh_tf = TaskGroup(group_id='dwh_transform')
# g_dwh_tf_check = TaskGroup(group_id='dwh_transform_check')


# Tasks
task_start = DummyOperator(
    task_id='task_start',
    dag=dag
)

start_dwh_schema_raw = DummyOperator(
    task_id='start_dwh_schema_raw',
    dag=dag
)

check_dwh_schema_raw = BranchSQLOperator(
    task_id='check_dwh_schema_raw',
    dag=dag,
    #task_group=g_dwh_raw_schema,
    conn_id=dwh,
    database='dwh',
    sql="""
        SELECT EXISTS (
            SELECT FROM 
                pg_catalog.pg_namespace
            WHERE 
                nspname = 'raw'
        )
    """,
    follow_task_ids_if_true=['end_dwh_schema_raw'],
    follow_task_ids_if_false=['create_dwh_schema_raw']
)

create_dwh_schema_raw = PostgresOperator(
    task_id='create_dwh_schema_raw',
    dag=dag,
    #task_group=g_dwh_raw_schema,
    postgres_conn_id=dwh,
    sql='sql/create_dwh_schema_raw.sql'
)

end_dwh_schema_raw = DummyOperator(
    task_id='end_dwh_schema_raw',
    trigger_rule='none_failed',
    dag=dag
)

start_check_raw_tables = DummyOperator(
    task_id='start_check_raw_tables',
    dag=dag
)

check_raw_table_customers = BranchSQLOperator(
    task_id='check_raw_table_customers',
    dag=dag,
    #task_group=g_dwh_raw_tables,
    conn_id=dwh,
    database='dwh',
    sql="""
        SELECT EXISTS (
            SELECT FROM 
                pg_tables
            WHERE 
                schemaname = 'raw' AND 
                tablename  = 'customer'
        )
    """,
    follow_task_ids_if_true=['end_check_raw_tables'],
    follow_task_ids_if_false=['create_raw_table_customers']
)

create_raw_table_customers = PostgresOperator(
    task_id='create_raw_table_customers',
    dag=dag,
    #task_group=g_dwh_raw_tables,
    postgres_conn_id=dwh,
    sql='sql/create_dwh_raw_customers.sql'
)

check_raw_table_orders = BranchSQLOperator(
    task_id='check_raw_table_orders',
    dag=dag,
    #task_group=g_dwh_raw_tables,
    conn_id=dwh,
    database='dwh',
    sql="""
        SELECT EXISTS (
            SELECT FROM 
                pg_tables
            WHERE 
                schemaname = 'raw' AND 
                tablename  = 'orders'
        )
    """,
    follow_task_ids_if_true=['end_check_raw_tables'],
    follow_task_ids_if_false=['create_raw_table_orders']
)

create_raw_table_orders = PostgresOperator(
    task_id='create_raw_table_orders',
    dag=dag,
    #task_group=g_dwh_raw_tables,
    postgres_conn_id=dwh,
    sql='sql/create_dwh_raw_orders.sql'
)

check_raw_table_invoices = BranchSQLOperator(
    task_id='check_raw_table_invoices',
    dag=dag,
    #task_group=g_dwh_raw_tables,
    conn_id=dwh,
    database='dwh',
    sql="""
        SELECT EXISTS (
            SELECT FROM 
                pg_tables
            WHERE 
                schemaname = 'raw' AND 
                tablename  = 'invoices'
        )
    """,
    follow_task_ids_if_true=['end_check_raw_tables'],
    follow_task_ids_if_false=['create_raw_table_invoices']
)

create_raw_table_invoices = PostgresOperator(
    task_id='create_raw_table_invoices',
    dag=dag,
    #task_group=g_dwh_raw_tables,
    postgres_conn_id=dwh,
    sql='sql/create_dwh_raw_invoices.sql'
)

check_raw_table_order_lines = BranchSQLOperator(
    task_id='check_raw_table_order_lines',
    dag=dag,
    #task_group=g_dwh_raw_tables,
    conn_id=dwh,
    database='dwh',
    sql="""
        SELECT EXISTS (
            SELECT FROM 
                pg_tables
            WHERE 
                schemaname = 'raw' AND 
                tablename  = 'order_lines'
        )
    """,
    follow_task_ids_if_true=['end_check_raw_tables'],
    follow_task_ids_if_false=['create_raw_table_order_lines']
)

create_raw_table_order_lines = PostgresOperator(
    task_id='create_raw_table_order_lines',
    dag=dag,
    #task_group=g_dwh_raw_tables,
    postgres_conn_id=dwh,
    sql='sql/create_dwh_raw_order_lines.sql'
)

check_raw_table_payments = BranchSQLOperator(
    task_id='check_raw_table_payments',
    dag=dag,
    #task_group=g_dwh_raw_tables,
    conn_id=dwh,
    database='dwh',
    sql="""
        SELECT EXISTS (
            SELECT FROM 
                pg_tables
            WHERE 
                schemaname = 'raw' AND 
                tablename  = 'payments'
        )
    """,
    follow_task_ids_if_true=['end_check_raw_tables'],
    follow_task_ids_if_false=['create_raw_table_payments']
)

create_raw_table_payments = PostgresOperator(
    task_id='create_raw_table_payments',
    dag=dag,
    #task_group=g_dwh_raw_tables,
    postgres_conn_id=dwh,
    sql='sql/create_dwh_raw_payments.sql'
)

check_raw_table_products = BranchSQLOperator(
    task_id='check_raw_table_products',
    dag=dag,
    #task_group=g_dwh_raw_tables,
    conn_id=dwh,
    database='dwh',
    sql="""
        SELECT EXISTS (
            SELECT FROM 
                pg_tables
            WHERE 
                schemaname = 'raw' AND 
                tablename  = 'products'
        )
    """,
    follow_task_ids_if_true=['end_check_raw_tables'],
    follow_task_ids_if_false=['create_raw_table_products']
)

create_raw_table_products = PostgresOperator(
    task_id='create_raw_table_products',
    dag=dag,
    #task_group=g_dwh_raw_tables,
    postgres_conn_id=dwh,
    sql='sql/create_dwh_raw_products.sql'
)

end_check_raw_tables = DummyOperator(
    task_id='end_check_raw_tables',
    trigger_rule='none_failed',
    dag=dag
)

start_load_raw_tables = DummyOperator(
    task_id='start_load_raw_tables',
    dag=dag
)

load_raw_customers = GenericTransfer(
    task_id='load_raw_customers',
    dag=dag,
    #task_group=g_dwh_raw_load,
    source_conn_id=db_source,
    destination_conn_id=dwh,
    destination_table='raw.customers',
    preoperator=[
        "TRUNCATE raw.customers"
    ],
    sql="""
        SELECT
            id, name
        FROM public.customers
    """
)

load_raw_orders = GenericTransfer(
    task_id='load_raw_orders',
    dag=dag,
    #task_group=g_dwh_raw_load,
    source_conn_id=db_source,
    destination_conn_id=dwh,
    destination_table='raw.orders',
    preoperator=[
        "TRUNCATE raw.orders"
    ],
    sql="""
        SELECT
            order_number, customer_id, "date"
        FROM public.orders
    """
)

load_raw_invoices = GenericTransfer(
    task_id='load_raw_invoices',
    dag=dag,
    #task_group=g_dwh_raw_load,
    source_conn_id=db_source,
    destination_conn_id=dwh,
    destination_table='raw.invoices',
    preoperator=[
        "TRUNCATE raw.invoices"
    ],
    sql="""
        SELECT
            invoice_number, order_number, "date"
        FROM public.invoices
    """
)

load_raw_order_lines = GenericTransfer(
    task_id='load_raw_order_lines',
    dag=dag,
    #task_group=g_dwh_raw_load,
    source_conn_id=db_source,
    destination_conn_id=dwh,
    destination_table='raw.order_lines',
    preoperator=[
        "TRUNCATE raw.order_lines"
    ],
    sql="""
        SELECT
            order_line_number, order_number, product_id, quantity, usd_amount
        FROM public.order_lines
    """
)

load_raw_payments = GenericTransfer(
    task_id='load_raw_payments',
    dag=dag,
    #task_group=g_dwh_raw_load,
    source_conn_id=db_source,
    destination_conn_id=dwh,
    destination_table='raw.payments',
    preoperator=[
        "TRUNCATE raw.payments"
    ],
    sql="""
        SELECT
            payment_number, invoice_number, "date"
        FROM public.payments
    """
)

load_raw_products = GenericTransfer(
    task_id='load_raw_products',
    dag=dag,
    #task_group=g_dwh_raw_load,
    source_conn_id=db_source,
    destination_conn_id=dwh,
    destination_table='raw.products',
    preoperator=[
        "TRUNCATE raw.products"
    ],
    sql="""
        SELECT
            id, name
        FROM public.products
    """
)

end_load_raw_tables = DummyOperator(
    task_id='end_load_raw_tables',
    trigger_rule='none_failed',
    dag=dag
)

start_raw_load_check = DummyOperator(
    task_id='start_raw_load_check',
    dag=dag
)

load_check_customers = SQLCheckOperator(
    task_id='load_check_customers',
    dag=dag,
    #task_group=g_dwh_raw_load_check,
    conn_id=dwh,
    database='dwh',
    sql="""SELECT count(1) FROM raw.customers"""
)

load_check_orders = SQLCheckOperator(
    task_id='load_check_orders',
    dag=dag,
    #task_group=g_dwh_raw_load_check,
    conn_id=dwh,
    database='dwh',
    sql="""SELECT count(1) FROM raw.orders"""
)

load_check_invoices = SQLCheckOperator(
    task_id='load_check_invoices',
    dag=dag,
    #task_group=g_dwh_raw_load_check,
    conn_id=dwh,
    database='dwh',
    sql="""SELECT count(1) FROM raw.invoices"""
)

load_check_order_lines = SQLCheckOperator(
    task_id='load_check_order_lines',
    dag=dag,
    #task_group=g_dwh_raw_load_check,
    conn_id=dwh,
    database='dwh',
    sql="""SELECT count(1) FROM raw.order_lines"""
)

load_check_payments = SQLCheckOperator(
    task_id='load_check_payments',
    dag=dag,
    #task_group=g_dwh_raw_load_check,
    conn_id=dwh,
    database='dwh',
    sql="""SELECT count(1) FROM raw.payments"""
)

load_check_products = SQLCheckOperator(
    task_id='load_check_products',
    dag=dag,
    #task_group=g_dwh_raw_load_check,
    conn_id=dwh,
    database='dwh',
    sql="""SELECT count(1) FROM raw.products"""
)

end_raw_load_check = DummyOperator(
    task_id='end_raw_load_check',
    trigger_rule='none_failed',
    dag=dag
)

start_dwh_schema_dwh = DummyOperator(
    task_id='start_dwh_schema',
    dag=dag
)

check_dwh_schema_dwh = BranchSQLOperator(
    task_id='check_dwh_schema_dwh',
    dag=dag,
    #task_group=g_dwh_schema,
    conn_id=dwh,
    database='dwh',
    sql="""
        SELECT EXISTS (
            SELECT FROM 
                pg_catalog.pg_namespace
            WHERE 
                nspname = 'dwh'
        )
    """,
    follow_task_ids_if_true=['end_dwh_schema_dwh'],
    follow_task_ids_if_false=['create_dwh_schema_dwh']
)

create_dwh_schema_dwh = PostgresOperator(
    task_id='create_dwh_schema_dwh',
    dag=dag,
    #task_group=g_dwh_schema,
    postgres_conn_id=dwh,
    sql='sql/create_dwh_schema_dwh.sql'
)

end_dwh_schema_dwh = DummyOperator(
    task_id='end_dwh_schema_dwh',
    trigger_rule='none_failed',
    dag=dag
)

start_dwh_tf_tables = DummyOperator(
    task_id='start_dwh_tf_tables',
    dag=dag
)

check_dwh_table_fact_oa = BranchSQLOperator(
    task_id='check_dwh_table_fact_oa',
    dag=dag,
    #task_group=g_dwh_tf_tables,
    conn_id=dwh,
    database='dwh',
    sql="""
        SELECT EXISTS (
            SELECT FROM 
                pg_tables
            WHERE 
                schemaname = 'dwh' AND 
                tablename  = 'fact_order_accumulating'
        )
    """,
    follow_task_ids_if_true=['finished_check_dwh_table_fact_oa'],
    follow_task_ids_if_false=['create_dwh_table_fact_oa']
)

create_dwh_table_fact_oa = PostgresOperator(
    task_id='create_dwh_table_fact_oa',
    dag=dag,
    #task_group=g_dwh_tf_tables,
    postgres_conn_id=dwh,
    sql='sql/create_dwh_fact_oa.sql'
)

finished_check_dwh_table_fact_oa = DummyOperator(
    task_id='finished_check_dwh_table_fact_oa',
    trigger_rule='none_failed',
    dag=dag
)

check_dwh_table_dim_date = BranchSQLOperator(
    task_id='check_dwh_table_dim_date',
    dag=dag,
    #task_group=g_dwh_tf_tables,
    conn_id=dwh,
    database='dwh',
    sql="""
        SELECT EXISTS (
            SELECT FROM 
                pg_tables
            WHERE 
                schemaname = 'dwh' AND 
                tablename  = 'dim_date'
        )
    """,
    follow_task_ids_if_true=['end_dwh_tf_tables'],
    follow_task_ids_if_false=['create_dwh_table_dim_date']
)

create_dwh_table_dim_date = PostgresOperator(
    task_id='create_dwh_table_dim_date',
    dag=dag,
    #task_group=g_dwh_tf_tables,
    postgres_conn_id=dwh,
    sql='sql/create_dwh_dim_date.sql'
)

check_dwh_table_dim_customer = BranchSQLOperator(
    task_id='check_dwh_table_dim_customer',
    dag=dag,
    #task_group=g_dwh_tf_tables,
    conn_id=dwh,
    database='dwh',
    sql="""
        SELECT EXISTS (
            SELECT FROM 
                pg_tables
            WHERE 
                schemaname = 'dwh' AND 
                tablename  = 'dim_customer'
        )
    """,
    follow_task_ids_if_true=['end_dwh_tf_tables'],
    follow_task_ids_if_false=['create_dwh_table_dim_customer']
)

create_dwh_table_dim_customer = PostgresOperator(
    task_id='create_dwh_table_dim_customer',
    dag=dag,
    #task_group=g_dwh_tf_tables,
    postgres_conn_id=dwh,
    sql='sql/create_dwh_dim_customer.sql'
)

end_dwh_tf_tables = DummyOperator(
    task_id='end_dwh_tf_tables',
    trigger_rule='none_failed',
    dag=dag
)

start_dwh_tf = DummyOperator(
    task_id='start_dwh_tf',
    dag=dag
)

load_dwh_tf_dim_date = GenericTransfer(
    task_id='load_dwh_tf_dim_date',
    dag=dag,
    #task_group=g_dwh_tf,
    source_conn_id=dwh,
    destination_conn_id=dwh,
    destination_table='dwh.dim_date',
    preoperator=[
        "TRUNCATE dwh.dim_date"
    ],
    sql=f"""
        {s.q_dim_date()}
        SELECT * FROM dim_date_base
    """
)

load_dwh_tf_fact_oa = GenericTransfer(
    task_id='load_dwh_tf_fact_oa',
    dag=dag,
    #task_group=g_dwh_tf,
    source_conn_id=dwh,
    destination_conn_id=dwh,
    destination_table='dwh.fact_order_accumulating',
    preoperator=[
        "TRUNCATE dwh.fact_order_accumulating"
    ],
    sql=f"""
        {s.q_fact_oa()}
    """
)

load_dwh_tf_dim_customer = GenericTransfer(
    task_id='load_dwh_tf_dim_customer',
    dag=dag,
    #task_group=g_dwh_tf,
    source_conn_id=dwh,
    destination_conn_id=dwh,
    destination_table='dwh.dim_customer',
    preoperator=[
        "TRUNCATE dwh.dim_customer"
    ],
    sql=f"""
        {s.q_dim_customer()}
    """
)

end_dwh_tf = DummyOperator(
    task_id='end_dwh_tf',
    trigger_rule='none_failed',
    dag=dag
)

start_dwh_tf_check = DummyOperator(
    task_id='start_dwh_tf_check',
    dag=dag
)

load_check_fact_oa = SQLCheckOperator(
    task_id='load_check_fact_oa',
    dag=dag,
    #task_group=g_dwh_tf_check,
    conn_id=dwh,
    database='dwh',
    sql="""SELECT count(1) FROM dwh.fact_order_accumulating"""
)

load_check_dim_date = SQLCheckOperator(
    task_id='load_check_dim_date',
    dag=dag,
    #task_group=g_dwh_tf_check,
    conn_id=dwh,
    database='dwh',
    sql="""SELECT count(1) FROM dwh.dim_date"""
)

load_check_dim_customer = SQLCheckOperator(
    task_id='load_check_dim_customer',
    dag=dag,
    #task_group=g_dwh_tf_check,
    conn_id=dwh,
    database='dwh',
    sql="""SELECT count(1) FROM dwh.dim_customer"""
)

end_dwh_tf_check = DummyOperator(
    task_id='end_dwh_tf_check',
    trigger_rule='none_failed',
    dag=dag
)

task_end = DummyOperator(
    task_id='task_end',
    dag=dag
)

# Tasks Depedencies
task_start >> start_dwh_schema_raw >> check_dwh_schema_raw 
check_dwh_schema_raw >> Label('raw schema: not exists') >> create_dwh_schema_raw >> end_dwh_schema_raw
check_dwh_schema_raw >> Label('raw schema: exists') >> end_dwh_schema_raw

end_dwh_schema_raw >> start_check_raw_tables
start_check_raw_tables >> [
    check_raw_table_customers,
    check_raw_table_orders,
    check_raw_table_invoices,
    check_raw_table_order_lines,
    check_raw_table_payments,
    check_raw_table_products
]
check_raw_table_customers >> Label('raw.customers table: not exists') >> create_raw_table_customers >> end_check_raw_tables
check_raw_table_orders >> Label('raw.orders table: not exists') >> create_raw_table_orders >> end_check_raw_tables
check_raw_table_invoices >> Label('raw.invoices table: not exists') >> create_raw_table_invoices >> end_check_raw_tables
check_raw_table_order_lines >> Label('raw.order_lines table: not exists') >> create_raw_table_order_lines >> end_check_raw_tables
check_raw_table_payments >> Label('raw.payments table: not exists') >> create_raw_table_payments >> end_check_raw_tables
check_raw_table_products >> Label('raw.products table: not exists') >> create_raw_table_products >> end_check_raw_tables

check_raw_table_customers >> Label('raw.customers table: exists') >> end_check_raw_tables
check_raw_table_orders >> Label('raw.orders table: exists') >> end_check_raw_tables
check_raw_table_invoices >> Label('raw.invoices table: exists') >> end_check_raw_tables
check_raw_table_order_lines >> Label('raw.order_lines table: exists') >> end_check_raw_tables
check_raw_table_payments >> Label('raw.payments table: exists') >> end_check_raw_tables
check_raw_table_products >> Label('raw.products table: exists') >> end_check_raw_tables

[
    create_raw_table_customers,
    create_raw_table_orders,
    create_raw_table_invoices,
    create_raw_table_order_lines,
    create_raw_table_payments,
    create_raw_table_products

] >> end_check_raw_tables

end_check_raw_tables >> start_load_raw_tables
start_load_raw_tables >> [
    load_raw_customers,
    load_raw_orders,
    load_raw_invoices,
    load_raw_order_lines,
    load_raw_payments,
    load_raw_products
] >> end_load_raw_tables

end_load_raw_tables >> start_raw_load_check
start_raw_load_check >> [
    load_check_customers,
    load_check_orders,
    load_check_invoices,
    load_check_order_lines,
    load_check_payments,
    load_check_products
] >> end_raw_load_check

end_raw_load_check >> start_dwh_schema_dwh >> check_dwh_schema_dwh
check_dwh_schema_dwh >> Label('dwh schema: not exist') >> create_dwh_schema_dwh >> end_dwh_schema_dwh
check_dwh_schema_dwh >> Label('dwh schema: exist') >> end_dwh_schema_dwh

end_dwh_schema_dwh >> start_dwh_tf_tables
start_dwh_tf_tables >> check_dwh_table_fact_oa
check_dwh_table_fact_oa >> Label('dwh.fact_oa table: not exist') >> create_dwh_table_fact_oa >> finished_check_dwh_table_fact_oa
check_dwh_table_fact_oa >> Label('dwh.fact_oa table: exist') >> finished_check_dwh_table_fact_oa

#[create_dwh_table_fact_oa, check_dwh_table_fact_oa] >> 
finished_check_dwh_table_fact_oa >> check_dwh_table_dim_date >> Label('dwh.dim_date table: not exist') >> create_dwh_table_dim_date >> end_dwh_tf_tables
#[create_dwh_table_fact_oa, check_dwh_table_fact_oa] >>
finished_check_dwh_table_fact_oa >> check_dwh_table_dim_customer >> Label('dwh.dim_customer table: not exist') >> create_dwh_table_dim_customer >> end_dwh_tf_tables
#[create_dwh_table_fact_oa, check_dwh_table_fact_oa] >>
finished_check_dwh_table_fact_oa >> check_dwh_table_dim_date >> Label('dwh.dim_date table: exist') >> end_dwh_tf_tables
finished_check_dwh_table_fact_oa >> check_dwh_table_dim_customer >> Label('dwh.dim_customer table: exist') >> end_dwh_tf_tables

end_dwh_tf_tables >> start_dwh_tf
start_dwh_tf >> load_dwh_tf_dim_date
load_dwh_tf_dim_date >> [load_dwh_tf_fact_oa, load_dwh_tf_dim_customer]
[load_dwh_tf_fact_oa, load_dwh_tf_dim_customer]>> end_dwh_tf

end_dwh_tf >> start_dwh_tf_check
start_dwh_tf_check >> [
    load_check_dim_customer,
    load_check_dim_date,
    load_check_fact_oa
]
[
    load_check_dim_customer,
    load_check_dim_date,
    load_check_fact_oa
] >> end_dwh_tf_check

end_dwh_tf_check >> task_end
