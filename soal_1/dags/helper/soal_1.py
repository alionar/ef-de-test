

def db_airflow_conn_id(db):
    if db == 'source':
        return 'db_source_postgres'
    elif db == 'dwh':
        return 'dwh_redshift'
    else:
        raise Exception('airflow db conn id: invalid')


def q_dim_date():
    q = """
        with all_date as (
            select 
                distinct "date" 
            from (
            select "date" from raw.invoices i
            union all
            select "date" from raw.orders o 
            union all
            select "date" from raw.payments p
            ) d
            order by 1 asc
        )
        , date_rn as (
            select
                "date",
                ROW_NUMBER () OVER (ORDER BY "date") as id
            from all_date
        )
        , dim_date_base as (
        select
            id, "date",
            extract(month from "date") as "month",
            extract(quarter from "date") as quarter_of_year,
            extract(year from "date") as quarter_of_year,
            case
                when extract(isodow FROM "date") = 6 or extract(isodow FROM "date") = 7 then TRUE
                else FALSE 
            end as is_weekend
        from date_rn
        )
    """
    return q


def q_fact_oa():
    q = f"""{q_dim_date()}
        , order_base as (
        select
            c.id as customer_id,
            o.order_number,
            o."date" as order_date,
            i.invoice_number,
            i."date" as invoice_date,
            p.payment_number,
            p."date" as payment_date,
            ol.order_line_number,
            ol.product_id,
            ol.quantity,
            ol.usd_amount,
            abs(DATE_PART('day', i."date") - DATE_PART('day', o."date")) as order_to_invoice_lag_days,
            abs(DATE_PART('day', p."date") - DATE_PART('day', i."date")) as invoice_to_payment_lag_days
        from raw.customers c 
        left join raw.orders o on c.id = o.customer_id 
        left join raw.invoices i on i.order_number = o.order_number 
        left join raw.payments p on i.invoice_number = p.invoice_number
        left join raw.order_lines ol on ol.order_number = o.order_number
        )
        , fact_oa as (
        select
            order_date,
            invoice_date,
            payment_date,
            customer_id,
            order_number,
            invoice_number,
            payment_number,
            sum(quantity) as total_order_quantity,
            sum(usd_amount) as total_order_usd_amount,
            order_to_invoice_lag_days,
            invoice_to_payment_lag_days
        from order_base
        group by 1,2,3,4,5,6,7,10,11
        )
        , odi as (
        select 
            ob.order_date,
            dd.id as order_date_id
        from order_base ob
        left join dim_date_base dd on ob.order_date = dd."date"
        group by 1,2
        )
        , idi as (
        select 
            ob.invoice_date,
            dd.id as invoice_date_id
        from order_base ob
        left join dim_date_base dd on ob.invoice_date = dd."date"
        group by 1,2
        )
        , pdi as (
        select 
            ob.payment_date,
            dd.id as payment_date_id
        from order_base ob
        left join dim_date_base dd on ob.payment_date = dd."date"
        group by 1,2
        )
        select
            odi.order_date_id,
            idi.invoice_date_id,
            pdi.payment_date_id,
            ob.customer_id,
            ob.order_number,
            ob.invoice_number,
            ob.payment_number,
            ob.total_order_quantity,
            ob.total_order_usd_amount,
            ob.order_to_invoice_lag_days,
            ob.invoice_to_payment_lag_days
        from fact_oa ob
        left join odi on odi.order_date = ob.order_date
        left join idi on idi.invoice_date = ob.invoice_date
        left join pdi on pdi.payment_date = ob.payment_date
    """
    return q


def q_dim_customer():
    q = """
        SELECT 
            id, name
        FROM raw.customers
    """
    return q
