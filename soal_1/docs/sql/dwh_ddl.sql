-- DWH Schema & Table creation
CREATE SCHEMA dwh AUTHORIZATION redshift;

CREATE TABLE IF NOT EXISTS dwh."dim_customer" (
  "id" int,
  "name" varchar,
  CONSTRAINT customer_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dwh."dim_date" (
  "id" int,
  "date" date,
  "month" int,
  "quater_of_year" int,
  "year" int,
  "is_weekend" boolean,
  CONSTRAINT date_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dwh."fact_order_accumulating" (
  "order_date_id" int,
  "invoice_date_id" int,
  "payment_date_id" int,
  "customer_id" int,
  "order_number" varchar,
  "invoice_number" varchar,
  "payment_number" varchar,
  "total_order_quantity" int,
  "total_order_usd_amount" decimal,
  "order_to_invoice_lag_days" int,
  "invoice_to_payment_lag_days" int,
  CONSTRAINT fk_customer FOREIGN KEY(customer_id) REFERENCES dwh.dim_customer(id),
  CONSTRAINT fk_order_date FOREIGN KEY(order_date_id) REFERENCES dwh.dim_date(id),
  CONSTRAINT fk_invoice_date FOREIGN KEY(invoice_date_id) REFERENCES dwh.dim_date(id),
  CONSTRAINT fk_payment_date FOREIGN KEY(payment_date_id) REFERENCES dwh.dim_date(id)
);

-- ETL Query
-- dim.customer
TRUNCATE dwh.dim_customer CASCADE;
INSERT INTO dwh.dim_customer (id, name) (SELECT id, name FROM raw.customers);

-- dim.date
TRUNCATE dwh.dim_date CASCADE;
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) (
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
  SELECT * FROM dim_date_base
);

-- fact_order_accumulation
TRUNCATE dwh.fact_order_accumulating CASCADE;
INSERT INTO dwh.fact_order_accumulating (order_date_id, invoice_date_id, payment_date_id, customer_id, order_number, invoice_number, payment_number, total_order_quantity, total_order_usd_amount, order_to_invoice_lag_days, invoice_to_payment_lag_days) (
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
);

-- Table data
-- dim.customer
INSERT INTO dwh.dim_customer (id, "name") VALUES(3923, 'Ani');
INSERT INTO dwh.dim_customer (id, "name") VALUES(3924, 'Budi');
INSERT INTO dwh.dim_customer (id, "name") VALUES(3925, 'Caca');

-- dim.date
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(1, '2020-02-25', 2, 1, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(2, '2020-03-02', 3, 1, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(3, '2020-03-05', 3, 1, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(4, '2020-03-08', 3, 1, 2020, true);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(5, '2020-03-09', 3, 1, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(6, '2020-06-01', 6, 2, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(7, '2020-07-13', 7, 3, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(8, '2020-07-19', 7, 3, 2020, true);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(9, '2020-08-16', 8, 3, 2020, true);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(10, '2020-08-22', 8, 3, 2020, true);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(11, '2020-09-10', 9, 3, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(12, '2020-09-17', 9, 3, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(13, '2020-10-05', 10, 4, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(14, '2020-10-09', 10, 4, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(15, '2020-10-13', 10, 4, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(16, '2020-10-23', 10, 4, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(17, '2020-11-16', 11, 4, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(18, '2020-12-04', 12, 4, 2020, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(19, '2020-12-13', 12, 4, 2020, true);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(20, '2021-01-11', 1, 1, 2021, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(21, '2021-01-23', 1, 1, 2021, true);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(22, '2021-02-01', 2, 1, 2021, false);
INSERT INTO dwh.dim_date (id, "date", "month", quater_of_year, "year", is_weekend) VALUES(23, '2021-02-02', 2, 1, 2021, false);

-- fact_order_accumulation
INSERT INTO dwh.fact_order_accumulating (order_date_id, invoice_date_id, payment_date_id, customer_id, order_number, invoice_number, payment_number, total_order_quantity, total_order_usd_amount, order_to_invoice_lag_days, invoice_to_payment_lag_days) VALUES(2, 3, 4, 3923, 'ORD-223', 'INV-525', 'PYM-777', 18, 213.50, 3, 3);
INSERT INTO dwh.fact_order_accumulating (order_date_id, invoice_date_id, payment_date_id, customer_id, order_number, invoice_number, payment_number, total_order_quantity, total_order_usd_amount, order_to_invoice_lag_days, invoice_to_payment_lag_days) VALUES(9, 10, 12, 3924, 'ORD-142', 'INV-642', 'PYM-817', 9, 10.80, 6, 5);
INSERT INTO dwh.fact_order_accumulating (order_date_id, invoice_date_id, payment_date_id, customer_id, order_number, invoice_number, payment_number, total_order_quantity, total_order_usd_amount, order_to_invoice_lag_days, invoice_to_payment_lag_days) VALUES(11, 12, 13, 3923, 'ORD-206', 'INV-557', 'PYM-792', 9, 10.80, 7, 12);
INSERT INTO dwh.fact_order_accumulating (order_date_id, invoice_date_id, payment_date_id, customer_id, order_number, invoice_number, payment_number, total_order_quantity, total_order_usd_amount, order_to_invoice_lag_days, invoice_to_payment_lag_days) VALUES(14, 15, 16, 3924, 'ORD-201', 'INV-581', 'PYM-802', 7, 73.50, 4, 10);
INSERT INTO dwh.fact_order_accumulating (order_date_id, invoice_date_id, payment_date_id, customer_id, order_number, invoice_number, payment_number, total_order_quantity, total_order_usd_amount, order_to_invoice_lag_days, invoice_to_payment_lag_days) VALUES(18, 19, 20, 3924, 'ORD-134', 'INV-587', 'PYM-761', 3, 3.60, 9, 2);
INSERT INTO dwh.fact_order_accumulating (order_date_id, invoice_date_id, payment_date_id, customer_id, order_number, invoice_number, payment_number, total_order_quantity, total_order_usd_amount, order_to_invoice_lag_days, invoice_to_payment_lag_days) VALUES(21, 22, 23, 3923, 'ORD-205', 'INV-647', 'PYM-803', 7, 123.20, 22, 1);
INSERT INTO dwh.fact_order_accumulating (order_date_id, invoice_date_id, payment_date_id, customer_id, order_number, invoice_number, payment_number, total_order_quantity, total_order_usd_amount, order_to_invoice_lag_days, invoice_to_payment_lag_days) VALUES(7, 8, NULL, 3925, 'ORD-181', 'INV-549', NULL, 9, 94.50, 6, NULL);
INSERT INTO dwh.fact_order_accumulating (order_date_id, invoice_date_id, payment_date_id, customer_id, order_number, invoice_number, payment_number, total_order_quantity, total_order_usd_amount, order_to_invoice_lag_days, invoice_to_payment_lag_days) VALUES(1, 5, NULL, 3924, 'ORD-170', 'INV-554', NULL, 15, 100.00, 16, NULL);
INSERT INTO dwh.fact_order_accumulating (order_date_id, invoice_date_id, payment_date_id, customer_id, order_number, invoice_number, payment_number, total_order_quantity, total_order_usd_amount, order_to_invoice_lag_days, invoice_to_payment_lag_days) VALUES(17, NULL, NULL, 3924, 'ORD-240', NULL, NULL, 4, 70.40, NULL, NULL);
INSERT INTO dwh.fact_order_accumulating (order_date_id, invoice_date_id, payment_date_id, customer_id, order_number, invoice_number, payment_number, total_order_quantity, total_order_usd_amount, order_to_invoice_lag_days, invoice_to_payment_lag_days) VALUES(6, NULL, NULL, 3925, 'ORD-225', NULL, NULL, 4, 42.00, NULL, NULL);
