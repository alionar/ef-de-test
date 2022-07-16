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
  "invoice_to_payment_lag_days" int
);
