CREATE TABLE IF NOT EXISTS raw."orders" (
  "order_number" varchar PRIMARY KEY,
  "customer_id" int,
  "date" date
);