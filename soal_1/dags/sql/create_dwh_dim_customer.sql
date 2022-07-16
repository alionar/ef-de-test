CREATE TABLE IF NOT EXISTS dwh."dim_customer" (
  "id" int,
  "name" varchar
);
--ALTER TABLE dwh."dim_customer" ADD FOREIGN KEY ("id") REFERENCES dwh."fact_order_accumulating" ("customer_id");