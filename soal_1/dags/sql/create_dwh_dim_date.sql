CREATE TABLE IF NOT EXISTS dwh."dim_date" (
  "id" int,
  "date" date,
  "month" int,
  "quater_of_year" int,
  "year" int,
  "is_weekend" boolean
);

--ALTER TABLE dwh."dim_date" ADD FOREIGN KEY ("id") REFERENCES dwh."fact_order_accumulating" ("order_date_id");
--ALTER TABLE dwh."dim_date" ADD FOREIGN KEY ("id") REFERENCES dwh."fact_order_accumulating" ("invoice_date_id");
--ALTER TABLE dwh."dim_date" ADD FOREIGN KEY ("id") REFERENCES dwh."fact_order_accumulating" ("payment_date_id");