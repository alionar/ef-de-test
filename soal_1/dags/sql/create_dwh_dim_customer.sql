CREATE TABLE IF NOT EXISTS dwh."dim_customer" (
  "id" int,
  "name" varchar,
  CONSTRAINT customer_pk PRIMARY KEY (id)
);