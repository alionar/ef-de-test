CREATE TABLE IF NOT EXISTS raw."order_lines" (
  "order_line_number" varchar PRIMARY KEY,
  "order_number" varchar,
  "product_id" int,
  "quantity" int,
  "usd_amount" decimal
);