CREATE TABLE IF NOT EXISTS raw."invoices" (
  "invoice_number" varchar PRIMARY KEY,
  "order_number" varchar,
  "date" date
);