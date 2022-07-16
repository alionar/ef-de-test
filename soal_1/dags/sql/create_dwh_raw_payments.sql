CREATE TABLE IF NOT EXISTS raw."payments" (
  "payment_number" varchar PRIMARY KEY,
  "invoice_number" varchar,
  "date" date
);