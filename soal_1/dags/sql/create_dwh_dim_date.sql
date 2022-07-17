CREATE TABLE IF NOT EXISTS dwh."dim_date" (
  "id" int,
  "date" date,
  "month" int,
  "quater_of_year" int,
  "year" int,
  "is_weekend" boolean,
  CONSTRAINT date_pk PRIMARY KEY (id)
);