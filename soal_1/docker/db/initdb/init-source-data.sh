#!/bin/bash
set -e

query=$(</docker-entrypoint-initdb.d/soal-1.sql)

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	$query
EOSQL
