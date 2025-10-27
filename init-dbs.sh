#!/bin/bash
set -e

# Create additional databases
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE prefect;
    GRANT ALL PRIVILEGES ON DATABASE prefect TO "$POSTGRES_USER";
EOSQL
