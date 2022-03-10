#!/bin/bash

set -x
env | grep '^PG'

# If you want to run this script for your own postgresql (run with
# docker-compose) it will look like this:
# PGHOST=127.0.0.1 PGUSER=root PGPASSWORD=password PGDATABASE=postgres \
PGUSER="${PGUSER:-postgres}"
export PGUSER
PGPORT="${PGPORT:-5432}"
export PGPORT
PGHOST="${PGHOST:-localhost}"

i=10
while [ $i -gt -1 ]; do
	if pg_isready -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" ; then
        echo "PostgreSQL service is ready"
		break
	fi

    echo "PostgreSQL service not ready, wait 10 more sec, attempts left: $i"
    sleep 10
    ((i--))
done;

createdb dbttest

psql -c "CREATE ROLE root WITH PASSWORD 'password';"
psql -c "ALTER ROLE root WITH LOGIN;"
psql -c "GRANT CREATE, CONNECT ON DATABASE dbttest TO root WITH GRANT OPTION;"

psql -c "CREATE ROLE noaccess WITH PASSWORD 'password' NOSUPERUSER;"
psql -c "ALTER ROLE noaccess WITH LOGIN;"
psql -c "GRANT CONNECT ON DATABASE dbttest TO noaccess;"

# Load data from csv files to database
dbt --log-format json seed --project-dir ./tests/jaffle_shop --profiles-dir ./tests/jaffle_shop/profiles --profile jaffle_shop

set +x
