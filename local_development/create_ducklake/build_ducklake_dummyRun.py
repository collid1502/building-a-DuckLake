# imports
import duckdb as db
import psycopg2
import pandas as pd


# POSTGRES SETUP
# Connection parameters (match your docker-compose.yml) NOTE - This would never actually be saved in code
# outside of mock examples
# Create the database in a context-managed connection
POSTGRES_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "mydatabase",  # Connect to a DB that is NOT the one you are dropping
    "user": "duckLakeAdmin",
    "password": "duckLakePW"
}
# Manually manage the connection to control autocommit properly
conn = psycopg2.connect(**POSTGRES_CONN)
try:
    conn.autocommit = True  # Required for CREATE/DROP DATABASE

    with conn.cursor() as cur:
        print("Clear existing database ...")
        cur.execute("DROP DATABASE IF EXISTS ducklake_catalog;")
        print("Database dropped")

        print("Create new database ...")
        cur.execute("CREATE DATABASE ducklake_catalog;")
        print("New ducklake catalog database created")

finally:
    conn.close()

# =============================================================================================

# DUCKLAKE SETUP
# execute duckdb commands to create a ducklake, which uses Postgres as the Catalog Database
# Create an in-memory DuckDB connection
con = db.connect()  # or duckdb.connect(database=':memory:')

# install ducklkae extension
con.execute("INSTALL ducklake;")
con.execute("INSTALL postgres;") # as we will be using postgres as the catalog backing

# create the ducklake, providing details to our postgres host
# NOTE - this is a demo example, typically, secrets would be used and the connection details NOT stored in code
create_ducklake = """
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=postgres user=duckLakeAdmin password=duckLakePW' AS retail_ducklake
(DATA_PATH 'local_development/data/') ;

USE retail_ducklake ;
"""
con.execute(create_ducklake)

## Simple Test
simple_test = """
CREATE SCHEMA IF NOT EXISTS retail_bronze ;

CREATE TABLE retail_bronze.nl_train_stations AS
    FROM 'https://blobs.duckdb.org/nl_stations.csv';
"""
con.execute(simple_test)

test_df = con.execute("SELECT * FROM retail_bronze.nl_train_stations LIMIT 5").fetchdf()
print(test_df)

# perform a test update 
con.execute("UPDATE retail_bronze.nl_train_stations SET name_long='Johan Cruijff ArenA' WHERE code = 'ASB';")
# check it worked
test_df = con.execute("SELECT name_long FROM retail_bronze.nl_train_stations WHERE code = 'ASB';").fetchdf()
print(test_df)

# perfect, all works as expected!
# drop the test table
con.execute("DROP TABLE retail_bronze.nl_train_stations ;")

# Query the DuckDB catalog for tables
tables_df = con.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema = 'retail_bronze'
""").fetchdf()
print(tables_df)

query = """
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name = 'retail_bronze'
"""
result = con.execute(query).fetchdf()
print(result)


# detach from ducklake
con.execute("""
USE memory ;
DETACH retail_ducklake ;         
""")

# close connection to DuckDB
con.close()

# exit
exit()