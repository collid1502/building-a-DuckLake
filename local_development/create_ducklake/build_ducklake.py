# imports
import duckdb as db
import psycopg2
import pandas as pd



def build():
    """
    Builds a simple duck-lake with medallion setup
    """
    # POSTGRES SETUP
    # Connection parameters (match your docker-compose.yml) NOTE - This would never actually be saved in code
    # outside of mock examples
    # Create the database in a context-managed connection
    POSTGRES_CONN = {
        "host": "postgres",
        "port": 5432,
        "dbname": "mydatabase",  # Connect to a DB that is NOT the one you are dropping
        "user": "duckLakeAdmin",
        "password": "duckLakePW" # DO NOT HARD CODE PASSWORD IN ACTUAL SOLUTION
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
    con = db.connect(database=':memory:')

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

    ## Build Bronze Silver & Gold Layers
    build_medallion = """
    CREATE SCHEMA IF NOT EXISTS retail_bronze ;
    CREATE SCHEMA IF NOT EXISTS retail_silver ;
    CREATE SCHEMA IF NOT EXISTS retail_gold ;
    """
    con.execute(build_medallion)

    # detach from ducklake
    con.execute("""
    USE memory ;
    DETACH retail_ducklake ;         
    """)
    # close connection to DuckDB
    con.close()


# Execute Build
if __name__ == '__main__':
    print("Building Ducklake ...")
    build() 
    print("Ducklake Build Complete")
