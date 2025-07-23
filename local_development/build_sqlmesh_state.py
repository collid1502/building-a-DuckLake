# imports
import psycopg2
import os


def build():
    """
    Builds a state database in Postgres Instance for sqlmesh to use
    """
    # POSTGRES SETUP
    # Connection parameters (match your docker-compose.yml) NOTE - This would never actually be saved in code
    # access env variables
    pg_host = os.getenv('PG_HOST')
    pg_user = os.getenv('PG_USER')
    pg_password = os.getenv('PG_PASSWORD')
    # outside of mock examples
    # Create the database in a context-managed connection
    POSTGRES_CONN = {
        "host": pg_host,
        "port": 5432,
        "dbname": "mydatabase",  # Connect to a DB that is NOT the one you are dropping
        "user": pg_user,
        "password": pg_password # DO NOT HARD CODE PASSWORD IN ACTUAL SOLUTION
    }
    # Manually manage the connection to control autocommit properly
    conn = psycopg2.connect(**POSTGRES_CONN)
    try:
        conn.autocommit = True  # Required for CREATE/DROP DATABASE

        with conn.cursor() as cur:
            print("Clear existing database ...")
            cur.execute("DROP DATABASE IF EXISTS sqlmesh_state;")
            print("Database dropped")

            print("Create new database ...")
            cur.execute("CREATE DATABASE sqlmesh_state;")
            print("New sqlmesh state database created")

    finally:
        conn.close()


# Execute Build
if __name__ == '__main__':
    print("Building Sqlmesh State database ...")
    build() 
    print("State Database Build Complete")