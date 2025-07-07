# imports
import psycopg2


# Connection parameters (match your docker-compose.yml)
conn = psycopg2.connect(
    host="postgres",         # Use service name from docker-compose
    port=5432,
    dbname="mydatabase",
    user="duckLakeAdmin",
    password="duckLakePW"
)

# Create a cursor
cur = conn.cursor()

# Run a simple query
cur.execute("SELECT version();")
version = cur.fetchone()
print(f"PostgreSQL version: {version[0]}")

# Clean up
cur.close()
conn.close()

exit()