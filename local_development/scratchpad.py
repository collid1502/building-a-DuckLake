# =============================================
# This script will use SQL Frame to mock the
# PySpark API, but backed by DuckDB & DuckLake
# =============================================

# imports
import duckdb
import pandas as pd
from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb import functions as F


# Start by connecting to an "In-Memory" DuckDB connection, and connecting to DuckLake
# NOTE - build_ducklake.py must have been executed already 
ducklake_conn = duckdb.connect(database=":memory:")
# Outside Development, do NOT store passwords in code 
ducklake_conn.execute("""
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=postgres user=duckLakeAdmin password=duckLakePW' AS retail_ducklake 
(DATA_PATH 'local_development/data/');
                      
USE retail_ducklake ;
""")

# now, with that setup, we can mock the PySpark API, but backed with teh DuckDB in-memory session
# This allows us to keep the syntax without actually needing Spark under the hood 
spark = DuckDBSession(conn=ducklake_conn)

# create mock dataframe 
df_employee = spark.createDataFrame(
    [
        {"id": 1, "fname": "Jack", "lname": "Shephard", "age": 37, "store_id": 1},
        {"id": 2, "fname": "John", "lname": "Locke", "age": 65, "store_id": 2},
        {"id": 3, "fname": "Kate", "lname": "Austen", "age": 37, "store_id": 3},
        {"id": 4, "fname": "Claire", "lname": "Littleton", "age": 27, "store_id": 1},
        {"id": 5, "fname": "Hugo", "lname": "Reyes", "age": 29, "store_id": 3},
    ]
)

# create new column - assign each employee a store name based on store_id 
df_employee_with_store = (
    df_employee.withColumn(
        "store_name",
        F.when(F.col("store_id") == 1, 'London')
        .when(F.col("store_id") == 2, 'Manchester')
        .when(F.col("store_id") == 3, 'Cardiff')
        .otherwise(F.lit(None))
    )
)

# Write data to Bronze Layer in the Retail Ducklake
df_employee_with_store.write.mode("overwrite").saveAsTable("retail_bronze.store_employees")

# let's read that table back in!
bronze_employees = spark.read.table("retail_bronze.store_employees")
bronze_employees.show(truncate=False)


# can we delete a record from the table? 
# well, not with PySpark directly, but with the underlying table class from SqlFrame
remove_manchester = bronze_employees.delete(
    where=bronze_employees["store_id"] == 2
)
# execute the delete
remove_manchester.execute()

# now show the table again ...
bronze_employees.show(truncate=False)

# what if we read in the data to another dataframe?
new_bronze_employees = spark.read.table("retail_bronze.store_employees")
new_bronze_employees.show(truncate=False)

# we can see that the Manchester Store has indeed been deleted!
# so, all this has happened on our ducklake

# Close Session
spark._conn.close()

# end 