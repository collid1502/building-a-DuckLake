# imports 
import os
import datetime
import duckdb
import argparse
from sqlframe.duckdb import DuckDBSession, functions as F, types as T


# main function to process the transformation of customer data from Bronze to Silver
def main(extract_date: str):
    """
    Process the ETL stage of loading raw store data to bronze layer of DuckLake.
    Automatically manages connection context to ensure clean closure.
    """
    pg_host = os.getenv('PG_HOST')
    pg_user = os.getenv('PG_USER')
    pg_password = os.getenv('PG_PASSWORD')

    conn = duckdb.connect(database=":memory:")
    # Use the underlying connection to attach to DuckLake
    conn.execute(f"""
    ATTACH 'ducklake:postgres:dbname=ducklake_catalog host={pg_host} user={pg_user} password={pg_password}'
    AS retail_ducklake ;
    """)
    conn.execute("USE retail_ducklake ;") # type: ignore

    # create spark session via DuckDB
    spark = DuckDBSession(conn=conn)

    # Define the schema for dim_customer
    dim_customer_schema = T.StructType([
        T.StructField("customer_id",              T.IntegerType(),   nullable=True),
        T.StructField("customer_joined",          T.TimestampType(), nullable=True),
        T.StructField("name",                     T.StringType(),    nullable=True),
        T.StructField("dob",                      T.DateType(),      nullable=True),
        T.StructField("profession",               T.StringType(),    nullable=True),
        T.StructField("email",                    T.StringType(),    nullable=True),
        T.StructField("rewards_programme_member", T.BooleanType(),   nullable=True),
        T.StructField("validFrom",                T.TimestampType(), nullable=True),
        T.StructField("validTo",                  T.TimestampType(), nullable=True),
        T.StructField("isCurrent",                T.BooleanType(),   nullable=True),
    ])
    # Create an empty DataFrame with that schema
    empty_dim_customer_df = spark.createDataFrame([], dim_customer_schema)
    # Create the table if it does not exist (and do nothing if it already exists)
    empty_dim_customer_df.write.mode("ignore").saveAsTable("retail_silver.dim_customer")

    # Now, we need to read the latest data from the bronze layer for customer_src_raw
    src_cust_df = (
        spark.table("retail_bronze.customer_src_raw")
        .filter(F.col("extract_date") == extract_date)
        .select(
            F.col("customerId").alias("customer_id"),
            F.col("customerJoined").alias("customer_joined"),
            F.concat(F.col("firstName"), F.lit(" "), F.col("lastName")).alias("name"),
            F.col("dob"),
            F.col("profession"),
            F.col("emailAddress").alias("email"),
            F.col("rewardsMember").alias("rewards_programme_member")
        )
        .withColumn(
            "check_hash",
            F.hash(
                F.col("name"), F.col("dob"), F.col("profession"),
                F.col("email"), F.col("rewards_programme_member"),
            )
        )
    )

    # Load target (current records only)
    trgt_df = (
        spark.table("retail_silver.dim_customer")
        .filter(F.col("isCurrent") == True)
        .withColumn(
            "current_hash",
            F.hash(
                F.col("name"), F.col("dob"), F.col("profession"),
                F.col("email"), F.col("rewards_programme_member"),
            )
        )
    )

    # create update flag based on new raw source data (bronze) being compared to current dim table (silver)
    updated_df = (
        src_cust_df
        .join(
            trgt_df,
            on=["customer_id"],
            how="left"
        )
        .select(
            src_cust_df["*"],
            trgt_df["current_hash"]
        )
        .withColumn(
            "change_type",
            F.when(F.col("current_hash").isNull(), F.lit("NEW"))
            .when(F.col("check_hash") != F.col("current_hash"), F.lit("CHANGED"))
            .otherwise(F.lit("UNCHANGED"))
        )
        .filter(F.col("change_type").isin(["NEW", "CHANGED"]))
    )

    # Temp Save Table to schema - will be wiped after
    updated_df.write.mode("overwrite").saveAsTable("retail_silver.TEMP_UPDATED_CUSTOMERS")

    # Process the Merge-Into to update existing records
    extract_dt = datetime.datetime.strptime(extract_date, "%Y-%m-%d").date()
    now_dt = datetime.datetime.now()
    # replace just the date part
    fixed_dt = now_dt.replace(year=extract_dt.year, month=extract_dt.month, day=extract_dt.day)

    # Write Merge-Into Statement. Not yet supported in SQLFrame PySpark, but we can directly use underlying DuckDB
    Merge_Stmt = f"""
    MERGE INTO retail_silver.dim_customer AS trgt
    USING 
        (
        SELECT
            *,
            '{fixed_dt}' AS validFrom,
            NULL AS validTo,
            TRUE AS isCurrent
        FROM retail_silver.TEMP_UPDATED_CUSTOMERS 
    ) AS src
    ON (src.customer_id = trgt.customer_id)
    WHEN MATCHED AND trgt.isCurrent = TRUE THEN UPDATE
    SET
        validTo = '{fixed_dt}',
        isCurrent = FALSE
    WHEN NOT MATCHED THEN
    INSERT (
        customer_id, customer_joined, name, dob, profession, email, 
        rewards_programme_member, validFrom, validTo, isCurrent
    )
    VALUES (
        src.customer_id, src.customer_joined, src.name, src.dob, src.profession, src.email,
        src.rewards_programme_member, src.validFrom, src.validTo, src.isCurrent
    ) ;
    """
    spark._conn.execute(Merge_Stmt) # type: ignore
    
    # End of SCD2 Customer Dim Update - drop TEMP table from earlier
    spark._conn.execute("DROP TABLE retail_silver.TEMP_UPDATED_CUSTOMERS") # type: ignore
    spark._conn.execute("USE memory ;") # type: ignore
    spark._conn.execute("DETACH retail_ducklake ;") # type: ignore
    # close connection
    spark.stop()


    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL process for customer dim silver layer")
    parser.add_argument("--extract-date", type=str, help="Extract date in YYYY-MM-DD format")
    args = parser.parse_args()
    extract_date = args.extract_date
    print("Running ETL process for SILVER -- dim_customer ...")
    main(extract_date=extract_date) # process ETL
    print("Data Load to `retail_silver.dim_customer` completed")
