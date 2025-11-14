# imports 
import os
import argparse
import duckdb
from sqlframe.duckdb import DuckDBSession, functions as F, types as T
from src.utilities.ducklake_helpers import connectToDucklake, ducklakeMerge


# main function to process the transformation of customer data from Bronze to Silver
def main(extract_date: str):
    """
    Process the ETL stage of loading converting Bronze Data to Silver Data: `dim_customer`
    """
    # specify ducklake connection creds for ETL workloads
    ducklake_creds = {
        "catalog": "ducklake_catalog",
        "pg_host": os.getenv('PG_HOST'),
        "pg_user": os.getenv('PG_USER'),
        "pg_password": os.getenv('PG_PASSWORD')
    }

    print("connecting to ducklake ...")
    conn = connectToDucklake("retail_ducklake", ducklake_creds) # provide a connection object for Ducklake

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

    # Now, we need to read the latest data from the bronze layer for customers_src_raw
    src_cust_df = (
        spark.table("retail_bronze.customers_src_raw")
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
        .drop("change_type", "check_hash", "current_hash")
    )

    # process the Merge Into using custom utility function
    ducklakeMerge(
        session=spark,
        targetSchema="retail_silver",
        targetTbl="dim_customer",
        srcDF=updated_df,
        extract_date=extract_date,
        merge_on_id="customer_id"
    )
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
