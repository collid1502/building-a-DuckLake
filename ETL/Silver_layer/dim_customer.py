# imports 
import os
import datetime
import duckdb
import argparse


# main function to process the transformation of customer data from Bronze to Silver
def main(extract_date: str):
    """
    Process the ETL stage of loading raw store data to bronze layer of DuckLake.
    Automatically manages connection context to ensure clean closure.
    """
    pg_host = os.getenv('PG_HOST')
    pg_user = os.getenv('PG_USER')
    pg_password = os.getenv('PG_PASSWORD')

    # create in-memory duckdb session
    with duckdb.connect(database=":memory:") as con:
        con.execute(f"""
        ATTACH 'ducklake:postgres:dbname=ducklake_catalog host={pg_host} user={pg_user} password={pg_password}'
        AS retail_ducklake ;

        USE retail_ducklake ;
        """)

        # create table if does not already exist
        create_tbl_qry = """
        CREATE TABLE IF NOT EXISTS retail_silver.dim_customer (
            customer_id INT,
            customer_joined TIMESTAMP,
            name TEXT,
            dob DATE,
            profession TEXT,
            email TEXT,
            rewards_programme_member BOOLEAN,
            validFrom TIMESTAMP,
            validTo TIMESTAMP,
            isCurrent BOOLEAN
        ) ;
        """
        con.execute(create_tbl_qry)

        # Now, we need to read the latest data from the bronze layer for customer_src_raw 
        # & process updates into the dim_customer
        new_cust_data = f"""
        CREATE OR REPLACE TEMP TABLE src_cust AS
        SELECT
            customerId AS customer_id,
            customerJoined AS customer_joined,
            TRIM(
                CONCAT(
                    COALESCE(firstName, ''), 
                    CASE WHEN firstName IS NOT NULL AND lastName IS NOT NULL THEN ' ' ELSE '' END,
                    COALESCE(lastName, '')
                )
            ) AS name,
            dob,
            profession,
            emailAddress AS email,
            rewardsMember AS rewards_programme_member
        FROM retail_bronze.customer_src_raw
        WHERE
        extract_date = '{extract_date}'
        ;
        """
        con.execute(new_cust_data)

        # current records from target table
        update_records = f"""
        CREATE OR REPLACE TEMP TABLE new_cust_stg AS
        SELECT 
            src.*,
            CASE
                WHEN trgt.customer_id IS NULL THEN TRUE
                WHEN trgt.customer_id IS NOT NULL
                    AND HASH(
                        src.name,
                        src.dob,
                        src.profession,
                        src.email,
                        src.rewards_programme_member
                    ) <> HASH(
                        trgt.name,
                        trgt.dob,
                        trgt.profession,
                        trgt.email,
                        trgt.rewards_programme_member
                    )
                THEN TRUE
                ELSE FALSE
            END AS to_be_updated
        FROM src_cust AS src
        LEFT JOIN 
            (SELECT * FROM retail_silver.dim_customer WHERE isCurrent = TRUE) AS trgt
        ON src.customer_id = trgt.customer_id 
        ;
        """
        con.execute(update_records)

        # Process the Merge-Into to update existing records
        extract_dt = datetime.datetime.strptime(extract_date, "%Y-%m-%d").date()
        now_dt = datetime.datetime.now()
        # replace just the date part
        fixed_dt = now_dt.replace(year=extract_dt.year, month=extract_dt.month, day=extract_dt.day)

        update_dim_cust = f"""
        UPDATE retail_silver.dim_customer
        SET
            validTo = '{fixed_dt}',
            isCurrent = FALSE

        WHERE isCurrent = TRUE
        AND customer_id IN (SELECT DISTINCT customer_id FROM new_cust_stg WHERE to_be_updated = TRUE)
        ;
        """
        con.execute(update_dim_cust)

        # insert new records
        insert_dim_cust = f"""
        INSERT INTO retail_silver.dim_customer
        SELECT
            customer_id, customer_joined, name, dob, profession, email, rewards_programme_member,
            '{fixed_dt}' AS validFrom,
            NULL AS validTo,
            TRUE AS isCurrent
        FROM new_cust_stg
        WHERE to_be_updated IS TRUE
        ;
        """
        con.execute(insert_dim_cust)

        # end of SCD2 Customer Dim Update
        con.execute("USE memory ;")
        con.execute("DETACH retail_ducklake ;")

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL process for customer dim silver layer")
    parser.add_argument("--extract-date", type=str, help="Extract date in YYYY-MM-DD format")
    args = parser.parse_args()
    extract_date = args.extract_date
    print("Running ETL process for SILVER -- dim_customer ...")
    main(extract_date=extract_date) # process ETL
    print("Data Load to `retail_silver.dim_customer` completed")
