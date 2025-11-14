# imports
import duckdb
from sqlframe.duckdb import DuckDBSession, DuckDBDataFrame
import re
import datetime


def _safe_identifier(name: str) -> str:
    # Allow only letters, numbers, and underscores
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise ValueError(f"Invalid identifier: {name}")
    return name


def connectToDucklake(ducklake: str, connCredentials: dict):
    """ 
    A simple helper function which uses an in-memory duckdb database, and takes the connection details
    provided, and connects to the relevant ducklake, returning a duckdb connection object
    """
    conn = duckdb.connect(database=":memory:")
    ducklake_conn_str = (
        f"ducklake:postgres:dbname={connCredentials['catalog']} "
        f"host={connCredentials['pg_host']} "
        f"user={connCredentials['pg_user']} " 
        f"password={connCredentials['pg_password']}"
    )
    # Pass the connection string as a bound parameter
    alias = _safe_identifier(ducklake)  # e.g. "retail_ducklake"
    conn.execute(f"ATTACH '{ducklake_conn_str}' AS {alias} (CREATE_IF_NOT_EXISTS false) ;")
    conn.execute(f"USE {alias};")
    return conn


def ducklakeMerge(
        session: DuckDBSession,
        targetSchema: str,
        targetTbl: str,
        srcDF: DuckDBDataFrame,
        extract_date: str,
        merge_on_id: str
):
    """
    A function to work with SQLFrame, but support the DuckDB MERGE INTO, until SQLFrame sets up
    support for this through the PySpark API / Table class (used for other SQL engines)
    """
    # Temp Save Table to schema - will be wiped after
    srcDF.write.mode("overwrite").saveAsTable(f"{targetSchema}.TEMP_{targetTbl}")
    
    extract_dt = datetime.datetime.strptime(extract_date, "%Y-%m-%d").date()
    now_dt = datetime.datetime.now()
    # replace just the date part
    fixed_dt = now_dt.replace(year=extract_dt.year, month=extract_dt.month, day=extract_dt.day)

    # create column lists for MERGE INTO based on supplied dataframe
    select_list = ", ".join([f"{c}" for c in srcDF.columns])
    src_select = ", ".join([f"src.{c}" for c in srcDF.columns])

    # Write Merge-Into Statement. Not yet supported in SQLFrame PySpark,
    # but we can directly use underlying DuckDB
    Merge_Stmt = f"""
    MERGE INTO {targetSchema}.{targetTbl} AS trgt
    USING 
        (
        SELECT
            {select_list},
            '{fixed_dt}' AS validFrom,
            NULL AS validTo,
            TRUE AS isCurrent
        FROM {targetSchema}.TEMP_{targetTbl}
    ) AS src
    ON (src.{merge_on_id} = trgt.{merge_on_id})
    WHEN MATCHED AND trgt.isCurrent = TRUE THEN UPDATE
    SET
        validTo = '{fixed_dt}',
        isCurrent = FALSE
    WHEN NOT MATCHED THEN
    INSERT ({select_list}, validFrom, validTo, isCurrent)
    VALUES ({src_select}, src.validFrom, src.validTo, src.isCurrent) 
    ;
    """
    session._conn.execute(Merge_Stmt) # type: ignore
    # End of SCD2 Customer Dim Update - drop TEMP table from earlier
    session._conn.execute(f"DROP TABLE {targetSchema}.TEMP_{targetTbl}") # type: ignore
    return None
