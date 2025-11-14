-- Sarah is an analyst. She should only see Silver & Gold layers, not Bronze!
-- Let's test

ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=postgres user=sarah password=sarah_pw' 
AS retail_ducklake (CREATE_IF_NOT_EXISTS false);

USE retail_ducklake ;

