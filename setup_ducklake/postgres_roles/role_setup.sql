-- Set Up Users & Roles for example Project

-- Group roles (no login)
CREATE ROLE data_analyst NOLOGIN;
CREATE ROLE data_engineer NOLOGIN;


GRANT SELECT ON ALL TABLES IN SCHEMA public TO data_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO data_engineer;


-- Individual humans
CREATE ROLE sarah LOGIN PASSWORD 'sarah_pw';
CREATE ROLE tim   LOGIN PASSWORD 'tim_pw';

-- Sarah is an analyst, Tim is an engineer
GRANT data_analyst  TO sarah;
GRANT data_engineer TO tim;

-- Alter Ducklake Schema to restrict rows on what schemas users can see/query
ALTER TABLE public.ducklake_schema ENABLE ROW LEVEL SECURITY;

-- Analyst Policy for Silver & Gold Schemas only
CREATE POLICY data_analyst_read_p
ON public.ducklake_schema
FOR SELECT
TO data_analyst
USING (ducklake_schema.schema_name IN ('retail_silver', 'retail_gold'));

-- Data Engineer policy for all schemas
CREATE POLICY data_engineer_read_p
ON public.ducklake_schema
FOR SELECT
TO data_engineer
USING (true);  -- no restriction


-- ===================================
-- TESTING 

-- SET ROLE sarah;

-- SELECT * FROM public.ducklake_schema;

-- ===================================
