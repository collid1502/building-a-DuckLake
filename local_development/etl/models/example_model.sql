MODEL (
  name example_model,
  allow_partials true,
  kind FULL
);

SELECT
  1 AS id,
  'test' AS message,
  CURRENT_DATE AS run_date;
