{{ config(materialized='table', alias='acris_clean') }}

SELECT *
FROM read_csv_auto('/opt/airflow/data/first_500k_acris.csv', header=True, delim=',')