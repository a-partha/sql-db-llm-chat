
  
    
    

    create  table
      "legal_demo"."main"."acris_clean__dbt_tmp"
  
    as (
      

SELECT *
FROM read_csv_auto('/opt/airflow/data/first_500k_acris.csv', header=True, delim=',')
    );
  
  