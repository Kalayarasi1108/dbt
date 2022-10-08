{%- set source_model      = "hash_stage"    -%}
{%- set src_pk            = "CUST_HK"       -%}
{%- set src_nk            = "CUST_ID"       -%}
{%- set src_extra_columns = ["ID_NAME_HK","CUST_NAME","CUST_ADD","CUST_COUNTRY","CUST_PHONE","CUST_ACCTBAL","ETL_ROW_DELETED_FLAG"] %}
{%- set src_ldts          = "LOAD_DATETIME" -%}
{%- set src_source        = "RECORD_SOURCE" -%}

WITH HUB AS(
{{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk,src_extra_columns=src_extra_columns, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}
)
select * from HUB
{{config(
    materialized='delta_patterns',
    primary_keys= src_nk ,
    target_database= 'DBT_DEV_DB',
    target_schema= 'SANDHIYA',
    target_table= 'VAULT_FULL_APPLY_TARGET',
    job_name='FULL_APPLY',
    etl_insert_job_run_id = '1022',
    etl_update_job_run_id = '1023',
    etl_insert_job_name = 'INSERTED',
    etl_update_job_name = 'UPDATED',
    src_date_column='LOAD_DATETIME',
    effective_start_date='EFFECTIVE_START_DATETIME',
    effective_end_date='EFFECTIVE_END_DATETIME',
    job_run_id= 222
)}}
