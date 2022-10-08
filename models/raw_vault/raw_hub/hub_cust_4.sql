{%- set source_model      = "hash_stage"    -%}
{%- set src_pk            = "CUST_HK"       -%}
{%- set src_nk            = "CUST_ID"       -%}
{%- set src_ldts          = "LOAD_DATETIME" -%}
{%- set src_source        = "RECORD_SOURCE" -%}

{% set hash_stg_rel = adapter.get_relation(database=this.database,schema=this.schema,identifier=source_model) %}
{%- set hash_stage_col = (adapter.get_columns_in_relation(hash_stg_rel)) -%}
{%- set hash_stage_col_csv = get_quoted_csv(hash_stage_col | map(attribute = "column")) -%}
{%- set hash_stage_col_list = hash_stage_col_csv.replace(" ","").split(',') %}
{% set src_extra_columns = [] %}

{% for column in hash_stage_col_list %}
  {% if (column.replace('"','') not in [src_pk,src_nk,src_ldts,src_source]) and (column.replace('"','').startswith('EFFECTIVE_')==False) %}
    {% do src_extra_columns.append(column)%}
  {% endif %}
{% endfor %}


{{config(
    materialized='delta_patterns',
    primary_keys= src_nk ,
    target_database= 'DBT_DEV_DB',
    target_schema= 'SANDHIYA',
    target_table= 'VAULT_APPEND_ONLY_TARGET',
    job_name='APPEND_ONLY',
    etl_insert_job_run_id = '1022',
    etl_update_job_run_id = '1023',
    etl_insert_job_name = 'INSERTED',
    etl_update_job_name = 'UPDATED',
    src_date_column='LOAD_DATETIME',
    effective_start_date='EFFECTIVE_START_DATETIME',
    effective_end_date='EFFECTIVE_END_DATETIME'
)}} 


WITH HUB AS(
{{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk,src_extra_columns=src_extra_columns, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}
)
select * from HUB
