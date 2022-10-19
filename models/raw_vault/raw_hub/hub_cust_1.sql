{%- set source_model      = "hash_stage"    -%}
{%- set src_pk            = "PRIMARY_HK"    -%}
{%- set src_nk            = ["CUST_ID","CUST_NAME"]  -%}
{%- set src_ldts          = "LOAD_DATETIME" -%}
{%- set src_source        = "RECORD_SOURCE" -%}

{% set hash_stg_rel = adapter.get_relation(database=this.database,schema=this.schema,identifier=source_model) %}
{%- set hash_stage_col = (adapter.get_columns_in_relation(hash_stg_rel)) -%}
{%- set hash_stage_col_csv = get_quoted_csv(hash_stage_col | map(attribute = "column")) -%}
{%- set hash_stage_col_list = hash_stage_col_csv.replace(" ","").split(',') %}
{% set src_extra_columns = [] %}

{% for column in hash_stage_col_list %}
  {% if (column.replace('"','') not in [src_pk,src_ldts,src_source]) and (column.replace('"','') not in src_nk) and (column.replace('"','').startswith('EFFECTIVE_')==False) %}
    {% do src_extra_columns.append(column)%}
  {% endif %}
{% endfor %}

{% set hub_relation = adapter.get_relation(database=this.database,schema=this.schema,identifier=this.identifier) %}

{% if hub_relation is none %}
    {% set hub %}
        create or replace view {{this}} as(
            {{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk,src_extra_columns=src_extra_columns, src_ldts=src_ldts,
                        src_source=src_source, source_model=source_model) }}
        );
    {% endset %}
  {% do run_query(hub)%}
{% endif %}

{{config(
    materialized='delta_patterns',
    primary_keys= src_pk,
    target_database= 'DBT_DEV_DB',
    target_schema= 'KALAYARASI',
    target_table= 'VAULT_SCD_TARGET',
    job_name='SCD',
    src_date_column='LOAD_DATETIME',
    effective_start_date='EFFECTIVE_START_DATETIME',
    effective_end_date='EFFECTIVE_END_DATETIME'
)}} 



{{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk,src_extra_columns=src_extra_columns, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}