{%- materialization delta_patterns, adapter='snowflake'-%}

{%- set target_database       = config.get('target_database')          -%}
{%- set target_schema         = config.get('target_schema')            -%}
{%- set target_table          = config.get('target_table')             -%}
{%- set source_database       = this.database                          -%}
{%- set source_schema         = this.schema                            -%}
{%- set source_table          = this.identifier                        -%}
{%- set primary_keys_csv      = config.get('primary_keys')             -%}
{%- set effective_start_date  = config.get('effective_start_date')     -%}
{%- set effective_end_date    = config.get('effective_end_date')       -%}
{%- set etl_insert_job_run_id = config.get('etl_insert_job_run_id')    -%}
{%- set etl_update_job_run_id = config.get('etl_update_job_run_id')    -%}
{%- set etl_insert_job_name   = config.get('etl_insert_job_name')      -%}
{%- set etl_update_job_name   = config.get('etl_update_job_name')      -%}
{%- set src_date_column       = config.get('src_date_column')          -%}
{%- set job_name              = config.get('job_name')                 -%}
{%  set etl_columns = 'ETL_INSERT_JOB_RUN_ID,ETL_UPDATE_JOB_RUN_ID,ETL_EFFECTIVE_DATETIME,ETL_EXPIRY_DATETIME,
ETL_CURRENT_FLAG,ETL_DELETED_FLAG,ETL_INSERT_DATETIME,ETL_UPDATE_DATETIME,ETL_INSERT_JOB_NAME,ETL_UPDATE_JOB_NAME'%}

{% set fetch_last_job_run_id %}

    SELECT max(JOB_RUN_ID)

    FROM audit.job_run_log;

{% endset %}

{% if execute %}

    {% set result = run_query(fetch_last_job_run_id) %}

    {% set job_run_id = result.columns[0].values() %}

    {% if job_run_id[0] is none %}

        {% set job_run_id = 1 %}

    {% else %}

        {% set job_run_id = (job_run_id[0] + 1) %}

    {% endif %}

{% endif %}

{% if job_name=='SCD' %}
    {%- set build_sql = scd_pattern(
        job_run_id,
        target_database,
        target_schema,
        target_table,
        source_database,
        source_schema,
        source_table,
        primary_keys_csv,
        'EFFECTIVE_START_DATETIME',
        'EFFECTIVE_END_DATETIME',
        '1022',
        '1023',
        source_table,
        source_table,
        src_date_column,
    ) -%}
{% elif job_name=='DELTA' %}
    {%- set build_sql = delta_apply(
        job_run_id,
        target_database,
        target_schema,
        target_table,
        source_database,
        source_schema,
        source_table,
        primary_keys_csv,
        '1022',
        '1023',
        source_table,
        source_table
    )-%}
{% elif job_name=='FULL_APPLY' %}
    {%- set build_sql = full_apply(
        job_run_id,
        target_database,
        target_schema,
        target_table,
        source_database,
        source_schema,
        source_table,
        primary_keys_csv,
        etl_columns,
        '1022',
        source_table,
        '1023',
        source_table
    )-%}
{% else %}
    {%- set build_sql = append_only(
        job_run_id,
        target_database,
        target_schema,
        target_table,
        source_database,
        source_schema,
        source_table,
        '1022',
        '1023',
        source_table,
        source_table
    )-%}
{% endif %}

{%- call statement('main') -%}
    {{ build_sql }}  
{%- endcall %}

{{ return({'relations': [this]}) }}

{%- endmaterialization -%}