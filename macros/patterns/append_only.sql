{% macro append_only(
        target_database,
        target_schema,
        target_table,
        source_database,
        source_schema,
        source_table
    ) %}

    {% set run_dict = {} %}

    {# getting path for source,
    target,
    stage TABLE #} 
    {% set target_relation = adapter.get_relation(database = target_database,schema = target_schema,identifier = target_table) %}

    {% set stg_table = source_database ~ '.' ~ source_schema ~ '.' ~ source_table ~ '_STG' %}
    {# getting column names #} {%- set src_columns = (adapter.get_columns_in_relation(this)) -%}
    {%- set src_columns_csv = get_quoted_csv(src_columns | map(attribute = "column")) -%}
    {# here converted th column names INTO A single list #} 
    {%- set src_columns_list = src_columns_csv.replace(" ","").split(',') %}


    {# checking THE source TABLE have ANY etl COLUMNS are NOT ! if THE source TABLE have ANY etl COLUMNS just remove it.#} 
    {%- for column in src_columns_list -%}
        {% set src_column = column.replace('"',"") %}
        {%- if(src_column.startswith('ETL_') or src_column.startswith('WATERMARK_') or 
        'JOB_NAME' in src_column or 'EFFECTIVE_START_DATETIME' in src_column or 'EFFECTIVE_END_DATETIME' in src_column) -%}
            {%- do src_columns_list.remove(column) %}
        {% endif %}
    {% endfor %}

    {% set stg_sql %}
SELECT
    {% for col in src_columns_list %}
        {{ col }}

        {% if not loop.last %}
        ,
        {% endif %}
    {% endfor %}
FROM
    {{ this }}

    {% endset %}


    {% set sql_stg %}  

    {# stage stable creation #} 
    {{ create_table_as(
        true,
        stg_table,
        stg_sql
    ) }}
ALTER TABLE
    {{ stg_table }}
    SET data_retention_time_in_days = 0;
    
   {% endset %}

{% set result =  run_query(sql_stg) %}
    {% set stg_dict = {} %}
    {% set status = result.columns[0].values()[0]%}
    {% if 'successfully' in status %}
        {% do stg_dict.update( {"row_count":0,"job_activity" : "append_only","job_status" : "Successful",           
                "job_message" : 'Creating Stage',"model_name" : this.identifier,
                "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                
        {% do run_dict.update( {"stg_dict" : stg_dict} )%}
    {% else %}
        {% do stg_dict.update( {"row_count":0,"job_activity" : "append_only","job_status" : "Failed",           
                "job_message" : 'Creating Stage',"model_name" : this.identifier,
                "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                
        {% do run_dict.update( {"stg_dict" : stg_dict} )%}
       
    {% endif %}

    {# checking THE target TABLE IS already exist OR NOT !#} 
    {# sql_tgt #}

    
    {% if target_relation is none %}

        {% set target_relation = target_database ~ '.' ~ target_schema ~ '.' ~ target_table %}


    {# target TABLE creation #} 
        
     {% set sql_tgt %}


        CREATE OR REPLACE TABLE {{ target_relation }} AS
                SELECT *
                FROM {{ stg_table }}
                WHERE 1 = 2;


        {# adding etl COLUMNS INTO target TABLE #}
    ALTER TABLE
    {{ target_relation }}
    ADD(
    etl_insert_invocation_id NVARCHAR,
    etl_update_invocation_id NVARCHAR,  
    etl_row_effective_date timestamp_ltz(9), 
    etl_row_expiry_date timestamp_ltz(9), 
    etl_row_current_flag BOOLEAN, 
    etl_row_deleted_flag BOOLEAN, 
    etl_insert_datetime timestamp_ltz(9), 
    etl_update_datetime timestamp_ltz(9), 
    etl_insert_job_name VARCHAR(200), 
    etl_update_job_name VARCHAR(200));
    {% endset %}


    {% set result = run_query(sql_tgt) %}
            {% set tgt_dict ={} %}
            {% set status = result.columns[0].values()[0]%}
            {% if 'successfully' in status %}
            {% do tgt_dict.update( {"row_count":0,"job_activity" : "append_only","job_status" : "Successful",           
                    "job_message" : 'Creating target table and adding etl columns',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"tgt_dict" : tgt_dict} )%}
            {% else %}
            {% do tgt_dict.update( {"row_count":0,"job_activity" : "append_only","job_status" : "Failed",           
                    "job_message" : 'Creating target table and adding etl columns',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}

            {% do run_dict.update( {"tgt_dict" : tgt_dict} )%}
            {% endif %}

    {# inserting THE VALUESINTO target TABLE #}
    {% endif %}

    {% set get_invocation_id %}
        select 
            invocation_id
        from 
            {{target_schema}}_meta.STG_DBT_AUDIT_LOG
        where 
            event_name = 'run started'
        order by 
            EVENT_TIMESTAMP
        desc limit 1;
    {% endset %}

    {% if execute %}
        {% set result = run_query(get_invocation_id) %}
        {% set etl_invocation_id = (result.columns[0].values())[0] %}
    {% endif %}

    {% set insert %}


    INSERT INTO
        {{ target_relation }}
    SELECT
        {% for col in src_columns_list %}
            {{ col }},
        {% endfor %}
    
        '{{ etl_invocation_id }}' AS etl_insert_job_run_id,
        '{{ etl_invocation_id }}' AS etl_update_job_run_id,
        CURRENT_TIMESTAMP AS etl_row_effective_date,
        CAST(
            '9999-12-31 23:59:59.999' AS timestamp_ltz
        ) AS etl_row_expiry_date,
        TRUE AS etl_row_current_flag,
        FALSE AS etl_row_deleted_flag,
        CURRENT_TIMESTAMP AS etl_insert_datetime,
        CURRENT_TIMESTAMP AS etl_update_datetime,
        '{{this.identifier}}' AS etl_insert_job_name,
        '{{this.identifier}}' AS etl_update_job_name
    FROM {{ stg_table }}

    {% endset %}
    {% if execute %}
            {% set result = run_query(insert) %}
            {% set return_value  = result.columns[0].values() %}
            {% set row_count = return_value[0] %}
            {% set insert_dict ={} %}
            {% do insert_dict.update( {"job_activity" : "append_only","job_status" : "Successful",           
                    "job_message" : 'Inserting Values',"model_name" : this.identifier,"row_count":row_count,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
            
            {% do insert_dict.update( {"row_count":row_count})%}
            {% do run_dict.update( {"insert_dict" : insert_dict} )%}




    {% else %}
            {% set insert_dict ={} %}
            {% do insert_dict.update( {"job_activity" : "append_only","job_status" : "Failed",           
                    "job_message" : 'Inserting Values',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
            
            {% do insert_dict.update( {"row_count":row_count})%}
            {% do run_dict.update( {"insert_dict" : insert_dict} )%}
            

    {% endif %}

    {{ log_job_plan(run_dict,target_schema,target_table) }}

{% endmacro %}