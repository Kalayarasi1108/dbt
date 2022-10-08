{%- macro delta_apply(target_database,target_schema,target_table,source_database,source_schema,source_table,primary_keys_csv,etl_insert_job_run_id,etl_update_job_run_id,etl_insert_job_name,etl_update_job_name) %}
    {% set run_dict = {} %}

    {% if  primary_keys_csv== 'NULL' or primary_keys_csv =='' %}
        {% do exceptions.raise_compiler_error("No Primary Key Detected") %}
    {% else %}

    {% set primary_keys_list = primary_keys_csv.split(',') %}

    {%- set src_table = source_database~'.'~source_schema~'.'~source_table -%}
    {%- set tgt_table = target_database~'.'~target_schema~'.'~target_table -%}

    {% set source_relation = adapter.get_relation(database=source_database,schema=source_schema,identifier=source_table) %}
    {% set target_relation = adapter.get_relation(database=target_database,schema=target_schema,identifier=target_table) %}
    
    {%- set src_columns = (adapter.get_columns_in_relation(source_relation)) -%}
    {%- set src_columns_csv = get_quoted_csv(src_columns | map(attribute="column")) -%}
    {%- set src_columns_list = src_columns_csv.replace(" ", "").split(',') %}

    {%- for column in src_columns_list-%}
        {% set src_column = column.replace('"', "") %}
        {%- if(src_column.startswith('ETL_') or src_column.startswith('WATERMARK_') or 'JOB_NAME' in src_column or 
         'EFFECTIVE_START_DATETIME' in src_column or 'EFFECTIVE_END_DATETIME' in src_column) and (src_column!='ETL_ROW_DELETED_FLAG')-%}
            {%- do src_columns_list.remove(column) %}
        {%  endif %}
    {% endfor %}
    {# set stage #}
    {% set sql_stg %} 
    {%- set stg_sql %}
        select 
            {% for column in src_columns_list %}
                {{column}}
                {% if not loop.last %}
                  ,
                {% endif %}
            {% endfor %}
         from {{source_database}}.{{source_schema}}.{{source_table}}
    {% endset -%}

    CREATE OR REPLACE TEMPORARY TABLE {{src_table~"_STG"}} as (
        {{stg_sql}}
    );
    {%- do src_columns_list.remove('"ETL_ROW_DELETED_FLAG"') %}
    {% endset %}
    {% set result =  run_query(sql_stg) %}
    {% set stg_dict ={} %}
    {% set status = result.columns[0].values()[0]%}
    {# {{print(status)}} #}
    {% if 'successfully' in status %}
        {% do stg_dict.update( {"row_count":0,"job_activity" : "DELTA","job_status" : "Successful",           
                "job_message" : 'Creating Stage',"model_name" : this.identifier,
                "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                
        {% do run_dict.update( {"stg_dict" : stg_dict} )%}
        {{print('stg_sql_execution Successfull')}}
    {% else %}
        {% do stg_dict.update( {"row_count":0,"job_activity" : "DELTA","job_status" : "Failed",           
                "job_message" : 'Creating Stage',"model_name" : this.identifier,
                "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                
        {% do run_dict.update( {"stg_dict" : stg_dict} )%}
        {{print('stg_sql_execution Failed')}}
    {% endif %}

    {%- if target_relation is none  -%} 
        {# Creating Target Table - Sandhiya #}
        {# sql_tgt #}
        {% set sql_tgt %}
        CREATE OR REPLACE TABLE {{tgt_table}} AS(
            select 
            {% for column in src_columns_list %}
              {{column}}
              {% if not loop.last %}
                ,
              {% endif %}
            {% endfor %}
            
            from {{src_table~"_STG"}} where 1=2
        );

        {# Adding ETL Columns - Sandhiya#}
        ALTER TABLE {{tgt_table}} 
        ADD ( 
            ETL_INSERT_JOB_RUN_ID NUMBER(38,0), 
            ETL_UPDATE_JOB_RUN_ID NUMBER(38,0),
            ETL_EFFECTIVE_DATETIME TIMESTAMP_LTZ(9),
            ETL_EXPIRY_DATETIME TIMESTAMP_LTZ(9),
            ETL_CURRENT_FLAG BOOLEAN,
            ETL_DELETED_FLAG BOOLEAN,
            ETL_INSERT_DATETIME TIMESTAMP_LTZ(9),
            ETL_UPDATE_DATETIME TIMESTAMP_LTZ(9),
            ETL_INSERT_JOB_NAME VARCHAR(200),
            ETL_UPDATE_JOB_NAME VARCHAR(200)
        );

        {# Adding Primary Keys- Sandhiya #}
        ALTER TABLE {{tgt_table}}
        ADD PRIMARY KEY ({{primary_keys_csv}});
         {% endset %}
         {% set result = run_query(sql_tgt) %}
            {% set tgt_dict ={} %}
            {% set status = result.columns[0].values()[0]%}
            {% if 'successfully' in status %}
            {% do tgt_dict.update( {"row_count":0,"job_activity" : "DELTA","job_status" : "Successful",           
                    "job_message" : 'Creating Target Table',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"tgt_dict" : tgt_dict} )%}
            {% else %}
            {% do tgt_dict.update( {"row_count":0,"job_activity" : "DELTA","job_status" : "Failed",           
                    "job_message" : 'Creating Target Table',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"tgt_dict" : tgt_dict} )%}
        {% endif %}
    {% endif %}
    {# sql_cdc #}
    {% set sql_cdc %}
    CREATE OR REPLACE TEMPORARY TABLE {{tgt_table~"_CDC"}} AS (
        SELECT
            {% for column in src_columns_list %}
                CASE
                    WHEN src.ETL_ROW_DELETED_FLAG = 'Y' THEN tgt.{{column}}
                    ELSE src.{{column}}
                END AS {{column}},
            {% endfor %}
            CASE
                WHEN 
                {% for key in primary_keys_list %}
                  tgt.{{key}} IS NULL
                  {% if not loop.last %}
                    AND
                  {% endif %}
                {% endfor %}
                THEN 'Y'
                ELSE 'N'
            END AS ETL_New_Row_Flag,
            CASE
                WHEN src.ETL_ROW_DELETED_FLAG = 'Y'
                AND tgt.ETL_DELETED_FLAG = FALSE THEN 'Y'
                ELSE 'N'
            END AS ETL_DELETED_FLAG,
            CASE
                WHEN
                    {% for key in primary_keys_list %}
                        src.{{key}} IS NOT NULL
                        AND
                    {% endfor %}
                    tgt.ETL_DELETED_FLAG = TRUE
			        AND tgt.ETL_Current_Flag = TRUE 
                THEN 'Y'
                WHEN
                {% for column in src_columns_list if column.replace('"','') not in primary_keys_list %}
                        ((src.{{ column }} = tgt.{{ column }})
                        OR (src.{{ column }} IS NULL
                        AND tgt.{{ column }} IS NULL))
                    {% if not loop.last %}
                            AND
                    {% endif %}
                {% endfor %}
                THEN 'N'
                WHEN
                {% for key in primary_keys_list %}
                  tgt.{{key}} IS NULL
                  {% if not loop.last %}
                    AND
                  {% endif %}
                {% endfor %}
                THEN 'N'
                WHEN src.ETL_ROW_DELETED_FLAG = 'Y' THEN 'N'
			    ELSE 'Y'
		    END AS ETL_UPDATED_FLAG,
		    CURRENT_TIMESTAMP AS ETL_EFFECTIVE_DATE
	    FROM {{src_table~"_STG"}} src
        LEFT JOIN {{tgt_table}} tgt ON
            {% for key in primary_keys_list %}
                  ((TRIM(src.{{ key }}) = TRIM(tgt.{{ key }}))
                OR (src.{{ key }} IS NULL
                AND tgt.{{ key }} IS NULL)) 
                {% if not loop.last %}
                    AND
                {% endif %}
            {% endfor %}
        WHERE
            ({% for key in primary_keys_list %}
                tgt.{{ key }} IS NULL 
                {% if not loop.last %}
                    AND
                {% endif %}
            {% endfor %})
            OR (tgt.etl_current_flag = TRUE
            AND tgt.etl_insert_job_name = '{{etl_insert_job_name}}')
    );
    {% endset %}
    ALTER TABLE {{tgt_table~"_CDC"}} 
    SET data_retention_time_in_days = 0;
    
     {% set result =  run_query(sql_cdc) %}
            {% set cdc_dict ={} %}
            {% set status = result.columns[0].values()[0]%}
            {% if 'successfully' in status %}
            {% do cdc_dict.update( {"row_count":0,"job_activity" : "DELTA","job_status" : "Successful",           
                    "job_message" : 'Creating CDC',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"cdc_dict" : cdc_dict} )%}
            {{print('sql_cdc executed')}}
            {% else %}
            {% do cdc_dict.update( {"row_count":0,"job_activity" : "DELTA","job_status" : "Failed",           
                    "job_message" : 'Creating CDC',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"cdc_dict" : cdc_dict} )%}
            {{print('sql_cdc failed')}}
            {% endif %}

    {# sql_tgt_update #}
    {% set sql_tgt_update %}
    UPDATE {{tgt_table}}
    SET
    ETL_UPDATE_JOB_RUN_ID = {{etl_update_job_run_id}},
    ETL_UPDATE_JOB_NAME= '{{etl_update_job_name}}',
    ETL_EXPIRY_DATETIME=DATEADD(millisecond,-1,src.ETL_EFFECTIVE_DATE),
    ETL_Current_Flag = 0,
    ETL_UPDATE_DATETIME=CURRENT_TIMESTAMP
    FROM
    {{tgt_table~"_CDC"}}  as src
    WHERE
    (src.ETL_Updated_Flag = 'Y'
		OR src.ETL_Deleted_Flag = 'Y')
	AND {{tgt_table}}.ETL_CURRENT_FLAG = TRUE
	AND 
    {% for key in primary_keys_list %}
      ((TRIM({{tgt_table}}.{{key}}) = TRIM(src.{{key}}))
		OR ({{tgt_table}}.{{key}} IS NULL
			AND src.{{key}} IS NULL))
        {% if not loop.last %}
          AND
        {% endif %}
    {% endfor %}
	AND {{tgt_table}}.ETL_Insert_Job_Name = '{{etl_insert_job_name}}';
    {% endset %}
        {% if execute %}
            {% do run_query(sql_tgt_update) %}
            {% set update_dict ={} %}
            {% do update_dict.update( {"row_count":0,"job_activity" : "DELTA","job_status" : "Successful",           
                    "job_message" : 'Updating Target',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"update_dict" : update_dict} )%}
            {{print('sql_tgt_update executed')}}
    {% else %}
            {% set update_dict ={} %}
            {% do update_dict.update( {"row_count":0,"job_activity" : "DELTA","job_status" : "Failed",           
                    "job_message" : 'Updating Target',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"update_dict" : update_dict} )%}
            {{print('sql_tgt_update Failed')}}
    {% endif %}

     {% set insert %}
    INSERT INTO {{tgt_table}}

    SELECT
        {% for column in src_columns_list %}
          {{column}},
        {% endfor %}
        {{etl_insert_job_run_id}} AS etl_insert_job_run_id,

        NULL AS etl_update_job_run_id,

        ETL_EFFECTIVE_DATE AS ETL_EFFECTIVE_DATETIME,

        CAST(

            '9999-12-31 23:59:59.999' AS timestamp_ltz

        ) AS ETL_EXPIRY_DATETIME,

        TRUE AS etl_current_flag,

        CASE

            WHEN etl_deleted_flag = 'Y' 
            THEN TRUE
            ELSE FALSE

        END AS etl_deleted_flag,

        CURRENT_TIMESTAMP AS etl_insert_datetime,

        CURRENT_TIMESTAMP AS etl_update_datetime,

        '{{etl_insert_job_name}}' AS etl_insert_job_name,

        NULL AS etl_update_job_name

    FROM

        {{tgt_table~"_CDC"}}

    WHERE
        etl_updated_flag = 'Y'

        OR etl_deleted_flag = 'Y'

        OR etl_new_row_flag = 'Y' ;
        {% endset %}
        {% if execute %}
            {% set result = run_query(insert) %}
            {% set return_value  = result.columns[0].values() %}
            {% set row_count = return_value[0] %}
            {% set insert_dict ={} %}
            {% do insert_dict.update( {"job_activity" : "DELTA","job_status" : "Successful",           
                    "job_message" : 'Inserting Values',"model_name" : this.identifier,"row_count":row_count,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
            
            {% do insert_dict.update( {"row_count":row_count})%}
            {% do run_dict.update( {"insert_dict" : insert_dict} )%}
            {{print('insert executed')}}
    {% else %}
            {% set insert_dict ={} %}
            {% do insert_dict.update( {"job_activity" : "DELTA","job_status" : "Failed",           
                    "job_message" : 'Inserting Values',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
            
            {% do insert_dict.update( {"row_count":row_count})%}
            {% do run_dict.update( {"insert_dict" : insert_dict} )%}
            {{print('insert Failed')}}

    {% endif %}
        {{ log_job_plan(run_dict,target_schema,target_table)}}
    {% endif %}
    
{% endmacro %}