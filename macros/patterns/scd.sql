{%- macro scd_pattern(job_run_id,target_database,target_schema,target_table,source_database,source_schema,source_table,
primary_keys_csv,effective_start_date,effective_end_date,etl_insert_job_run_id,etl_update_job_run_id,
etl_insert_job_name,etl_update_job_name,src_date_column) %}
    
    {% set run_dict = {} %}
    {% if  primary_keys_csv== 'NULL' or primary_keys_csv =='' %}
        {% do exceptions.raise_compiler_error("No Primary Key Detected") %}
    {% else %}

    {%- set primary_keys_list = primary_keys_csv.split(',') -%}

    {%- set src_table = source_database~'.'~source_schema~'.'~source_table -%}
    {%- set tgt_table = target_database~'.'~target_schema~'.'~target_table -%}

    {% set primary_edate_list = [] %}
    {% for column in primary_keys_list %}
      {% do primary_edate_list.append(column) %}
    {% endfor %}
    {%- do primary_edate_list.append(effective_start_date)%}
    {% do primary_edate_list.append(effective_end_date) %}

    {% set source_relation = adapter.get_relation(database=source_database,schema=source_schema,
    identifier=source_table) %}

    {% set target_relation = adapter.get_relation(database=target_database,schema=target_schema,
    identifier=target_table) %}
    
    {%- set src_columns = (adapter.get_columns_in_relation(source_relation)) -%}
    {%- set src_columns_csv = get_quoted_csv(src_columns | map(attribute="column")) -%}
    {%- set src_columns_list = src_columns_csv.replace(' ', '').split(',') %}
    {%- for column in src_columns_list-%}
        {% set src_column = column.replace('"', '') %}
        {%- if(src_column.startswith('ETL_') or src_column.startswith('WATERMARK_') or 'JOB_NAME' in src_column 
        or ('EFFECTIVE_START_DATETIME' in src_column and src_column!=src_date_column)
        or 'EFFECTIVE_END_DATETIME'in src_column )-%}
            {%- do src_columns_list.remove(column)%}
        {%  endif %}
    {% endfor %}
    {# set stage #}
    {% set sql_stg %}  
        CREATE OR REPLACE TEMPORARY TABLE {{src_table~"_STG"}} as (
            select 
                {% for column in src_columns_list %}                
                    {% if column.replace('"','')!= src_date_column %}
                        {{column}}
                    {% else %}
                        {{column}} as {{effective_start_date}}
                    {% endif %}
                    {% if not loop.last %}
                    ,
                    {% endif %}
                {% endfor %}
            from {{source_database}}.{{source_schema}}.{{source_table}} 
        );
    {% endset %}
    {% set result =  run_query(sql_stg) %}
    {% set stg_dict ={} %}
    {% set status = result.columns[0].values()[0]%}
    {{print(status)}}
    {% if 'success' in status %}
        {% do stg_dict.update( {"row_count":0,"job_activity" : "SCD","job_status" : "Successful",           
                "job_message" : 'Creating Stage',"model_name" : this.identifier,
                "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                
        {% do run_dict.update( {"stg_dict" : stg_dict} )%}
    {% else %}
        {% do stg_dict.update( {"row_count":0,"job_activity" : "SCD","job_status" : "Failed",           
                "job_message" : 'Creating Stage',"model_name" : this.identifier,
                "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                
        {% do run_dict.update( {"stg_dict" : stg_dict} )%}
        {{print('stg_sql_execution Failed')}}
    {% endif %}

    {% if target_relation is none %}
            {# sql_tgt #}
        {% set sql_tgt %}
            {%- set tgt_sql %}
                select * from {{src_table~"_STG"}} where 1=2
            {% endset -%}

            CREATE TABLE {{tgt_table}} AS ( {{tgt_sql}} );

            ALTER TABLE  {{tgt_table}}  ADD ({{effective_end_date}} TIMESTAMP_LTZ);

            ALTER TABLE {{tgt_table}} 
            ADD ( 
                etl_insert_job_run_id NUMBER(38,0),
                etl_update_job_run_id NUMBER(38,0),
                ETL_EFFECTIVE_DATETIME TIMESTAMP_LTZ(9),
                ETL_EXPIRY_DATETIME TIMESTAMP_LTZ(9),
                ETL_CURRENT_FLAG BOOLEAN,
                ETL_DELETED_FLAG BOOLEAN,
                ETL_INSERT_DATETIME TIMESTAMP_LTZ(9),
                ETL_UPDATE_DATETIME TIMESTAMP_LTZ(9),
                etl_insert_job_name VARCHAR(200),
                etl_update_job_name VARCHAR(200)
            );

            ALTER TABLE {{tgt_table}}
            ADD PRIMARY KEY ({{primary_keys_csv}});
        {% endset %}
        
        {% set result =  run_query(sql_tgt) %}
        {% set tgt_dict ={} %}
        {% set status = result.columns[0].values()[0]%}
        {{print(status)}}
        {% if 'success' in status %}
            {% do tgt_dict.update( {"row_count":0,"job_activity" : "SCD","job_status" : "Successful",           
                    "job_message" : 'Creating Target',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"tgt_dict" : tgt_dict} )%}
        {% else %}
            {% do tgt_dict.update( {"row_count":0,"job_activity" : "SCD","job_status" : "Failed",           
                    "job_message" : 'Creating Target',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"tgt_dict" : tgt_dict} )%}
        {% endif %}

    {% endif %}

    {%- set stg_columns_list = src_columns_list %}
    {% for i in range(stg_columns_list|length) %}
      {% if src_date_column in stg_columns_list[i]%}
        {% do stg_columns_list.remove(stg_columns_list[i])%}
        {% set effective_start_date = '"'~effective_start_date~'"' %}
        {% do stg_columns_list.insert(i,effective_start_date)%}
      {% endif %}
    {% endfor %}
    {# sql_view #}
    {% set sql_view %}
    CREATE OR REPLACE TEMPORARY VIEW {{src_table~"_TGT_VW"}} AS(
        SELECT
            {% for column in stg_columns_list %}
                {{column}} as {{column.replace("_"," ")}},
            {% endfor %}
            {{effective_end_date}} as "{{effective_end_date.replace("_"," ")}}"
        FROM {{tgt_table}} 
        WHERE ETL_CURRENT_FLAG = TRUE AND ETL_DELETED_FLAG = FALSE
    ); 
    {% endset %}
    {% if execute %}
    {{print('view_sql_executed')}}
    {% endif %}
    {# sql_stg_scd_original #}
    {% set sql_stg_scd_original %}
    CREATE OR REPLACE TEMPORARY TABLE {{src_table~ "_STG_SCD_ORIGINAL"}} AS (
        SELECT
            ROW_NUMBER() OVER( PARTITION BY 
                {% for column in primary_keys_list %}
                original.{{column}}
                {% if not loop.last %}
                    ,
                {% endif %}
                {% endfor %}
            ORDER BY {{effective_start_date}}) AS ETL_ROW_NUMBER,
            {{effective_start_date}},
            {% for column in stg_columns_list if column.replace('"','')!= effective_start_date %}
                original.{{column}}
                {% if not loop.last %}
                    ,
                {% endif %}
            {% endfor %}
        FROM {{src_table~"_STG"}} original       
    );
    {% endset %}
    {% if execute %}
    {% do run_query(sql_stg_scd_original)%}
    {{print('sql_stg_scd_original executed')}}
    {% endif %}
    {# sql_stg_scd_deduplicated #}
    {% set sql_stg_scd_deduplicated %}
    CREATE OR REPLACE TEMPORARY TABLE {{src_table~ "_STG_SCD_DEDUPLICATED"}} AS (
        SELECT
            ETL_ROW_NUMBER,
            {% for column in stg_columns_list if column!=effective_start_date %}
                original.{{column}}
                {% if not loop.last %}
                    ,
                {% endif %}
            {% endfor %}
        FROM {{src_table~ "_STG_SCD_ORIGINAL"}} original
        EXCEPT
        SELECT
            ETL_ROW_NUMBER + 1 AS ETL_ROW_NUMBER,
            {% for column in stg_columns_list if column!=effective_start_date %}
                original.{{column}}
                {% if not loop.last %}
                    ,
                {% endif %}
            {% endfor %}
        FROM {{src_table~ "_STG_SCD_ORIGINAL"}} original
    );
    {% endset %}
    {% if execute %}
    {% do run_query(sql_stg_scd_deduplicated) %}
    {{print('sql_stg_scd_deduplicated executed ')}}
    {% endif %}
    {# stg_scd #}
    {% set stg_scd %}
    CREATE OR REPLACE TEMPORARY TABLE {{src_table~ "_STG_SCD"}} AS (
        SELECT
            {% for column in stg_columns_list if column.replace('"','')!=effective_start_date %}
                original.{{column}} ,
            {% endfor %}
            {{effective_start_date}},
            CASE
                WHEN
                    {{'NEXT_'~effective_start_date}} IS NULL
                    THEN CAST('9999-12-31 23:59:59.999999' AS TIMESTAMP_LTZ)
                ELSE DATEADD(microsecond,-1,{{'NEXT_'~effective_start_date}})
            END AS {{effective_end_date}}
        FROM(
            SELECT
                {% for column in stg_columns_list if column.replace('"','')!= effective_start_date %}
                    original.{{column}} ,
                {% endfor %}
                original.{{effective_start_date}},
                LEAD(original.{{effective_start_date}}) OVER
                (PARTITION BY
                    {% for column in primary_keys_list %}
                        original.{{column}}
                        {% if not loop.last %}
                            ,
                        {% endif %}
                    {% endfor %}
                
                ORDER BY original.{{effective_start_date}}) 
                AS {{'NEXT_'~effective_start_date}}
            FROM {{src_table~ "_STG_SCD_ORIGINAL"}} original
            INNER JOIN {{src_table~ "_STG_SCD_DEDUPLICATED"}} deduplicated 
            ON original.ETL_Row_Number = deduplicated.ETL_Row_Number
            AND 
                {% for column in primary_keys_list %}
                    original.{{column}} = deduplicated.{{column}}
                    {% if not loop.last %}
                        AND
                    {% endif %}
                {% endfor %}
        ) original
    );
    {% endset %}
    {% if execute %}
    {% do run_query (stg_scd) %}
    {{print('stg_scd executed')}}
    {% endif %}
    {# sql_stg_cdc #}
    {% set sql_stg_cdc %}
    CREATE OR REPLACE TEMPORARY TABLE {{src_table~"_STG_CDC"}} AS (
        SELECT
            {% for col in stg_columns_list if col.replace('"','')!=effective_start_date %}
                CASE
                    WHEN {% for key in primary_edate_list %}
                        src.{{ key }} IS NULL 
                        {% if not loop.last %}
                            AND
                        {% endif %}
                    {% endfor %}
                    THEN tgt.{{ col }}
                    ELSE src.{{ col }}
                END AS {{ col }},
            {% endfor %}
            CASE
                WHEN {% for key in primary_edate_list %}
                    src.{{ key }} IS NULL {% if not loop.last %}
                        AND
                    {% endif %}
                {% endfor %}
                THEN tgt.{{effective_start_date}}
                ELSE src.{{effective_start_date}}
            END AS {{effective_start_date}},
            CASE
                    WHEN {% for key in primary_edate_list %}
                        src.{{ key }} IS NULL {% if not loop.last %}
                            AND
                        {% endif %}
                    {% endfor %}
                    THEN tgt.{{effective_end_date}}
                    ELSE src.{{effective_end_date}}
            END AS {{effective_end_date}},
            CASE
                WHEN {% for key in primary_edate_list %}
                    tgt.{{ key }} IS NULL {% if not loop.last %}
                        AND
                    {% endif %}
                {% endfor %}
                THEN 'Y'
                ELSE 'N'
            END AS ETL_NEW_ROW_FLAG,
            CASE
                WHEN {% for key in primary_edate_list %}
                    src.{{ key }} IS NULL
                    AND
                {% endfor %}
                tgt.ETL_DELETED_FLAG = FALSE THEN 'Y'
                ELSE 'N'
            END AS ETL_DELETED_FLAG,
            CASE
                WHEN {% for key in primary_edate_list %}
                    src.{{ key }} IS NOT NULL
                    AND
                {% endfor %}
                tgt.ETL_DELETED_FLAG = TRUE
                AND tgt.etl_current_flag = TRUE THEN 'Y'
                WHEN 
                {% for column in stg_columns_list if (column.replace('"','')!=effective_start_date 
                    and column not in primary_edate_list) %}

                        ((src.{{ column }} = tgt.{{ column }})
                        OR (src.{{ column }} IS NULL
                        AND tgt.{{ column }} IS NULL)) 
                        {% set flag = true %}
                    {% if not loop.last %}
                            AND
                    {% endif %}
                {% endfor %}
                THEN 'N'
                WHEN {% for key in primary_edate_list %}
                    tgt.{{ key }} IS NULL {% if not loop.last %}
                        AND
                    {% endif %}
                {% endfor %}
                THEN 'N'
                WHEN {% for key in primary_edate_list %}
                    src.{{ key }} IS NULL {% if not loop.last %}
                        AND
                    {% endif %}
                {% endfor %}
                THEN 'N'
                ELSE 'Y'
            END AS ETL_UPDATED_FLAG,
            CURRENT_TIMESTAMP AS ETL_EFFECTIVE_DATETIME
        FROM
            {{src_table~ "_STG_SCD"}}
            src 
            FULL OUTER JOIN {{ tgt_table }}
            tgt
            ON {% for key in primary_edate_list %}
                ((TRIM(src.{{ key }}) = TRIM(tgt.{{ key }}))
                OR (src.{{ key }} IS NULL
                AND tgt.{{ key }} IS NULL)) 
                {% if not loop.last %}
                    AND
                {% endif %}
            {% endfor %}
        WHERE
            ({% for key in primary_edate_list %}
                tgt.{{ key }} IS NULL 
                {% if not loop.last %}
                    AND
                {% endif %}
            {% endfor %})
            OR (tgt.etl_current_flag = TRUE
            AND tgt.etl_insert_job_name = '{{etl_insert_job_name}}')
    );
    {% endset %}
        {% set result =  run_query(sql_stg_cdc) %}
        {% set cdc_dict ={} %}
        {% set status = result.columns[0].values()[0]%}
        {{print(status)}}
        {% if 'success' in status %}
            {% do cdc_dict.update( {"row_count":0,"job_activity" : "SCD","job_status" : "Successful",           
                    "job_message" : 'Creating CDC',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"cdc_dict" : cdc_dict} )%}
        {% else %}
            {% do cdc_dict.update( {"row_count":0,"job_activity" : "SCD","job_status" : "Failed",           
                    "job_message" : 'Creating CDC',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"cdc_dict" : cdc_dict} )%}
        {% endif %}
    
    {# sql_tgt_update #}
    {% set sql_tgt_update %}
    UPDATE {{tgt_table}}
    SET
        etl_update_job_run_id = {{etl_update_job_run_id}},
        etl_update_job_name = '{{etl_update_job_name}}',
        ETL_EXPIRY_DATETIME = DATEADD(second,-1,src.ETL_EFFECTIVE_DATETIME),
        ETL_CURRENT_FLAG = 0,
        ETL_UPDATE_DATETIME = CURRENT_TIMESTAMP,
        EFFECTIVE_END_DATETIME=DATEADD(second,-1,src.EFFECTIVE_START_DATETIME)
    FROM {{src_table~"_STG_CDC"}} src
    WHERE
        (src.ETL_UPDATED_FLAG = 'Y' OR src.ETL_DELETED_FLAG = 'Y')
        AND
        {{target_table}}.ETL_CURRENT_FLAG = TRUE
        AND
            {% for key in primary_edate_list %}
                ((TRIM({{tgt_table}}.{{key}}) = TRIM(src.{{key}}))
		        OR ({{tgt_table}}.{{key}} IS NULL
			    AND src.{{key}} IS NULL))
                AND
            {% endfor %}
        {{tgt_table}}.etl_insert_job_name = '{{etl_insert_job_name}}';
    {% endset %}
     {% if execute %}
            {% set result = run_query(sql_tgt_update) %}
            {% set return_value  = result.columns[0].values() %}
            {% set row_count = return_value[0] %}
            {% set update_dict ={} %}
            {% do update_dict.update( {"job_activity" : "SCD","job_status" : "Successful",           
                    "job_message" : 'updating Values Into Target',"model_name" : this.identifier,
                    "row_count":row_count,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
            
            {% do update_dict.update( {"row_count":row_count})%}
            {% do run_dict.update( {"update_dict" : update_dict} )%}
            {{print('update executed')}}
    {% else %}
        {% do update_dict.update( {"row_count":0,"job_activity" : "SCD","job_status" : "Failed",   
        "job_message" : 'updating Values Into Target',"model_name" : this.identifier,
        "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
        {% do run_dict.update( {"update_dict" : update_dict} )%}
    {% endif %}
    
    {% set insert %}
      INSERT INTO {{tgt_table}}
    SELECT
        {% for column in stg_columns_list %}
        {{column}},
        {% endfor %}
        {{effective_end_date}},
        {{etl_insert_job_run_id}} as etl_insert_job_run_id,
        NULL as etl_update_job_run_id,
        ETL_EFFECTIVE_DATETIME AS ETL_EFFECTIVE_DATETIME,
        CAST('9999-12-31 23:59:59.999' AS TIMESTAMP_LTZ) AS ETL_EXPIRY_DATETIME,
        TRUE AS ETL_CURRENT_FLAG,
        CASE
            WHEN ETL_DELETED_FLAG = 'Y' THEN TRUE
            ELSE FALSE
        END AS ETL_DELETED_FLAG,
        CURRENT_TIMESTAMP AS ETL_INSERT_DATETIME,
        NULL AS ETL_UPDATE_DATETIME ,
        '{{etl_insert_job_name}}' AS etl_insert_job_name,
        NULL AS etl_update_job_name
    FROM {{src_table~"_STG_CDC"}}
    WHERE
        ETL_UPDATED_FLAG = 'Y'
        OR ETL_DELETED_FLAG = 'Y'
        OR ETL_NEW_ROW_FLAG = 'Y'
    {% endset %}
    {% if execute %}
            {% set result = run_query(insert) %}
            {% set return_value  = result.columns[0].values() %}
            {% set row_count = return_value[0] %}
            {% set insert_dict ={} %}
            {% do insert_dict.update( {"job_activity" : "SCD","job_status" : "Successful",           
                    "job_message" : 'Inserting Values Into Target',"model_name" : this.identifier,
                    "row_count":row_count,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
            
            {% do insert_dict.update( {"row_count":row_count})%}
            {% do run_dict.update( {"insert_dict" : insert_dict} )%}
            {{print('insert executed')}}
    {% else %}
        {% do insert_dict.update( {"row_count":0,"job_activity" : "SCD","job_status" : "Failed",   
        "job_message" : 'Inserting Values Into Target',"model_name" : this.identifier,
        "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
        {% do run_dict.update( {"insert_dict" : insert_dict} )%}
    
    {% endif %}

    {{ log_job_plan(job_run_id,run_dict)}}

    {% endif %}
        
{%- endmacro -%} 