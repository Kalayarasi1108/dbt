{%- macro full_apply(target_database,target_schema,target_table,source_database,source_schema,source_table,primary_keys_csv,etl_columns,etl_insert_run_id,etl_insert_job_name,etl_update_run_id,etl_update_job_name) %}
    {# Getting Relation for source and target table using adapter function #}
    {% set run_dict = {} %}

    ALTER SESSION
    set timezone='Asia/Kolkata';
      
    {% set source_relation = adapter.get_relation(database=source_database,schema=source_schema,identifier=source_table) %}
    {% set target_relation = adapter.get_relation(database=target_database,schema=target_schema,identifier=target_table) %}
    {# Primary Key List  #}
    {%- set primary_keys_list = (primary_keys_csv.split(',')) -%}
    {# Creating stage table  #}

    {# Declaring stage file path #}
    {%- set stg_table = source_database~'.'~source_schema~'.'~source_table ~ "_STG" -%}
    {# select query for creating stage file from source #}
    {% set sql_stg %}
    {%- set stg_sql %}
        select * from {{source_database}}.{{source_schema}}.{{source_table}}
    {% endset -%}
    {# Creating stage table using default function create_table_as(true[temporary]/false[transient],table path,sql ) #}
    {{ create_table_as(false, stg_table, stg_sql) }}
    {% endset %}
    {% set result =  run_query(sql_stg) %}
    {% set stg_dict ={} %}
    {% set status = result.columns[0].values()[0]%}
    {% if 'successfully' in status %}
        {% do stg_dict.update( {"row_count":0,"job_activity" : "FULL APPLY","job_status" : "Successful",           
                "job_message" : 'Creating Stage',"model_name" : this.identifier,
                "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                
        {% do run_dict.update( {"stg_dict" : stg_dict} )%}
    {% else %}
        {% do stg_dict.update( {"row_count":0,"job_activity" : "FULL APPLY","job_status" : "Failed",           
                "job_message" : 'Creating Stage',"model_name" : this.identifier,
                "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                
        {% do run_dict.update( {"stg_dict" : stg_dict} )%}
        {{print('stg_sql_execution Failed')}}
    {% endif %}

    {# Getting Column Names and Removing ETL Columns  #}
    {%- set src_columns = (adapter.get_columns_in_relation(source_relation)) -%}
    {%- set src_non_etl_columns_csv = get_quoted_csv(src_columns | map(attribute="column")) -%}
    {%- set src_non_etl_columns_list = src_non_etl_columns_csv.replace(" ", "").split(',') %}
        
    {# {%- for column in src_non_etl_columns_list-%}
        
        {% set src_column = column.replace('"', '') %}
        {%- if(src_column.startswith('ETL_') or src_column.startswith('WATERMARK_')) or
           'JOB_NAME' in src_column or 'EFFECTIVE_START_DATETIME' in src_column or  'EFFECTIVE_END_DATETIME' in src_column-%}
           {{print(column)}}
            {%- do src_non_etl_columns_list.remove(column) %}
            alter table {{stg_table}} drop column {{ src_column}};
        {%  endif %}
    {% endfor %} #}
    {%- set tgt_table= target_database~'.'~target_schema~'.'~target_table-%}
    {%- set etl_list = (etl_columns.split(',')) -%}
    
    
    {%- if target_relation is none  -%} 
        {# Creating Target Table #}
        {% set sql_tgt %}
        {%- set tgt_sql %}
            select * from {{stg_table}} where 1=2
        {% endset -%}
        {{ create_table_as(false, tgt_table, tgt_sql) }}
        
        {# Adding ETL Columns #}
        ALTER TABLE {{tgt_table}} 
        ADD ( 

            {{etl_list[0]}} NUMBER(38,0),                      
            {{etl_list[1]}} NUMBER(38,0),
            {{etl_list[2]}} TIMESTAMP_LTZ(9),
            {{etl_list[3]}} TIMESTAMP_LTZ(9),
            {{etl_list[4]}} BOOLEAN,
            {{etl_list[5]}} BOOLEAN,
            {{etl_list[6]}} TIMESTAMP_LTZ(9),
            {{etl_list[7]}} TIMESTAMP_LTZ(9),
            {{etl_list[8]}} VARCHAR(200),
            {{etl_list[9]}} VARCHAR(200)
        );

        {# Adding Primary Keys #}
        ALTER TABLE {{tgt_table}}
        ADD PRIMARY KEY ({{primary_keys_csv}});
         {% endset %}
         {% set result = run_query(sql_tgt) %}
            {% set tgt_dict ={} %}
            {% set status = result.columns[0].values()[0]%}
            {% if 'successfully' in status %}
            {% do tgt_dict.update( {"row_count":0,"job_activity" : "FULL APPLY","job_status" : "Successful",           
                    "job_message" : 'Creating Target Table',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"tgt_dict" : tgt_dict} )%}
            {% else %}
            {% do tgt_dict.update( {"row_count":0,"job_activity" : "FULL APPLY","job_status" : "Failed",           
                    "job_message" : 'Creating Target Table',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"tgt_dict" : tgt_dict} )%}
        {% endif %}

    {% endif %}

    {# Renaming Columns without underscore and getting active records #}
    {%- set tgt_cur_vw = tgt_table~'_VW'-%}
    CREATE OR REPLACE TEMPORARY VIEW {{tgt_cur_vw}} AS (
        SELECT
            {% for col in src_non_etl_columns_list %}
                {{col}} as {{col.replace("_", " ")}}
                {% if not loop.last %}  
                    ,
                {% endif %}
            {% endfor %}
        FROM {{tgt_table}}
        WHERE {{etl_list[4]}} = TRUE AND {{etl_list[5]}} = FALSE
    ); 
    
    {# Creating CDC Table #}
    {%- set cdc_table = source_schema~'.'~source_table ~ "_CDC" -%}
    {% set sql_cdc %}
    CREATE OR REPLACE TABLE {{cdc_table}} AS 
    (
        SELECT
            {% for col in src_non_etl_columns_list %}
            CASE
                WHEN
                {% for key in primary_keys_list %}
                        src.{{key}} IS NULL
                    {% if not loop.last %}  
                        AND
                    {% endif %}
                {% endfor %}
                THEN tgt.{{col}}
                ELSE src.{{col}}
            END AS {{col}}
            ,
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
			    WHEN
                {% for key in primary_keys_list %}
                    src.{{key}} IS NULL 
                    AND      
                {% endfor %}
                tgt.{{etl_list[5]}} = FALSE 

                THEN 'Y'
			    ELSE 'N'
		    END AS {{etl_list[5]}},
            CASE
                WHEN
                {% for key in primary_keys_list %}
                        src.{{key}} IS NOT NULL AND
                {% endfor %}
                tgt.{{etl_list[5]}} = true
			    AND tgt.{{etl_list[4]}} = true 
                THEN 'Y'
                WHEN
                {% for column in src_non_etl_columns_list %}
                    {% if column not in primary_keys_list %}  
                        ((src.{{column}}= tgt.{{column}})
				        OR (src.{{column}} IS NULL
					    AND tgt.{{column}} IS NULL))
                        {% if not loop.last %}  
                        AND
                        {% endif %}
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
                WHEN
                {% for key in primary_keys_list %}
                        src.{{key}} IS NULL
                    {% if not loop.last %}  
                        AND
                    {% endif %}
                {% endfor %} 
                THEN 'N'
                ELSE 'Y'
            END AS ETL_Row_Updated_Flag,
		CURRENT_TIMESTAMP AS {{etl_list[2]}}
	FROM
    {{stg_table}} src
    FULL OUTER JOIN {{tgt_table}} tgt ON
    {% for key in primary_keys_list %}
        ((src.{{key}} = tgt.{{key}})
		OR (src.{{key}} IS NULL
		AND tgt.{{key}} IS NULL))
        {% if not loop.last %}  
            AND
        {% endif %}
    {% endfor %} 
    WHERE
    ({% for key in primary_keys_list %}
        tgt.{{key}} IS NULL
        {% if not loop.last %}  
            AND
        {% endif %}
    {% endfor %})
	OR tgt.{{etl_list[4]}} = TRUE
);

{# Setting Data data_retention_time for CDC table #}
ALTER TABLE {{cdc_table}} 
SET data_retention_time_in_days = 0;
{% endset %} 
        {% set result =  run_query(sql_cdc) %}
            {% set cdc_dict ={} %}
            {% set status = result.columns[0].values()[0]%}
            {% if 'successfully' in status %}
            {% do cdc_dict.update( {"row_count":0,"job_activity" : "FULL APPLY","job_status" : "Successful",           
                    "job_message" : 'Creating CDC',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"cdc_dict" : cdc_dict} )%}
            {{print('sql_stg_cdc executed')}}
            {% else %}
            {% do cdc_dict.update( {"row_count":0,"job_activity" : "FULL APPLY","job_status" : "Failed",           
                    "job_message" : 'Creating CDC',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"cdc_dict" : cdc_dict} )%}
            {{print('sql_stg_cdc failed')}}
            {% endif %} 

{# Update #}
{% set sql_update %}
UPDATE {{tgt_table}}
SET 
	{{etl_list[1]}} =  '{{etl_update_run_id}}', 
    {{etl_list[9]}} =  '{{etl_update_job_name}}',
	{{etl_list[3]}} =  DATEADD(millisecond,-1,src.{{etl_list[2]}}),
	{{etl_list[4]}} = 0,
	{{etl_list[7]}} = CURRENT_TIMESTAMP
FROM
	{{cdc_table}} src
    WHERE
	(src.ETL_Row_Updated_Flag = 'Y' 
		OR src.{{etl_list[5]}} = 'Y')
	AND {{etl_list[4]}} = TRUE
	AND (
        {% for i in primary_keys_list %}
                ( {{tgt_table}}.{{i}} = src.{{i}} ) OR
                    ({{tgt_table}}.{{i}} is NULL AND src.{{i}} is NULL )
                {% if not loop.last %}
                    and
                {% endif %}
        {% endfor %}        
        );
{% endset%}
{% if execute %}
            {% do run_query(sql_update) %}
            {% set update_dict ={} %}
            {% do update_dict.update( {"row_count":0,"job_activity" : "FULL APPLY","job_status" : "Successful",           
                    "job_message" : 'Updating Target',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"update_dict" : update_dict} )%}
            {{print('sql_update executed')}}
    {% else %}
            {% set update_dict ={} %}
            {% do update_dict.update( {"row_count":0,"job_activity" : "FULL APPLY","job_status" : "Failed",           
                    "job_message" : 'Updating Target',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
                    
            {% do run_dict.update( {"update_dict" : update_dict} )%}
            {{print('sql_update Failed')}}
    {% endif %}
{# Inserting  #}
{% set insert %}
INSERT INTO {{ tgt_table}}
select 
{% for col in src_non_etl_columns_list %}
                {{(col)}},
{% endfor %} 
{{etl_insert_run_id}} AS {{etl_list[0]}},
NULL AS {{etl_list[1]}},
{{etl_list[2]}},
CAST('9999-12-31 23:59:59.999' AS timestamp_ltz) AS {{etl_list[3]}},
TRUE AS {{etl_list[4]}},
CASE
    WHEN {{etl_list[5]}} = 'Y' THEN TRUE
    ELSE FALSE
END AS {{etl_list[5]}},
CURRENT_TIMESTAMP AS {{etl_list[6]}},
NULL AS {{etl_list[7]}},
'{{etl_insert_job_name}}' AS {{etl_list[8]}},
NULL AS {{etl_list[9]}}
from {{cdc_table}}  
where
    etl_row_updated_flag = 'Y' OR
    {{etl_list[5]}} = 'Y' OR
    etl_new_row_flag = 'Y' ;
    {% endset%}
{% if execute %}
            {% set result = run_query(insert) %}
            {% set return_value  = result.columns[0].values() %}
            {% set row_count = return_value[0] %}
            {% set insert_dict ={} %}
            {% do insert_dict.update( {"job_activity" : "FULL APPLY","job_status" : "Successful",           
                    "job_message" : 'Inserting Values',"model_name" : this.identifier,"row_count":row_count,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
            
            {% do insert_dict.update( {"row_count":row_count})%}
            {% do run_dict.update( {"insert_dict" : insert_dict} )%}
            {{print('insert executed')}}
    {% else %}
            {% set insert_dict ={} %}
            {% do insert_dict.update( {"job_activity" : "FULL APPLY","job_status" : "Failed",           
                    "job_message" : 'Inserting Values',"model_name" : this.identifier,
                    "created_timestamp":modules.datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]} )%}
            
            {% do insert_dict.update( {"row_count":row_count})%}
            {% do run_dict.update( {"insert_dict" : insert_dict} )%}
            {{print('insert Failed')}}

    {% endif %}
    {{ log_job_plan(run_dict,target_schema,target_table)}}

{%- endmacro -%} 