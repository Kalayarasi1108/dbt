{%- macro full_apply(target_database,target_schema,target_table,source_database,source_schema,source_table,primary_keys_csv,effective_start_date,effective_end_date,etl_insert_job_run_id,etl_update_job_run_id,etl_insert_job_name,etl_update_job_name,src_date_column,etl_columns) %}
    {# Getting Relation for source and target table using adapter function #}    
    
    {% set source_relation = adapter.get_relation(database=source_database,schema=source_schema,identifier=source_table) %}
    {% set target_relation = adapter.get_relation(database=target_database,schema=target_schema,identifier=target_table) %}
    {# Primary Key List  #}
    {%- set primary_keys_list = (primary_keys_csv.split(',')) -%}
    {# Creating stage table  #}
   
    {{print(primary_keys_list)}}

    {# Declaring stage file path #}
    {%- set stg_table = source_database~'.'~source_schema~'.'~source_table ~ "_STG" -%}
    {# select query for creating stage file from source #}
    {%- set stg_sql %}
        select * from {{source_database}}.{{source_schema}}.{{source_table}}
    {% endset -%}
    {{print("stage created successfully")}}
    {# Creating stage table using default function create_table_as(true[temporary]/false[transient],table path,sql ) #}
    {{ create_table_as(true, stg_table, stg_sql) }}


    {# Getting Column Names and Removing ETL Columns  #}
    {%- set src_columns = (adapter.get_columns_in_relation(source_relation)) -%}
    {%- set src_non_etl_columns_csv = get_quoted_csv(src_columns | map(attribute="column")) -%}
    {%- set src_non_etl_columns_list = src_non_etl_columns_csv.replace(" ", "").split(',') %}
 
    {%- for column in src_non_etl_columns_list-%}
        {% set src_column = column.replace('"', "") %}
        {%- if(src_column.startswith('ETL_') or src_column.startswith('WATERMARK_')) or
           'JOB_NAME' in src_column or 'EFFECTIVE_START_DATETIME' in src_column or  'EFFECTIVE_END_DATETIME' in src_column-%}
            {%- do src_non_etl_columns_list.remove(column) %}
            alter table {{stg_table}} drop column {{ src_column}};
        {%  endif %}
    {% endfor %}
    {%- set tgt_table= target_database~'.'~target_schema~'.'~target_table-%}
    {%- set etl_list = (etl_columns.split(',')) -%}
      {{print(etl_columns)}}
    {%- set i = etl_list[8] -%}
    {%- set j = etl_list[9] -%}
    
    
    {%- if target_relation is none  -%} 
        {# Creating Ta
        rget Table #}
        {{print("Target table created successfully")}}
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
            {{j}} VARCHAR(200)
        );

        {# Adding Primary Keys #}
        ALTER TABLE {{tgt_table}}
        ADD PRIMARY KEY ({{primary_keys_csv}});

    {% endif %}

    {# Renaming Columns without underscore and getting active records #}
    {%- set tgt_cur_vw = tgt_table~'_VW'-%}
    CREATE OR REPLACE VIEW {{tgt_cur_vw}} AS (
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
{{print("CDC created successfully")}}
{# Setting Data data_retention_time for CDC table #}
ALTER TABLE {{cdc_table}} 
SET data_retention_time_in_days = 0;

{# Update #}

{% set ETL_INSERT_JOB_RUN_ID_VALUE = '1022'%}
{% set ETL_UPDATE_JOB_NAME_VALUE = 'SAMPLE_TESTING_JOB_1'%}
{%- set e_time = modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] -%}
UPDATE {{tgt_table}}
SET
	{{etl_list[0]}} = {{ETL_INSERT_JOB_RUN_ID_VALUE}} , 
	{{j}} =  '{{ETL_UPDATE_JOB_NAME_VALUE}}', 
	{{etl_list[3]}} =  cast(src.{{etl_list[2]}} as timestamp_ntz) + interval '-1 second',
	{{etl_list[4]}} = 0,
	{{etl_list[7]}} = cast('{{e_time}}' as timestamp_ntz) 
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

{# Inserting  #}
{%- set ins_time = modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] -%}
{%- set up_time = modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] -%}
{%- set i= 'NULL' %}
{%- set j= 'NULL' %}
INSERT INTO {{ tgt_table}}
select 
{% for col in src_non_etl_columns_list %}
                {{col}},
{% endfor %} 
1022 AS {{etl_list[0]}},
1022 AS {{etl_list[1]}},
{{etl_list[2]}},
CAST('9999-12-31 23:59:59.999' AS timestamp_ltz) AS {{etl_list[3]}},
TRUE AS {{etl_list[4]}},
CASE
    WHEN {{etl_list[5]}} = 'Y' THEN TRUE
    ELSE FALSE
END AS {{etl_list[5]}},
'{{ins_time}}' AS {{etl_list[6]}},
'{{up_time}}' AS {{etl_list[7]}},
{{i}},
{{j}}
from {{cdc_table}}  
where
    etl_row_updated_flag = 'Y' OR
    {{etl_list[5]}} = 'Y' OR
    etl_new_row_flag = 'Y' ;
    {{print("value insearted to target table successfully")}}
{%- endmacro -%} 