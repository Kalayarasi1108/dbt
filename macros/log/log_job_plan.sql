{% macro log_job_plan(run_dict,target_schema,target_table) %}

{% set get_invocation_id %}
        select 
            invocation_id, event_user
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
    {% set id = result.columns[0].values() %}
    {% set user = result.columns[1].values() %}
{% endif %}
INSERT INTO audit.job_run_log 
(
    invocation_id,
    event_user,
    job_activity,
    job_status,
    row_count,
    job_message,
    created_timestamp,
    target_table,
    target_schema,
    model_name
)
Values
{% for job, job_return_dict in run_dict.items() %}
    (
        '{{id[0]}}',
        '{{user[0]}}',
        '{{job_return_dict["job_activity"]}}',
        '{{job_return_dict["job_status"]}}',
        {{job_return_dict["row_count"]}},
        '{{job_return_dict["job_message"]}}',
        '{{job_return_dict["created_timestamp"]}}',
        '{{target_table}}',
        '{{target_schema}}',
        '{{job_return_dict["model_name"]}}'
    )
    {% if not loop.last %}
        ,
    {% else %}
    ;
    {% endif %}
    
{% endfor %}

{% endmacro %}