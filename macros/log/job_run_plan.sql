{% macro log_job_plan(job_run_id,run_dict) %}



INSERT INTO audit.job_run_log

(

    job_run_id,

    job_activity,

    job_status,

    row_count,

    job_message,

    created_timestamp,

    model_name

)

Values

{% for job, col_dict in run_dict.items() %}

    (

        {{job_run_id}},

        '{{col_dict["job_activity"]}}',

        '{{col_dict["job_status"]}}',

        {{col_dict["row_count"]}},

        '{{col_dict["job_message"]}}',

        '{{col_dict["created_timestamp"]}}',

        '{{col_dict["model_name"]}}'

    )

    {% if not loop.last %}

        ,
    {% else %}
    ;
    {% endif %}

   

{% endfor %}



{% endmacro %}

