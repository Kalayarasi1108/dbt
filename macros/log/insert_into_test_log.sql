{% macro insert_into_test_log(model_name = '', test_result = 'SUCCESSFUL', test_name = '') %}

    {% set sql %}
        INSERT INTO audit.test_log (
            model_name,
            test_result,
            test_name,
            test_timestamp
            )
        VALUES
            ('{{model_name}}', '{{test_result}}', '{{test_name}}', current_timestamp());
    {% endset %}
    
    {% if execute %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}