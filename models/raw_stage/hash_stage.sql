{%- set yaml_metadata -%}
source_model: raw_stage
derived_columns:
  RECORD_SOURCE: "!VAULT_DEMO"
  LOAD_DATETIME: effective_date
  EFFECTIVE_FROM: effective_date
hashed_columns:
  CUST_HK: "CUST_ID" 
  
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

WITH HASH_SOURCE AS(
{{ dbtvault.stage(include_source_columns=true,
                  source_model=metadata_dict['source_model'],
                  derived_columns=metadata_dict['derived_columns'],
                  null_columns=none,
                  hashed_columns=metadata_dict['hashed_columns'],
                  ranked_columns=none) }}
)
select * from HASH_SOURCE