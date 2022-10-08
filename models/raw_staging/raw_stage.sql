select 
    cust.c_custkey as cust_id,
    cust.c_name as cust_name,
    cust.c_address as cust_add,
    nat.n_name as cust_country,
    cust.c_phone as cust_phone,
    cust.c_acctbal as cust_acctbal,
    cust.effective_date,
    cust.ETL_ROW_DELETED_FLAG as ETL_ROW_DELETED_FLAG
from "DBT_DEV_DB"."SOURCE_SCHEMA"."CUST_SOURCE" as cust
INNER JOIN "DBT_DEV_DB"."SOURCE_SCHEMA"."NATION_SOURCE" as nat
ON cust.c_nationkey = nat.n_nationkey   