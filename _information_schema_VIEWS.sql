with schema_info as(
    select * from `one-global-dde-prod.region-asia-southeast1`.INFORMATION_SCHEMA.VIEWS
    --union all 
    --select * from `one-global-dde-insights-prod.region-asia-southeast1`.INFORMATION_SCHEMA.VIEWS
    --union all
    --select * from `one-global-dde-uat.region-asia-southeast1`.INFORMATION_SCHEMA.VIEWS
    --union all 
    --select * from `one-global-prod.region-asia-southeast1`.INFORMATION_SCHEMA.VIEWS

)

select 
    *
from schema_info
