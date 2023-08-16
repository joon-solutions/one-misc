with cte as (
  SELECT 
    PK,
    STAY_DYS

 FROM `one-global-dde-uat.bq_log_sink.OL88_SNAPSHOT_1_MONTH` WHERE SNAP_DT is not null
)

select
  sum(stay_dys), 'normal sum' as item
from cte

union all
select
  sum(stay_dys), 'distinct sum'
from (select distinct pk, stay_dys from cte)

