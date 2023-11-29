with a as (
  select * from one-global-dde-uat.DWH.DMS_BKG_MASTER
  where date(N1ST_POL_ETD_GDT) BETWEEN '2022-01-01' AND  '2022-06-01'

)
select
    count(distinct bkg_no) as bkg_no
from a
