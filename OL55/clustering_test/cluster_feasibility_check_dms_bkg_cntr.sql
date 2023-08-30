with a as (
  select * from one-global-dde-uat.DWH.DMS_BKG_CNTR
  where date(EDW_UPD_DT) BETWEEN '2022-01-01' AND  '2022-06-01'

)
select
    count(distinct bkg_no) as bkg_no,
    count(distinct cntr_tpsz_cd) as cntr_tpsz_cd,
    count(distinct cntr_no) as cntr_no
from a
