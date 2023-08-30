create or replace table one-global-dde-uat.bq_log_sink.ol55_cluster_dms_bkg_cntr
partition by date(edw_upd_dt) 
cluster by 
  act_mty_pkup_yd_cd,
  cntr_no,
  bkg_no,
  cop_no	

as 
  select * from one-global-dde-prod.DWH.DMS_BKG_CNTR
  where date(edw_upd_dt) between '2022-01-01' and  '2022-06-01'
