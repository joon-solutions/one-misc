create or replace table one-global-dde-uat.bq_log_sink.ol55_cluster_dms_bkg_master
partition by date(edw_upd_dt) 
cluster by 
  trd_cd,
  rlane_cd,
  inter_rmk,
  bkg_no

as 
  select * from one-global-dde-uat.DWH.DMS_BKG_MASTER
  where date(n1st_pol_etd_gdt) between '2022-01-01' and  '2022-06-01'
