CREATE OR REPLACE TABLE one-global-dde-uat.bq_log_sink.OL55_SNAPSHOT_CLUSTER_3
PARTITION BY DATE(EDW_UPD_DT) 
CLUSTER BY 
  BKG_NO,
  cntr_tpsz_cd,
  cntr_no

AS 
  select * from one-global-dde-uat.DWH.DMS_BKG_CNTR
  where date(EDW_UPD_DT) BETWEEN '2022-01-01' AND  '2022-06-01'
