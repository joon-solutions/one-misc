/*
This is the logic behind DMC_EQ_TPSZ_CD_V, which is used in the Question 6 joins (clustered by CNTR_SZ_CD field)
--------
SELECT 
  TPSZ.CNTR_TPSZ_CD,      -----> this is the cluster field 
  TRIM(TPSZ.CNTR_TPSZ_DESC) CNTR_TPSZ_DESC,
  TPSZ.CNTR_SZ_CD,
  TRIM(REPLACE(SZ.CNTR_SZ_DESC,'CONTAINER', '')) AS CNTR_SZ_DESC,
  TPSZ.CNTR_TP_CD,
  TRIM(REPLACE(TP.CNTR_TP_DESC,'CONTAINER', '')) AS CNTR_TP_DESC,
  INTG_CD_VAL_CTNT EQ_KND_CD,
  TRIM(INTG_CD_VAL_DP_DESC) EQ_KND_DESC,
  RPT_DP_SEQ + 500 RPT_DP_SEQ,
  TPSZ.CNTR_TPSZ_ISO_CD
FROM `one-global-dde-prod.OPUS.DWC_CNTR_TP_SZ` TPSZ,   ---> I will test if this table, with clustered by built-in UPPER() vs non-upper case (as is) makes any difference
  `one-global-dde-prod.OPUS.DWC_CNTR_SZ` SZ,     
  `one-global-dde-prod.OPUS.DWC_CNTR_TP` TP ,
  `one-global-dde-prod.OPUS.DWC_COM_CD_DTL` CD
WHERE SZ.CNTR_SZ_CD = TPSZ.CNTR_SZ_CD
  AND TP.CNTR_TP_CD = TPSZ.CNTR_TP_CD
  AND CD.INTG_CD_ID = 'CD01132'
  AND CD.INTG_CD_VAL_CTNT = 'U'
UNION ALL
SELECT 
  TPSZ.EQ_TPSZ_CD,
  TPSZ.DIFF_DESC,
  CAST(TPSZ.EQ_TPSZ_REP_CD AS STRING),
  CAST(TPSZ.EQ_TPSZ_REP_CD AS STRING),
  SUBSTR(TPSZ.EQ_TPSZ_CD, 1, 2) EQ_TP ,
  TPSZ.DIFF_DESC ,
  CD.INTG_CD_VAL_CTNT,
  CD.INTG_CD_VAL_DP_DESC,
  TPSZ.DP_SEQ + 1000 RPT_DP_SEQ,
  '' AS CNTR_TPSZ_ISO_CD
from `one-global-dde-prod.OPUS.DWL_EQ_TP_SZ` TPSZ ,    
  `one-global-dde-prod.OPUS.DWC_COM_CD_DTL` CD
WHERE CD.INTG_CD_ID = 'CD01132'
  AND TPSZ.EQ_KND_CD = CD.INTG_CD_VAL_CTNT
*/

---- FIRST, replace the table underneath the view
CREATE TABLE one-global-dde-uat.bq_log_sink.DWC_CNTR_TP_SZ_upper_cluster

CLUSTER BY CNTR_SZ_CD --cannot apply UPPER here
AS (
  SELECT 
    * except(CNTR_SZ_CD),
    UPPER(CNTR_SZ_CD) as CNTR_SZ_CD
  FROM one-global-dde-prod.OPUS.DWC_CNTR_TP_SZ -- this table has only 34 records, so clustering doesn't make sense here much.
)
;

---- SECOND, replace the table in the VIEW
CREATE VIEW one-global-dde-uat.bq_log_sink.DMC_EQ_TPSZ_CD_V_upper_cluster
AS (
      SELECT 
  TPSZ.CNTR_TPSZ_CD,
  TRIM(TPSZ.CNTR_TPSZ_DESC) CNTR_TPSZ_DESC,
  TPSZ.CNTR_SZ_CD,
  TRIM(REPLACE(SZ.CNTR_SZ_DESC,'CONTAINER', '')) AS CNTR_SZ_DESC,
  TPSZ.CNTR_TP_CD,
  TRIM(REPLACE(TP.CNTR_TP_DESC,'CONTAINER', '')) AS CNTR_TP_DESC,
  INTG_CD_VAL_CTNT EQ_KND_CD,
  TRIM(INTG_CD_VAL_DP_DESC) EQ_KND_DESC,
  RPT_DP_SEQ + 500 RPT_DP_SEQ,
  TPSZ.CNTR_TPSZ_ISO_CD
FROM one-global-dde-uat.bq_log_sink.DWC_CNTR_TP_SZ_upper_cluster TPSZ,
  `one-global-dde-prod.OPUS.DWC_CNTR_SZ` SZ,
  `one-global-dde-prod.OPUS.DWC_CNTR_TP` TP ,
  `one-global-dde-prod.OPUS.DWC_COM_CD_DTL` CD
WHERE SZ.CNTR_SZ_CD = TPSZ.CNTR_SZ_CD
  AND TP.CNTR_TP_CD = TPSZ.CNTR_TP_CD
  AND CD.INTG_CD_ID = 'CD01132'
  AND CD.INTG_CD_VAL_CTNT = 'U'
UNION ALL
SELECT 
  TPSZ.EQ_TPSZ_CD,
  TPSZ.DIFF_DESC,
  CAST(TPSZ.EQ_TPSZ_REP_CD AS STRING),
  CAST(TPSZ.EQ_TPSZ_REP_CD AS STRING),
  SUBSTR(TPSZ.EQ_TPSZ_CD, 1, 2) EQ_TP ,
  TPSZ.DIFF_DESC ,
  CD.INTG_CD_VAL_CTNT,
  CD.INTG_CD_VAL_DP_DESC,
  TPSZ.DP_SEQ + 1000 RPT_DP_SEQ,
  '' AS CNTR_TPSZ_ISO_CD
from `one-global-dde-prod.OPUS.DWL_EQ_TP_SZ` TPSZ ,            # TO CHANGE
  `one-global-dde-prod.OPUS.DWC_COM_CD_DTL` CD
WHERE CD.INTG_CD_ID = 'CD01132'
  AND TPSZ.EQ_KND_CD = CD.INTG_CD_VAL_CTNT

)
