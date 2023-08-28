--CRNT_YD_CD, EQ_TPSZ_CD
CREATE OR REPLACE TABLE one-global-dde-uat.bq_log_sink.OL88_SNAPSHOT_CLUSTER_3
PARTITION BY DATE_TRUNC(SNAP_DT, MONTH) --as original: partition by month
CLUSTER BY 
    EQ_TPSZ_CD
    CRNT_YD_CD,
    --CNMV_STS_CD,
    --N1ST_DY_SUN_FLG,
    
AS (
    SELECT
        *
    FROM
        `one-global-dde-uat.DWH.DML_CNTR_LTST_MVMT_SNAP`
    WHERE
        DATE(SNAP_DT) BETWEEN '2023-01-01' AND  '2023-06-01'
)