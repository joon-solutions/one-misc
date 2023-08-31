--CRNT_YD_CD
CREATE OR REPLACE TABLE one-global-dde-uat.bq_log_sink.OL88_SNAPSHOT_CLUSTER_1
PARTITION BY DATE_TRUNC(SNAP_DT, MONTH) --as original: partition by month
CLUSTER BY 
    CRNT_YD_CD
    --CNMV_STS_CD,
    --N1ST_DY_SUN_FLG,
    --EQ_TPSZ_CD
AS (
    SELECT
        *
    FROM
        `one-global-dde-uat.DWH.DML_CNTR_LTST_MVMT_SNAP`
    WHERE
        DATE(SNAP_DT) BETWEEN '2023-01-01' AND  '2023-06-01'
)
