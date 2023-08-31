--CRNT_YD_CD
CREATE OR REPLACE TABLE one-global-dde-uat.bq_log_sink.OL88_SNAPSHOT_CLUSTER_ORIGINAL
PARTITION BY DATE_TRUNC(SNAP_DT, MONTH) --as original: partition by month
CLUSTER BY --identical clustering fields as original (found Aug 28th)
    N1ST_DY_SUN_FLG,
    CNMV_STS_CD,
    EQ_TPSZ_CD,
    CRNT_YD_CD
AS (
    SELECT
        *
    FROM
        `one-global-dde-prod.DWH.DML_CNTR_LTST_MVMT_SNAP`
    WHERE
        DATE(SNAP_DT) BETWEEN '2023-01-01' AND  '2023-06-01'
)
