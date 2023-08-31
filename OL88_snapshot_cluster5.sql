CREATE OR REPLACE TABLE one-global-dde-uat.bq_log_sink.OL88_SNAPSHOT_CLUSTER_5
PARTITION BY DATE_TRUNC(SNAP_DT, MONTH) --as original: partition by month 
CLUSTER BY --as seen in 28th August
    N1ST_DY_SUN_FLG,
    EQ_TPSZ_CD,
    CRNT_YD_CD
AS (
    SELECT
        *
    FROM
        `one-global-dde-prod.DWH.DML_CNTR_LTST_MVMT_SNAP` -- take from prod instead
    WHERE
        DATE(SNAP_DT) BETWEEN '2023-01-01' AND  '2023-06-01'
)