--CRNT_YD_CD
CREATE OR REPLACE TABLE one-global-dde-uat.bq_log_sink.OL88_SNAPSHOT_CLUSTER_ORIGINAL_OLD
PARTITION BY DATE_TRUNC(SNAP_DT, MONTH) --as original: partition by month
CLUSTER BY --identical clustering fields as original (found BEFORE Aug 28th)
    SNAP_YRWK,
    EQ_TPSZ_CD,
    EQ_LSTM_CD,
    EQ_MFT_DT
AS (
    SELECT
        *
    FROM
        `one-global-dde-prod.DWH.DML_CNTR_LTST_MVMT_SNAP`
    WHERE
        DATE(SNAP_DT) BETWEEN '2023-01-01' AND  '2023-06-01'
)
