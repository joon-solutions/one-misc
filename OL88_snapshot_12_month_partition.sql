CREATE OR REPLACE TABLE one-global-dde-uat.bq_log_sink.OL88_SNAPSHOT_12_MONTH_PARTITION

PARTITION BY DATE(SNAP_DT) --replaced from MONTH, changed to DATE
CLUSTER BY --identical clustering fields as original
    SNAP_YRWK,
    EQ_TPSZ_CD,
    EQ_LSTM_CD,
    EQ_MFT_DT
AS (
    SELECT
        *
    FROM
        `one-global-dde-uat.DWH.DML_CNTR_LTST_MVMT_SNAP`
    WHERE
        DATE(SNAP_DT) between '2022-08-01' and '2023-08-01'
)
