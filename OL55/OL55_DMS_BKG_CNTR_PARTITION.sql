CREATE OR REPLACE TABLE one-global-dde-uat.bq_log_sink.OL55_DMS_BKG_CNTR_PARTITION

PARTITION BY DATE(EDW_UPD_DT) --replaced from MONTH, changed to DATE
CLUSTER BY --identical clustering fields as original
    BKG_NO,
    CNTR_TPSZ_CD,
    CNTR_NO
AS (
    SELECT
        *
    FROM
        `one-global-dde-prod.DWH.DMS_BKG_CNTR`
    WHERE
        DATE(EDW_UPD_DT) between '2022-08-01' and '2023-08-01'
)