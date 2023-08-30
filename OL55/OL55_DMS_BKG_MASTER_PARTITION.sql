CREATE OR REPLACE TABLE one-global-dde-uat.bq_log_sink.OL55_DMS_BKG_MASTER_PARTITION

PARTITION BY DATE(N1ST_POL_ETD_GDT) --replaced from MONTH, changed to DATE
CLUSTER BY --identical clustering fields as original
    BKG_NO
AS (
    SELECT
        *
    FROM
        `one-global-dde-uat.DEV_DWH.DMS_BKG_MASTER`
    WHERE
        DATE(N1ST_POL_ETD_GDT) between '2022-08-01' and '2023-08-01'
)