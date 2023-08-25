
-- DROP TABLE IF EXISTS one-global-dde-uat.bq_log_sink.OL88_SNAPSHOT_only_pk;
CREATE OR REPLACE TABLE one-global-dde-uat.bq_log_sink.OL88_SNAPSHOT_only_pk
PARTITION BY DATE_TRUNC(SNAP_DT, MONTH) --this is the original logic
CLUSTER BY --identical clustering fields as original
    SNAP_YRWK,
    EQ_TPSZ_CD,
    EQ_LSTM_CD,
    EQ_MFT_DT
    
AS (
    SELECT
        *,
        MD5(
            SNAP_YRWK || --SNAPSHOT YEAR WEEK --> REDUNDANT
            DATE(SNAP_DT) || --SNAPSHOT DATE
            EQ_TPSZ_CD || --"CONTAINER TYPE SIZE CODE"
            EQ_LSTM_CD || --"CONTAINER LEASE TERM CODE"
            EQ_MFT_DT || EQ_OWN_VNDR_SEQ || --"CONTAINER LEASE LESSOR CODE"
            EQ_KND_CD || --EQUIPMENT KIND CODE
            CRNT_YD_CD || --CONTAINER MOVEMENT YARD CODE
            CRNT_EQ_CTRL_OFC_CD || --FK --
            CRNT_VVD_CD || 
            BKG_CGO_TP_CD || --FK, BOOKING CARGO TYPE CODE
            CHSS_POOL_CD || -- CHASSIS POOL CODE
            CNMV_STS_CD || --FK
            FULL_FLG || --FK
            EQ_SUB_LSE_VNDR_SEQ || CNTR_AGMT_NO || CNMV_CO_CD || PRNT_OP_CO_CD || CRNT_OP_CO_CD || IO_BND_CD || RCV_DE_TERM_CD || N1ST_OB_FULL_CY_YD_CD || N1ST_LODG_YD_CD || LST_DCHG_YD_CD || LST_IB_FULL_CY_YD_CD || CNTR_STS_CD || DISP_FLG
        ) -- IS_DISP_PK
        AS PK
    FROM
        `one-global-dde-uat.DWH.DML_CNTR_LTST_MVMT_SNAP`
    --WHERE -- no more sample testing, this is FULL TABLE now
    --    DATE(SNAP_DT) between '2022-08-01' and '2023-08-01'
)
