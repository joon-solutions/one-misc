--CRNT_YD_CD, EQ_TPSZ_CD
CREATE OR REPLACE TABLE one-global-dde-uat.bq_log_sink.OL88_SNAPSHOT_CLUSTER_4
PARTITION BY DATE(SNAP_DT) 
CLUSTER BY 
    EQ_TPSZ_CD,
    CRNT_YD_CD,
    SNAP_DT
    --CNMV_STS_CD,
    --N1ST_DY_SUN_FLG,
    
AS (
    SELECT
        *
    EXCEPT(
            EQ_TPSZ_CD,
            FULL_FLG,
            EQ_OWN_VNDR_SEQ
        ),
        CASE
            WHEN EQ_TPSZ_CD = '*' THEN NULL
            WHEN EQ_TPSZ_CD = ' ' THEN NULL
            ELSE EQ_TPSZ_CD
        END AS EQ_TPSZ_CD,
        CASE
            WHEN FULL_FLG = '*' THEN NULL
            WHEN FULL_FLG = 'Y' THEN 'FULL'
            ELSE 'EMPTY'
        END AS FULL_FLG,
        CASE
            WHEN EQ_OWN_VNDR_SEQ = 0 THEN NULL
            ELSE EQ_OWN_VNDR_SEQ
        END AS EQ_OWN_VNDR_SEQ,
        MD5(
            SNAP_YRWK || --SNAPSHOT YEAR WEEK --> REDUNDANT
            DATE(SNAP_DT) || --SNAPSHOT DATE
            EQ_TPSZ_CD || --"CONTAINER TYPE SIZE CODE"
            EQ_LSTM_CD || --"CONTAINER LEASE TERM CODE"
            EQ_MFT_DT || EQ_OWN_VNDR_SEQ || --"CONTAINER LEASE LESSOR CODE"
            EQ_KND_CD || --EQUIPMENT KIND CODE
            CRNT_YD_CD || --CONTAINER MOVEMENT YARD CODE
            CRNT_EQ_CTRL_OFC_CD || --FK --
            CRNT_VVD_CD || BKG_CGO_TP_CD || --FK, BOOKING CARGO TYPE CODE
            CHSS_POOL_CD || -- CHASSIS POOL CODE
            CNMV_STS_CD || --FK
            FULL_FLG || --FK
            EQ_SUB_LSE_VNDR_SEQ || CNTR_AGMT_NO || CNMV_CO_CD || PRNT_OP_CO_CD || CRNT_OP_CO_CD || IO_BND_CD || RCV_DE_TERM_CD || N1ST_OB_FULL_CY_YD_CD || N1ST_LODG_YD_CD || LST_DCHG_YD_CD || LST_IB_FULL_CY_YD_CD || CNTR_STS_CD || DISP_FLG
        ) -- IS_DISP_PK
        AS PK
    FROM
        `one-global-dde-uat.DWH.DML_CNTR_LTST_MVMT_SNAP`
    WHERE
        DATE(SNAP_DT) BETWEEN '2023-01-01' AND  '2023-06-01'
)
