SELECT
    (DATE(dml_cntr_ltst_mvmt_snap.SNAP_DT )) AS dml_cntr_ltst_mvmt_snap_snap_dt_date,
    CASE
    WHEN DML_CNTR_LTST_MVMT_SNAP.EQ_TPSZ_CD = '*' THEN NULL
    WHEN DML_CNTR_LTST_MVMT_SNAP.EQ_TPSZ_CD = ' ' THEN NULL
    ELSE DML_CNTR_LTST_MVMT_SNAP.EQ_TPSZ_CD
    END   AS dml_cntr_ltst_mvmt_snap_eq_tpsz_cd,
    CASE
          WHEN DML_CNTR_LTST_MVMT_SNAP.FULL_FLG = '*' THEN NULL
          WHEN DML_CNTR_LTST_MVMT_SNAP.FULL_FLG = 'Y' THEN 'Full'
          ELSE 'Empty'
          END   AS dml_cntr_ltst_mvmt_snap_full_flg,
    CASE
          WHEN DML_CNTR_LTST_MVMT_SNAP.CNMV_STS_CD = '*' THEN NULL
          ELSE DML_CNTR_LTST_MVMT_SNAP.CNMV_STS_CD
          END AS dml_cntr_ltst_mvmt_snap_cnmv_sts_cd,
    dml_cntr_ltst_mvmt_snap.CNMV_CO_CD  AS dml_cntr_ltst_mvmt_snap_cnmv_co_cd,
    dmc_lse_term.LSTM_NM  AS dmc_lse_term_lstm_nm,
    dml_cntr_ltst_mvmt_snap.STAY_DYS  AS dml_cntr_ltst_mvmt_snap_stay_dys,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( dml_cntr_ltst_mvmt_snap.STAY_DYS  ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( dml_cntr_ltst_mvmt_snap.SNAP_YRWK ||
          (DATE(dml_cntr_ltst_mvmt_snap.SNAP_DT )) ||
          dml_cntr_ltst_mvmt_snap.EQ_TPSZ_CD ||
          dml_cntr_ltst_mvmt_snap.EQ_LSTM_CD ||
          dml_cntr_ltst_mvmt_snap.EQ_MFT_DT ||
          dml_cntr_ltst_mvmt_snap.EQ_OWN_VNDR_SEQ ||
          dml_cntr_ltst_mvmt_snap.EQ_KND_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_YD_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_EQ_CTRL_OFC_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_VVD_CD ||
          dml_cntr_ltst_mvmt_snap.BKG_CGO_TP_CD ||
          dml_cntr_ltst_mvmt_snap.CHSS_POOL_CD ||
          dml_cntr_ltst_mvmt_snap.CNMV_STS_CD ||
          dml_cntr_ltst_mvmt_snap.FULL_FLG ||
          dml_cntr_ltst_mvmt_snap.EQ_SUB_LSE_VNDR_SEQ ||
          dml_cntr_ltst_mvmt_snap.CNTR_AGMT_NO ||
          dml_cntr_ltst_mvmt_snap.CNMV_CO_CD ||
          dml_cntr_ltst_mvmt_snap.PRNT_OP_CO_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_OP_CO_CD ||
          dml_cntr_ltst_mvmt_snap.IO_BND_CD ||
          dml_cntr_ltst_mvmt_snap.RCV_DE_TERM_CD ||
          dml_cntr_ltst_mvmt_snap.N1ST_OB_FULL_CY_YD_CD ||
          dml_cntr_ltst_mvmt_snap.N1ST_LODG_YD_CD ||
          dml_cntr_ltst_mvmt_snap.LST_DCHG_YD_CD ||
          dml_cntr_ltst_mvmt_snap.LST_IB_FULL_CY_YD_CD ||
          dml_cntr_ltst_mvmt_snap.CNTR_STS_CD ||
          dml_cntr_ltst_mvmt_snap.DISP_FLG
          AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( dml_cntr_ltst_mvmt_snap.SNAP_YRWK ||
          (DATE(dml_cntr_ltst_mvmt_snap.SNAP_DT )) ||
          dml_cntr_ltst_mvmt_snap.EQ_TPSZ_CD ||
          dml_cntr_ltst_mvmt_snap.EQ_LSTM_CD ||
          dml_cntr_ltst_mvmt_snap.EQ_MFT_DT ||
          dml_cntr_ltst_mvmt_snap.EQ_OWN_VNDR_SEQ ||
          dml_cntr_ltst_mvmt_snap.EQ_KND_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_YD_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_EQ_CTRL_OFC_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_VVD_CD ||
          dml_cntr_ltst_mvmt_snap.BKG_CGO_TP_CD ||
          dml_cntr_ltst_mvmt_snap.CHSS_POOL_CD ||
          dml_cntr_ltst_mvmt_snap.CNMV_STS_CD ||
          dml_cntr_ltst_mvmt_snap.FULL_FLG ||
          dml_cntr_ltst_mvmt_snap.EQ_SUB_LSE_VNDR_SEQ ||
          dml_cntr_ltst_mvmt_snap.CNTR_AGMT_NO ||
          dml_cntr_ltst_mvmt_snap.CNMV_CO_CD ||
          dml_cntr_ltst_mvmt_snap.PRNT_OP_CO_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_OP_CO_CD ||
          dml_cntr_ltst_mvmt_snap.IO_BND_CD ||
          dml_cntr_ltst_mvmt_snap.RCV_DE_TERM_CD ||
          dml_cntr_ltst_mvmt_snap.N1ST_OB_FULL_CY_YD_CD ||
          dml_cntr_ltst_mvmt_snap.N1ST_LODG_YD_CD ||
          dml_cntr_ltst_mvmt_snap.LST_DCHG_YD_CD ||
          dml_cntr_ltst_mvmt_snap.LST_IB_FULL_CY_YD_CD ||
          dml_cntr_ltst_mvmt_snap.CNTR_STS_CD ||
          dml_cntr_ltst_mvmt_snap.DISP_FLG
          AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( dml_cntr_ltst_mvmt_snap.SNAP_YRWK ||
          (DATE(dml_cntr_ltst_mvmt_snap.SNAP_DT )) ||
          dml_cntr_ltst_mvmt_snap.EQ_TPSZ_CD ||
          dml_cntr_ltst_mvmt_snap.EQ_LSTM_CD ||
          dml_cntr_ltst_mvmt_snap.EQ_MFT_DT ||
          dml_cntr_ltst_mvmt_snap.EQ_OWN_VNDR_SEQ ||
          dml_cntr_ltst_mvmt_snap.EQ_KND_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_YD_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_EQ_CTRL_OFC_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_VVD_CD ||
          dml_cntr_ltst_mvmt_snap.BKG_CGO_TP_CD ||
          dml_cntr_ltst_mvmt_snap.CHSS_POOL_CD ||
          dml_cntr_ltst_mvmt_snap.CNMV_STS_CD ||
          dml_cntr_ltst_mvmt_snap.FULL_FLG ||
          dml_cntr_ltst_mvmt_snap.EQ_SUB_LSE_VNDR_SEQ ||
          dml_cntr_ltst_mvmt_snap.CNTR_AGMT_NO ||
          dml_cntr_ltst_mvmt_snap.CNMV_CO_CD ||
          dml_cntr_ltst_mvmt_snap.PRNT_OP_CO_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_OP_CO_CD ||
          dml_cntr_ltst_mvmt_snap.IO_BND_CD ||
          dml_cntr_ltst_mvmt_snap.RCV_DE_TERM_CD ||
          dml_cntr_ltst_mvmt_snap.N1ST_OB_FULL_CY_YD_CD ||
          dml_cntr_ltst_mvmt_snap.N1ST_LODG_YD_CD ||
          dml_cntr_ltst_mvmt_snap.LST_DCHG_YD_CD ||
          dml_cntr_ltst_mvmt_snap.LST_IB_FULL_CY_YD_CD ||
          dml_cntr_ltst_mvmt_snap.CNTR_STS_CD ||
          dml_cntr_ltst_mvmt_snap.DISP_FLG
          AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( dml_cntr_ltst_mvmt_snap.SNAP_YRWK ||
          (DATE(dml_cntr_ltst_mvmt_snap.SNAP_DT )) ||
          dml_cntr_ltst_mvmt_snap.EQ_TPSZ_CD ||
          dml_cntr_ltst_mvmt_snap.EQ_LSTM_CD ||
          dml_cntr_ltst_mvmt_snap.EQ_MFT_DT ||
          dml_cntr_ltst_mvmt_snap.EQ_OWN_VNDR_SEQ ||
          dml_cntr_ltst_mvmt_snap.EQ_KND_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_YD_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_EQ_CTRL_OFC_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_VVD_CD ||
          dml_cntr_ltst_mvmt_snap.BKG_CGO_TP_CD ||
          dml_cntr_ltst_mvmt_snap.CHSS_POOL_CD ||
          dml_cntr_ltst_mvmt_snap.CNMV_STS_CD ||
          dml_cntr_ltst_mvmt_snap.FULL_FLG ||
          dml_cntr_ltst_mvmt_snap.EQ_SUB_LSE_VNDR_SEQ ||
          dml_cntr_ltst_mvmt_snap.CNTR_AGMT_NO ||
          dml_cntr_ltst_mvmt_snap.CNMV_CO_CD ||
          dml_cntr_ltst_mvmt_snap.PRNT_OP_CO_CD ||
          dml_cntr_ltst_mvmt_snap.CRNT_OP_CO_CD ||
          dml_cntr_ltst_mvmt_snap.IO_BND_CD ||
          dml_cntr_ltst_mvmt_snap.RCV_DE_TERM_CD ||
          dml_cntr_ltst_mvmt_snap.N1ST_OB_FULL_CY_YD_CD ||
          dml_cntr_ltst_mvmt_snap.N1ST_LODG_YD_CD ||
          dml_cntr_ltst_mvmt_snap.LST_DCHG_YD_CD ||
          dml_cntr_ltst_mvmt_snap.LST_IB_FULL_CY_YD_CD ||
          dml_cntr_ltst_mvmt_snap.CNTR_STS_CD ||
          dml_cntr_ltst_mvmt_snap.DISP_FLG
          AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS dml_cntr_ltst_mvmt_snap_totald_stay_dys
FROM `DWH.DML_CNTR_LTST_MVMT_SNAP`
     AS dml_cntr_ltst_mvmt_snap
LEFT JOIN `DM_VIEWS.DMC_LSE_TERM_V` AS dmc_lse_term ON dml_cntr_ltst_mvmt_snap.EQ_LSTM_CD=dmc_lse_term.LSTM_CD
WHERE ((UPPER(( dml_cntr_ltst_mvmt_snap.CNMV_CO_CD  )) = UPPER('ONE'))) AND (((( dml_cntr_ltst_mvmt_snap.SNAP_DT  ) >= ((DATETIME(TIMESTAMP('2022-10-01 00:00:00')))) AND ( dml_cntr_ltst_mvmt_snap.SNAP_DT  ) < ((DATETIME_ADD(DATETIME(TIMESTAMP('2022-10-01 00:00:00')), INTERVAL 1 DAY))))) OR ((( dml_cntr_ltst_mvmt_snap.SNAP_DT  ) >= ((DATETIME(TIMESTAMP('2022-10-08 00:00:00')))) AND ( dml_cntr_ltst_mvmt_snap.SNAP_DT  ) < ((DATETIME_ADD(DATETIME(TIMESTAMP('2022-10-08 00:00:00')), INTERVAL 1 DAY))))) OR ((( dml_cntr_ltst_mvmt_snap.SNAP_DT  ) >= ((DATETIME(TIMESTAMP('2022-10-15 00:00:00')))) AND ( dml_cntr_ltst_mvmt_snap.SNAP_DT  ) < ((DATETIME_ADD(DATETIME(TIMESTAMP('2022-10-15 00:00:00')), INTERVAL 1 DAY))))) OR ((( dml_cntr_ltst_mvmt_snap.SNAP_DT  ) >= ((DATETIME(TIMESTAMP('2022-10-22 00:00:00')))) AND ( dml_cntr_ltst_mvmt_snap.SNAP_DT  ) < ((DATETIME_ADD(DATETIME(TIMESTAMP('2022-10-22 00:00:00')), INTERVAL 1 DAY))))) OR ((( dml_cntr_ltst_mvmt_snap.SNAP_DT  ) >= ((DATETIME(TIMESTAMP('2022-10-29 00:00:00')))) AND ( dml_cntr_ltst_mvmt_snap.SNAP_DT  ) < ((DATETIME_ADD(DATETIME(TIMESTAMP('2022-10-29 00:00:00')), INTERVAL 1 DAY))))))
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7
ORDER BY
    1 DESC