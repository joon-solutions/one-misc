SELECT
    dmc_period_snap.YRWK_DESC  AS dmc_period_snap_yrwk_desc,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( dml_cntr_ltst_mvmt_snap.CNTR_TEU_QTY  ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( dml_cntr_ltst_mvmt_snap.SNAP_YRWK ||
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
          AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS dml_cntr_ltst_mvmt_snap_totald_cntr_teu_qty
FROM `DWH.DML_CNTR_LTST_MVMT_SNAP`
     AS dml_cntr_ltst_mvmt_snap
LEFT JOIN `DM_VIEWS.DMC_PERIOD_V`  AS dmc_period_snap ON REGEXP_REPLACE(cast(date(dml_cntr_ltst_mvmt_snap.SNAP_DT) as string),"-","")=dmc_period_snap.BSE_DT
WHERE ((UPPER(( dml_cntr_ltst_mvmt_snap.CRNT_OP_CO_CD  )) = UPPER('O'))) AND ((UPPER(( dml_cntr_ltst_mvmt_snap.EQ_LSTM_CD  )) <> UPPER('SH') OR ( dml_cntr_ltst_mvmt_snap.EQ_LSTM_CD  ) IS NULL)) AND ((UPPER(( FORMAT_DATETIME('%A', dml_cntr_ltst_mvmt_snap.SNAP_DT ) )) = UPPER('Monday'))) AND ((UPPER(( dml_cntr_ltst_mvmt_snap.CNTR_STS_CD  )) <> UPPER('MUO') AND UPPER(( dml_cntr_ltst_mvmt_snap.CNTR_STS_CD  )) <> UPPER('LSO') AND UPPER(( dml_cntr_ltst_mvmt_snap.CNTR_STS_CD  )) <> UPPER('SBO') AND UPPER(( dml_cntr_ltst_mvmt_snap.CNTR_STS_CD  )) <> UPPER('TLL') AND UPPER(( dml_cntr_ltst_mvmt_snap.CNTR_STS_CD  )) <> UPPER('MUI') OR ( dml_cntr_ltst_mvmt_snap.CNTR_STS_CD  ) IS NULL)) AND ((UPPER(( dmc_period_snap.BSE_YR  )) LIKE UPPER('2022%') OR UPPER(( dmc_period_snap.BSE_YR  )) LIKE UPPER('2023%') OR UPPER(( dmc_period_snap.BSE_YR  )) LIKE UPPER('2024%')))
and date(EDW_UPD_DT) BETWEEN '2022-01-01' AND  '2022-06-01'
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 500
