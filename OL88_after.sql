SELECT
    (DATE(dml_cntr_ltst_mvmt_snap.SNAP_DT )) AS dml_cntr_ltst_mvmt_snap_snap_dt_date,
    CASE
    WHEN DML_CNTR_LTST_MVMT_SNAP.EQ_TPSZ_CD = '*' THEN NULL
    WHEN DML_CNTR_LTST_MVMT_SNAP.EQ_TPSZ_CD = ' ' THEN NULL
    ELSE DML_CNTR_LTST_MVMT_SNAP.EQ_TPSZ_CD
    END   AS dml_cntr_ltst_mvmt_snap_eq_tpsz_cd,
    DML_CNTR_LTST_MVMT_SNAP.FULL_FLG AS dml_cntr_ltst_mvmt_snap_full_flg,
DML_CNTR_LTST_MVMT_SNAP.CNMV_STS_CD AS dml_cntr_ltst_mvmt_snap_cnmv_sts_cd,
    dml_cntr_ltst_mvmt_snap.CNMV_CO_CD  AS dml_cntr_ltst_mvmt_snap_cnmv_co_cd,
    dmc_lse_term.LSTM_NM  AS dmc_lse_term_lstm_nm,
    dml_cntr_ltst_mvmt_snap.STAY_DYS  AS dml_cntr_ltst_mvmt_snap_stay_dys,
    SUM(STAY_DYS ) AS dml_cntr_ltst_mvmt_snap_totald_stay_dys
FROM one-global-dde-uat.bq_log_sink.OL88_SNAPSHOT_1_MONTH
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