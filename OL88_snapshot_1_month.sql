-- test first with 1 month
-- Fixes include: partition by date, cluster by fields, create ID field instead
-- then test with 2 months
-- compare if linear --> if not has to do full table clone
CREATE TABLE one-global-dde-uat.bq_log_sink.OL88_SNAPSHOT_1_MONTH 
  PARTITION BY date(SNAP_DT)
  CLUSTER BY EQ_TPSZ_CD, CNMV_STS_CD, N1ST_DY_SUN_FLG, CRNT_YD_CD
  AS (
      SELECT  
      *, 
      SNAP_YRWK || --snapshot year week --> redundant
          DATE(SNAP_DT) || --snapshot date
          EQ_TPSZ_CD || --"Container Type Size Code"
          EQ_LSTM_CD || --"Container Lease Term Code"
          EQ_MFT_DT ||
          EQ_OWN_VNDR_SEQ || --"Container Lease Lessor Code"
          EQ_KND_CD || --equipment kind code
          CRNT_YD_CD || --container movement yard code
          CRNT_EQ_CTRL_OFC_CD || --FK --
          crnt_vvd_cd ||
          BKG_CGO_TP_CD || --FK, booking cargo type code
          chss_pool_cd || -- Chassis Pool code
          cnmv_sts_cd || --FK
          full_flg || --FK
          eq_sub_lse_vndr_seq ||
          cntr_agmt_no ||
          cnmv_co_cd ||
          prnt_op_co_cd ||
          crnt_op_co_cd ||
          io_bnd_cd ||
          rcv_de_term_cd ||
          n1st_ob_full_cy_yd_cd ||
          n1st_lodg_yd_cd ||
          lst_dchg_yd_cd ||
          lst_ib_full_cy_yd_cd ||
          cntr_sts_cd ||
          DISP_FLG -- is_disp_pk
          as PK
      FROM `one-global-dde-uat.DWH.DML_CNTR_LTST_MVMT_SNAP` 
      where date(SNAP_DT) = '2022-10-01'
  )

