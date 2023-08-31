WITH dmc_sls_wk AS (select distinct
            BSE_YRWK
          , YRWK_DESC
          , MRKMON_DESC
          , ACC_MRKQTR_DESC
          , ACC_MRKYR
      from `DM_VIEWS.DMC_PERIOD_V` )
SELECT
    dmc_sls_wk.ACC_MRKYR  AS dmc_sls_wk_acc_mrkyr,
    COALESCE(SUM(dms_bkg_cntr.CNTR_TEU_QTY ), 0) AS dms_bkg_cntr_total_cntr_teu_qty
FROM `DWH.DMS_BKG_CNTR`  AS dms_bkg_cntr
LEFT JOIN one-global-dde-uat.bq_log_sink.ol55_cluster_dms_bkg_master AS dms_bkg_master ON dms_bkg_cntr.BKG_NO=dms_bkg_master.BKG_NO
LEFT JOIN `DM_VIEWS.DMC_OFC_LM_V`  AS dmc_office_bkg_cre ON dmc_office_bkg_cre.OFC_CD=dms_bkg_master.BKG_OFC_CD
LEFT JOIN `DM_VIEWS.DMC_BKG_STS_V`  AS dmc_bkg_sts ON dms_bkg_master.BKG_STS_CD=dmc_bkg_sts.BKG_STS_CD
LEFT JOIN `DM_VIEWS.DMC_BKG_CGO_TP_V`  AS dmc_bkg_cgo_tp ON dmc_bkg_cgo_tp.BKG_CGO_TP_CD=dms_bkg_master.BKG_CGO_TP_CD
LEFT JOIN `DM_VIEWS.DMC_SVC_SCP_V`  AS dmc_svc_scp ON dmc_svc_scp.SVC_SCP_CD=dms_bkg_master.SVC_SCP_CD
LEFT JOIN `DM_VIEWS.DMC_CUSTOMER_LM_V`  AS dmc_customer_bkg_ctrl ON dms_bkg_master.BKG_CTRL_PTY_CUST_CD=dmc_customer_bkg_ctrl.CUST_CD
LEFT JOIN `DM_VIEWS.DMC_LOCATION_V` AS dmc_loc_del ON dms_bkg_master.DEL_CD=dmc_loc_del.LOC_CD
LEFT JOIN dmc_sls_wk ON dms_bkg_master.cost_yrwk=dmc_sls_wk.BSE_YRWK
where dms_bkg_master.trd_cd = 'WAT'
  and dms_bkg_master.rlane_cd = 'AR1WA'

GROUP BY
    1
ORDER BY
    1
LIMIT 50