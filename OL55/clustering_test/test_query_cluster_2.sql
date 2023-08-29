SELECT
    dms_bkg_master.BKG_CGO_TP_CD  AS dms_bkg_master_bkg_cgo_tp_cd,
    dms_bkg_master.BKG_NO  AS dms_bkg_master_bkg_no,
    dmc_bkg_rqst.XTER_BKG_RQST_CD  AS dmc_bkg_rqst_xter_rqst_cd,
    dmc_bkg_rqst.XTER_BKG_RQST_NM  AS dmc_bkg_rqst_xter_rqst_nm,
    dms_bkg_master.BKG_REF_NO  AS dms_bkg_master_bkg_ref_no,
    dms_bkg_master.BKG_SH_REF_NO  AS dms_bkg_master_bkg_shpr_ref_no,
    dms_bkg_master.BKG_FRT_FWRD_REF_NO  AS dms_bkg_master_bkg_frt_fwrd_ref_no,
    dmc_bkg_rqst_si.XTER_BKG_RQST_CD  AS dmc_bkg_rqst_si_xter_rqst_cd,
    dms_bkg_master.SI_REF_NO  AS dms_bkg_master_si_ref_no,
    dms_bkg_master.SI_SH_REF_NO  AS dms_bkg_master_si_sh_ref_no,
    dms_bkg_master.SI_FRT_FWRD_REF_NO  AS dms_bkg_master_si_frt_fwrd_ref_no,
    dmc_bkg_r_d_term_cntr.bkg_rd_term_nm  AS dmc_bkg_r_d_term_cntr_bkg_rd_term_nm,
    dms_bkg_cntr.CNTR_NO  AS dms_bkg_cntr_cntr_no,
    dmc_bkg_sts.BKG_STS_CD_DESC  AS dmc_bkg_sts_bkg_sts_desc,
    dms_bkg_master.SC_RFA_NO  AS dms_bkg_master_ctrt_no,
    dmc_customer_ctrt.CUST_CD  AS dmc_customer_ctrt_cust_cd,
    dmc_customer_ctrt.CUST_NM  AS dmc_customer_ctrt_cust_nm,
    dmc_customer_bkg_ctrl.CUST_CD  AS dmc_customer_bkg_ctrl_cust_cd,
    dmc_customer_bkg_ctrl.CUST_NM  AS dmc_customer_bkg_ctrl_cust_nm,
    dmc_customer_shpr.CUST_CD  AS dmc_customer_shpr_cust_cd,
    dmc_customer_shpr.CUST_NM  AS dmc_customer_shpr_cust_nm,
    dmc_customer_cnee.CUST_CD  AS dmc_customer_cnee_cust_cd,
    dmc_customer_cnee.CUST_NM  AS dmc_customer_cnee_cust_nm,
    dmc_customer_fwrd.CUST_CD  AS dmc_customer_fwrd_cust_cd,
    dmc_customer_fwrd.CUST_NM  AS dmc_customer_fwrd_cust_nm,
    dmc_customer_ntfy.CUST_CD  AS dmc_customer_ntfy_cust_cd,
    dmc_customer_ntfy.CUST_NM  AS dmc_customer_ntfy_cust_nm,
    dmc_customer_antfy.CUST_CD  AS dmc_customer_antfy_cust_cd,
    dmc_customer_antfy.CUST_NM  AS dmc_customer_antfy_cust_nm,
    dmc_customer_esi_bkg.CUST_CD  AS dmc_customer_esi_bkg_cust_cd,
    dmc_customer_esi_bkg.CUST_NM  AS dmc_customer_esi_bkg_cust_nm,
        (FORMAT_TIMESTAMP('%F %T', timestamp( dms_bkg_master.XTER_RQST_RQST_DT ) )) AS dms_bkg_master_bkg_rqst_dt_time,
        (FORMAT_TIMESTAMP('%F %T', timestamp(dms_bkg_cntr.ACT_MTY_PKUP_DT ) )) AS dms_bkg_cntr_act_mty_pkup_dt_time,
    dmc_yard_act_mty_pkup.nod_nm  AS dmc_yard_act_mty_pkup_yd_nm,
        (FORMAT_TIMESTAMP('%F %T', timestamp( dms_bkg_cntr.CGO_RCV_DT ) )) AS dms_bkg_cntr_cgo_rcv_dt_time,
    dmc_loc_por.CNT_CD  AS dmc_loc_por_cnt_cd,
    dmc_loc_por.LOC_NM  AS dmc_loc_por_loc_nm,
    dmc_trs_mod_ob.TRS_MOD_NM  AS dmc_trs_mod_ob_trs_mod_nm,
    dmc_yard_n1st_pol.LOC_NM  AS dmc_yard_n1st_pol_loc_nm,
    dmc_yard_trnk_pol.LOC_NM  AS dmc_yard_trnk_pol_loc_nm,
    dmc_yard_trnk_pod.LOC_NM  AS dmc_yard_trnk_pod_loc_nm,
    dmc_yard_trnk_pod.nod_nm  AS dmc_yard_trnk_pod_yd_nm,
    dmc_yard_trnk_pod.nod_cd  AS dmc_yard_trnk_pod_yd_cd,
    dmc_yard_lst_pod.LOC_NM  AS dmc_yard_lst_pod_loc_nm,
    dmc_yard_lst_pod.nod_nm  AS dmc_yard_lst_pod_yd_nm,
    dmc_yard_lst_pod.nod_cd  AS dmc_yard_lst_pod_yd_cd,
    dmc_trs_mod_ib.TRS_MOD_NM  AS dmc_trs_mod_ib_trs_mod_nm,
    dmc_loc_del.LOC_NM  AS dmc_loc_del_loc_nm,
    dmc_yard_del.nod_nm  AS dmc_yard_del_yd_nm,
    dmc_yard_del.nod_cd  AS dmc_yard_del_yd_cd,
    dmc_loc_del.CNT_CD  AS dmc_loc_del_cnt_cd,
        (FORMAT_TIMESTAMP('%F %T', timestamp( dms_bkg_cntr.LST_IB_HUB_AVAL_DT ) )) AS dms_bkg_cntr_lst_ib_hub_aval_dt_time,
        (FORMAT_TIMESTAMP('%F %T', timestamp( dms_bkg_cntr.FULL_GATE_OUT_DT ) )) AS dms_bkg_cntr_full_gate_out_dt_time,
        (FORMAT_TIMESTAMP('%F %T', timestamp( dms_bkg_cntr.ACT_MTY_RTN_DT ) )) AS dms_bkg_cntr_act_mty_rtn_dt_time,
    dmc_yard_act_mty_rtn.nod_nm  AS dmc_yard_act_mty_rtn_yd_nm,
        (FORMAT_TIMESTAMP('%F %T', timestamp( dms_bkg_master.POD_ETA_DT ) )) AS dms_bkg_master_lst_pod_eta_dt_time,
        (FORMAT_TIMESTAMP('%F %T', timestamp(dms_bkg_master.LST_POD_ATA_DT ) )) AS dms_bkg_master_lst_pod_ata_dt_time,
        (FORMAT_TIMESTAMP('%F %T', timestamp(dms_bkg_master.POL_ETD_DT ) )) AS dms_bkg_master_n1st_pol_etd_dt_time,
        (FORMAT_TIMESTAMP('%F %T', timestamp( dms_bkg_master.N1ST_POL_ATD_DT ) )) AS dms_bkg_master_n1st_pol_atd_dt_time,
    dmc_period_bkg_rqst.YRMON_DESC  AS dmc_period_bkg_rqst_yrmon_desc
FROM `one-global-dde-uat.DWH.DMS_BKG_CNTR`  AS dms_bkg_cntr
LEFT JOIN `one-global-dde-uat.DWH.DMS_BKG_MASTER`  AS dms_bkg_master ON dms_bkg_cntr.BKG_NO=dms_bkg_master.BKG_NO
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_BKG_STS_V`  AS dmc_bkg_sts ON dms_bkg_master.BKG_STS_CD=dmc_bkg_sts.BKG_STS_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_BKG_RD_TERM_V`  AS dmc_bkg_r_d_term_cntr ON dmc_bkg_r_d_term_cntr.bkg_rd_term_cd=dms_bkg_cntr.RD_TERM_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_NOD_LM_V`  AS dmc_yard_trnk_pol ON dmc_yard_trnk_pol.nod_cd= dms_bkg_master.TRNK_POL_NOD_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_NOD_LM_V`  AS dmc_yard_trnk_pod ON dmc_yard_trnk_pod.nod_cd= dms_bkg_master.TRNK_POD_NOD_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_CUSTOMER_LM_V`  AS dmc_customer_shpr ON dms_bkg_master.SHPR_CUST_CD=dmc_customer_shpr.CUST_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_CUSTOMER_LM_V`  AS dmc_customer_antfy ON dms_bkg_master.ANTFY_CUST_CD=dmc_customer_antfy.CUST_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_CUSTOMER_LM_V`  AS dmc_customer_cnee ON dms_bkg_master.CNEE_CUST_CD=dmc_customer_cnee.CUST_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_BKG_RQST_MZD_V`  AS dmc_bkg_rqst ON dmc_bkg_rqst.XTER_BKG_RQST_CD=dms_bkg_master.XTER_BKG_RQST_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_BKG_RQST_MZD_V`  AS dmc_bkg_rqst_si ON dmc_bkg_rqst_si.XTER_BKG_RQST_CD=dms_bkg_master.XTER_SI_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_CUSTOMER_LM_V`  AS dmc_customer_bkg_ctrl ON dms_bkg_master.BKG_CTRL_PTY_CUST_CD=dmc_customer_bkg_ctrl.CUST_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_CUSTOMER_LM_V`  AS dmc_customer_ctrt ON dms_bkg_master.SC_CUST_CD=dmc_customer_ctrt.CUST_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_CUSTOMER_LM_V`  AS dmc_customer_fwrd ON dms_bkg_master.FWRD_CUST_CD=dmc_customer_fwrd.CUST_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_CUSTOMER_LM_V`  AS dmc_customer_ntfy ON dms_bkg_master.NTFY_CUST_CD=dmc_customer_ntfy.CUST_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_CUSTOMER_LM_V`  AS dmc_customer_esi_bkg ON dms_bkg_master.BKG_PTY_CUST_CD=dmc_customer_esi_bkg.CUST_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_NOD_LM_V`  AS dmc_yard_n1st_pol ON dms_bkg_master.N1ST_POL_NOD_CD=dmc_yard_n1st_pol.nod_cd
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_NOD_LM_V`  AS dmc_yard_lst_pod ON dms_bkg_master.POD_NOD_CD=dmc_yard_lst_pod.nod_cd
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_LOCATION_V` AS dmc_loc_por ON dms_bkg_master.POR_CD=dmc_loc_por.LOC_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_LOCATION_V` AS dmc_loc_del ON dms_bkg_master.DEL_CD=dmc_loc_del.LOC_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_NOD_LM_V`  AS dmc_yard_del ON dms_bkg_master.DEL_NOD_CD=dmc_yard_del.nod_cd
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_NOD_LM_V`  AS dmc_yard_act_mty_pkup ON dms_bkg_cntr.ACT_MTY_PKUP_YD_CD=dmc_yard_act_mty_pkup.nod_cd
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_NOD_LM_V`  AS dmc_yard_act_mty_rtn ON dms_bkg_cntr.ACT_MTY_RTN_YD_CD=dmc_yard_act_mty_rtn.nod_cd
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_TRS_MOD_V`  AS dmc_trs_mod_ob ON dmc_trs_mod_ob.TRS_MOD_CD = dms_bkg_master.OB_TRSP_MOD_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_TRS_MOD_V`  AS dmc_trs_mod_ib ON dmc_trs_mod_ib.TRS_MOD_CD = dms_bkg_master.IB_TRSP_MOD_CD
LEFT JOIN `one-global-dde-uat.DM_VIEWS.DMC_PERIOD_V`  AS dmc_period_bkg_rqst ON (DATE(timestamp( dms_bkg_master.XTER_RQST_RQST_DT ) ))=(DATE(PARSE_DATE("%Y%m%d", dmc_period_bkg_rqst.BSE_DT) ))
WHERE (timestamp(dms_bkg_master.POL_ETD_DT ) ) >= (TIMESTAMP('2022-01-01 00:00:00'))
  and date(dms_bkg_cntr.EDW_UPD_DT) BETWEEN '2022-01-01' AND  '2022-06-01' 
  and dms_bkg_cntr.BKG_NO = 'CMBA07436800'
  and dms_bkg_cntr.CNTR_TPSZ_CD = 'D5'
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    31,
    32,
    33,
    34,
    35,
    36,
    37,
    38,
    39,
    40,
    41,
    42,
    43,
    44,
    45,
    46,
    47,
    48,
    49,
    50,
    51,
    52,
    53,
    54,
    55,
    56,
    57,
    58,
    59,
    60
ORDER BY
    32 DESC
LIMIT 500
