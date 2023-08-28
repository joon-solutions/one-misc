create or replace table one-global-looker-dev.test_table_materialization.DMC_LOCATION_V as 
    SELECT 
       B.LOC_CD,
       B.LOC_NM,
       A.SCC_CD,
       A.ECC_CD,
       A.LCC_CD,
       A.RCC_CD,
       B.RGN_CD,
       E.RGN_NM,
       B.STE_CD,
       F.STE_NM,
       B.CNT_CD,
       C.CNT_NM,
       C.SCONTI_CD,
       D.SCONTI_NM,
       B.CONTI_CD,
       G.CONTI_NM,
       B.PORT_INLND_FLG,
       B.DELT_FLG,
       B.NEW_LOC_LAT,
       B.NEW_LOC_LON
    FROM one-global-dde-prod.OPUS.DWC_LOCATION B 
       LEFT OUTER JOIN  one-global-dde-prod.OPUS.DWC_EQ_ORZ_CHT A  
           ON B.SCC_CD = A.SCC_CD
       LEFT OUTER JOIN one-global-dde-prod.OPUS.DWC_COUNTRY C 
           ON B.CNT_CD = C.CNT_CD
       LEFT OUTER JOIN one-global-dde-prod.OPUS.DWC_SUBCONTINENT D 
           ON C.SCONTI_CD = D.SCONTI_CD
       LEFT OUTER JOIN one-global-dde-prod.OPUS.DWC_REGION E 
           ON B.RGN_CD = E.RGN_CD
       LEFT OUTER JOIN one-global-dde-prod.OPUS.DWC_STATE F 
           ON B.CNT_CD = F.CNT_CD AND B.STE_CD = F.STE_CD
       LEFT OUTER JOIN one-global-dde-prod.OPUS.DWC_CONTINENT G 
           ON B.CONTI_CD = G.CONTI_CD