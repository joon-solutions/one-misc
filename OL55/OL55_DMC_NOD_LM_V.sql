create or replace table one-global-looker-dev.test_table_materialization.DMC_NOD_LM_V as 
    with Y as (
        SELECT   YD_CD 
               , YD_NM 
               , YD_LAT
               , YD_LON 
               , OFC_CD
               , YD_CSTMS_NO
               , YD_PROCU_CD
               , YD_OSHP_CD
          FROM `one-global-dde-prod.OPUS.DWC_YARD`
        UNION ALL
        SELECT   ZN_CD
               , ZN_NM
               , NULL AS YD_LAT 
               , NULL AS YD_LON
               , NULL AS OFC_CD
               , NULL AS YD_CSTMS_NO
               , NULL AS YD_PROCU_CD
               , NULL AS YD_OSHP_CD
          FROM `one-global-dde-prod.OPUS.DWC_ZONE`

    ),

    LOC as (
        SELECT  
            OWN_YD_CD AS YD_CD
            ,MAX( CHSS_POOL_CD) AS CHSS_POOL_CD 
        FROM   `one-global-dde-prod.OPUS.DWL_POOL_CHSS_LOC`  
        WHERE  1 = 1
        GROUP BY OWN_YD_CD 
    ),

    C as (
        SELECT  
            LOC.YD_CD AS YD_CD
            ,LOC.CHSS_POOL_CD  AS CHSS_POOL_CD
            ,POOL.CHSS_POOL_NM AS CHSS_POOL_NM 
        FROM LOC 
        LEFT OUTER JOIN `one-global-dde-prod.OPUS.DWL_CHSS_POOL` POOL 
            ON LOC.CHSS_POOL_CD = POOL.CHSS_POOL_CD
    )

    SELECT  
        Y.YD_CD  AS NOD_CD
       ,Y.YD_NM  AS NOD_NM
       ,Y.YD_LAT AS NOD_LAT
       ,Y.YD_LON AS NOD_LON
       ,L.LOC_CD
       ,L.LOC_NM
       ,LW.NEW_LOC_LAT
       ,LW.NEW_LOC_LON
       ,L.ECC_CD
       ,ECC_GEO.NEW_LOC_LAT AS NEW_LOC_LAT_ECC
       ,ECC_GEO.NEW_LOC_LON AS NEW_LOC_LON_ECC
       ,L.SCC_CD
       ,L.LCC_CD
       ,L.RCC_CD
       ,L.RGN_CD
       ,L.RGN_NM
       ,L.STE_CD
       ,L.STE_NM
       ,L.CNT_CD
       ,L.CNT_NM
       ,L.SCONTI_CD
       ,L.SCONTI_NM
       ,L.CONTI_CD
       ,L.CONTI_NM
       ,C.CHSS_POOL_CD -- 2022.03.02, ISSUE 55
       ,C.CHSS_POOL_NM -- 2022.03.02, ISSUE 55
       ,Y.OFC_CD
       ,TP.CODE AS YD_TP_CD -- 2022.09.16, PI4V2-3251
       ,TP.NAME AS YD_TP_NM -- 2022.09.16, PI4V2-3251
       ,Y.YD_CSTMS_NO --2022.09.22, PI4V2-3251
       ,Y.YD_PROCU_CD --2022.09.22, PI4V2-3251
       ,Y.YD_OSHP_CD   --2022.09.22, PI4V2-3251
       ,OSHP.YD_OSHP_NM --2022.09.22, PI4V2-3251
    FROM  Y
    LEFT OUTER JOIN `one-global-dde-prod.OPUS.DWC_LOCATION` LW
        ON SUBSTR(Y.YD_CD,1,5) = LW.LOC_CD
    LEFT OUTER JOIN `one-global-dde-prod.DM_VIEWS.DMC_LOCATION_V` L      
        ON LW.LOC_CD = L.LOC_CD
    LEFT OUTER JOIN `one-global-dde-prod.OPUS.DWC_LOCATION` ECC_GEO 
        ON L.ECC_CD = ECC_GEO.LOC_CD
    LEFT OUTER JOIN C 
        ON Y.YD_CD = C.YD_CD
    LEFT OUTER JOIN `one-global-dde-prod.DM_VIEWS.DMC_YD_TP_V` TP 
        ON Y.YD_CD = TP.YD_CD
    LEFT OUTER JOIN `one-global-dde-prod.DM_VIEWS.DMC_YD_OSHP_V` OSHP 
        ON Y.YD_OSHP_CD = OSHP.YD_OSHP_CD



