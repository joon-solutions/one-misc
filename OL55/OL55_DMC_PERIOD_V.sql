create or replace table one-global-looker-dev.test_table_materialization.DMC_PERIOD_V as 
with base as (
    SELECT 
        BSE_DT,
        BSE_YR,
        BSE_YRQTR,
        YRQTR_DESC,
        BSE_YRMON,
        YRMON_DESC,
        BSE_YRWK,
        YRWK_DESC,
        SUBSTR (BSE_YRWK, 1, 4) AS BSE_MRKYR,
        CASE 
            WHEN CAST(SUBSTR(BSE_YRWK, 5, 2) AS NUMERIC) <= 13 THEN 
                CASE 
                    WHEN CEIL(MOD(CAST(SUBSTR(BSE_YRWK, 5, 2) AS NUMERIC),13) / 4) = 1 THEN '01' 
                    WHEN CEIL(MOD(CAST(SUBSTR(BSE_YRWK, 5, 2) AS NUMERIC),13) / 4) = 2 THEN '02' 
                    ELSE '03' 
                END 
            WHEN CAST(SUBSTR(BSE_YRWK, 5, 2) AS NUMERIC) <= 26 THEN 
                CASE 
                    WHEN CEIL(MOD(CAST(SUBSTR(BSE_YRWK, 5, 2) AS NUMERIC),13) / 4) = 1 THEN '04' 
                    WHEN CEIL(MOD(CAST(SUBSTR(BSE_YRWK, 5, 2) AS NUMERIC),13) / 4) = 2 THEN '05' 
                    ELSE '06' 
                END 
            WHEN CAST(SUBSTR(BSE_YRWK, 5, 2) AS NUMERIC) <= 39 THEN 
                CASE 
                    WHEN CEIL(MOD(CAST(SUBSTR(BSE_YRWK, 5, 2) AS NUMERIC),13) / 4) = 1 THEN '07' 
                    WHEN CEIL(MOD(CAST(SUBSTR(BSE_YRWK, 5, 2) AS NUMERIC),13) / 4) = 2 THEN '08' 
                    ELSE '09' 
                END 
            WHEN CAST(SUBSTR(BSE_YRWK, 5, 2) AS NUMERIC) <= 52 THEN 
                CASE 
                    WHEN CEIL(MOD(CAST(SUBSTR(BSE_YRWK, 5, 2) AS NUMERIC),13) / 4) = 1 THEN '10' 
                    WHEN CEIL(MOD(CAST(SUBSTR(BSE_YRWK, 5, 2) AS NUMERIC),13) / 4) = 2 THEN '11' 
                    ELSE '12' 
                END ELSE '12' 
        END AS MRK_YRMON, 
        CASE 
            WHEN SUBSTR(BSE_YRMON, 5, 2) > '03' THEN BSE_YR 
            ELSE CAST((CAST(BSE_YR AS NUMERIC)-1) AS STRING) 
        END AS BSE_ACCYR, 
        ISO_YRWK, 
        ISO_YRWK_DESC 
    FROM `one-global-dde-prod.DWH.DMC_PERIOD`
),

logics as (
    select
        *,
        CASE 
            WHEN CAST(MRK_YRMON AS NUMERIC) <= 3 THEN 'Q4' 
            WHEN CAST(MRK_YRMON AS NUMERIC) <= 6 THEN 'Q1' 
            WHEN CAST(MRK_YRMON AS NUMERIC) <= 9 THEN 'Q2' 
            ELSE 'Q3' 
        END as quarter,
        CASE 
            WHEN MRK_YRMON > '03' THEN BSE_MRKYR 
            ELSE CAST((CAST(BSE_MRKYR AS NUMERIC) -1) AS STRING) 
        END AS ACC_MRKYR
    from base
)

SELECT 
    BSE_DT, 
    BSE_YR,
    BSE_YRQTR,
    YRQTR_DESC,
    BSE_YRMON,
    YRMON_DESC,
    BSE_YRWK,
    YRWK_DESC,
    ISO_YRWK, 
    ISO_YRWK_DESC,
    ACC_MRKYR, 
    BSE_ACCYR AS BSE_MRKYR ,
    SUBSTR(BSE_YRWK, 1, 4) || MRK_YRMON AS BSE_MRKMON,
    SUBSTR(BSE_YRWK, 1, 4) || '/M' ||MRK_YRMON AS MRKMON_DESC,
    BSE_ACCYR || quarter  AS BSE_MRKQTR,
    BSE_ACCYR || '/' || quarter AS MRKQTR_DESC,
    BSE_ACCYR,
    BSE_ACCYR || quarter AS BSE_ACCQTR,
    BSE_ACCYR || '/' || quarter AS ACCQTR_DESC ,
    ACC_MRKYR || quarter AS ACC_MRKQTR ,
    ACC_MRKYR || '/' || quarter AS ACC_MRKQTR_DESC
FROM logics



