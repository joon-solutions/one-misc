BEGIN
/***************************************************************************************************
        프로그램명  : SP_DML_CNTR_LTST_MVMT_SNAP_TD01
        작성일자    : 2022. 02. 16
        작성자      : 홍순익
        변경이력    :
        배치간격    :  -- 1.1.15 Column add 20220802
                      -- PK Column ADD (DISP_FLG, CNTR_STS_CD) 20230609 (SR0229306)
***************************************************************************************************/
    DECLARE vPROC_NAME  STRING DEFAULT "SP_DML_CNTR_LTST_MVMT_SNAP_TD01";   /*프로시저명*/
    DECLARE vDATA_SET   STRING DEFAULT "DWH";       /*타겟 데이터셋 명*/
    DECLARE vTARGET_TABLE  STRING DEFAULT "DML_CNTR_LTST_MVMT_SNAP";       /*타겟 테이블 명*/
    DECLARE vJOB_PARAM  STRING DEFAULT "F_DATE=" || pF_DATE;

    DECLARE vWORK_TIME  STRING DEFAULT FORMAT_DATETIME('%Y%m%d %H%M%S', DATETIME(current_timestamp(),"Asia/Singapore"));  /*작업시간 (싱가폴시간대)*/
    DECLARE vSTART_TIME DATETIME;   /*프로시저내에 단위 작업별 시작시간을 추출하기 위해 선언*/

    DECLARE vROWS INT64;            /* 실행건수를 받기위해 선언*/



    DECLARE F_DATE DATETIME;
    DECLARE T_DATE DATETIME;

    /*파이프라인명*/
    DECLARE vPIPELINE_NAME  STRING DEFAULT "MLOG_DML_CNTR_LTST_MVMT_SNAP_TD01";

    /*파라메터 설정 입력값이 NULL이면 COMMON.JOB_PARAM 에서 가져오기*/
    -- 파라메터 설정
    SET pF_DATE = (SELECT COALESCE(pF_DATE, CAST(DATETIME_TRUNC(CURRENT_DATETIME("Asia/Singapore"),DAY) AS STRING FORMAT 'YYYYMMDD')));
--    SET pT_DATE = (SELECT COALESCE(pT_DATE, FORMAT_DATETIME('%Y%m%d%H', current_datetime('Asia/Singapore'))));
    SET vJOB_PARAM = (SELECT "F_DATE=" || pF_DATE);

    SET F_DATE = PARSE_DATETIME('%Y%m%d', pF_DATE);

    /**********************  Before SQL ***************/
    /********************************************************/
    SET vSTART_TIME = DATETIME(current_timestamp(),"Asia/Singapore");

    DELETE
    FROM DWH.DML_CNTR_LTST_MVMT_SNAP
    WHERE SNAP_DT = F_DATE;


    /* 실행건수 추출*/
    SET vROWS = (select @@row_count);
    /* 로그 테이블에 기록*/
    INSERT INTO COMMON.JOB_PROCEDURE_LOG(WORK_TIME, PROC_NAME, WORK_NAME, START_TIME, END_TIME, RUN_TIME, AFFECT_ROWS, DATA_SET,TARGET_TABLE,JOB_PARAM, EDW_UPD_DT)
    VALUES (vWORK_TIME,vPROC_NAME, '1. Before SQL - DELETE DML_CNTR_LTST_MVMT_SNAP', vSTART_TIME, DATETIME(current_timestamp(),"Asia/Singapore")
            , SUBSTR(CAST( DATETIME(current_timestamp(),"Asia/Singapore")- vSTART_TIME AS STRING), 5, INSTR(CAST( DATETIME(current_timestamp(),"Asia/Singapore")- vSTART_TIME AS STRING), '.') -5 )
            , vROWS, vDATA_SET,vTARGET_TABLE,vJOB_PARAM, DATETIME(current_timestamp(),"Asia/Singapore"));





    /********************** Source SQL **********************/
    /********************************************************/


       CREATE TEMP TABLE LTST_MVMT AS (
       /* CONTAINER */
       SELECT B.CNTR_TPSZ_CD AS EQ_TPSZ_CD
            , CNTR_LSTM_CD AS EQ_LSTM_CD
            , REPLACE(SUBSTR(CAST(CNTR_MFT_DT AS STRING),1,10),'-','') AS EQ_MFT_DT
            , CNTR_OWN_VNDR_SEQ AS EQ_OWN_VNDR_SEQ
            , 'U' AS EQ_KND_CD
            , ORG_YD_CD
            , CRNT_EQ_CTRL_OFC_CD
            , CRNT_VVD_CD
            , BKG_CGO_TP_CD
            , CHSS_POOL_CD
            , A.CNMV_STS_CD
            , A.FULL_FLG
            , SUM(CNTR_QTY) AS CNTR_QTY
            , SUM(TRUNC(DATETIME_DIFF(CURRENT_DATETIME("Asia/Singapore"),A.CNMV_DT, SECOND)/(60*60*24))) AS STAY_DYS
            , COALESCE(CNTR_SUB_LSE_VNDR_SEQ,0) AS EQ_SUB_LSE_VNDR_SEQ
            , CNTR_AGMT_NO
            , CNMV_CO_CD
            , A.PRNT_OP_CO_CD
            , A.CRNT_OP_CO_CD
            , A.IO_BND_CD
            , A.BKG_NO
            , A.RCV_TERM_CD
            , A.DE_TERM_CD
            , A.N1ST_OC_YD_CD
            , A.N1ST_VL_YD_CD
            , A.LST_VD_YD_CD
            , A.TRK_GATE_IO_YD_CD TRK_GATE_IO_YD_CD
             --------------20230609 PK Column ADD on Oracle 
            , A.CNTR_STS_CD
            , A.DISP_FLG
       FROM   DWH.DML_CNTR_LTST_MVMT A
       INNER JOIN one-global-dde-prod.OPUS.DWL_CONTAINER  B ON A.CNTR_NO = B.CNTR_NO
       WHERE  1=1
       AND    A.CNTR_NO IS NOT NULL
       AND    B.ACIAC_DIV_CD = 'A'
       GROUP BY B.CNTR_TPSZ_CD
              , CNTR_LSTM_CD
              , REPLACE(SUBSTR(CAST(CNTR_MFT_DT AS STRING),1,10),'-','')
              , CNTR_OWN_VNDR_SEQ
              , ORG_YD_CD
              , CRNT_EQ_CTRL_OFC_CD
              , CRNT_VVD_CD
              , BKG_CGO_TP_CD
              , CHSS_POOL_CD
              , A.CNMV_STS_CD
              , A.FULL_FLG
              , CNTR_SUB_LSE_VNDR_SEQ
              , CNTR_AGMT_NO
              , CNMV_CO_CD
              , A.PRNT_OP_CO_CD
              , A.CRNT_OP_CO_CD
              , A.IO_BND_CD
              , A.BKG_NO
              , A.RCV_TERM_CD
              , A.DE_TERM_CD
              , A.N1ST_OC_YD_CD
              , A.N1ST_VL_YD_CD
              , A.LST_VD_YD_CD
              , A.TRK_GATE_IO_YD_CD
               --------------20230609 PK Column ADD
              , A.CNTR_STS_CD
              , A.DISP_FLG
       UNION ALL
       /* CHASSIS */
       SELECT A.CHSS_TPSZ_CD AS EQ_TPSZ_CD
            , A.CHSS_LSTM_CD AS EQ_LSTM_CD
            , REPLACE(SUBSTR(CAST(CHSS_MFT_DT AS STRING),1,10),'-','') AS EQ_MFT_DT
            , A.CHSS_OWN_VNDR_SEQ AS EQ_OWN_VNDR_SEQ
            , 'Z'            AS EQ_KND_CD
            , A.ORG_YD_CD
            , A.CRNT_EQ_CTRL_OFC_CD
            , A.CRNT_VVD_CD
            , A.BKG_CGO_TP_CD
            , A.CHSS_POOL_CD
            , A.CNMV_STS_CD
            , A.FULL_FLG
            , SUM(A.CHSS_QTY)     AS CHSS_QTY
            , SUM(TRUNC(DATETIME_DIFF(CURRENT_DATETIME("Asia/Singapore"),A.CNMV_DT, SECOND)/(60*60*24))) AS STAY_DYS
            , COALESCE(A.CHSS_SUB_LSE_VNDR_SEQ,0) AS EQ_SUB_LSE_VNDR_SEQ
            , A.CNTR_AGMT_NO
            , A.CNMV_CO_CD
            , A.PRNT_OP_CO_CD
            , A.CRNT_OP_CO_CD
            , A.IO_BND_CD
            , A.BKG_NO
            , A.RCV_TERM_CD
            , A.DE_TERM_CD
            , A.N1ST_OC_YD_CD
            , A.N1ST_VL_YD_CD
            , A.LST_VD_YD_CD
            , A.TRK_GATE_IO_YD_CD  TRK_GATE_IO_YD_CD
             --------------20230609 PK Column ADD
            , A.CNTR_STS_CD
            , A.DISP_FLG
       FROM   DWH.DML_CNTR_LTST_MVMT A
       WHERE 1=1
       AND   CHSS_NO IS NOT NULL
       GROUP BY
              CHSS_TPSZ_CD
            , CHSS_LSTM_CD
            , REPLACE(SUBSTR(CAST(CHSS_MFT_DT AS STRING),1,10),'-','')
            , CHSS_OWN_VNDR_SEQ
            , ORG_YD_CD,CRNT_EQ_CTRL_OFC_CD,CRNT_VVD_CD
            , BKG_CGO_TP_CD,CHSS_POOL_CD
            , CNMV_STS_CD,FULL_FLG
            , CHSS_SUB_LSE_VNDR_SEQ
            , CNTR_AGMT_NO
            , CNMV_CO_CD
            , PRNT_OP_CO_CD
            , CRNT_OP_CO_CD
            , A.IO_BND_CD
            , A.BKG_NO
            , A.RCV_TERM_CD
            , A.DE_TERM_CD
            , A.N1ST_OC_YD_CD
            , A.N1ST_VL_YD_CD
            , A.LST_VD_YD_CD
            , A.TRK_GATE_IO_YD_CD
              --------------20230609 PK Column ADD
            , A.CNTR_STS_CD
            , A.DISP_FLG

       UNION ALL
       /* GENSET */
       SELECT MGST_TPSZ_CD AS EQ_TPSZ_CD
            , MGST_LSTM_CD AS EQ_LSTM_CD
            , REPLACE(SUBSTR(CAST(MGST_MFT_DT AS STRING),1,10),'-','') AS EQ_MFT_DT
            , NULL AS EQ_OWN_VNDR_SEQ
            , 'G' AS EQ_KND_CD
            , ORG_YD_CD
            , CRNT_EQ_CTRL_OFC_CD
            , CRNT_VVD_CD
            , BKG_CGO_TP_CD
            , CHSS_POOL_CD
            , CNMV_STS_CD
            , FULL_FLG
            , SUM(MGST_QTY) AS MGST_QTY
            , SUM(TRUNC(DATETIME_DIFF(CURRENT_DATETIME("Asia/Singapore"),A.CNMV_DT, SECOND)/(60*60*24))) AS STAY_DYS
            , 0 AS EQ_SUB_LSE_VNDR_SEQ
            , CNTR_AGMT_NO
            , CNMV_CO_CD
            , PRNT_OP_CO_CD
            , CRNT_OP_CO_CD
            , A.IO_BND_CD
            , A.BKG_NO
            , A.RCV_TERM_CD
            , A.DE_TERM_CD
            , A.N1ST_OC_YD_CD
            , A.N1ST_VL_YD_CD
            , A.LST_VD_YD_CD
            , A.TRK_GATE_IO_YD_CD AS TRK_GATE_IO_YD_CD
              --------------20230609 PK Column ADD
            , A.CNTR_STS_CD
            , A.DISP_FLG
       FROM   DWH.DML_CNTR_LTST_MVMT                  A
       LEFT JOIN one-global-dde-prod.OPUS.DWC_SVC_SCP B ON A.SVC_SCP_CD = B.SVC_SCP_CD
       WHERE  1=1
       AND MGST_NO IS NOT NULL
       GROUP BY
              MGST_TPSZ_CD
            , MGST_LSTM_CD
            , REPLACE(SUBSTR(CAST(MGST_MFT_DT AS STRING),1,10),'-','')
            , ORG_YD_CD
            , CRNT_YD_CD
            , CRNT_EQ_CTRL_OFC_CD
            , CRNT_VVD_CD
            , BKG_CGO_TP_CD
            , CHSS_POOL_CD
            , CNMV_STS_CD
            , FULL_FLG
            , CNTR_AGMT_NO
            , CNMV_CO_CD
            , PRNT_OP_CO_CD
            , CRNT_OP_CO_CD
            , A.IO_BND_CD
            , A.BKG_NO
            , A.RCV_TERM_CD
            , A.DE_TERM_CD
            , A.N1ST_OC_YD_CD
            , A.N1ST_VL_YD_CD
            , A.LST_VD_YD_CD
            , A.TRK_GATE_IO_YD_CD
              --------------20230609 PK Column ADD
            , A.CNTR_STS_CD
            , A.DISP_FLG
       );

       INSERT INTO DWH.DML_CNTR_LTST_MVMT_SNAP
       (  SNAP_YRWK
       	, SNAP_DT
       	, EQ_TPSZ_CD
       	, EQ_LSTM_CD
       	, EQ_MFT_DT
       	, EQ_OWN_VNDR_SEQ
       	, EQ_KND_CD
       	, CRNT_YD_CD
       	, CRNT_EQ_CTRL_OFC_CD
       	, CRNT_VVD_CD
       	, BKG_CGO_TP_CD
       	, CHSS_POOL_CD
       	, CNMV_STS_CD
       	, FULL_FLG
       	, CNTR_QTY
       	, STAY_DYS
       	, CNTR_TEU_QTY
       	, EQ_SUB_LSE_VNDR_SEQ
       	, CNTR_AGMT_NO
       	, EDW_UPD_DT
       	, CNMV_CO_CD
       	, PRNT_OP_CO_CD
       	, CRNT_OP_CO_CD
       	, IO_BND_CD
       	, IO_BND_NM
       	, RCV_DE_TERM_CD
       	, RCV_DE_TERM_NM
       	, N1ST_OB_FULL_CY_YD_CD
       	, N1ST_LODG_YD_CD
       	, LST_DCHG_YD_CD
       	, LST_IB_FULL_CY_YD_CD
       	, N1ST_DY_SUN_FLG
          , STAY_DYS_CD
          , STAY_DYS_NM
          , RCC_DT
         --------------20230531 PK Column ADD
          , CNTR_STS_CD
          , DISP_FLG   
               
       )
       SELECT S2.BSE_YRWK                           AS SNAP_YRWK
            , F_DATE                                AS SNAP_DT
            , COALESCE(S1.EQ_TPSZ_CD, ' ')          AS EQ_TPSZ_CD
            , COALESCE(S1.EQ_LSTM_CD, ' ')          AS EQ_LSTM_CD
            , COALESCE(S1.EQ_MFT_DT, ' ')           AS EQ_MFT_DT
            , COALESCE(S1.EQ_OWN_VNDR_SEQ, 0)       AS EQ_OWN_VNDR_SEQ
            , COALESCE(S1.EQ_KND_CD, ' ')           AS EQ_KND_CD
            , COALESCE(S1.ORG_YD_CD, ' ')           AS CRNT_YD_CD
            , COALESCE(S1.CRNT_EQ_CTRL_OFC_CD, ' ') AS CRNT_EQ_CTRL_OFC_CD
            , COALESCE(S1.CRNT_VVD_CD, ' ')         AS CRNT_VVD_CD
            , COALESCE(S1.BKG_CGO_TP_CD, ' ')       AS BKG_CGO_TP_CD
            , COALESCE(S1.CHSS_POOL_CD, ' ')        AS CHSS_POOL_CD
            , COALESCE(S1.CNMV_STS_CD, ' ')         AS CNMV_STS_CD
            , COALESCE(S1.FULL_FLG, ' ')            AS FULL_FLG
            , SUM(CAST(S1.CNTR_QTY AS INTEGER))     AS CNTR_QTY
            , SUM(COALESCE(CAST(S1.STAY_DYS AS INTEGER),0)) AS STAY_DYS
            , SUM(CASE SUBSTR(S1.EQ_TPSZ_CD,-1) WHEN '2' THEN CAST(S1.CNTR_QTY AS INTEGER) WHEN '3' THEN CAST(S1.CNTR_QTY AS INTEGER) ELSE CAST(S1.CNTR_QTY AS INTEGER) *2 END) AS CNTR_TEU_QTY
            , COALESCE(S1.EQ_SUB_LSE_VNDR_SEQ, 0)   AS EQ_SUB_LSE_VNDR_SEQ
            , COALESCE(S1.CNTR_AGMT_NO, ' ')        AS CNTR_AGMT_NO
            , CURRENT_DATETIME("Asia/Singapore")    AS EDW_UPD_DT
            , COALESCE(CNMV_CO_CD, ' ')             AS CNMV_CO_CD
            , COALESCE(PRNT_OP_CO_CD, ' ')          AS PRNT_OP_CO_CD
            , COALESCE(CRNT_OP_CO_CD, ' ')          AS CRNT_OP_CO_CD
            , COALESCE(S1.IO_BND_CD , ' ')          AS IO_BND_CD
            , COALESCE(MAX(CASE S1.IO_BND_CD WHEN 'I' THEN 'Inbound' WHEN 'O' THEN 'Outbound' END), ' ') AS IO_BND_NM
            , COALESCE(CASE WHEN COALESCE(S1.RCV_TERM_CD, '*')||COALESCE(S1.DE_TERM_CD, '*') = '**' THEN NULL ELSE S1.RCV_TERM_CD||'/'||S1.DE_TERM_CD END, ' ') AS RCV_DE_TERM_CD
            , COALESCE(MAX(CASE WHEN COALESCE(S1.RCV_TERM_CD, '*')||COALESCE(S1.DE_TERM_CD, '*') = '**' THEN NULL ELSE CD1.INTG_CD_VAL_DP_DESC||'/'|| CD2.INTG_CD_VAL_DP_DESC END), ' ') RCV_DE_TERM_NM
            , COALESCE(S1.N1ST_OC_YD_CD, ' ') N1ST_OB_FULL_CY_YD_CD
            , COALESCE(S1.N1ST_VL_YD_CD, ' ') N1ST_LODG_YD_CD
            , COALESCE(S1.LST_VD_YD_CD, ' ') LST_DCHG_YD_CD
            , COALESCE(S1.TRK_GATE_IO_YD_CD, ' ') LST_IB_FULL_CY_YD_CD
            , CASE WHEN CAST(CURRENT_DATETIME("Asia/Singapore") AS STRING FORMAT 'DY') = 'SUN' THEN 'Y' ELSE 'N' END AS N1ST_DY_SUN_FLG
            , CASE WHEN SUM(COALESCE(CAST(S1.STAY_DYS AS INTEGER),0)) BETWEEN 0 AND 30 THEN '01'
                   WHEN SUM(COALESCE(CAST(S1.STAY_DYS AS INTEGER),0)) BETWEEN 31 AND 60 THEN '02'
                   WHEN SUM(COALESCE(CAST(S1.STAY_DYS AS INTEGER),0)) BETWEEN 61 AND 90 THEN '03'
                   WHEN SUM(COALESCE(CAST(S1.STAY_DYS AS INTEGER),0)) > 90 THEN '04'
                   ELSE '99' END AS STAY_DYS_CD -- 20220802 ADD
            , CASE WHEN SUM(COALESCE(CAST(S1.STAY_DYS AS INTEGER),0)) BETWEEN 0 AND 30 THEN '1-30 Days'
                  WHEN SUM(COALESCE(CAST(S1.STAY_DYS AS INTEGER),0)) BETWEEN 31 AND 60 THEN '31-60 Days'
                  WHEN SUM(COALESCE(CAST(S1.STAY_DYS AS INTEGER),0)) BETWEEN 61 AND 90 THEN '61-90 Days'
                  WHEN SUM(COALESCE(CAST(S1.STAY_DYS AS INTEGER),0)) > 90 THEN '>90 Days'
              ELSE 'N/A' END AS STAY_DYS_NM -- 20220802 ADD
            , MAX(DWH.TIME_LOCAL_FNC(DL.RCC_CD, 'KR')) AS RCC_DT -- 20220802 ADD
            -----20220609 PK COLUMN ADD
            , COALESCE(S1.CNTR_STS_CD, ' ') AS CNTR_STS_CD
            , COALESCE(S1.DISP_FLG, ' ') AS DISP_FLG
       FROM   LTST_MVMT         S1
       INNER JOIN DM_VIEWS.DMC_PERIOD_V S2 ON CAST(CURRENT_DATETIME("Asia/Singapore") AS STRING FORMAT 'YYYYMMDD')  = S2.BSE_DT
       LEFT JOIN  (SELECT INTG_CD_VAL_CTNT, INTG_CD_VAL_DP_DESC FROM one-global-dde-prod.OPUS.DWC_COM_CD_DTL WHERE INTG_CD_ID  = 'CD01725' QUALIFY ROW_NUMBER() OVER(PARTITION BY INTG_CD_VAL_CTNT) =1) CD1 ON S1.RCV_TERM_CD = CD1.INTG_CD_VAL_CTNT
       LEFT JOIN  (SELECT INTG_CD_VAL_CTNT, INTG_CD_VAL_DP_DESC FROM one-global-dde-prod.OPUS.DWC_COM_CD_DTL WHERE INTG_CD_ID  = 'CD01725' QUALIFY ROW_NUMBER() OVER(PARTITION BY INTG_CD_VAL_CTNT) =1) CD2 ON S1.DE_TERM_CD = CD2.INTG_CD_VAL_CTNT
       LEFT JOIN DM_VIEWS.DMC_YARD_V DY ON COALESCE(S1.ORG_YD_CD, ' ') = DY.YD_CD -- 20220802 ADD
       LEFT JOIN DM_VIEWS.DMC_LOCATION_V DL ON DY.LOC_CD = DL.LOC_CD -- 20220802 ADD
       WHERE  1=1
       GROUP BY S2.BSE_YRWK
            , COALESCE(S1.EQ_TPSZ_CD, ' ')
            , COALESCE(S1.EQ_LSTM_CD, ' ')
            , COALESCE(S1.EQ_MFT_DT, ' ')
            , COALESCE(S1.EQ_OWN_VNDR_SEQ, 0)
            , COALESCE(S1.EQ_KND_CD, ' ')
            , COALESCE(S1.ORG_YD_CD, ' ')
            , COALESCE(S1.CRNT_EQ_CTRL_OFC_CD, ' ')
            , COALESCE(S1.CRNT_VVD_CD, ' ')
            , COALESCE(S1.BKG_CGO_TP_CD, ' ')
            , COALESCE(S1.CHSS_POOL_CD, ' ')
            , COALESCE(S1.CNMV_STS_CD, ' ')
            , COALESCE(S1.FULL_FLG, ' ')
            , COALESCE(S1.EQ_SUB_LSE_VNDR_SEQ, 0)
            , COALESCE(S1.CNTR_AGMT_NO, ' ')
            , COALESCE(CNMV_CO_CD, ' ')
            , COALESCE(PRNT_OP_CO_CD, ' ')
            , COALESCE(CRNT_OP_CO_CD, ' ')
            , COALESCE(S1.IO_BND_CD , ' ')
            , COALESCE(CASE WHEN COALESCE(S1.RCV_TERM_CD, '*')||COALESCE(S1.DE_TERM_CD, '*') = '**' THEN NULL ELSE S1.RCV_TERM_CD||'/'||S1.DE_TERM_CD END, ' ')
            , COALESCE(S1.N1ST_OC_YD_CD, ' ')
            , COALESCE(S1.N1ST_VL_YD_CD, ' ')
            , COALESCE(S1.LST_VD_YD_CD, ' ')
            , COALESCE(S1.TRK_GATE_IO_YD_CD, ' ')
            -----20220609 PK COLUMN ADD
            , COALESCE(S1.CNTR_STS_CD, ' ')
            , COALESCE(S1.DISP_FLG, ' ')
            ;
    /* 실행건수 추출*/
    SET vROWS = (select @@row_count);
    /* 로그 테이블에 기록*/
    INSERT INTO COMMON.JOB_PROCEDURE_LOG(WORK_TIME, PROC_NAME, WORK_NAME, START_TIME, END_TIME, RUN_TIME, AFFECT_ROWS, DATA_SET,TARGET_TABLE,JOB_PARAM, EDW_UPD_DT)
    VALUES (vWORK_TIME,vPROC_NAME, '2. Source SQL - Insert DML_CNTR_LTST_MVMT_SNAP', vSTART_TIME, DATETIME(current_timestamp(),"Asia/Singapore")
            , SUBSTR(CAST( DATETIME(current_timestamp(),"Asia/Singapore")- vSTART_TIME AS STRING), 5, INSTR(CAST( DATETIME(current_timestamp(),"Asia/Singapore")- vSTART_TIME AS STRING), '.') -5 )
            , vROWS, vDATA_SET,vTARGET_TABLE,vJOB_PARAM, DATETIME(current_timestamp(),"Asia/Singapore"));



/* 오류 발생시 로그테이블에 오류메세지 기록*/
EXCEPTION WHEN ERROR THEN

    -- EXCEPTION 에서는 변수를 사용하지 못하여 값을 넣어야 함. WORK_TIME, PROC_NAME 컬럼
    INSERT INTO COMMON.JOB_PROCEDURE_LOG(WORK_TIME, PROC_NAME, WORK_NAME, START_TIME,  END_TIME, ERROR_MSG, ERROR_STATEMENT, EDW_UPD_DT)
    VALUES (FORMAT_DATETIME('%Y%m%d %H%M%S', CURRENT_DATETIME("Asia/Singapore")),'SP_DML_CNTR_LTST_MVMT_SNAP_TD01', 'Failure', CURRENT_DATETIME("Asia/Singapore"), CURRENT_DATETIME("Asia/Singapore"),  @@error.message, @@error.statement_text, CURRENT_DATETIME("Asia/Singapore"));

    RAISE USING message = FORMAT("When you executed %s at %s, it caused an error: %s. Please check COMMON.JOB_PROCEDURE_LOG", @@error.statement_text, @@error.formatted_stack_trace, @@error.message);

END