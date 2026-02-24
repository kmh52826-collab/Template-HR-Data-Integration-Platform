# HR Fact Tables ETL Pipeline
--------------------------------
#### 자격증 / 학력 / 상벌 / 어학 / 경력 / 발령

**데이터 처리 구조:**
- 시스템A 3사는 테이블이 각 회사마다 분리되어 있어 brz(3사 각 테이블) → slv(3사 테이블 통합) → gld(통합 테이블 사용)
  - slv 레이어를 사용하는 이유: 각 테이블마다 3번씩 추가하면 유지보수가 어렵고 쿼리가 길어져 파악이 난해하므로 slv에서 단일 테이블 생성
- 시스템B: brz → gld 직접 처리

---

## FCT 자격증 f_hr_license
--------------------------------------

**주요 특징:**
- 시스템B + 시스템A 각각 유니온된 상태
  - 두 시스템은 서로 다른 로직을 사용하므로 문제 발생 시 각각 수정 필요
  - 시스템B 기준으로 컬럼 생성, 시스템A에 없는 컬럼은 NULL 처리

- 시스템A는 EMP_NO만 존재, EMP_ID는 존재하지 않아 EMP_ID = 알파벳 + EMP_NO로 구성

**컬럼 추가 시 주의사항:**
- 대시보드 추가를 위해 컬럼 추가 시, 시스템A & 시스템B 모두 추가 필요
- UNION 기준으로 쿼리 컬럼 수와 순서가 일치해야 테이블 수정 가능
- 컬럼 수와 순서를 맞췄는데 오류 발생 시 CAST도 확인 필요
  - 시스템B: 대부분 timestamp 형태
  - 시스템A: text 형태로 CAST 구문 추가 필요
- 날짜 형식 변경 시 데이터가 안 나오는 경우가 많으므로 CAST 후 데이터 확인 필수


```
%python
# 자격증 정보 통합 쿼리
df = spark.sql("""
-- 시스템B 쿼리               
SELECT   
LICENSE.MOD_USER_ID  AS MOD_PRSN,  
LICENSE.MOD_DATE AS MOD_DTT,  
LICENSE.TZ_CD AS TZ_CD,  
LICENSE.TZ_DATE AS TZ_DTT,  
CAST(LICENSE.EMP_ID AS string ) AS EMP_ID,  
LICENSE.PHM_LICENSE_ID AS EMP_LCNS_ID,  
LICENSE.LICENSE_COMPANY_NM AS ISSUE_ORG,  
LICENSE.LICENSE_TYPE_CD AS LCNS_DIV_CD,  
COMM_LICENSE_TY.CD_NM AS LCNS_DIV_NM,  
COMM_LICENSE.CD_NM AS LCNS_NM,  
LICENSE.NOTE AS NOTE,  
LICENSE.BONUS_TYPE AS BONUS_PAY_DIV_CD,  
LICENSE.REG_YN AS REG_YN,  
LICENSE.LICENSE_NO AS LCNS_NO,  
to_timestamp(LICENSE.END_YMD , 'yyyyMMdd')AS VALID_DT,  
to_timestamp(LICENSE.STA_YMD , 'yyyyMMdd') AS GET_DT,  
LICENSE.PERSON_ID  AS PRSNL_ID,  
LICENSE.LICENSE_CD AS LCNS_CD  
FROM brz.system_b.PHM_LICENSE LICENSE  
LEFT JOIN brz.system_b.VE_FRM_CODE COMM_LICENSE 
  ON COMM_LICENSE.CD_KIND ='PHM_LICENSE_CD' AND LICENSE.LICENSE_CD = COMM_LICENSE.CD  
LEFT JOIN brz.system_b.VE_FRM_CODE COMM_LICENSE_TY 
  ON COMM_LICENSE_TY.CD_KIND ='PHM_LICENSE_TYPE_CD' AND LICENSE.LICENSE_TYPE_CD = COMM_LICENSE_TY.CD  
  
UNION  
-- 시스템A 쿼리
SELECT 
NULL as MOD_PRSN,  
l.UPDATE_DTS as MOD_DTT,  
NULL as TZ_CD,  
NULL as TZ_DTT,   
CASE  
 WHEN l.COMPANY_CD = '3000' THEN 'a' || l.EMP_NO -- 회사A
 WHEN l.COMPANY_CD = '2000' THEN 'q' || l.EMP_NO -- 회사B
 WHEN l.COMPANY_CD = '1000' THEN 'f' || l.EMP_NO -- 회사C
  ELSE l.EMP_NO  
 END AS EMP_ID,
NULL as EMP_LCNS_ID,  
l.ISSUE_ISTN_NM as ISSUE_ORG,  
NULL as LCNS_DIV_CD,  
NULL as LCNS_DIV_NM,  
l.QUA_CARD_NM as LCNS_NM,  
l.RMK_DC as NOTE,  
NULL as BONUS_PAY_DIV_CD,  
NULL as REG_YN,  
NULL as LCNS_NO,  
to_timestamp(l.VLID_DT , 'yyyyMMdd') as VALID_DT,  
to_timestamp(l.ACQS_DT , 'yyyyMMdd') as GET_DT,  
NULL as PRSNL_ID,  
NULL as LCNS_CD  
FROM slv.system_a.hr_license_mst l  
WHERE l.COMPANY_CD in ('1000', '2000', '3000') 
""")

# gld 레이어에 저장
df.write.mode("overwrite").save("abfss://gld@storage_account.dfs.core.windows.net/f_hr_license")
```
```
%sql
-- Delta 테이블 생성
USE gld.default;
CREATE TABLE IF NOT EXISTS f_hr_license
USING DELTA
LOCATION 'abfss://gld@storage_account.dfs.core.windows.net/f_hr_license';

-- 데이터 건수 확인
SELECT COUNT(*) FROM gld.default.f_hr_license;
```


## FCT 학력 정보 f_hr_scholar
-----------------------------------------

**시스템B 특징:**
- 최종학력여부 컬럼[LAST_YN]을 사용할 수 없음 (다수 회사에서 학력 여부를 표시하지 않았기 때문)
- 최종학교명 로직 규칙: 졸업일자가 존재하고, 졸업일자가 더 최신인 학교, 졸업일자가 동일한 경우 학력코드가 더 높은 쪽이 최종학교명으로 설정
- 전공명[MJR_NM]: 공통테이블에서 가져온 전공명을 사용하되, 전공명이 null인 경우 학력 팩트 자체적으로 담겨있는 전공명으로 대체 (24.10.23 추가)
  - 원천테이블에서 이분화되는 데이터들이 존재하여 위와 같이 대체되게 설정

**시스템A 특징:**
- 최종학력 컬럼 사용 가능 [LAST_LEDU_YN = 'Y']일 때 최종학교명으로 설정


```
%python
# 학력 정보 통합 쿼리
DF = spark.sql("""
-- 시스템B 쿼리 
WITH ranked_scholar AS (  
    SELECT  
        SCH.PERSON_ID,  
        comm.CD_NM AS FIN_SCH_NM,  
        RANK() OVER (  
            PARTITION BY SCH.PERSON_ID  
            ORDER BY SCH.SCH_GRD_CD DESC, COALESCE(SCH.END_YM, '999912') DESC, 
                     SCH.STA_YM DESC, comm.CD_NM ASC  
        ) AS rn,  
        SCH.STA_YM AS STA_YM,  
        SCH.END_YM AS GRAD_DT,  
        SCH.SCH_KIND_CD AS SCH_KIND_CD,  
        SCH.SCH_GRD_CD AS SCH_GRD_CD  
    FROM brz.system_b.PHM_SCHOLAR SCH  
    LEFT JOIN brz.system_b.VE_FRM_CODE comm 
      ON comm.cd_kind = 'PHM_SCH_CD' AND sch.SCH_CD = comm.cd  
    WHERE SCH.END_YM IS NOT NULL
), 
final_scholar AS (  
    SELECT  
        PERSON_ID,  
        FIN_SCH_NM,  
        SCH_KIND_CD,  
        MAX(STA_YM) AS MAX_STA_YM,  
        MAX(GRAD_DT) AS MAX_GRAD_DT,  
        MAX(SCH_GRD_CD) AS MAX_SCH_GRD_CD  
    FROM ranked_scholar  
    WHERE rn = 1  
    GROUP BY PERSON_ID, FIN_SCH_NM, SCH_KIND_CD  
)  
SELECT DISTINCT 
SCH.SUB_MAJOR_NM AS SUBMJR_NM,
SCH.SUB_MAJOR_CD AS SUBMJR_CD,
SCH.ENTRANCE_CD AS SCHENT_DIV_CD,
SCH.SCH_KIND_CD AS SCH_DIV_CD,
SCH.DAY_KIND_CD AS DAY_NIGHT_DIV_CD,
SCH.SCH_PLACE_CD AS SCH_PLC_CD,
SCH.SCH_PLACE_NM AS SCH_PLC_NM,
SCH.SCH_PLACE_DET_CD AS SCH_PLC_DTLS_CD,
SCH.SCH_PLACE_DET_NM AS PLC_REGION_DTLS,
SCH.LAST_YN AS LAST_SCHLST_YN,
SCH.CAREER_NUM AS CAREER_ADMIT_MTH_CNT,
SCH.NOTE AS NOTE,
SCH.MOD_USER_ID AS MOD_PRSN,
SCH.MOD_DATE AS MOD_DTT,
SCH.TZ_CD AS TZ_CD,
SCH.TZ_DATE AS TZ_DTT,
SCH.PHM_SCHOLAR_ID AS EMP_SCHLST_ID,
CAST(SCH.EMP_ID AS varchar(20)) AS EMP_ID,
SCH.PERSON_ID AS PRSNL_ID,
SCH.STA_YM AS SCHENT_YYYYMM,
SCH.END_YM AS GRAD_YYYYMM,
dsch.DTL_CD AS SCHLST_CD,
SCH.SCH_CD AS SCH_CD,
CAST(SCH.MAJOR_CD AS VARCHAR(20)) AS MAJOR_CD,
SCH.DOU_MAJOR_CD AS DOU_MAJOR_CD,
SCH.DOU_MAJOR_NM AS DOU_MAJOR_NM,
dsch.DTL_NM AS SCHLST_NM,
CAST(comm.CD_NM AS VARCHAR(40)) AS SCH_NM,
COALESCE(list.CD_NM, SCH.MAJOR_NM) AS MJR_NM,
CASE  
    WHEN comm.CD_NM = fs.FIN_SCH_NM 
         AND (SCH.END_YM = fs.MAX_GRAD_DT OR SCH.END_YM IS NULL) 
         AND SCH.STA_YM = fs.MAX_STA_YM 
         AND SCH.SCH_GRD_CD = fs.MAX_SCH_GRD_CD 
         AND (fs.MAX_SCH_GRD_CD >= '01' OR list.CD_NM IS NOT NULL)  
    THEN fs.FIN_SCH_NM  
    ELSE NULL  
END AS LAST_SCH_NM
FROM brz.system_b.PHM_SCHOLAR SCH   
LEFT JOIN brz.system_b.VE_FRM_CODE comm 
  ON comm.cd_kind = 'PHM_SCH_CD' AND sch.SCH_CD = comm.cd     
LEFT JOIN brz.system_b.VE_FRM_CODE list  
  ON list.cd_kind = 'PHM_MAJOR_CD' AND sch.MAJOR_CD = list.cd
LEFT JOIN common_schema.apps.m_com_hr_itg_cd_mn dsch 
  ON dsch.df_cd = 'LaSchRatCl' AND sch.SCH_GRD_CD = dsch.Value1
LEFT JOIN gld.default.d_hr_school grad 
  ON sch.SCH_GRD_CD = grad.SCHL_CD  
LEFT JOIN final_scholar fs ON fs.PERSON_ID = SCH.PERSON_ID

UNION
-- 시스템A 쿼리
SELECT 
NULL AS SUBMJR_NM,
NULL AS SUBMJR_CD,
NULL AS SCHENT_DIV_CD,
NULL AS SCH_DIV_CD,
NULL AS DAY_NIGHT_DIV_CD,
NULL AS SCH_PLC_CD,
NULL AS SCH_PLC_NM,
NULL AS SCH_PLC_DTLS_CD,
NULL AS PLC_REGION_DTLS,
sch.LAST_LEDU_YN AS LAST_SCHLST_YN,
NULL AS CAREER_ADMIT_MTH_CNT,
NULL AS NOTE,
NULL AS MOD_PRSN,
sch.UPDATE_DTS AS MOD_DTT,
NULL AS TZ_CD,
NULL AS TZ_DTT,
NULL AS EMP_SCHLST_ID,
CASE
    WHEN sch.COMPANY_CD = '3000' THEN 'a' || sch.EMP_NO -- 회사A
    WHEN sch.COMPANY_CD = '2000' THEN 'q' || sch.EMP_NO -- 회사B
    WHEN sch.COMPANY_CD = '1000' THEN 'f' || sch.EMP_NO -- 회사C
    ELSE sch.EMP_NO
END AS EMP_ID,
NULL AS PRSNL_ID,
TO_CHAR(TO_DATE(sch.MTRC_DT, 'yyyyMMdd'), 'yyyyMM') AS SCHENT_YYYYMM,
TO_CHAR(TO_DATE(sch.GRDT_DT, 'yyyyMMdd'), 'yyyyMM') AS GRAD_YYYYMM,
COALESCE(dicd.DTL_CD, docd.DTL_CD, stcd.DTL_CD, NULL) AS SCHLST_CD,
sch.SCHL_CD AS SCH_CD,
CAST(sch.DMJ_CD AS VARCHAR(20)) AS MAJOR_CD,
sch.PL_MJR_CD AS DOU_MAJOR_CD,
sch.PL_MJR_NM AS DOU_MAJOR_NM,
COALESCE(dicd.DTL_NM, docd.DTL_NM, stcd.DTL_NM, NULL) AS SCHLST_NM,
CAST(sch.SCHL_NM AS VARCHAR(40)) AS SCH_NM,
sch.MJR_NM AS MJR_NM,
CASE
    WHEN sch.LAST_LEDU_YN = 'Y' THEN SCHL_NM
    ELSE NULL
END AS LAST_SCH_NM
FROM slv.system_a.HR_SCHOCARE_MST sch
LEFT JOIN (
    SELECT DISTINCT DTL_CD, DTL_NM, Value7 
    FROM common_schema.apps.m_com_hr_itg_cd_mn 
    WHERE df_cd = 'LaSchRatCl' AND Value7 IS NOT NULL
) dicd ON dicd.Value7 = sch.LEDU_CD AND sch.COMPANY_CD = '3000'
LEFT JOIN (
    SELECT DISTINCT DTL_CD, DTL_NM, Value3 
    FROM common_schema.apps.m_com_hr_itg_cd_mn 
    WHERE df_cd = 'LaSchRatCl' AND Value3 IS NOT NULL
) docd ON docd.Value3 = sch.LEDU_CD AND sch.COMPANY_CD = '1000'
LEFT JOIN (
    SELECT DISTINCT DTL_CD, DTL_NM, Value5 
    FROM common_schema.apps.m_com_hr_itg_cd_mn 
    WHERE df_cd = 'LaSchRatCl' AND Value5 IS NOT NULL
) stcd ON stcd.Value5 = sch.LEDU_CD AND sch.COMPANY_CD = '2000'
WHERE COMPANY_CD IN ('1000', '2000', '3000')
""")

# gld 레이어에 저장
DF.write.mode("overwrite").save("abfss://gld@storage_account.dfs.core.windows.net/f_hr_scholar")
```

```
%sql
-- Delta 테이블 생성
USE gld.default;
CREATE TABLE IF NOT EXISTS f_hr_scholar
USING DELTA
LOCATION 'abfss://gld@storage_account.dfs.core.windows.net/f_hr_scholar';

-- 데이터 건수 확인
SELECT COUNT(*) FROM gld.default.f_hr_scholar;
```


## FCT 상벌정보 f_hr_reward_penalty
-----------------------------------------

**데이터 구조:**
- 시스템B: 징계 & 포상이 한 테이블에 존재 [brz.system_b.PPM_MNT]
- 시스템A: 징계 & 포상이 각각 다른 테이블로 존재
  - 포상: SLV.system_a.hr_huprize_list
  - 징계: SLV.system_a.HR_HUDISCIP_LIST
- 최종적으로 UNION 3번 진행 (시스템B 상벌 + 시스템A 포상 + 시스템A 징계)


```
%sql
-- Delta 테이블 생성
USE gld.default;
CREATE TABLE IF NOT EXISTS f_hr_scholar
USING DELTA
LOCATION 'abfss://gld@storage_account.dfs.core.windows.net/f_hr_scholar';

-- 데이터 건수 확인
SELECT COUNT(*) FROM gld.default.f_hr_scholar;
# 상벌 정보 통합 쿼리
DF = spark.sql("""
-- 시스템B 쿼리
SELECT 
MNT.APPLY_YN AS CAM_APPLY_YN,
MNT.DEL_YN AS CAM_DEL_YN,
MNT.COMPANY_CD AS HR_FIELD_CD,
CAST(MNT.EMP_ID AS STRING) AS EMP_ID,
MNT.TYPE_CD AS RWD_RNTY_DIV_CD,
MNT.KIND_CD AS RWD_RNTY_KIND_CD,
MNT.INOFF_CD AS INOUT_HOUSE_DIV_CDD,
MNT.PPM_NO AS RWD_RNTY_NO,
cls.CD_NM AS RWD_RNTY_NM,
MNT.PPM_YMD AS RWD_RNTY_DT,
MNT.PPM_MON AS RWD_AMT,
MNT.PPM_DESC AS RWD_PNTY_RSN,
MNT.END_YMD AS END_DT,
MNT.CUST_COL1 AS PERCOM_DT_FRST,
MNT.INPUT_TYPE_CD AS INPUT_DATA_TYPE_CD,
MNT.MNT_POOL_ID AS MENTOR_POOL_ID,
MNT.CAM_YN AS CAM_REG_YN,
MNT.CAM_DATE AS CAM_LINK_DTT,
MNT.CAM_DOC_ID AS CAM_CNF_DOC_ID,
MNT.CAM_PRE_ID AS CAM_RCPNT_ID,
MNT.NOTE AS NOTE,
MNT.MOD_USER_ID AS MOD_PRSN,
MNT.MOD_DATE AS MOD_DTT,
MNT.TZ_CD AS TZ_CD,
MNT.TZ_DATE AS TZ_DTT,
MNT.PPM_MNT_ID AS RWD_DTLS_MNGMNT_ID
FROM brz.system_b.PPM_MNT MNT 
LEFT JOIN brz.system_b.VE_FRM_CODE cls 
  ON cls.CD_KIND = 'PPM_TYPE_CD' AND MNT.TYPE_CD = cls.CD

UNION
-- 시스템A 쿼리 - 포상
SELECT  
NULL AS CAM_APPLY_YN, 
NULL AS CAM_DEL_YN, 
NULL AS HR_FIELD_CD, 
CASE
    WHEN COMPANY_CD = '3000' THEN 'a' || EMP_NO -- 회사A
    WHEN COMPANY_CD = '2000' THEN 'q' || EMP_NO -- 회사B
    WHEN COMPANY_CD = '1000' THEN 'f' || EMP_NO -- 회사C
    ELSE EMP_NO  
END AS EMP_ID,  
NULL AS RWD_RNTY_DIV_CD, 
NULL AS RWD_RNTY_KIND_CD, 
NULL AS INOUT_HOUSE_DIV_CDD,
NULL AS RWD_RNTY_NO, 
'포상' AS RWD_RNTY_NM,
TO_TIMESTAMP(PRZ_DT, 'yyyyMMdd') AS RWD_RNTY_DT,
NULL AS RWD_AMT, 
PRZ_DC AS RWD_PNTY_RSN,
NULL AS END_DT, 
NULL AS PERCOM_DT_FRST, 
NULL AS INPUT_DATA_TYPE_CD,  
NULL AS MENTOR_POOL_ID, 
NULL AS CAM_REG_YN,
NULL AS CAM_LINK_DTT, 
NULL AS CAM_CNF_DOC_ID, 
NULL AS CAM_RCPNT_ID,
NULL AS NOTE, 
NULL AS MOD_PRSN, 
UPDATE_DTS AS MOD_DTT,
NULL AS TZ_CD,
NULL AS TZ_DTT, 
NULL AS RWD_DTLS_MNGMNT_ID
FROM SLV.system_a.hr_huprize_list

UNION
-- 시스템A 쿼리 - 징계
SELECT  
NULL AS CAM_APPLY_YN, 
NULL AS CAM_DEL_YN, 
NULL AS HR_FIELD_CD, 
CASE
    WHEN COMPANY_CD = '3000' THEN 'a' || EMP_NO -- 회사A
    WHEN COMPANY_CD = '2000' THEN 'q' || EMP_NO -- 회사B
    WHEN COMPANY_CD = '1000' THEN 'f' || EMP_NO -- 회사C
    ELSE EMP_NO  
END AS EMP_ID,  
NULL AS RWD_RNTY_DIV_CD, 
NULL AS RWD_RNTY_KIND_CD, 
NULL AS INOUT_HOUSE_DIV_CDD,
NULL AS RWD_RNTY_NO, 
'징계' AS RWD_RNTY_NM,
TO_TIMESTAMP(DCPL_DT, 'yyyyMMdd') AS RWD_RNTY_DT,
NULL AS RWD_AMT, 
DCPL_DC AS RWD_PNTY_RSN,
NULL AS END_DT, 
NULL AS PERCOM_DT_FRST, 
NULL AS INPUT_DATA_TYPE_CD,  
NULL AS MENTOR_POOL_ID, 
NULL AS CAM_REG_YN,
NULL AS CAM_LINK_DTT, 
NULL AS CAM_CNF_DOC_ID, 
NULL AS CAM_RCPNT_ID,
NULL AS NOTE, 
NULL AS MOD_PRSN, 
UPDATE_DTS AS MOD_DTT,
NULL AS TZ_CD,
NULL AS TZ_DTT, 
NULL AS RWD_DTLS_MNGMNT_ID
FROM SLV.system_a.HR_HUDISCIP_LIST
""")

# gld 레이어에 저장
DF.write.mode("overwrite").save("abfss://gld@storage_account.dfs.core.windows.net/f_hr_reward_penalty")
```

```
%sql
-- Delta 테이블 생성
USE gld.default;
CREATE TABLE IF NOT EXISTS f_hr_reward_penalty
USING DELTA
LOCATION 'abfss://gld@storage_account.dfs.core.windows.net/f_hr_reward_penalty';

-- 데이터 건수 확인
SELECT COUNT(*) FROM gld.default.f_hr_reward_penalty;
```

## FCT 경력정보 f_hr_career
-----------------------------------------

**특징:**
- 시스템B & 시스템A 모두 특별한 사항 없음
- 시스템A: 근무개월수를 MONTHS_BETWEEN 함수로 계산하여 제공

```
%python
# 경력 정보 통합 쿼리
DF = spark.sql("""
-- 시스템B 쿼리
SELECT  
    PHM_CAREER_ID AS EMP_CAREER_ID,  
    PLACE_NM AS PLC,  
    PERSON_ID AS PRSNL_ID,  
    STA_YMD AS WORK_STRT_DT,  
    END_YMD AS WORK_END_DT,  
    ORG_CORP_NM AS WORK_ORG_NM,  
    POSITION_NM AS LAST_JOB_GD_NM,  
    RETIRE_CAUSE AS RETRD_RSN,  
    RCAREER_NUM AS REAL_CAREER_MTH_CNT,  
    RECO_RATE AS ADMIT_RATE,  
    CAST(CAREER_NUM AS VARCHAR(20)) AS ADMIT_CAREER_MTH_CNT,  
    NOTE AS NOTE,  
    MOD_USER_ID AS MOD_PRSN,  
    MOD_DATE AS MOD_DTT,  
    TZ_CD AS TZ_CD,  
    TZ_DATE AS TZ_DTT,  
    ORG_ENG_NM AS EMGL_CMP_NM,  
    CAST(EMP_ID AS VARCHAR(20)) AS EMP_ID,
    WORK_NM 
FROM brz.system_b.phm_career  
  
UNION  
-- 시스템A 쿼리
SELECT  
    NULL AS EMP_CAREER_ID,  
    NULL AS PLC,  
    NULL AS PRSNL_ID,  
    TO_TIMESTAMP(JNCO_DT, 'yyyyMMdd') AS WORK_STRT_DT,  
    TO_TIMESTAMP(RETR_DT, 'yyyyMMdd') AS WORK_END_DT,  
    COMPANY_NM AS WORK_ORG_NM,  
    NULL AS LAST_JOB_GD_NM,  
    NULL AS RETRD_RSN,  
    NULL AS REAL_CAREER_MTH_CNT,  
    NULL AS ADMIT_RATE,  
    CAST(CAST(ROUND(MONTHS_BETWEEN(TO_DATE(RETR_DT, 'yyyyMMdd'), 
                                    TO_DATE(JNCO_DT, 'yyyyMMdd')), 0) AS INT) AS VARCHAR(20)) 
        AS ADMIT_CAREER_MTH_CNT,
    NULL AS NOTE,  
    NULL AS MOD_PRSN,  
    UPDATE_DTS AS MOD_DTT,  
    NULL AS TZ_CD,  
    NULL AS TZ_DTT,  
    NULL AS EMGL_CMP_NM,  
    CASE  
        WHEN COMPANY_CD = '3000' THEN 'a' || EMP_NO -- 회사A
        WHEN COMPANY_CD = '2000' THEN 'q' || EMP_NO -- 회사B
        WHEN COMPANY_CD = '1000' THEN 'f' || EMP_NO -- 회사C
        ELSE EMP_NO  
    END AS EMP_ID,
    RSPT_TSK_NM
FROM slv.system_a.hr_career_mst  
WHERE COMPANY_CD IN ('1000', '2000', '3000')
""")

# Spark 3.0부터 날짜 파싱이 변경되어 오류 발생 방지
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# gld 레이어에 저장
DF.write.mode("overwrite").save("abfss://gld@storage_account.dfs.core.windows.net/f_hr_career")
```


```
%sql
-- Delta 테이블 생성
USE gld.default;
CREATE TABLE IF NOT EXISTS f_hr_career
USING DELTA
LOCATION 'abfss://gld@storage_account.dfs.core.windows.net/f_hr_career';

-- 데이터 건수 확인
SELECT COUNT(*) FROM gld.default.f_hr_career;
```










