# 🛠 Fact To Gold Notebook Detail
--------------------------------
#### 자격증 / 학력 / 상벌 / 경력 / 병역 / 어학 / 발령

## 노트북 요약

**이 노트북은 HR 데이터 통합 ETL 파이프라인으로 다음 7개의 팩트 테이블을 생성합니다:**

1. **f_hr_license** - 자격증 정보
2. **f_hr_scholar** - 학력 정보
3. **f_hr_reward_penalty** - 상벌 정보
4. **f_hr_career** - 경력 정보
5. **f_hr_military** - 병역 정보
6. **f_hr_language** - 어학 정보
7. **f_hr_appoint_history** - 발령 정보

**데이터 통합 방식:**
- 시스템B(화이트)와 시스템A(더존 3사) 데이터를 UNION으로 통합
- 시스템A 3사: 회사코드로 구분 (1000=회사C, 2000=회사B, 3000=회사A)
- EMP_ID 생성 규칙: 회사별 접두사 + EMP_NO (f/q/a)

**주요 기술 사항:**
- Delta Lake 형식으로 Azure Storage에 저장
- UNION 시 컬럼 수, 순서, 데이터 타입 일치 필수
- 날짜 형식 변환 시 CAST 및 데이터 검증 필수
- 공통코드 테이블 조인으로 코드명 매핑

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

## FCT 병역정보 f_hr_military
-----------------------------------------

**특징:**
- 시스템B & 시스템A 모두 특별한 사항 없음
- 복무개월수는 MONTHS_BETWEEN 함수로 계산하여 제공

```
%python
# 병역 정보 통합 쿼리
DF = spark.sql("""
-- 시스템B 쿼리
SELECT 
CAST(ARMY.EMP_ID AS VARCHAR(20)) AS EMP_ID,
ARMY.ARMY_NO_CD AS INCMPL_RSN,
ARMY.ARMY_SERV_CD AS SRV_TYPE_CD,
ARMY.PHM_ARMY_ID AS EMP_MIL_SRVC_ID,
ARMY.ARMY_NO_REASON_CD AS MIL_CPLT_DIV_CD,
ARMY.ARMY_TYPE_CD AS MIL_TYPE_CD,
typ.CD_NM AS MIL_TYPE_NM, -- 군별명
ARMY.ARMY_BRANCH_CD AS MIL_BRNCH_CD,
ARMY.ARMY_CLASS_CD AS MIL_RANK_CD,
cla.CD_NM AS MIL_RANK_NM, -- 계급명
ARMY.ARMY_NO AS MIL_NO,
ARMY.ARMY_DISCHARGE_CD AS DSCHRG_DIV_CD,
ARMY.IN_YMD AS MIL_JOIN_DT,
ARMY.OUT_YMD AS DSCHRG_DT,
ARMY.PLUS_MM AS WRKPLC_RSRV_ARMY_JOIN_YN,
ARMY.ARMY_CLASSIFICATION_CD AS MIL_CLASS_CD,
cls.CD_NM AS MIL_CLASS_NM, -- 역종명
ARMY.ARMY_MTALENT_CD AS MIL_SP_CD,
mtl.CD_NM AS MIL_SP_NM, -- 주특기명
ARMY.NOTE AS NOTE,
ARMY.MOD_USER_ID AS MOD_PRSN,
ARMY.MOD_DATE AS MOD_DTT,
ARMY.TZ_CD AS TZ_CD,
ARMY.TZ_DATE AS TZ_DTT,
ARMY.PERSON_ID AS PRSNL_ID,
ROUND(MONTHS_BETWEEN(TO_DATE(DSCHRG_DT, 'yyyy-MM-dd'), 
                     TO_DATE(MIL_JOIN_DT, 'yyyy-MM-dd'))) AS MIL_ADMIT_MTH_CNT
FROM brz.system_b.PHM_ARMY ARMY
LEFT JOIN brz.system_b.VE_FRM_CODE cls 
  ON cls.CD_KIND = 'PHM_ARMY_CLASSIFICATION_CD' AND army.ARMY_CLASSIFICATION_CD = cls.cd
LEFT JOIN brz.system_b.VE_FRM_CODE mtl 
  ON mtl.CD_KIND = 'PHM_ARMY_MTALENT_CD' AND army.ARMY_MTALENT_CD = mtl.cd
LEFT JOIN brz.system_b.VE_FRM_CODE typ 
  ON typ.CD_KIND = 'PHM_ARMY_TYPE_CD' AND army.ARMY_TYPE_CD = typ.cd
LEFT JOIN brz.system_b.VE_FRM_CODE cla 
  ON cla.CD_KIND = 'PHM_ARMY_CLASS_CD' AND army.ARMY_CLASS_CD = cla.cd

UNION 
-- 시스템A 쿼리
SELECT DISTINCT
CASE
    WHEN f.COMPANY_CD = '3000' THEN 'a' || f.EMP_NO -- 회사A
    WHEN f.COMPANY_CD = '2000' THEN 'q' || f.EMP_NO -- 회사B
    WHEN f.COMPANY_CD = '1000' THEN 'f' || f.EMP_NO -- 회사C
    ELSE f.EMP_NO
END AS EMP_ID,
f.EXMT_REASON_CD AS INCMPL_RSN,
NULL AS SRV_TYPE_CD,
NULL AS EMP_MIL_SRVC_ID,
NULL AS MIL_CPLT_DIV_CD,
f.AMFT_CD AS MIL_TYPE_CD,
type.SYSDEF_NM AS MIL_TYPE_NM,
NULL AS MIL_BRNCH_CD, 
f.MLH_CD AS MIL_RANK_CD,
rank.SYSDEF_NM AS MIL_RANK_NM,
NULL AS MIL_NO, 
NULL AS DSCHRG_DIV_CD, 
TO_TIMESTAMP(f.ENST_DT, 'yyyyMMdd') AS MIL_JOIN_DT,
TO_TIMESTAMP(f.LAYOFF_DT, 'yyyyMMdd') AS DSCHRG_DT,
NULL AS WRKPLC_RSRV_ARMY_JOIN_YN, 
f.COSS_CD AS MIL_CLASS_CD,
clas.SYSDEF_NM AS MIL_CLASS_NM,
NULL AS MIL_SP_CD, 
NULL AS MIL_SP_NM, 
NULL AS NOTE, 
NULL AS MOD_PRSN, 
f.UPDATE_DTS AS MOD_DTT, 
NULL AS TZ_CD, 
NULL AS TZ_DTT, 
NULL AS PRSNL_ID, 
ROUND(MONTHS_BETWEEN(TO_TIMESTAMP(f.LAYOFF_DT, 'yyyyMMdd'), 
                     TO_TIMESTAMP(f.ENST_DT, 'yyyyMMdd'))) AS MIL_ADMIT_MTH_CNT
FROM slv.system_a.HR_EMPINFO_DTL f 
LEFT JOIN slv.system_a.ma_codedtl type 
  ON type.SYSDEF_CD = f.AMFT_CD AND type.MODULE_CD = 'HR' 
  AND type.FIELD_CD = 'P00770' AND type.COMPANY_CD IN ('1000', '2000', '3000')
LEFT JOIN slv.system_a.ma_codedtl rank 
  ON rank.SYSDEF_CD = f.AMFT_CD AND rank.MODULE_CD = 'HR' 
  AND rank.FIELD_CD = 'P00800' AND rank.COMPANY_CD IN ('1000', '2000', '3000')
LEFT JOIN slv.system_a.ma_codedtl clas 
  ON clas.SYSDEF_CD = f.AMFT_CD AND clas.MODULE_CD = 'HR' 
  AND clas.FIELD_CD = 'P06990' AND clas.COMPANY_CD IN ('1000', '2000', '3000')
WHERE f.company_cd IN ('1000', '2000', '3000')
""")

# gld 레이어에 저장
DF.write.mode("overwrite").save("abfss://gld@storage_account.dfs.core.windows.net/f_hr_military")
```

```
%sql
-- Delta 테이블 생성
USE gld.default;
CREATE TABLE IF NOT EXISTS f_hr_military
USING DELTA
LOCATION 'abfss://gld@storage_account.dfs.core.windows.net/f_hr_military';

-- 데이터 건수 확인
SELECT COUNT(*) FROM gld.default.f_hr_military;
```


## FCT 어학정보 f_hr_language
-----------------------------------------

**특징:**
- 시스템B & 시스템A 모두 특별한 사항 없음

```
%python
# 어학 정보 통합 쿼리
DF = spark.sql("""
-- 시스템B 쿼리
SELECT 
est.PHM_LANG_EST_ID AS EMP_LANG_ESTM_ID, -- 사원어학평가ID
CAST(est.EMP_ID AS VARCHAR(20)) AS EMP_ID, -- 사원ID
est.PERSON_ID AS PRSNL_ID, -- 개인ID
est.LANG_CD AS LANG_CD, -- 어학코드
LANG.cd_nm AS LANG_NM,
est.STD_YY AS STD_YYYY, -- 년도
est.EST_YMD AS ESTM_DT, -- 평가일자
est.VALID_YMD AS VALID_DT, -- 유효일자
est.EST_CD AS LANG_DIV_CD, -- 어학구분코드
estc.cd_nm AS LANG_DIV_NM, -- 어학구분명
est.EST_ORG_CD AS ESTM_ORG_CD, -- 평가기관코드
ORG.cd_nm AS EST_ORG_NM, -- 평가기관명
est.EST_PNT AS LANG_SCR, -- 어학점수, Functional level
est.EST_GRD_CD AS LANG_GRD_CD, -- 어학등급코드
GRD.cd_nm AS LANG_GRD_NM, -- 어학등급명
est.INTERNAL_PNT AS PRSNT_LEV, -- Present Level
est.NOTE, -- 비고
est.MOD_USER_ID AS MOD_PRSN,
est.MOD_DATE AS MOD_DTT,
est.TZ_CD AS TZ_CD,
est.TZ_DATE AS TZ_DTT
FROM brz.system_b.PHM_LANG_EST est 
LEFT JOIN brz.system_b.VE_FRM_CODE LANG 
  ON LANG.CD_KIND = 'PHM_LANG_CD' AND est.LANG_CD = LANG.cd
LEFT JOIN brz.system_b.VE_FRM_CODE estc 
  ON estc.CD_KIND = 'PHM_EST_CD' AND est.EST_CD = estc.cd
LEFT JOIN brz.system_b.VE_FRM_CODE ORG 
  ON ORG.CD_KIND = 'PHM_EST_ORG_CD' AND est.EST_ORG_CD = ORG.cd
LEFT JOIN brz.system_b.VE_FRM_CODE GRD 
  ON GRD.CD_KIND = 'PHM_EST_GRD_CD' AND est.EST_GRD_CD = GRD.cd

UNION 
-- 시스템A 쿼리
SELECT 
NULL AS EMP_LANG_ESTM_ID, 
CASE
    WHEN f.COMPANY_CD = '3000' THEN 'a' || f.EMP_NO -- 회사A
    WHEN f.COMPANY_CD = '2000' THEN 'q' || f.EMP_NO -- 회사B
    WHEN f.COMPANY_CD = '1000' THEN 'f' || f.EMP_NO -- 회사C
    ELSE f.EMP_NO
END AS EMP_ID,
NULL AS PRSNL_ID,
f.LANG_CD, -- 어학코드
m.SYSDEF_NM AS LANG_NM, -- 어학명
NULL AS STD_YYYY,
TO_TIMESTAMP(f.APYEXM_DT, 'yyyyMMdd') AS ESTM_DT, -- 평가일자
TO_TIMESTAMP(f.VLID_DT, 'yyyyMMdd') AS VALID_DT, -- 유효일자
f.LANG_FG_CD AS LANG_DIV_CD, -- 어학구분코드
fg.SYSDEF_NM AS LANG_DIV_NM, -- 어학구분명
f.TRIAL_ISTN_CD AS ESTM_ORG_CD, -- 평가기관코드
f.TRIAL_ISTN_NM AS EST_ORG_NM, -- 평가기관명
f.LANG_PT AS LANG_SCR, -- 어학점수
f.LANG_GRD_CD AS LANG_GRD_CD, -- 어학등급코드
f.LANG_GRD_NM AS LANG_GRD_NM, -- 어학등급명
NULL AS PRSNT_LEV,
f.RMK_DC AS NOTE, 
NULL AS MOD_PRSN,
f.UPDATE_DTS AS MOD_DTT,
NULL AS TZ_CD, 
NULL AS TZ_DTT 
FROM slv.system_a.hr_forelang_mst f 
LEFT JOIN slv.system_a.ma_codedtl m 
  ON m.SYSDEF_CD = f.lang_cd AND m.MODULE_CD = 'HR' 
  AND m.FIELD_CD = 'Z001_20343' AND m.COMPANY_CD IN ('1000', '2000', '3000')
LEFT JOIN slv.system_a.ma_codedtl fg 
  ON fg.SYSDEF_CD = f.lang_cd AND fg.MODULE_CD = 'HR' 
  AND fg.FIELD_CD = 'P00930' AND fg.COMPANY_CD IN ('1000', '2000', '3000')
WHERE f.COMPANY_CD IN ('1000', '2000', '3000')
""")

# gld 레이어에 저장
DF.write.mode("overwrite").save("abfss://gld@storage_account.dfs.core.windows.net/f_hr_language")
```

```
%sql
-- Delta 테이블 생성
USE gld.default;
CREATE TABLE IF NOT EXISTS f_hr_language
USING DELTA
LOCATION 'abfss://gld@storage_account.dfs.core.windows.net/f_hr_language';

-- 데이터 건수 확인
SELECT COUNT(*) FROM gld.default.f_hr_language;
```

## FCT 발령정보 f_hr_appoint_history
--------------------------

**데이터 구조:**
- 발령정보 = 시스템B 발령 + 시스템B 수기발령 + 시스템A 발령 총 3가지 테이블 유니온
- 시스템B 발령: brz.system_b.CAM_HISTORY
- 시스템B 수기발령: brz.system_b.cam_history_bef
- 시스템A 발령: slv.system_a.cam_past

**주요 특징:**
- 시스템A: 해당 과거 발령 slv.system_a.cam_past 테이블은 brz단에서 쿼리로 처리해서 가져옴
- 시스템A: 직급 컬럼은 **POSI_CD[직위]**로 사용 / **[PSTN_CD 사용안함]**
- 자발/비자발: 시스템A는 퇴직인 경우에 전체 자발/비자발 표현 불가
  - 자발/비자발 마스터: HR_EMP_SDTL 테이블에 존재하는 EMP_NO만 구분가능, 그 외 구분불가


```
%python
# 발령 정보 통합 쿼리
DF = spark.sql("""
SELECT DISTINCT  
-- 시스템B 발령
cam.REN_YMD AS LAYOFF_RTRN_SCHDL_DT,
cam.DIS_YN AS DSPTCH_YN,
cam.DIS_KIND_CD AS DSPTCH_DIV_CD,
cam.DIS_RTN_YMD AS DSPTCH_RTRN_SCHDL_DT,
cam.DIS_ORG_ID AS DSPTCH_DEPT_CD,
cam.DIS_AREA_NM AS DSPTCH_PLC_NM,
cam.HIRE_CD AS HIRED_DIV_CD,
cam.PUNISH_END_YMD AS PENAL_CNCL_SCHDL_DT,
cam.MAS_YN AS HR_RFLCTN_YN,
cam.PAY_DUTY_CD AS SLRY_JOB_TTLE_GRD,
cam.PRINT_YN AS PRNT_YN,
cam.CUST_COL2 AS JOB_GRP_CD,
cam.CUST_COL3 AS RFRNC_DIV_CD,
cam.CUST_COL4 AS ENTRST_YN,
cam.CUST_COL5 AS ENTRST_END_SCHDL_DT,
cam.CUST_COL7 AS DEL_YN,
cam.CUST_COL8 AS CNF_DOC_NO,
cam.CUST_COL9 AS CNF_DOC_NM,
cam.CUST_COL10 AS CAM_RSN,
cam.NOTE AS NOTE,
cam.MOD_USER_ID AS MOD_PRSN,
cam.MOD_DATE AS MOD_DTT,
cam.TZ_CD AS TZ_CD,
cam.TZ_DATE AS TZ_DTT,
cam.SUB_COMPANY_CD AS CMP_DIV_CD,
com.DTL_NM AS CMP_DIV_NM,
cam.PRO_GRADE_CD AS SLRY_STRT_SLRY_GRD_CD,
cam.PRO_KIND_CD AS FIRST_APPT_SLRY_KIND_CD,
cam.CNT_AMT AS YRLY_SLRY_AMT,
cam.SEND_YN AS JOB_SCND_YN,
cam.SEND_SUB_COMPANY_CD AS JOB_SCND_CAM_CMP_CD,
cam.SEND_JOP_CD AS JOB_SCND_JOB_GRP_CD,
cam.RETIRE_TYPE_CD AS RETRD_RSN_CD,
retire.CD_NM AS RETRD_RSN_NM,
cam.PAY_GRADE AS SLRY_GRD,
cam.PAY_JOP_CD AS SLRY_JOB_GRP,
cam.NEXT_POS_YMD AS NEXTERM_JOB_GD_PRMT_DT,
cam.CAM_HISTORY_ID AS CAM_HIST_ID,
CAST(cam.EMP_ID AS STRING) AS EMP_ID,
cam.PERSON_ID AS PRSNL_ID,
cam.CAM_DOC_ID AS CAM_CNF_ID,
cam.STA_YMD AS CAM_STRT_DT,
cam.END_YMD AS CAM_END_DT,
cam.CAM_YMD AS CAM_DT,
cam.SEQ AS CAM_ORDER,
cam.TYPE_CD AS CAM_CD,
COALESCE(mre.dtl_cd, cam.CAU_CD, NULL) AS CAM_CLASS_CD,
cam.COMPANY_CD AS CAM_HR_ZONE_CD,
cam.ORG_ID AS CAM_DEPT_ID,
cam.POS_CD AS CAM_JOB_RANK_CD,
rank.CD_NM AS CAM_JOB_RANK_NM,
cam.JOB_CD AS JOB_CD,
cam.DUTY_CD AS CAM_JOB_TTLE_CD,
duty.cd_nm AS CAM_JOB_TTLE_NM,
cam.EMP_KIND_CD AS CAM_EMP_KIND_CD,
cam.PAY_KIND_CD AS CAM_YRLY_SLRY_TYPE,
cam.PROBATION_YN AS PRBTN_YN,
cam.PROBATION_END_YMD AS PRBTN_END_DT,
cam.SEND_ORG_ID AS JOB_SCND_DEPT_ID,
cam.SEND_POS_CD AS JOB_SCND_JOB_GD_CD,
cam.SEND_POS_GRD_CD AS JOB_SCND_JOB_RANK_CD,
cam.SEND_JOB_CD AS JOB_SCND_JOB_CD,
cam.SEND_DUTY_CD AS JOB_SCND_JOB_TTLE_CD,
cam.REN_YN AS LAYOFF_YN,
COALESCE(mre.dtl_nm, cls.CD_NM, NULL) AS CAM_CLASS_NM,
COALESCE(mre.dtl_cd, 'EE9999') AS RETRD_TYPE,
CASE 
    WHEN COALESCE(mre.dtl_nm, '코드없음') = '퇴직(자발적)' THEN '자발적'
    WHEN COALESCE(mre.dtl_nm, '코드없음') = '퇴직(비자발적)' THEN '비자발적'
    ELSE NULL
END RETRD_TYPE_NM
FROM brz.system_b.CAM_HISTORY cam 
LEFT JOIN brz.system_b.VE_FRM_CODE cls 
  ON cls.CD_KIND = 'CAM_CAU_CD' AND cam.CAU_CD = cls.cd
LEFT JOIN common_schema.apps.m_com_hr_itg_cd_mn mre 
  ON mre.df_cd = 'VolRetCl' AND cam.CAU_CD = mre.Value1
LEFT JOIN brz.system_b.VE_FRM_CODE retire 
  ON retire.CD_KIND = 'CAM_RETIRE_CAU_CD' AND cam.RETIRE_TYPE_CD = retire.CD
LEFT JOIN common_schema.apps.m_com_cm_cd_mn com 
  ON com.DF_CD = 'ComCl' AND cam.SUB_COMPANY_CD = com.Value3
LEFT JOIN brz.system_b.VE_FRM_CODE duty 
  ON duty.CD_KIND = 'PHM_DUTY_CD' AND cam.DUTY_CD = duty.CD
LEFT JOIN brz.system_b.VE_FRM_CODE rank 
  ON rank.CD_KIND = 'PHM_POS_CD' AND cam.POS_CD = rank.CD

UNION ALL
-- 시스템B 수기 발령부분
SELECT DISTINCT  
NULL AS LAYOFF_RTRN_SCHDL_DT,  
NULL AS DSPTCH_YN,  
NULL AS DSPTCH_DIV_CD,  
NULL AS DSPTCH_RTRN_SCHDL_DT,  
NULL AS DSPTCH_DEPT_CD,  
NULL AS DSPTCH_PLC_NM,  
NULL AS HIRED_DIV_CD,  
NULL AS PENAL_CNCL_SCHDL_DT,  
NULL AS HR_RFLCTN_YN,  
NULL AS SLRY_JOB_TTLE_GRD,  
NULL AS PRNT_YN,  
NULL AS JOB_GRP_CD,  
NULL AS RFRNC_DIV_CD,  
NULL AS ENTRST_YN,  
NULL AS ENTRST_END_SCHDL_DT,  
NULL AS DEL_YN,  
NULL AS CNF_DOC_NO,  
NULL AS CNF_DOC_NM,  
NULL AS CAM_RSN,  
BEF.note AS NOTE,  
BEF.mod_user_id AS MOD_PRSN,  
BEF.mod_date AS MOD_DTT,  
BEF.tz_cd AS TZ_CD,  
BEF.tz_date AS TZ_DTT,  
NULL AS CMP_DIV_CD,
BEF.sub_company_nm AS CMP_DIV_NM,  
NULL AS SLRY_STRT_SLRY_GRD_CD,  
NULL AS FIRST_APPT_SLRY_KIND_CD,  
NULL AS YRLY_SLRY_AMT,  
NULL AS JOB_SCND_YN,  
NULL AS JOB_SCND_CAM_CMP_CD,  
NULL AS JOB_SCND_JOB_GRP_CD,  
NULL AS RETRD_RSN_CD,  
NULL AS RETRD_RSN_NM,  
NULL AS SLRY_GRD,  
NULL AS SLRY_JOB_GRP,  
NULL AS NEXTERM_JOB_GD_PRMT_DT,  
NULL AS CAM_HIST_ID,  
CAST(BEF.emp_id AS STRING) AS EMP_ID,  
NULL AS PRSNL_ID,  
NULL AS CAM_CNF_ID,  
NULL AS CAM_STRT_DT,  
NULL AS CAM_END_DT,  
BEF.sta_ymd AS CAM_DT,  
NULL AS CAM_ORDER,  
NULL AS CAM_CD,  
COALESCE(mre.dtl_cd, BEF.CAU_CD, NULL) AS CAM_CLASS_CD,
NULL AS CAM_HR_ZONE_CD,  
NULL AS CAM_DEPT_ID,  
NULL AS CAM_JOB_RANK_CD,  
BEF.pos_nm AS CAM_JOB_RANK_NM,  
NULL AS JOB_CD,  
NULL AS CAM_JOB_TTLE_CD,  
BEF.duty_nm AS CAM_JOB_TTLE_NM,  
NULL AS CAM_EMP_KIND_CD,  
NULL AS CAM_YRLY_SLRY_TYPE,  
NULL AS PRBTN_YN,  
NULL AS PRBTN_END_DT,  
NULL AS JOB_SCND_DEPT_ID,  
NULL AS JOB_SCND_JOB_GD_CD,  
NULL AS JOB_SCND_JOB_RANK_CD,  
NULL AS JOB_SCND_JOB_CD,  
NULL AS JOB_SCND_JOB_TTLE_CD,  
NULL AS LAYOFF_YN,  
COALESCE(mre.dtl_nm, cls.CD_NM, NULL) AS CAM_CLASS_NM,
COALESCE(mre.dtl_cd, 'EE9999') AS RETRD_TYPE,
CASE 
    WHEN COALESCE(mre.dtl_nm, NULL) = '퇴직(자발적)' THEN '자발적'
    WHEN COALESCE(mre.dtl_nm, NULL) = '퇴직(비자발적)' THEN '비자발적'
    ELSE '코드없음'  
END RETRD_TYPE_NM
FROM brz.system_b.cam_history_bef BEF  
LEFT JOIN brz.system_b.VE_FRM_CODE cls 
  ON cls.CD_KIND = 'CAM_CAU_CD' AND BEF.CAU_CD = cls.cd
LEFT JOIN common_schema.apps.m_com_hr_itg_cd_mn mre 
  ON mre.df_cd = 'VolRetCl' AND BEF.CAU_CD = mre.Value1

UNION ALL 
-- 시스템A 발령
SELECT
NULL AS LAYOFF_RTRN_SCHDL_DT,   
NULL AS DSPTCH_YN,   
NULL AS DSPTCH_DIV_CD, 
NULL AS DSPTCH_RTRN_SCHDL_DT, 
NULL AS DSPTCH_DEPT_CD,  
NULL AS DSPTCH_PLC_NM, 
NULL AS HIRED_DIV_CD,  
NULL AS PENAL_CNCL_SCHDL_DT,  
NULL AS HR_RFLCTN_YN,  
NULL AS SLRY_JOB_TTLE_GRD,  
NULL AS PRNT_YN,  
NULL AS JOB_GRP_CD,  
NULL AS RFRNC_DIV_CD,   
NULL AS ENTRST_YN,   
NULL AS ENTRST_END_SCHDL_DT,   
NULL AS DEL_YN,   
NULL AS CNF_DOC_NO,  
NULL AS CNF_DOC_NM, 
NULL AS CAM_RSN,
past.RMK_DC AS NOTE,
NULL AS MOD_PRSN,
NULL AS MOD_DTT,
NULL AS TZ_CD,   
NULL AS TZ_DTT, 
CASE 
    WHEN past.COMPANY_CD = '1000' THEN '1f'
    WHEN past.COMPANY_CD = '2000' THEN '1q'
    WHEN past.COMPANY_CD = '3000' THEN '1a'
    ELSE past.COMPANY_CD
END CMP_DIV_CD,
CASE 
    WHEN past.COMPANY_CD = '2000' THEN BIZAREA_NM
    ELSE BIZAREA_NM  
END CMP_DIV_NM,
NULL AS SLRY_STRT_SLRY_GRD_CD,  
NULL AS FIRST_APPT_SLRY_KIND_CD,   
NULL AS YRLY_SLRY_AMT,  
NULL AS JOB_SCND_YN, 
NULL AS JOB_SCND_CAM_CMP_CD, 
NULL AS JOB_SCND_JOB_GRP_CD,  
NULL AS RETRD_RSN_CD, 
NULL AS RETRD_RSN_NM,  
NULL AS SLRY_GRD, 
NULL AS SLRY_JOB_GRP,  
NULL AS NEXTERM_JOB_GD_PRMT_DT,  
NULL AS CAM_HIST_ID,
CASE    
    WHEN past.COMPANY_CD = '3000' THEN 'a' || past.EMP_NO -- 회사A
    WHEN past.COMPANY_CD = '2000' THEN 'q' || past.EMP_NO -- 회사B
    WHEN past.COMPANY_CD = '1000' THEN 'f' || past.EMP_NO -- 회사C
    ELSE past.EMP_NO    
END AS EMP_ID,
NULL AS PRSNL_ID,  
NULL AS CAM_CNF_ID,  
NULL AS CAM_STRT_DT,   
NULL AS CAM_END_DT,
TO_TIMESTAMP(past.GNFD_DT, 'yyyyMMdd') AS CAM_DT,
NULL AS CAM_ORDER,   
NULL AS CAM_CD,
NULL AS CAM_CLASS_CD,
NULL AS CAM_HR_ZONE_CD,   
NULL AS CAM_DEPT_ID,  
NULL AS CAM_JOB_RANK_CD,
past.POSI_NM AS CAM_JOB_RANK_NM,
NULL AS JOB_CD, 
NULL AS CAM_JOB_TTLE_CD, 
past.ODTY_NM AS CAM_JOB_TTLE_NM,
NULL AS CAM_EMP_KIND_CD, 
NULL AS CAM_YRLY_SLRY_TYPE,   
NULL AS PRBTN_YN, 
NULL AS PRBTN_END_DT,  
NULL AS JOB_SCND_DEPT_ID,  
NULL AS JOB_SCND_JOB_GD_CD, 
NULL AS JOB_SCND_JOB_RANK_CD, 
NULL AS JOB_SCND_JOB_CD, 
NULL AS JOB_SCND_JOB_TTLE_CD, 
NULL AS LAYOFF_YN,
COALESCE(dire.DTL_NM, dore.DTL_NM, stre.DTL_NM, past.HR_CD_NM) AS CAM_CLASS_NM,
CASE 
    WHEN mcls.MNG_DC = '1' THEN 'VolRetCl1'
    WHEN mcls.MNG_DC = '2' THEN 'VolRetCl2'
    ELSE 'EE9999' 
END RETRD_TYPE,
CASE 
    WHEN mcls.MNG_DC = '1' THEN '자발적'
    WHEN mcls.MNG_DC = '2' THEN '비자발적'
    ELSE '코드없음'
END RETRD_TYPE_NM
FROM slv.system_a.cam_past past
LEFT JOIN (  
    SELECT A.EMP_NO, A.MCLS_CD, A.COMPANY_CD, A.MNG_DC, RETR_DT  
    FROM slv.system_a.HR_EMP_SDTL A  
    INNER JOIN slv.system_a.HR_EMP_MST B 
      ON A.COMPANY_CD = B.COMPANY_CD AND A.EMP_NO = B.EMP_NO  
    INNER JOIN slv.system_a.HR_EMPINFO_DTL HED 
      ON HED.COMPANY_CD = b.COMPANY_CD AND HED.EMP_NO = b.EMP_NO  
    WHERE A.COMPANY_CD IN ('1000','2000','3000')  
      AND ((A.COMPANY_CD = '1000' AND MCLS_CD = 'HR002')  
        OR (A.COMPANY_CD = '2000' AND MCLS_CD = 'HR002')  
        OR (A.COMPANY_CD = '3000' AND MCLS_CD = 'HR002'))  
      AND RETR_DT IS NOT NULL
) mcls ON past.COMPANY_CD = mcls.COMPANY_CD 
  AND past.EMP_NO = mcls.EMP_NO 
  AND TO_CHAR(TO_TIMESTAMP(past.GNFD_DT, 'yyyyMMdd'), 'yyyyMMdd') = mcls.RETR_DT 
LEFT JOIN common_schema.apps.m_com_hr_itg_cd_mn dire 
  ON dire.df_cd = 'VolRetCl' AND dire.Value3 IS NOT NULL AND dire.Value3 = mcls.MNG_DC
LEFT JOIN common_schema.apps.m_com_hr_itg_cd_mn dore 
  ON dore.df_cd = 'VolRetCl' AND dore.Value5 IS NOT NULL AND dore.Value5 = mcls.MNG_DC
LEFT JOIN common_schema.apps.m_com_hr_itg_cd_mn stre 
  ON stre.df_cd = 'VolRetCl' AND stre.Value7 IS NOT NULL AND stre.Value7 = mcls.MNG_DC
""")

# gld 레이어에 저장
DF.write.mode("overwrite").save("abfss://gld@storage_account.dfs.core.windows.net/f_hr_appoint_history")
```

```
%sql
-- Delta 테이블 생성
USE gld.default;
CREATE TABLE IF NOT EXISTS f_hr_appoint_history
USING DELTA
LOCATION 'abfss://gld@storage_account.dfs.core.windows.net/f_hr_appoint_history';
```






