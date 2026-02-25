# HR 데이터 통합 플랫폼 (HR Data Integration Platform)
### Enterprise-Unified Analytics for Global Subsidiaries

> **10여 개 계열사의 이기종 HR 데이터를 통합하고 표준화하기 위한 엔드투엔드(End-to-End) 데이터 엔지니어링 솔루션입니다.**
>
> **[데이터 처리 및 현지화 안내]**
> * **현지화(Localization)**: 평가자의 이해를 돕기 위해 기존 시스템의 한글 필드명과 마스터 데이터를 모두 영어로 변환하여 구성했습니다.
> * **데이터 보안(Data Privacy)**: 기업의 민감 정보 보호를 위해 모든 실제 데이터는 **가데이터(Synthetic Data)** 로 대체되었으며, 이는 보안 정책을 준수합니다.


### **프로젝트 요약**
본 프로젝트는 10개가 넘는 계열사의 서로 다른 HR 데이터셋을 하나의 통합 플랫폼으로 구축한 사례입니다. <br>
**Azure Databricks**를 활용한 ETL 프로세스와 **ADF(Azure Data Factory)의 메타데이터 기반 동적 파이프라인**을 결합하여 데이터 수집·정제·적재 전 과정을 자동화하였으며,
**메달리온 아키텍처(Medallion Architecture)** 를 통해 고품질의 통합 데이터를 구축함으로써 **Power BI** 기반의 전사적 인사이트 시각화 체계를 구현하였습니다.

---
## 🏗️ 전체 시스템 아키텍처
<img width="2559" height="174" alt="image" src="https://github.com/user-attachments/assets/3302ae1d-ea9d-41f5-82c3-b81828ddb2df" />
<img width="2557" height="1436" alt="image" src="https://github.com/user-attachments/assets/e139007a-e937-48eb-9394-d13fa95a388d" />

---

## 🏗️ Azure Data Factory Pipeline
<img width="2556" height="745" alt="image" src="https://github.com/user-attachments/assets/e070fd3e-ed81-49df-8362-7b3167d40fd0" />

### Step 1. Logging
* **Process:** 파이프라인 시작 시 `SP_INS_RAW_PIP_INFO` 프로시저를 호출하여 고유 Execution ID를 생성하고 세션 컨텍스트를 기록합니다.
* **Engineering Rationale:** 대규모 분산 환경에서 프로세스의 **상태 추적** (State Tracking) 과 **감사 추적** (Audit Trail) 을 자동화하여 시스템 전체의 **관측성** (Observability) 을 확보했습니다.

### Step 2. Dynamic
* **Process:** `Get_Order_List` (Lookup) 단계를 통해 런타임 시점에 처리해야 할 소스 리스트와 메타데이터를 동적으로 쿼리합니다.
* **Engineering Rationale:** 비즈니스 로직과 데이터 소스를 분리하는 **디커플링** (Decoupling) 설계를 적용했습니다. 이를 통해 파이프라인 코드 수정 없이 DB 설정만으로 시스템 확장이 가능한 구조를 구현했습니다.

### Step 3. Scale
* **Process:** **ForEach 루프**를 활용하여 Apache Spark 기반의 Databricks 노트북을 병렬로 트리거하며, 각 태스크는 **지능형 재시도** (Retry) 로직을 포함합니다.
* **Engineering Rationale:** **병렬성** (Parallelism) 을 극대화하여 데이터 처리 시간을 단축하고, 개별 작업 실패가 전체 시스템 중단으로 이어지지 않도록 하는 **결함 허용** (Fault Tolerance) 능력을 강화했습니다.

### Step 4. Integrity
* **Process:** 모든 병렬 작업의 결과를 종합하여 `SP_INS_RAW_PIP_INFO`를 통해 최종 성공/실패 여부를 판별하고 시스템 상태를 업데이트합니다.
* **Engineering Rationale:** 트랜잭션의 **원자성** (Atomicity) 을 보장하기 위한 설계입니다. 모든 하위 작업이 검증된 경우에만 최종 상태를 동기화하여 하위 분석 계층에 데이터 무결성을 제공합니다.

---
## 🏗️ Databricks Medallion Architecture
데이터 신뢰성 확보를 위해 **Bronze(Raw) → Silver(Validated) → Gold(Enriched)** 단계로 데이터를 정제하는 레이어링 설계를 적용했습니다.

<img width="1594" height="691" alt="image" src="https://github.com/user-attachments/assets/e7d8f702-4eec-4337-9232-270f544aa38a" />

<br>
<br>

## 🛠 ETL Logic: `f_hr_scholar`

**`f_hr_scholar`** 테이블은 인사 분석의 핵심이 되는 자격증, 학력, 상벌, 어학, 경력, 발령 데이터를 담은 Fact Table입니다. <br>
이 테이블은 복잡한 소스 데이터를 단일 노트북(`NB_Fct_To_Gld`)을 통해 정제하고 통합하는 과정을 거쳐 생성됩니다.

[Click here to view the documentation](NB_Fct_To_Gld.md)

### 1. Unified Integration (단일 노트북 기반 통합 구조)
파편화된 ETL 스크립트 대신 `NB_Fct_To_Gld`라는 중앙 집중식 노트북을 설계하여 다음과 같은 성과를 거두었습니다.
* **데이터 일관성 확보:** 여러 레이어(Bronze, Silver)에 흩어진 인사 정보를 한 번에 동기화하여 데이터 불일치 문제를 해결했습니다.
* **유지보수 효율성:** 단일 지점에서 변환 로직을 관리함으로써, 향후 스키마 변경이나 로직 수정 시 발생할 수 있는 리스크를 최소화했습니다.

### 2. Core Transformation Steps (주요 변환 로직)
원천 데이터(Raw Data)를 분석용 데이터(Gold)로 전환하기 위해 다음의 4단계 로직을 적용했습니다.

| 단계 | 프로세스 | 상세 설명 |
| :--- | :--- | :--- |
| **Step 1** | **Source Aggregation** | `brz.white.phm_scholar`(원천 학력)와 `slv.dzn.hr_shocare_mst`(정제된 마스터) 데이터를 수집합니다. |
| **Step 2** | **Multi-Way Join** | `gld.default.d_hr_school`(학교 마스터) 및 공통 코드 테이블과 결합하여 코드 형태의 데이터를 사람이 읽을 수 있는 명칭으로 치환합니다. |
| **Step 3** | **Data Standardization** | 학위 수준(예: 학사, Bachelor, B.S.)이나 전공 명칭 등 일관성 없는 텍스트 데이터를 표준화된 포맷으로 정규화합니다. |
| **Step 4** | **Integrity Check** | 유효하지 않은 사번을 가진 레코드를 필터링하고, 졸업일자 등 필수 필드의 결측치를 처리하여 데이터 무결성을 보장합니다. |

### 3. Engineering Excellence (기술적 차별점)
* **Performance Optimization:** Bronze 레이어의 legacy 데이터 타입을 Spark/SQL 환경에 최적화된 타입으로 변환하여 쿼리 성능을 높였습니다.
* **Data Security & Privacy:** 기업 보안 정책 및 개인정보 보호를 위해 이메일, 사번 등 민감 정보에 마스킹(Masking) 및 익명화 로직을 적용했습니다.

> **Note:** 본 포트폴리오는 보안 준수를 위해 실제 기업의 스키마 명칭과 개인 식별 정보는 모두 가상화(Anonymized) 처리되었습니다.

---

## 🌟 주요 기술적 성과

### 1. 이기종 데이터 통합 및 표준화 (Heterogeneous Data Integration)
* **문제점**: 10개 이상의 계열사가 서로 다른 ERP(더존, 자체 시스템 등)를 사용하여 사원/부서 코드가 불일치함. 이로 인해 그룹 전체 인력 현황을 하나의 기준으로 집계하거나 분석하는 것이 불가능했음.
* **해결책**: 
    * **MDM 기반 코드 표준화**: BI-Matrix 인터페이스를 개발하여 각 계열사의 로컬 코드를 그룹 표준 코드에 1:1 매핑하는 마스터 데이터 관리(MDM) 로직을 구현함으로써 데이터 일관성 확보.
    * **데이터 저장 구조 최적화**: Azure Databricks 환경에서 메달리온 아키텍처(Bronze, Silver, Gold 레이어)를 도입하여, 정제되지 않은 Raw 데이터부터 분석용 표준 데이터까지 단계별 적재 및 가공 프로세스 구축.

### 2. 비용 효율적인 마스터 데이터 관리(MDM) 시스템 구축
* **문제점**: 전문 MDM 솔루션 도입을 위한 고가의 라이선스 예산 확보 및 복잡한 구축 과정의 어려움.
* **해결책**: 이미 사내에 도입되어 있는 **BI-Matrix**를 활용하여 현업 담당자가 직접 데이터를 수정하고 매핑할 수 있는 CRUD 인터페이스를 자체 설계. 이를 통해 고가의 외부 솔루션 도입 비용을 100% 절감하고 운영 편의성 증대.

### 3. 고도화된 HR 분석 대시보드 제공
* **성과**: **Power BI**를 통해 그룹 전체의 이직률, 노동 생산성, 인력 현황 등을 실시간으로 시각화하여 데이터 기반의 경영 의사결정(Data-driven Decision Making) 지원.

---

## 📂 프로젝트 구조 (Project Structure)
```text
├── src/
│   ├── etl/          # 데이터 변환 및 가공을 위한 PySpark 스크립트
│   └── mdm/          # 마스터 데이터(MDM) 매핑 및 검증 로직
├── sql/
│   └── schemas/      # 표준화된 HR 테이블 설계를 위한 DDL 및 Query
└── docs/             # 고해상도 아키텍처 설계도 및 프로젝트 상세 문서
