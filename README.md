# 🌐 HR Data Integration Platform
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
## 🏗️ Overall System Architecture
<img width="2559" height="174" alt="image" src="https://github.com/user-attachments/assets/3302ae1d-ea9d-41f5-82c3-b81828ddb2df" />
<img width="2557" height="1436" alt="image" src="https://github.com/user-attachments/assets/e139007a-e937-48eb-9394-d13fa95a388d" />

---
## 🏗️ Azure Data Factory Pipeline
<img width="2556" height="745" alt="image" src="https://github.com/user-attachments/assets/e070fd3e-ed81-49df-8362-7b3167d40fd0" />

### I engineered this **Metadata-Driven Dynamic Pipeline** to ensure high scalability and automated observability.

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
<img width="1594" height="691" alt="image" src="https://github.com/user-attachments/assets/e7d8f702-4eec-4337-9232-270f544aa38a" />

### To ensure data reliability, we implemented a layered architecture that refines data through **Bronze (Raw) → Silver (Validated) → Gold (Enriched)** stages.

---
## 🏗️ How the Gold Table is Built
<img width="1907" height="1375" alt="image" src="https://github.com/user-attachments/assets/99fd3a36-cf82-4487-8e65-55638f3d321a" />

### I engineered the **`Fct_To_Gld`** module, a critical ETL logic designed to synthesize validated data into high-performance Gold-layer assets. Beyond scholar records, it is architected to process a broad range of HR datasets as defined in the **`Scope`** below.

### 개요 (Overview)
* **Core Objective:** 시스템B(White) 및 시스템A(더존 3사)로 이원화된 소스 데이터를 통합하여 분석 최적화형 7종 **HR Fact Tables** 구축
* **Scope:** 자격증 / 학력 / 상벌 / 경력 / 병역 / 어학 / 발령 데이터 (f_hr_*)


### 🔗 **[View Detailed Transformation Logic (01.NB_Fct_To_Gld.ipynb)](ETL/pipeline/01.NB_Fct_To_Gld.ipynb)**
### 🔗 **[View Detailed Table Definition (02.Table_Definition.md)](ETL/pipeline/02.Table_Definition.md)**

---

## 🏗️ How I Integrated Code Across Different Systems
<img width="1273" height="869" alt="image" src="https://github.com/user-attachments/assets/7833de2e-086c-4181-bc0c-879901f7e304" />


### ✅ Heterogeneous Data Integration
* **문제점 (Problem)**
    * **데이터 파편화**: 서로 다른 ERP를 사용하는 10여 개 계열사의 마스터 코드와 데이터 구조가 불일치하여, 통일된 표준으로 그룹 전체의 인력 현황을 집계하거나 분석하는 것이 불가능했음.
    * **비용 장벽**: 마스터 데이터 관리(MDM) 시스템 외주 구축은 높은 비용으로 인해 재무적으로 불가능했으므로, 예산 내에서 기술적 목표를 달성하기 위한 자체 구축 솔루션이 필요했음.
* **해결책 (Solution)**: 사내에 이미 도입되어 있는 **BI-Matrix 플랫폼을 활용**하여 추가 비용 없이 자체 MDM 로직을 개발했으며, 각 계열사의 로컬 코드를 그룹 표준 코드에 1:1로 매핑함으로써 데이터 일관성을 확보함.

### Technical Note
> **BI-Matrix**: A specialized Low-code Business Intelligence (BI) platform used to rapidly design data interfaces and implement complex business logic.

---
## 🌟 Employee Turnover Analysis Dashboard
> ### This is one of the analytical dashboards I developed during the HR project
<img width="2545" height="1191" alt="image" src="https://github.com/user-attachments/assets/2523b92f-d81b-46ec-84ec-e700d0c14d51" />
<img width="2262" height="1431" alt="image" src="https://github.com/user-attachments/assets/78cdb405-8282-4a5d-a09d-54d86a2acc8e" />

---

## 📂 프로젝트 구조 (Project Structure)
```text
├── src/
│   ├── etl/          # 데이터 변환 및 가공을 위한 PySpark 스크립트
│   └── mdm/          # 마스터 데이터(MDM) 매핑 및 검증 로직
├── sql/
│   └── schemas/      # 표준화된 HR 테이블 설계를 위한 DDL 및 Query
└── docs/             # 고해상도 아키텍처 설계도 및 프로젝트 상세 문서
