# HR 데이터 통합 플랫폼 (HR Data Integration Platform)
### Enterprise-Unified Analytics for Global Subsidiaries

> **10여 개 계열사의 이기종 HR 데이터를 통합하고 표준화하기 위한 엔드투엔드(End-to-End) 데이터 엔지니어링 솔루션입니다.**
>
> **[데이터 처리 및 현지화 안내]**
> * **현지화(Localization)**: 평가자의 이해를 돕기 위해 기존 시스템의 한글 필드명과 마스터 데이터를 모두 영어로 변환하여 구성했습니다.
> * **데이터 보안(Data Privacy)**: 기업의 민감 정보 보호를 위해 모든 실제 데이터는 **가데이터(Synthetic Data)**로 대체되었으며, 이는 보안 정책을 준수합니다.


### **프로젝트 요약**
본 프로젝트는 10개가 넘는 계열사의 서로 다른 HR 데이터셋을 하나의 통합 플랫폼으로 구축한 사례입니다. **Azure Databricks**를 활용한 ETL 프로세스와 **ADF(Azure Data Factory)의 메타데이터 기반 동적 파이프라인**을 결합하여 데이터 수집·정제·적재 전 과정을 자동화하였으며, **메달리온 아키텍처(Medallion Architecture)**를 통해 고품질의 통합 데이터를 구축함으로써 **Power BI** 기반의 전사적 인사이트 시각화 체계를 구현하였습니다.

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

| Layer | Status | Key Engineering Process |
| :--- | :--- | :--- |
| **Bronze** | **Raw** | 소스 데이터 원형 보존 및 이력 관리 |
| **Silver** | **Validated** | **Cleaning** 및 **Schema Enforcement**를 통한 품질 표준화 |
| **Gold** | **Enriched** | 비즈니스 로직 기반 **Aggregation** (Power BI 최적화) |

### 🛠️ 핵심 성과 (Key Value)

* **데이터 무결성:** **Silver** 단계 스키마 강제로 하위 시스템 결함 유입 차단
* **성능 최적화:** **Gold** 단계 사전 연산으로 최종 대시보드 조회 속도 극대화
* **장애 대응:** **Bronze** 데이터 보존으로 로직 변경 시 언제든 데이터 재구성 가능
* **가시성 확보:** 계층 분리를 통한 데이터 흐름(Lineage) 추적 용이성 확보


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
