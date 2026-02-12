# HR 데이터 통합 플랫폼 (HR Data Integration Platform)
### Enterprise-Unified Analytics for Global Subsidiaries

> **10여 개 계열사의 이기종 HR 데이터를 통합하고 표준화하기 위한 엔드투엔드(End-to-End) 데이터 엔지니어링 솔루션입니다.**
>
> **[데이터 처리 및 현지화 안내]**
> * **현지화(Localization)**: 평가자의 이해를 돕기 위해 기존 시스템의 한글 필드명과 마스터 데이터를 모두 영어로 변환하여 구성했습니다.
> * **데이터 보안(Data Privacy)**: 기업의 민감 정보 보호를 위해 모든 실제 데이터는 **가데이터(Synthetic Data)**로 대체되었으며, 이는 보안 정책을 준수합니다.

<img width="2559" height="175" alt="image" src="https://github.com/user-attachments/assets/58065520-524c-4b8f-8067-f86344b294d7" />
<img width="2559" height="1439" alt="image" src="https://github.com/user-attachments/assets/ab4fdbfe-1e01-424a-8fa6-7dc76cc60ea0" />

---

## 🏗️ 시스템 아키텍처
### **프로젝트 요약**
본 프로젝트는 10개가 넘는 계열사의 서로 다른 HR 데이터셋을 하나의 통합 플랫폼으로 구축한 사례입니다. **Azure Databricks**를 활용한 ETL 프로세스와 **BI-Matrix**를 이용한 맞춤형 MDM 인터페이스를 통해, 추가적인 라이선스 비용 없이 각기 다른 ERP 시스템 간의 코드 불일치 문제를 해결했습니다.

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
