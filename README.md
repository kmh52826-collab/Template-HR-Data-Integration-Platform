# HR Data Integration Platform
### Enterprise-Unified Analytics for Global Subsidiaries

> **An End-to-End Data Engineering Solution for Heterogeneous HR Data Integration and Master Data Management (MDM).**


<img width="1849" height="1036" alt="image" src="https://github.com/user-attachments/assets/a9ecf389-5344-4fc5-8e37-79f5856b0b5e" />

---
## 🏗️ System Architecture
### **Executive Summary**
This project establishes a robust, scalable data platform to integrate fragmented HR datasets from over 10 subsidiaries. By leveraging **Azure Databricks** for ETL and **BI-Matrix** for a custom MDM interface, the system solves the challenge of inconsistent code systems across different ERPs without additional licensing costs.

---

## 🌟 Key Technical Achievements

### 1. Heterogeneous Data Integration & Standardization
* **Problem**: Inconsistent employee/department codes across 10+ subsidiaries prevented group-wide analytics.
* **Solution**: Developed a unified data schema using the **Medallion Architecture** (Bronze, Silver, Gold layers) on **Delta Lake**.

### 2. Cost-Effective Master Data Management (MDM)
* **Problem**: Budget constraints for purchasing professional MDM solutions.
* **Solution**: Engineered a custom CRUD interface using **BI-Matrix** to allow HR managers to map local codes to global standards directly, saving 100% of potential software procurement costs.

### 3. Advanced HR Analytics Dashboard
* **Impact**: Delivered real-time insights into employee turnover rates, labor productivity, and group-wide workforce distribution via **Power BI**.

---

## 📂 Project Structure
```text
├── src/
│   ├── etl/          # PySpark scripts for data transformation
│   └── mdm/          # Logic for Master Data mapping and validation
├── sql/
│   └── schemas/      # DDL for standardized HR tables
└── docs/             # High-resolution architecture diagrams & system design
