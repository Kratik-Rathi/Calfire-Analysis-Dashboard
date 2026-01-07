
# 🚨 California Wildfires Real-Time Analytics Dashboard (2017–2026)

This project delivers a **fully automated real-time wildfire analytics solution** for California, covering the last decade of activity (2017–2025). The goal was to move beyond static analysis and instead build a continuously updating BI system capable of reflecting evolving wildfire behavior, containment progress, and geographic spread — while maintaining historical reliability.

The dashboard enables:

- Year-over-year wildfire trend analysis  
- County-level incident distribution insights  
- Severity & duration analytics  
- Seasonal wildfire behavior interpretation  

---

## 📊 Dashboard Overview

The Tableau dashboard is structured into two major analytical surfaces:

---

### **1️⃣ Analysis Page**

**KPI Cards**
- YoY comparison of acres burned  
- YoY comparison of incident frequency  

**Incident Duration Donut Chart**
- Distribution of incidents grouped into 4 duration buckets  

**Seasonality Trend Bar Graph**
- Seasonal wildfire distribution  
- Month-level behavior enabled via tooltips  

**County-Level Filled Map**
- Burned acres visualization across California  
- Helps identify consistently vulnerable regions  

---

### **2️⃣ Metadata Page**
- Provides visibility into the source dataset  
- Exposes key attributes including:
  - Containment percentage  
  - Incident progression  
- Enables transparency into data lineage and trustworthiness  

---

## ⚙️ End-to-End Live Data Architecture

Wildfire data is inherently dynamic. Containment evolves, total acres burned changes, and incidents transition through states. To address this reality, I designed a **production-style automated ETL pipeline backed by AWS + GCP**, ensuring the dashboard stays continuously updated without manual intervention.

---

## 🧭 Data Flow Summary

| Component | Responsibility |
|----------|----------------|
| **CAL FIRE API** | Primary authoritative wildfire incident feed |
| **Amazon EventBridge** | Scheduled ingestion triggers (twice daily) |
| **AWS Lambda** | ETL engine: cleansing, KPI derivation, updating historical events |
| **S3 + Lambda Layering** | Dependency management for optimized execution |
| **CloudWatch** | Lambda logging for monitoring and reliability |
| **Google Sheets** | Lightweight analytics datastore powering Tableau |
| **GCP IAM** | Secure cross-cloud data access governance |
| **Tableau Public** | Visualization & stakeholder consumption |

---

## 📐 Architecture Diagram

```mermaid
flowchart LR

A[CAL FIRE Public API] --> B[Amazon EventBridge - Scheduled Trigger]

B --> C[AWS Lambda (ETL Processor)]
C -->|Schema Standardization| C
C -->|Data Quality & Aggregation| C
C -->|Historical Record Updates| C

C --> D[(Amazon S3 - Package Storage)]
D --> C

C --> E[Google Sheets - Analytics Dataset]

E --> F[Tableau Dashboard]
F -->|Analysis View|
F -->|Metadata View|
```

---

## 🗂️ Repository Structure

```
california-wildfire-analytics/
│
├── README.md
│
├── tableau_dashboard/
│   ├── Calfire_Analysis_Dashboard.twbx
│
├── lambda_function/
│   ├── calfire_script.py
│   ├── requirements.txt
```
---

## 🧩 Key Technical Highlights

- Event-driven ETL ensuring timely refresh cycles  
- Idempotent update logic to maintain historical consistency  
- Strong data validation including:
  - Null handling  
  - Standardized schema enforcement  
  - KPI derivation during processing  
- Optimized dependency strategy:
  - Python packages stored in S3  
  - Consumed via Lambda layer with IAM-secured access  
- Secure multi-cloud integration:
  - AWS for compute + scheduling  
  - GCP Sheets for lightweight analytics store  
- Designed for extensibility:
  - Can integrate with RDS, BigQuery, Redshift, or Data Lakes in future  

---

## 📢 Why This Project Matters

This solution demonstrates:

- Ability to build **production-ready BI pipelines**
- Experience handling **streaming / near-real-time data**
- Strong grounding in **AWS serverless engineering**
- Capability to convert raw operational feeds into **business-ready analytics**

It reflects how real enterprise analytics systems should operate — **automated, resilient, secure, and transparent.**
