# 🚀 Tenna Lakehouse Data Platform - Microsoft Fabric

An automated data engineering solution built on Microsoft Fabric that centralizes equipment tracking data from **20 Tenna API endpoints** into a Lakehouse architecture with Delta tables, enabling real-time analytics and automated reporting.

---

## 🎯 Project Overview

This project delivers an end-to-end data platform that:

- Ingests data from API endpoints  
- Stores all data as Delta tables in Microsoft Fabric OneLake  
- Implements incremental loading with deduplication logic  
- Provides automated refresh pipelines for zero-touch reporting  
- Powers interactive Power BI dashboards for equipment analytics  

---

## 🏗️ Architecture
API (20 endpoints)
↓
PySpark Ingestion Layer (Python + Dataflow Gen2)
↓
Delta Tables in OneLake
↓
Semantic Model (Relationships + DAX Measures)
↓
Power BI Dashboards


---

## 📊 Data Sources

1. `asset_master` – Core asset/equipment master data  
2. `asset_financial_ledger` – Financial data, purchase prices, and depreciation  
3. `asset_assignment_history` – Equipment assignment and personnel tracking  
4. `asset_tag_associations` – Asset labeling and category mapping  
5. `asset_diagnostic_codes` – Diagnostic trouble and fault code definitions  
6. `asset_label_definitions` – Master definitions for tags and categories  
7. `asset_org_history` – Internal department and business unit alignment  
8. `asset_compliance_data` – Registration, permits, and compliance records  
9. `asset_site_history` – Historical site and project location logs  
10. `asset_warranty_tracking` – Warranty information and expiration details  
11. `telematics_hardware_inventory` – GPS trackers and IoT sensor hardware inventory  
12. `parts_master_catalog` – Maintenance and repair parts  
13. `parts_inventory_levels` – Warehouse and site stock tracking  
14. `parts_asset_allocations` – Installed parts tracking  
15. `parts_transaction_audit` – Audit trail of part movements  
16. `daily_meter_readings` – Equipment readings (hours, odometer)  
17. `daily_utilization_metrics` – Runtime and performance metrics  
18. `daily_site_utilization` – Site-level activity tracking  
19. `daily_efficiency_analytics` – Idle time and efficiency metrics  
20. `Trip` – Equipment movement logs  

---

## 🚀 Key Features

### 1. Multi-Pattern Ingestion Engine

The ingestion framework supports multiple data loading strategies:

> **Full Refresh:** Used for smaller datasets (e.g., `asset_master`) for full consistency  
> **Incremental Load:** Used for high-volume telemetry data to optimize performance  

---

### 2. Reusable Ingestion Framework

- Handles API pagination automatically  
- Flattens nested JSON into tabular format  
- Enforces strict data types (timestamps, longs, booleans, doubles)  
- Converts complex objects into JSON strings  
- Performs schema validation and cleanup  

---

### 3. Incremental Loading & Deduplication

- Tracks last successful run using control tables  
- Uses `date_from` for incremental API pulls  
- Uses `MERGE` for Delta upserts  
- Supports composite keys (`asset_id + date`)  

---

### 4. Intelligent Watermarking

- Tracks `last_updated_at` per endpoint  
- Uses a **10-minute buffer** to prevent missing late data  
- Ensures no data gaps  

---

### 5. Resilient API Integration

- Handles `429 / 430` rate limits with exponential backoff  
- Configurable retries  
- Pagination across large datasets  
- Graceful failure handling  

---

### 6. Schema Enforcement & Data Cleaning

- Enforces `Timestamp`, `Double`, `Long`, `Boolean` types  
- Converts nested structures to JSON  
- Removes schema inconsistencies  
- Ensures Power BI compatibility  

---

### 7. Delta MERGE Upserts

- Prevents duplicate records  
- Supports composite keys  
- Ensures atomic transactions  

---

## 📁 Repository Structure
├── notebooks/
│ ├── ten_tables_ingestion.ipynb
│ └── daily_ingestion_function.ipynb
├── config/
│ └── config_template.json
├── sql/
│ └── validation_queries.sql
├── docs/
│ ├── architecture.md
│ └── setup_guide.md
├── images/
│ └── dashboard.png
├── powerbi/
│ └── dashboard.pbix
├── requirements.txt
└── README.md


---

## 🛠️ Technology Stack

- **Platform:** Microsoft Fabric  
- **Storage:** OneLake (Delta Lake)  
- **Processing:** PySpark (Python)  
- **ETL:** Dataflow Gen2  
- **Orchestration:** Fabric Pipelines  
- **Visualization:** Power BI  
- **API:** Tenna REST API  

---

## ⚙️ Setup Instructions

### Prerequisites
- Microsoft Fabric workspace  
- Tenna API token  
- Python 3.8+  

---

### Configuration

#### 1. Create Lakehouse
- Create Lakehouse in Fabric workspace  
- Name: `Tenna_Raw`  

#### 2. Set API Credentials

```python
API_TOKEN = "your_token_here"
BASE_URL = "https://api.tenna.com/v1"

# Run static tables first
ten_tables_ingestion.ipynb

# Then run incremental
daily_ingestion_function.ipynb

-- Row count
SELECT COUNT(*) FROM Tenna_Raw.assets;

-- Null check
SELECT COUNT(*) FROM Tenna_Raw.assets WHERE asset_id IS NULL;

-- Duplicate detection
SELECT asset_id, COUNT(*)
FROM Tenna_Raw.assets
GROUP BY asset_id
HAVING COUNT(*) > 1;


---

## 👍 You’re good to go

Just:
1. Copy everything  
2. Paste into `README.md`  
3. Commit  

---

If you want one last boost, I can add:
- :contentReference[oaicite:0]{index=0}
- :contentReference[oaicite:1]{index=1}
- :contentReference[oaicite:2]{index=2}

That would push this into **top-tier portfolio level**.
