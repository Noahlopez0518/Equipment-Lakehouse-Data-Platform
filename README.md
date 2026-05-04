# Tenna Lakehouse Data Platform - Microsoft Fabric

An automated data engineering solution built on Microsoft Fabric that centralizes equipment tracking data from 14 Tenna API endpoints into a lakehouse architecture with Delta tables, enabling real-time analytics and automated reporting.

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
Semantic Model (relationships + DAX measures)
↓
Power BI Dashboards


---

## 📊 Data Sources

### Static Tables (Full Refresh)

- asset_financials — Financial data, purchases, rentals, depreciation  
- assets — Core asset/equipment master data  
- asset_assignee_history — Equipment assignment tracking  
- asset_label_associations — Asset labeling and categorization  
- asset_dt_codes — Diagnostic trouble codes  
- asset_labels — Label definitions  
- asset_organization_history — Organizational changes  
- asset_registrations — Registration and compliance data  
- asset_site_history — Site location history  
- asset_warranties — Warranty information  

### Daily Tables (Incremental Load)

- asset_readings_daily — Daily equipment readings (hours, odometer)  
- asset_utilizations_daily — Daily utilization metrics  
- asset_site_daily_utilizations — Site-level daily utilization  
- asset_net_working_hours_daily — Net working hours calculations  

---

## 🚀 Key Features

### 1. Reusable Ingestion Function

- Handles API pagination automatically  
- Flattens nested JSON structures  
- Type enforcement (timestamps, longs, booleans, doubles)  
- Converts complex types to JSON strings  
- Schema validation and cleanup  

---

### 2. Incremental Loading with Deduplication

- Tracks last successful run time  
- Uses `date_from` parameter for incremental loads  
- MERGE operations prevent duplicates  
- Composite key handling (e.g., asset_id + date)  

---

### 3. Rate Limit Handling

- Exponential backoff retry logic  
- Configurable retry attempts (default: 5)  
- Automatic delay between requests  
- Graceful degradation on persistent failures  

---

### 4. Data Quality

- SQL validation queries for row counts  
- Null value identification  
- Schema correctness verification  
- Duplicate detection post-load  

---

### 5. Control Table Pattern

- `control_last_run` — Tracks pipeline execution metadata  
- `pipeline_last_run` — Records incremental load checkpoints  

---

## 📁 Repository Structure
```text
.
├── notebooks/
│   ├── ingestion_engine.ipynb      # Centralized PySpark ingestion logic
│   └── validation_suite.ipynb      # Data quality and integrity checks
├── config/
│   └── config_template.json        # Environment and API configuration
├── sql/
│   └── validation_queries.sql      # SQL-based quality & audit scripts
├── docs/
│   ├── architecture.md             # System design documentation
│   └── setup_guide.md              # Environment setup instructions
├── .gitignore                      # Protected credentials filter
├── requirements.txt                # Python dependency manifest
└── README.md                       # Project documentation


---

## 🛠️ Technology Stack

- **Platform:** Microsoft Fabric  
- **Data Storage:** OneLake (Delta Lake format)  
- **Processing:** PySpark (Python)  
- **ETL:** Dataflow Gen2 (for large endpoints)  
- **Orchestration:** Fabric Data Pipelines  
- **Visualization:** Power BI  
- **API:** Tenna REST API v1  

---

## ⚙️ Setup Instructions

### Prerequisites

- Microsoft Fabric workspace access  
- Tenna API token with read permissions  
- Python 3.8+ (for local development)  

---

### Configuration

Create Lakehouse:

- Navigate to Fabric workspace  
- Create new Lakehouse named **"Tenna_Raw"**

Set API Credentials:

```python
API_TOKEN = "your_tenna_api_token_here"
BASE_URL = "https://api.tenna.com/v1"

Upload Notebooks:

Import ten_tables_ingestion.ipynb (static tables)
Import daily_ingestion_function.ipynb (daily tables)

Configure Pipeline:

Create Fabric Pipeline
Add notebook activities in sequence
Schedule daily refresh (recommended: 2 AM)
First Run
# Run ten_tables_ingestion.ipynb first (one-time setup)
# This creates all static tables and control tables

# Then run daily_ingestion_function.ipynb
# This handles incremental daily data
🔄 Automated Refresh Pipeline
Run Python Notebook → Refresh static tables
Refresh Dataflows → Process large endpoints
Refresh Semantic Model → Update relationships/measures
Sync to Power BI → Publish dashboards

Result: 100% automated, zero-touch reporting

📊 Power BI Dashboard

⚠️ IMPORTANT: Dashboard uses fictional data for demonstration only.

Equipment Fleet Analytics Dashboard

Key Features:

Real-time KPI monitoring (fleet utilization, total hours, asset count)
Trend analysis vs target benchmarks
Asset performance tracking
Fleet composition breakdown
Interactive filtering and drill-down

Technical Implementation:

Star schema data model
Custom DAX measures
Cards, line charts, bar charts, tables, pie charts
Professional UI styling

Dataset Includes:

100 equipment assets
6 months of daily utilization data
Multiple sites and assignments
Full financial + operational metrics
🧪 Data Quality Checks
-- Row count validation
SELECT COUNT(*) as total_rows FROM Tenna_Raw.assets;

-- Null check
SELECT COUNT(*) as null_count 
FROM Tenna_Raw.assets 
WHERE asset_id IS NULL;

-- Duplicate detection
SELECT asset_id, COUNT(*) as dup_count
FROM Tenna_Raw.assets
GROUP BY asset_id
HAVING COUNT(*) > 1;
🔍 Code Highlights
Type Enforcement
datetime_cols=["created_at","updated_at","purchase_date"]
long_int_cols=["year","cumulative_hours_end_of_day"]
boolean_cols=["billable","ecu_hours_provided"]
double_cols=["utilization_percentage","expected_run_hours"]
Incremental Loading
if last_run_time:
    params["date_from"] = max_date.strftime('%Y-%m-%d')
Deduplication with MERGE
delta_table.alias("target").merge(
    df.alias("source"),
    "target.asset_id = source.asset_id"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()
📊 Performance
Initial Load: ~2–3 hours
Daily Incremental: ~10–15 minutes
Millions of records processed
Fully automated daily pipeline
🤝 Contributing

This is a demonstration project showcasing Microsoft Fabric capabilities.

📝 License

MIT License

🙋 Questions?

Open an issue or connect via LinkedIn.

Note: Credentials and sensitive endpoints removed for security.


If you want next step, I can also:
- :contentReference[oaicite:0]{index=0}
- or :contentReference[oaicite:1]{index=1}
- or :contentReference[oaicite:2]{index=2}
