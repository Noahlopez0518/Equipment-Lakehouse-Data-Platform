# Tenna Lakehouse Data Platform - Microsoft Fabric

An automated data engineering solution built on Microsoft Fabric that centralizes equipment tracking data from 14 Tenna API endpoints into a lakehouse architecture with Delta tables, enabling real-time analytics and automated reporting.

## 🎯 Project Overview

This project delivers an end-to-end data platform that:
- Ingests data from API endpoints 
- Stores all data as Delta tables in Microsoft Fabric OneLake
- Implements incremental loading with deduplication logic
- Provides automated refresh pipelines for zero-touch reporting
- Powers interactive Power BI dashboards for equipment analytics

## 🏗️ Architecture

```
API (20 endpoints)
    ↓
PySpark Ingestion Layer (Python + Dataflow Gen2)
    ↓
Delta Tables in OneLake
    ↓
Semantic Model (relationships + DAX measures)
    ↓
Power BI Dashboards
```

## 📊 Data Sources
1. `asset_master` - Core asset/equipment master data
2. `asset_financial_ledger` - Financial data, purchase prices, and depreciation
3. `asset_assignment_history` - Equipment assignment and personnel tracking
4. `asset_tag_associations` - Asset labeling and category mapping
5. `asset_diagnostic_codes` - Diagnostic trouble and fault code definitions
6. `asset_label_definitions` - Master definitions for tags and categories
7. `asset_org_history` - Internal department and business unit alignment
8. `asset_compliance_data` - Registration, permits, and compliance records
9. `asset_site_history` - Historical site and project location logs
10. `asset_warranty_tracking` - Warranty information and expiration details
11. `telematics_hardware_inventory` - GPS trackers and IoT sensor hardware inventory
12. `parts_master_catalog` - Master list of maintenance and repair parts
13. `parts_inventory_levels` - Real-time warehouse and site stock tracking
14. `parts_asset_allocations` - Ledger of parts currently installed on equipment
15. `parts_transaction_audit` - Audit trail of part movements and cost transfers
16. `daily_meter_readings` - Daily equipment readings (hours, odometer)
17. `daily_utilization_metrics` - Calculated daily performance and runtime metrics
18. `daily_site_utilization` - Site-level daily deployment and activity
19. `daily_efficiency_analytics` - Net working hours and idle time calculations
20. `Trip`


## 🚀 Key Features

### 1. Multi-Pattern Ingestion Engine
The ingestion framework supports multiple data loading strategies depending on dataset size and volatility:

> **Full Refresh:** Used for smaller master datasets (e.g., `asset_master`) to ensure complete data consistency and accuracy.  
> **Incremental Load:** Applied to high-volume telemetry tables to reduce API calls and optimize performance.

Automatically selects the ingestion strategy based on pipeline configuration.

---

### 2. Reusable Ingestion Framework
A centralized ingestion function standardizes all API-to-Lakehouse ingestion workflows:

- Handles API pagination automatically across all endpoints  
- Flattens nested JSON structures into tabular format  
- Enforces strict data types (timestamps, longs, booleans, doubles)  
- Converts complex/nested objects into JSON strings for compatibility  
- Includes schema validation and automatic cleanup logic  

---

### 3. Incremental Loading & Deduplication Engine
Ensures efficient processing of large datasets while preventing duplicate records:

- Tracks last successful pipeline execution using control tables  
- Uses `date_from` parameters for incremental API pulls  
- Implements `MERGE` logic for upserts into Delta tables  
- Supports composite keys (e.g., `asset_id + date`) for precise deduplication  

---

### 4. Intelligent Watermarking & Boundary Buffer
Prevents data loss and ensures continuity across ingestion cycles:

- Maintains a Delta-based watermark table tracking `last_updated_at` per endpoint  
- Implements a **10-minute lookback buffer** to capture delayed or late-arriving API records  
- Ensures no gaps between pipeline executions  

---

### 5. Resilient API Integration Layer
Built to handle unstable or rate-limited REST APIs at scale:

- Exponential backoff logic for `429 / 430` rate limit responses  
- Configurable retry mechanism with automatic delays  
- Robust pagination handling for multi-page dataset extraction  
- Graceful failure handling with controlled retries and logging  

---

### 6. Dynamic Schema Enforcement & Data Cleaning
Standardizes raw API data into analytics-ready structures:

- Enforces strict typing for `Timestamp`, `Double`, `Long`, and `Boolean` fields  
- Detects and converts nested `Structs`, `Arrays`, and `Maps` into JSON strings  
- Removes schema inconsistencies before loading into Lakehouse  
- Ensures downstream compatibility with Power BI and analytics models  

---

### 7. Atomic Upserts with Delta MERGE
Guarantees data integrity and a single source of truth:

- Uses Delta Lake `MERGE` operations for insert/update logic  
- Prevents duplicate records across ingestion cycles  
- Supports both single-key and composite-key deduplication strategies  
- Ensures atomic transactions within Microsoft Fabric Lakehouse  

## 📁 Repository Structure

```
├── notebooks/
│   ├── ten_tables_ingestion.ipynb       # Static tables (full refresh)
│   └── daily_ingestion_function.ipynb   # Daily tables (incremental)
├── config/
│   └── config_template.json             # Configuration template
├── sql/
│   └── validation_queries.sql           # Data quality checks
├── docs/
│   ├── architecture.md                  # Detailed architecture
│   └── setup_guide.md                   # Setup instructions
├── .gitignore
├── requirements.txt
└── README.md
```

## 🛠️ Technology Stack

- **Platform**: Microsoft Fabric
- **Data Storage**: OneLake (Delta Lake format)
- **Processing**: PySpark (Python)
- **ETL**: Dataflow Gen2 (for large endpoints)
- **Orchestration**: Fabric Data Pipelines
- **Visualization**: Power BI
- **API**: Tenna REST API v1

## ⚙️ Setup Instructions

### Prerequisites
- Microsoft Fabric workspace access
- Tenna API token with read permissions
- Python 3.8+ (for local development)

### Configuration

1. **Create Lakehouse**
   ```
   - Navigate to Fabric workspace
   - Create new Lakehouse named "Tenna_Raw"
   ```

2. **Set API Credentials**
   ```python
   # In your notebook, set:
   API_TOKEN = "your_tenna_api_token_here"
   BASE_URL = "https://api.tenna.com/v1"
   ```

3. **Upload Notebooks**
   - Import `ten_tables_ingestion.ipynb` for static tables
   - Import `daily_ingestion_function.ipynb` for daily tables

4. **Configure Pipeline**
   - Create Fabric Pipeline
   - Add notebook activities in sequence
   - Schedule daily refresh (recommended: 2 AM)

### First Run

```python
# Run ten_tables_ingestion.ipynb first (one-time setup)
# This creates all static tables and control tables

# Then run daily_ingestion_function.ipynb
# This handles incremental daily data
```

## 🔄 Automated Refresh Pipeline

The Fabric Pipeline orchestrates:

1. **Run Python Notebook** → Refresh static tables
2. **Refresh Dataflows** → Process large endpoints
3. **Refresh Semantic Model** → Update relationships/measures
4. **Sync to Power BI** → Publish updated dashboards

**Result**: 100% automated, zero-touch reporting

## 📊 Power BI Dashboard

> **⚠️ IMPORTANT:** The dashboard shown below uses **completely fictional data** created for demonstration purposes. The actual production dashboard contains sensitive company and client information that cannot be publicly shared.

![Equipment Fleet Analytics Dashboard](images/dashboard.png)

This sample dashboard demonstrates the analytical capabilities and visual design delivered to stakeholders, including:

### Key Features:
- **Real-time KPI monitoring** - Fleet utilization, total hours, asset count, fleet value
- **Trend analysis** - Utilization patterns over time with target benchmarks
- **Asset performance** - Top performing equipment identification
- **Fleet composition** - Distribution by manufacturer and operational status
- **Interactive filtering** - Drill-down capabilities across all dimensions

### Technical Implementation:
- **Data Model:** Star schema with 2 fact tables and relationships
- **DAX Measures:** Custom calculations for utilization, aggregations, and KPIs
- **Visualizations:** Cards, line charts, bar charts, tables, and pie charts
- **Design:** Consistent color theming with professional styling

**📁 Full Power BI file (.pbix) and sample data available in the [`/powerbi/`](powerbi/) folder**

The fictional dataset includes:
- **100 equipment assets** across multiple manufacturers and types
- **6 months of daily utilization data** 
- **Multiple work sites** and operator assignments
- **Complete financial and operational metrics**

### Dashboard Preview Features:
1. **KPI Cards** - High-level metrics at-a-glance
2. **Utilization Trend Line Chart** - Historical performance with target line
3. **Manufacturer Distribution Bar Chart** - Fleet composition analysis  
4. **Top 10 Assets Table** - Performance leaderboard
5. **Status Pie Chart** - Operational vs maintenance breakdown

## 🧪 Data Quality Checks

Example validation queries:

```sql
-- Row count validation
SELECT COUNT(*) as total_rows FROM Tenna_Raw.assets;

-- Check for nulls in critical fields
SELECT COUNT(*) as null_count 
FROM Tenna_Raw.assets 
WHERE asset_id IS NULL;

-- Duplicate detection
SELECT asset_id, COUNT(*) as dup_count
FROM Tenna_Raw.assets
GROUP BY asset_id
HAVING COUNT(*) > 1;

-- Pro-rated Monthly Charge Calculation
-- Replicates the 13, 8, 10 day split for January 2025
SELECT 
    asset_id,
    MonthYear,
    organization_department,
    days_on_job,
    -- Calculate Tier based on Total Days for the Asset in that Month
    CASE 
        WHEN SUM(days_on_job) OVER(PARTITION BY asset_id, MonthYear) >= 22 THEN 1.00
        WHEN SUM(days_on_job) OVER(PARTITION BY asset_id, MonthYear) >= 15 THEN 0.75
        WHEN SUM(days_on_job) OVER(PARTITION BY asset_id, MonthYear) >= 8  THEN 0.50
        WHEN SUM(days_on_job) OVER(PARTITION BY asset_id, MonthYear) >= 1  THEN 0.25
        ELSE 0 
    END AS billing_tier,
    -- Pro-rate the cost across multiple departments in one month
    (days_on_job / CAST(SUM(days_on_job) OVER(PARTITION BY asset_id, MonthYear) AS FLOAT)) 
    * (internal_rental_rate * billing_tier) as monthly_charge
FROM ExpandedAssetBilling;
```

## 🔍 Code Highlights

### Type Enforcement
```python
# Timestamp columns
datetime_cols=["created_at", "updated_at", "purchase_date"]

# Long integer columns
long_int_cols=["year", "cumulative_hours_end_of_day"]

# Boolean columns
boolean_cols=["billable", "ecu_hours_provided"]

# Double columns (percentages, rates)
double_cols=["utilization_percentage", "expected_run_hours"]
```

### Incremental Loading
```python
# Fetch only new data since last run
if last_run_time:
    params["date_from"] = max_date.strftime('%Y-%m-%d')
```

### Deduplication with MERGE
```python
delta_table.alias("target").merge(
    df.alias("source"),
    f"target.{merge_key} = source.{merge_key}"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()
```

## 📊 Performance

- **Initial Load**: ~2-3 hours for all 14 endpoints
- **Daily Incremental**: ~10-15 minutes
- **Total Data Volume**: Millions of records across tables
- **Refresh Frequency**: Daily (automated)

## 🤝 Contributing

This is a demonstration project showcasing Microsoft Fabric capabilities. Feel free to adapt the patterns for your own use cases.

## 📝 License

MIT License - See LICENSE file for details

## 🙋 Questions?

For questions about this implementation approach, please open an issue or reach out via LinkedIn.

---

**Note**: This repository contains sanitized code with credentials removed. You'll need to configure your own API tokens and workspace details to run this code.
