# E-Commerce Data Lakehouse Pipeline
*An End-to-End Medallion Lakehouse Architecture built on Databricks, PySpark, and Delta Lake*

[![Platform: Databricks](https://img.shields.io/badge/Platform-Databricks-orange?logo=databricks)](https://databricks.com/)
[![Engine: PySpark](https://img.shields.io/badge/Engine-PySpark-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![Storage: Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-0052CC)](https://delta.io/)

---

## Executive Summary & Architecture
This repository contains a full-lifecycle Data Engineering pipeline designed to process raw, multi-source e-commerce data into a highly optimized, BI-ready Data Lakehouse. 

Built using the **Medallion Architecture pattern (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)**, the pipeline isolates raw data ingestion, schema enforcement/data cleansing, and final analytical aggregation into distinct computational layers.




```
   +-----------------------------------------------------------------+
   |                       RAW DATA SOURCES                          |
   |             (E-Commerce Orders, Customers, Products)            |
   +-----------------------------------------------------------------+
                                 |
                                 v
+-------------------------------------------------------------------------+
|  BRONZE LAYER (Append-Only / Raw Ingestion)                             |
|  - Autoloader/Stream ingestion into raw Delta tables                    |
|  - Preserves exact source schema and audit metadata (_ingest_timestamp)  |
+-------------------------------------------------------------------------+
                                 |
                                 v
+-------------------------------------------------------------------------+
|  SILVER LAYER (Cleansed & Enriched)                                     |
|  - Schema enforcement, null handling, and type casting                  |
|  - Deduplication via Delta Merge (SCD Type 1/2)                         |
|  - Data quality constraints and validation checks                       |
+-------------------------------------------------------------------------+
                                 |
                                 v
+-------------------------------------------------------------------------+
|  GOLD LAYER (Business-Ready Analytics)                                  |
|  - Star-schema dimensional modeling (Fact & Dimension tables)           |
|  - Pre-aggregated KPIs for Executive and BI dashboards                 |
|  - Query performance optimization via Z-Ordering                        |
+-------------------------------------------------------------------------+
                                 |
                                 v
+-----------------------------------------------------------------+
|                  BI DASHBOARDS & DOWNSTREAM AD-HOC               |
|                (Power BI, Tableau, Databricks SQL)               |
+-----------------------------------------------------------------+

```

---

## Key Technical Decisions & Engineering Highlights

* **ACID Transactions & Lineage:** Utilized Delta Lake storage format across all layers to ensure ACID guarantees, schema evolution capabilities, and full data time-travel/lineage tracking.
* **Idempotent Pipelines:** Designed ingestion and transformation steps using `MERGE INTO` upsert logic to ensure re-runnable, idempotent execution without data duplication.
* **Data Quality Isolation:** Implemented automated data quality validation at the Silver boundary to quarantine malformed records before reaching business layer aggregates.
* **Storage Performance Tuning:** Applied `OPTIMIZE` and `Z-ORDERING` indexing techniques on primary join keys (e.g., `customer_id`, `product_id`) in Gold layers to maximize file pruning and minimize scan latency during BI query execution.

---

## Directory & File Structure

```text
Project_Ecommerce/
│
├── 1_setup/
│   └── setup_catalog.py                 # Unity Catalog / Database initialization & storage volumes setup
│
├── 1_medallion_processing_dim/
│   └── 1_dim_bronze.py                  # Ingestion, validation, and dimensional modeling for entities
│
└── 3_medallion_processing_fact/
    ├── 1_fact_bronze.py                 # Raw fact stream ingestion & append-logging into Bronze Delta tables
    ├── 2_fact_silver.py                 # Data cleaning, deduplication, constraint validation & enrichment
    └── 3_fact_gold.py                   # Business aggregation, dimensional joins, & star-schema optimization

```

---

## Detailed Pipeline Breakdown

### 1. Catalog & Environment Setup (`1_setup/`)

* Establishes isolated catalogs (`ecommerce`) and environment schemas (`bronze`, `silver`, `gold`).
* Configures Unity Catalog volumes (`/Volumes/ecommerce/source_data/raw`) to manage external raw data landing zones safely.

### 2. Dimension Pipeline (`1_medallion_processing_dim/`)

* Processes core dimensional entities (e.g., Customer profiles, Product catalogs, Store directories).
* Standardizes data types, handles missing attributes, and builds clean dimension tables to enable seamless dimensional joining.

### 3. Fact Pipeline (`3_medallion_processing_fact/`)

#### Bronze Layer (`1_fact_bronze.py`)

* **Goal:** Reliable, non-blocking ingestion of raw transactional event streams.
* **Strategy:** Reads raw payload inputs, appends metadata columns (`_ingest_time`, `_source_file`), and writes directly to raw Delta storage.

#### Silver Layer (`2_fact_silver.py`)

* **Goal:** Cleanse, standardize, and enforce structural data integrity.
* **Strategy:**
* Casts string fields to strict numerical/timestamp formats.
* Filters out invalid records (e.g., negative prices, missing primary keys).
* Executes deduplication logic to maintain strict row uniqueness.



#### Gold Layer (`3_fact_gold.py`)

* **Goal:** Business-ready dimensional data models built for analytics.
* **Strategy:**
* Joins clean Silver facts with dimensions to build a dimensional Star Schema model.
* Calculates core revenue, order volume, and customer lifetime metrics pre-aggregated by daily/monthly cohorts.
* Optimizes Delta storage layouts to deliver low-latency queries for BI tools like Power BI, Tableau, or Databricks SQL Dashboards.



---

## How to Run & Reproduce

### Prerequisites

1. Databricks Community Edition or Cloud Instance (AWS / Azure / GCP).
2. Runtime: Databricks Runtime 10.4 LTS or higher (Spark 3.x+).

### Step-by-Step Execution

1. Clone this repository into Databricks via **Repos / Git Folders**.
2. Run `1_setup/setup_catalog.py` to initialize database structures.
3. Execute dimension scripts under `1_medallion_processing_dim/`.
4. Sequentially execute fact scripts in order:
* `3_medallion_processing_fact/1_fact_bronze.py`
* `3_medallion_processing_fact/2_fact_silver.py`
* `3_medallion_processing_fact/3_fact_gold.py` 
