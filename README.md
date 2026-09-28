# Enterprise Multi-Layer Data Warehouse (EDW) Blueprint

[![SQL Standard](https://img.shields.io/badge/SQL-ANSI%20%2F%20Postgres-blue?style=flat-square&logo=postgresql)](https://www.postgresql.org/)
[![Architecture](https://img.shields.io/badge/Architecture-ELT%20%7C%20Kimball%20Star%20Schema-orange?style=flat-square)](#part-1-the-multi-layer-architecture)
[![Orchestration](https://img.shields.io/badge/Orchestration-Airflow%20%7C%20dbt-green?style=flat-square)](#part-4-data-engineering-operations-plan)

A production-grade, enterprise-ready reference blueprint for building a **Multi-Layer Data Warehouse** using SQL. This design implements a modern, highly performant **ELT (Extract-Load-Transform)** pattern, moving the computational overhead of data cleansing, historical tracking (SCD Type 2), and dimensional modeling into the data warehouse engine.

---

## Table of Contents
1. [The Multi-Layer Architecture](#part-1-the-multi-layer-architecture)
2. [Database & Schema Setup](#part-2-database--schema-setup)
3. [Data Ingestion Strategy](#data-ingestion-strategy)
4. [Data Engineering Pipeline (The SQL Code)](#part-3-data-engineering-pipeline-the-sql-code)
    * [Layer 1: Raw (The Ingestion Layer)](#layer-1-raw-the-ingestion-layer)
    * [Layer 2: Staging (The Cleanse & Cast Layer)](#layer-2-staging-the-cleanse--cast-layer)
    * [Layer 3: Core (The Dimensional Model)](#layer-3-core-the-dimensional-model)
    * [Layer 4: Presentation (The BI Data Marts)](#layer-4-presentation-the-bi-data-marts)
5. [Data Quality & Testing Framework](#data-quality--testing-framework)
6. [Performance Optimization & Tuning](#performance-optimization--tuning)
7. [Data Engineering Operations Plan](#part-4-data-engineering-operations-plan)

---

# Part 1: The Multi-Layer Architecture

This implementation utilizes a **4-Layer Architecture** to decouple raw storage from high-performance analytical views. This ensures schema flexibility, continuous data lineage tracking, and sub-second BI query performance.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS (OLTP, APIs)                     │
└───────────────────────────────────┬────────────────────────────────────
                                    │ (Fivetran, Airflow, Copy Into)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ LAYER 1: RAW / LANDING (raw_customer, raw_orders)                     │  <-- No changes, schema-on-read/variant/text
└───────────────────────────────────┬────────────────────────────────────
                                    │ (SQL Cleanse, Deduplicate, Cast)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ LAYER 2: STAGING (stg_customers, stg_orders)                           │  <-- Cleaned, casted, standardized
└───────────────────────────────────┬────────────────────────────────────
                                    │ (SQL Kimball Model - Keys, SCD 2)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ LAYER 3: CORE WAREHOUSE (dim_customers, fact_orders)                   │  <-- Star Schema (Single Source of Truth)
└───────────────────────────────────┬────────────────────────────────────
                                    │ (SQL Aggregations, Business Logic)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ LAYER 4: PRESENTATION / MART (dm_sales_performance)                    │  <-- Fast, BI-ready views/tables
└────────────────────────────────────────────────────────────────────────┘
```

---

# Part 2: Database & Schema Setup

Run this script to initialize your database clusters and isolate permissions across functional layers.

```sql
-- Create Database
CREATE DATABASE corporate_dwh;
\c corporate_dwh; -- Connect (PostgreSQL syntax)

-- Create Schemas for each Layer
CREATE SCHEMA layer1_raw;          -- Landing zone for raw ingestion
CREATE SCHEMA layer2_staging;      -- Data cleansing & type casting
CREATE SCHEMA layer3_core;         -- Enterprise Dimensional Model (Star Schema)
CREATE SCHEMA layer4_presentation; -- Departmental Data Marts (BI ready)
```

---

# Data Ingestion Strategy

Before moving data into **Layer 1 (Raw)**, select the optimal extraction tool based on your source type:

```
                  ┌────────────────────────┐
                  │     SOURCE SYSTEMS     │
                  └───────────┬────────────┘
                              │
             ┌────────────────┴────────────────┐
             ▼ (Standard SaaS/DBs)             ▼ (High Volume Files/Logs)
      ┌──────────────┐                  ┌──────────────┐
      │   Fivetran   │                  │  S3 / GCS    │
      └──────┬───────┘                  └──────┬───────┘
             │                                 │ (COPY INTO)
             └────────────────┬────────────────┘
                              ▼
                     ┌──────────────────┐
                     │   DWH: RAW L1    │
                     └──────────────────┘
                              ▲
                              │ (Triggered & Orchestrated by)
                     ┌──────────────────┐
                     │  Apache Airflow  │
                     └──────────────────┘
```

*   **Fivetran / Airbyte (API & DB Replication):** Best for SaaS applications (Salesforce, Stripe) and Change Data Capture (CDC) from operational PostgreSQL/MySQL/Oracle engines. High resilience, automatic schema drift handling.
*   **`COPY INTO` / Cloud Ingestion (Bulk Storage):** Best for high-frequency logs, Parquet, or JSON files deposited in AWS S3 or Google Cloud Storage. Directly leverages internal warehouse engines to execute bulk loads at minimal cost.
*   **Apache Airflow / Prefect (Orchestrator):** Do not write heavy custom ingestion scripts here. Use the orchestrator to trigger Fivetran sync tasks or run bulk database loader operations.

---

# Part 3: Data Engineering Pipeline (The SQL Code)

---

## LAYER 1: RAW (The Ingestion Layer)

In the Raw layer, we optimize for **Ingestion Speed** and **Flexibility**. Columns are typed as `VARCHAR(255)` or unstructured `JSONB`. Schema changes in production source databases will not break this ingestion.

```sql
-- Raw Customers (from CRM)
CREATE TABLE layer1_raw.raw_customers (
    id VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    city VARCHAR(255),
    country VARCHAR(255),
    ingested_at VARCHAR(255)
);

-- Raw Orders (from ERP system)
CREATE TABLE layer1_raw.raw_orders (
    order_id VARCHAR(255),
    customer_id VARCHAR(255),
    order_date VARCHAR(255),
    product_sku VARCHAR(255),
    quantity VARCHAR(255),
    unit_price VARCHAR(255),
    discount VARCHAR(255),
    ingested_at VARCHAR(255)
);
```

---

## LAYER 2: STAGING (The Cleanse & Cast Layer)

Staging is constructed as views (or ephemeral tables if using massive volumes). Here, we:
1. Standardize text strings using `TRIM()` and `LOWER()`.
2. Convert and validate data types (`INT`, `DECIMAL`, `DATE`, `TIMESTAMP`).
3. Handle missing values and enforce defaults with `COALESCE()`.
4. Deduplicate raw operational inputs via window partitioning.

```sql
CREATE OR REPLACE VIEW layer2_staging.stg_customers AS
WITH deduplicated AS (
    SELECT 
        id::INT AS source_customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        COALESCE(TRIM(city), 'Unknown') AS city,
        COALESCE(TRIM(country), 'Unknown') AS country,
        ingested_at::TIMESTAMP AS source_updated_at,
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY ingested_at DESC) as row_num
    FROM layer1_raw.raw_customers
)
SELECT 
    source_customer_id, 
    first_name, 
    last_name, 
    email, 
    city, 
    country, 
    source_updated_at
FROM deduplicated
WHERE row_num = 1; -- Keeps only the latest record per customer


CREATE OR REPLACE VIEW layer2_staging.stg_orders AS
SELECT 
    order_id::INT AS source_order_id,
    customer_id::INT AS source_customer_id,
    order_date::DATE AS order_date,
    TRIM(product_sku) AS product_sku,
    COALESCE(quantity::INT, 0) AS quantity,
    COALESCE(unit_price::NUMERIC(10,2), 0.00) AS unit_price,
    COALESCE(discount::NUMERIC(10,2), 0.00) AS discount_amount,
    ingested_at::TIMESTAMP AS source_loaded_at
FROM layer1_raw.raw_orders
WHERE order_id IS NOT NULL;
```

---

## LAYER 3: CORE (The Dimensional Model)

The Core layer structures cleansed business data into an optimized **Kimball Star Schema** containing reusable Dimension (SCD Type 2) and Fact tables.

### 1. Create Core Tables

```sql
-- A. Date Dimension (Standard Static Table)
CREATE TABLE layer3_core.dim_date (
    date_key INT PRIMARY KEY,
    calendar_date DATE UNIQUE,
    year INT,
    quarter INT,
    month INT,
    day_name VARCHAR(15)
);

-- B. Customer Dimension (SCD Type 2)
CREATE TABLE layer3_core.dim_customers (
    customer_sk SERIAL PRIMARY KEY, -- Surrogate Key
    source_customer_id INT NOT NULL, -- Natural Key
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- C. Fact Sales
CREATE TABLE layer3_core.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    date_key INT REFERENCES layer3_core.dim_date(date_key),
    customer_sk INT REFERENCES layer3_core.dim_customers(customer_sk),
    product_sku VARCHAR(50),
    quantity INT,
    gross_amount NUMERIC(12,2),
    discount_amount NUMERIC(12,2),
    net_amount NUMERIC(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2. Dimension Ingestion Pipeline (Generating dates statically)

Run this once to pre-populate the static `dim_date` dimension:

```sql
INSERT INTO layer3_core.dim_date
SELECT 
    TO_CHAR(datum, 'YYYYMMDD')::INT AS date_key,
    datum AS calendar_date,
    EXTRACT(YEAR FROM datum) AS year,
    EXTRACT(QUARTER FROM datum) AS quarter,
    EXTRACT(MONTH FROM datum) AS month,
    TO_CHAR(datum, 'Day') AS day_name
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::interval) datum;
```

### 3. DATA ENGINEERING PIPELINE: SCD Type 2 Load Script (Customers)

This pipeline processes customer profiles. When a customer's `city`, `country`, or `email` updates, the script cleanly updates historical bounds (`valid_to`, `is_current = FALSE`) and appends a new state record.

```sql
-- Step A: Close modified existing records
UPDATE layer3_core.dim_customers tgt
SET 
    valid_to = stg.source_updated_at - INTERVAL '1 second',
    is_current = FALSE
FROM layer2_staging.stg_customers stg
WHERE tgt.source_customer_id = stg.source_customer_id
  AND tgt.is_current = TRUE
  AND (tgt.city <> stg.city OR tgt.country <> stg.country OR tgt.email <> stg.email);

-- Step B: Insert NEW records and CHANGED records
INSERT INTO layer3_core.dim_customers (
    source_customer_id, first_name, last_name, email, city, country, valid_from, valid_to, is_current
)
SELECT 
    stg.source_customer_id, 
    stg.first_name, 
    stg.last_name, 
    stg.email, 
    stg.city, 
    stg.country, 
    stg.source_updated_at AS valid_from, 
    NULL::TIMESTAMP AS valid_to, 
    TRUE AS is_current
FROM layer2_staging.stg_customers stg
LEFT JOIN layer3_core.dim_customers tgt
  ON stg.source_customer_id = tgt.source_customer_id 
  AND tgt.is_current = TRUE
WHERE tgt.customer_sk IS NULL; -- New customer, or closed old record in Step A
```

### 4. DATA ENGINEERING PIPELINE: Fact Table Load Script (Sales)

The sales transaction pipeline converts transactional natural keys into their corresponding database **Surrogate Keys**. It joins against the customer dimension, asserting correctness based on the point-in-time timestamp of when the purchase occurred.

```sql
INSERT INTO layer3_core.fact_sales (
    order_id, date_key, customer_sk, product_sku, quantity, gross_amount, discount_amount, net_amount
)
SELECT 
    stg.source_order_id AS order_id,
    -- Lookup Date Key (YYYYMMDD)
    TO_CHAR(stg.order_date, 'YYYYMMDD')::INT AS date_key,
    -- Lookup historical Customer Surrogate Key (SCD Type 2 join)
    COALESCE(cust.customer_sk, -99) AS customer_sk, 
    stg.product_sku,
    stg.quantity,
    (stg.quantity * stg.unit_price) AS gross_amount,
    stg.discount_amount,
    ((stg.quantity * stg.unit_price) - stg.discount_amount) AS net_amount
FROM layer2_staging.stg_orders stg
-- Join ensures we grab the customer record that was active WHEN the order was placed
LEFT JOIN layer3_core.dim_customers cust
  ON stg.source_customer_id = cust.source_customer_id
  AND stg.order_date BETWEEN cust.valid_from AND COALESCE(cust.valid_to, '9999-12-31'::TIMESTAMP)
-- Left join protection: Avoid duplication if loading incrementally
LEFT JOIN layer3_core.fact_sales f
  ON stg.source_order_id = f.order_id
WHERE f.order_id IS NULL; -- Ensures only new orders are loaded
```

---

## LAYER 4: PRESENTATION (The BI Data Marts)

The final layer is optimized for low-latency BI reading. Pre-joining static dimensions and measures saves end-user execution overhead on dashboards (such as Power BI or Tableau).

```sql
CREATE OR REPLACE VIEW layer4_presentation.dm_sales_by_customer_region AS
SELECT 
    d.year AS order_year,
    d.month AS order_month,
    c.country AS customer_country,
    c.city AS customer_city,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.net_amount) AS total_net_revenue
FROM layer3_core.fact_sales f
JOIN layer3_core.dim_date d ON f.date_key = d.date_key
JOIN layer3_core.dim_customers c ON f.customer_sk = c.customer_sk
GROUP BY d.year, d.month, c.country, c.city;
```

---

# Data Quality & Testing Framework

Before updating your downstream data tables, use SQL validations within your orchestration loops to catch data quality exceptions automatically.

### 1. Primary Key Uniqueness & Nullability Tests
Ensures surrogate keys are unique and non-nullable inside the Core layer.
```sql
-- Test: Uniqueness of Customer Surrogate Key
-- EXPECTED RESULT: 0
SELECT COUNT(customer_sk) - COUNT(DISTINCT customer_sk) AS duplicate_sk_count
FROM layer3_core.dim_customers;

-- Test: Non-Nullability of Core Keys
-- EXPECTED RESULT: 0
SELECT COUNT(*) AS null_key_failures
FROM layer3_core.fact_sales
WHERE customer_sk IS NULL OR date_key IS NULL;
```

### 2. Referential Integrity Tests
Verifies orphaned records do not exist. In standard analytical warehouses, physical constraints are dropped or set as logical-only to maximize ingestion performance.
```sql
-- Test: Fact table rows referencing non-existent dimension records
-- EXPECTED RESULT: 0
SELECT COUNT(f.sales_key) AS orphaned_fact_count
FROM layer3_core.fact_sales f
LEFT JOIN layer3_core.dim_customers c ON f.customer_sk = c.customer_sk
WHERE c.customer_sk IS NULL;
```

---

# Performance Optimization & Tuning

As your transaction volume grows into millions of records, apply these scaling enhancements.

### 1. Indexing Strategy
Execute these indexes to minimize the cost of joining dimensional structures on foreign keys.
```sql
-- Indexes on Fact Table Join Keys
CREATE INDEX idx_fact_sales_date ON layer3_core.fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON layer3_core.fact_sales(customer_sk);

-- Indexes on Dimension Natural Keys & Lookups
CREATE INDEX idx_dim_cust_natural ON layer3_core.dim_customers(source_customer_id, is_current);
```

### 2. Table Partitioning (Engines > 100M+ Rows)
If scaling in local/traditional databases (e.g., PostgreSQL), partition the `fact_sales` table by year boundaries.
```sql
-- Example PostgreSQL range partitioning concept
CREATE TABLE layer3_core.fact_sales_partitioned (
    sales_key INT,
    order_id INT,
    date_key INT,
    customer_sk INT,
    product_sku VARCHAR(50),
    quantity INT,
    gross_amount NUMERIC(12,2),
    discount_amount NUMERIC(12,2),
    net_amount NUMERIC(12,2)
) PARTITION BY RANGE (date_key);
```
*(Note: If migrating this script to a cloud columnar database like Snowflake or BigQuery, physical indexing is redundant. Define `Cluster Keys` on `date_key` and `customer_sk` instead.)*

---

# Part 4: Data Engineering Operations Plan

To operationalize this code successfully, run your tasks sequentially.

```
[12:00 AM] Raw Load: Run Ingestion Tools (Fivetran/Airflow) -> populate `layer1_raw`
    │
    ▼
[01:00 AM] Cleanse Step: Trigger `layer2_staging` views
    │
    ▼
[01:15 AM] Core Dimensions: Run SCD Type 2 SQL Script to update `layer3_core.dim_customers`
    │
    ▼
[01:30 AM] Core Facts: Run Fact Sales SQL Load to populate `layer3_core.fact_sales`
    │
    ▼
[02:00 AM] Mart Refresh: Run `REFRSH MATERIALIZED VIEW` on `layer4_presentation` tables.
```

### Modern Tooling Integration
*   **Transformation:** Build this layout in **dbt (Data Build Tool)**. Instead of raw SQL DDLs, write standard SQL `SELECT` statements, and let dbt manage creation types (incremental, table, view, ephemeral).
*   **Orchestration:** Use **Dagster**, **Prefect**, or **Apache Airflow**. Structure pipeline dependencies as a Directed Acyclic Graph (DAG) using the chronological ordering provided above.
*   **CI/CD Pipeline:** Configure staging environments in your CI engine (such as GitHub Actions). Run analytical tests on your staging branches (`dbt test` or raw SQL assertions) before committing code updates to production environments.
