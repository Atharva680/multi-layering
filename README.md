# Enterprise Multi-Layer Data Warehouse (DWH) Blueprint

This is a complete, production-grade blueprint for building a **Multi-Layer Enterprise Data Warehouse** using SQL. 

We will design a modern **ELT (Extract-Load-Transform)** architecture. In this paradigm, data is loaded raw and then transformed inside the database using SQL (commonly orchestrated via tools like **dbt** or **Apache Airflow**).

---

# Part 1: The Multi-Layer Architecture

We will implement a **4-Layer Architecture** to isolate raw data from analytics, ensuring data quality, traceability, and high performance.

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

Run this SQL script to create the distinct database environments and schemas for your engineering layers.

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

# Part 3: Data Engineering Pipeline (The SQL Code)

We will walk through the lifecycle of two datasets: **Customers** (SCD Type 2 - tracks history) and **Orders** (Transactions).

---

## LAYER 1: RAW (The Ingestion Layer)
Data arrives here directly from sources. Columns are kept as flexible text (`VARCHAR`) or JSON to prevent load jobs from breaking due to upstream schema changes.

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
Data Engineers use SQL to:
1. Cast strings to correct datatypes (`INT`, `DECIMAL`, `TIMESTAMP`).
2. Handle `NULL` values.
3. Deduplicate records.

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
Here we implement the Star Schema. 

* **`dim_customers`** will use **Slowly Changing Dimension Type 2 (SCD 2)** to preserve historical customer addresses.
* **`fact_sales`** will use surrogate keys from the dimensions.

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

### 2. DATA ENGINEERING PIPELINE: SCD Type 2 Load Script (Customers)
This SQL script handles changes. If a customer moves from "New York" to "London", it closes the old record (`is_current = FALSE`) and opens a new record with the new city.

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

### 3. DATA ENGINEERING PIPELINE: Fact Table Load Script (Sales)
This script loads data into the Fact table by executing a **Surrogate Key Lookup**. It links the transactional order to the correct historical record of the customer using timestamps.

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
This layer contains views or pre-aggregated tables optimized for downstream consumption by tools like Power BI, Tableau, or Excel.

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

# Part 4: Data Engineering Operations Plan

To put this system into production, follow this operations guide:

### 1. The Orchestration Logic (Scheduling)
You should schedule the SQL scripts to run sequentially. Here is a typical nightly batch pipeline:

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

### 2. Implementation Tooling Recommendations
* **Modern Stack:** Use **Snowflake** or **BigQuery** as your database engine, paired with **dbt (Data Build Tool)**. dbt manages the transformation layer using simple SQL `SELECT` statements and handles table/view generation automatically.
* **Orchestration:** Use **Prefect**, **Astronomer (Airflow)**, or **Dagster** to trigger and monitor pipeline executions.
* **Testing & Quality Assurance:** In your staging and core layers, set up automated data tests:
  * Check that `customer_sk` is unique and non-null in dimensions.
  * Check that `customer_sk` in `fact_sales` matches an existing record in `dim_customers` (Referential Integrity).
