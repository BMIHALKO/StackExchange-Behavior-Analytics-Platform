# Project 2: E-Commerce Data Warehouse, Analytics & Governance

## Overview

Extend the real-time e-commerce pipeline built in **Project 1** by loading the processed data into a **Snowflake** data warehouse using a **medallion architecture**, transforming it with **dbt**, building interactive dashboards with **Power BI** and **Streamlit**, and implementing **data quality, governance, and DataOps** practices. This project covers **Weeks 5–7** of the Data Engineering curriculum.

> **Prerequisite:** Completed Project 1 with transformed Parquet datasets (hourly sales, top products, regional revenue, order status breakdown).

---

## Business Scenario

The e-commerce company now wants to:

1. **Centralize** all raw and processed order data in a cloud data warehouse (Snowflake).
2. **Model** the data using dimensional design (star schema) with a medallion architecture (Bronze → Silver → Gold).
3. **Transform** data with dbt for repeatable, tested, version-controlled analytics models.
4. **Visualize** KPIs through Power BI dashboards and a real-time Streamlit monitoring app.
5. **Govern** the data with quality checks, PII masking, RBAC, lineage tracking, and CI/CD automation.

---

## Architecture

```
                       Project 1 Output
                     (Parquet / CSV files)
                             │
                             ▼
               ┌─────────────────────────┐
               │  Snowflake: BRONZE Layer│  ← Raw ingestion (Snowpipe / COPY INTO)
               │  (raw_orders,           │
               │   raw_regions)          │
               └────────────┬────────────┘
                            │
                            ▼
               ┌─────────────────────────┐
               │  dbt: SILVER Layer      │  ← Cleaning, dedup, type casting
               │  (stg_orders,           │
               │   stg_regions)          │
               └────────────┬────────────┘
                            │
                            ▼
               ┌─────────────────────────┐
               │  dbt: GOLD Layer        │  ← Business-ready dimensional models
               │  (fact_orders,          │
               │   dim_products,         │
               │   dim_regions,          │
               │   dim_customers,        │
               │   mart_hourly_sales,    │
               │   mart_regional_revenue)│
               └────────────┬────────────┘
                            │
                ┌───────────┴───────────┐
                ▼                       ▼
     ┌──────────────────┐    ┌──────────────────┐
     │   Power BI       │    │   Streamlit App   │
     │   Dashboards     │    │   (Live Metrics)  │
     └──────────────────┘    └──────────────────┘

               Data Quality & Governance
     ┌──────────────────────────────────────────┐
     │  dbt tests · RBAC · PII masking · CI/CD  │
     │  Data lineage · Compliance (GDPR)        │
     └──────────────────────────────────────────┘
```

---

## Tech Stack

| Technology       | Purpose                                           | Curriculum Week |
|------------------|---------------------------------------------------|:---------------:|
| Snowflake        | Cloud data warehouse, medallion architecture      | Week 5          |
| SnowSQL          | CLI operations, bulk loading, queries              | Week 5          |
| Snowpipe         | Automated data ingestion                          | Week 5          |
| dbt              | Data transformations, testing, documentation      | Week 5          |
| Power BI         | Business dashboards, DAX, reports                 | Week 6          |
| Streamlit        | Interactive Python-based monitoring dashboard     | Week 6          |
| dbt tests        | Data quality validation                           | Week 7          |
| CI/CD (GitHub Actions) | Automated dbt runs, Airflow DAG tests       | Week 7          |
| Snowflake RBAC   | Role-based access control, PII masking            | Week 7          |

---

## Detailed Requirements

### Module 5 — Snowflake Data Warehouse (Week 5 Mon–Wed)

**Goal:** Set up a Snowflake warehouse with medallion architecture and load Project 1 data.

#### 5A — Warehouse Setup & Bronze Layer

- Create a Snowflake account (trial or provided).
- Set up the following hierarchy:
  ```
  Database:   ECOMMERCE_DW
  Schemas:    BRONZE, SILVER, GOLD
  Warehouse:  ECOMMERCE_WH (X-Small)
  ```
- Create Bronze layer tables:
  ```sql
  BRONZE.RAW_ORDERS (
      order_id STRING, customer_id STRING, product_id STRING,
      product_name STRING, category STRING, quantity INTEGER,
      unit_price FLOAT, total_amount FLOAT, order_status STRING,
      region STRING, customer_email STRING, timestamp TIMESTAMP,
      _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
  )

  BRONZE.RAW_REGIONS (
      region_code STRING, region_name STRING, country STRING, timezone STRING
  )
  ```
- Load Project 1's Parquet/CSV output using:
  - **`COPY INTO`** command with a named file format
  - **Snowpipe** for automated ingestion (configure with a stage)
- Create **views** on the Bronze tables for quick inspection.

#### 5B — Querying & Semi-Structured Data

- Write SnowSQL queries to explore the loaded data:
  - Total orders and revenue
  - Orders by region and status
  - Time-series analysis (orders per hour/day)
- Store a sample order as **VARIANT** (semi-structured JSON) and query it using dot notation and `LATERAL FLATTEN`.
- Demonstrate **database replication** by cloning the `ECOMMERCE_DW` database.

#### 5C — UDFs, Streams & Tasks

- Create a **UDF** to classify orders by revenue tier:
  ```
  < $50 → "Low"  |  $50–$200 → "Medium"  |  > $200 → "High"
  ```
- Set up a **Stream** on `BRONZE.RAW_ORDERS` to capture CDC (Change Data Capture).
- Create a **Task** that runs every 5 minutes to process stream changes and insert into the Silver layer.

#### 5D — Schema Design (Star Schema)

- Design a **star schema** for the Gold layer:
  - **Fact Table:** `GOLD.FACT_ORDERS` (order_key, product_key, customer_key, region_key, quantity, revenue, order_date_key)
  - **Dimension Tables:**
    - `GOLD.DIM_PRODUCTS` (product_key, product_id, product_name, category)
    - `GOLD.DIM_REGIONS` (region_key, region_code, region_name, country, timezone)
    - `GOLD.DIM_CUSTOMERS` (customer_key, customer_id, email_masked)
    - `GOLD.DIM_DATE` (date_key, full_date, year, quarter, month, day, day_of_week)

---

### Module 6 — dbt Transformations (Week 5 Thu–Fri)

**Goal:** Build a dbt project to manage all Silver and Gold transformations.

#### 6A — dbt Project Setup

- Initialize a dbt project:
  ```
  dbt_ecommerce/
  ├── dbt_project.yml
  ├── profiles.yml            # Snowflake connection
  ├── models/
  │   ├── staging/             # Silver layer
  │   │   ├── _staging.yml     # Sources definition
  │   │   ├── stg_orders.sql
  │   │   └── stg_regions.sql
  │   ├── marts/               # Gold layer
  │   │   ├── dim_products.sql
  │   │   ├── dim_regions.sql
  │   │   ├── dim_customers.sql
  │   │   ├── dim_date.sql
  │   │   ├── fact_orders.sql
  │   │   ├── mart_hourly_sales.sql
  │   │   └── mart_regional_revenue.sql
  │   └── schema.yml           # Column-level docs & tests
  ├── seeds/
  │   └── regions.csv          # Seed file for region dimension
  ├── snapshots/
  │   └── snap_orders.sql      # SCD Type 2 snapshot on order_status
  └── tests/
      ├── assert_positive_revenue.sql
      └── assert_valid_status.sql
  ```

#### 6B — Staging Models (Silver Layer)

- Define **sources** pointing to `BRONZE.RAW_ORDERS` and `BRONZE.RAW_REGIONS`.
- Create `stg_orders.sql`:
  - Cast types, rename columns, deduplicate on `order_id`
  - Filter out null `order_id` records
  - Add `order_date` derived from `timestamp`
- Create `stg_regions.sql`:
  - Standardize region codes to uppercase
- Use `{{ ref() }}` for downstream model dependencies.

#### 6C — Gold Layer Models

- **`fact_orders`**: Join staging orders with dimension keys, compute `revenue = quantity × unit_price`.
- **`dim_products`**: Distinct products from staging orders.
- **`dim_customers`**: Distinct customers with masked emails (using a dbt macro).
- **`dim_date`**: Generate a date spine using `dbt_utils.date_spine`.
- **`mart_hourly_sales`**: Aggregated hourly sales (replaces Spark version).
- **`mart_regional_revenue`**: Revenue by region with region names (replaces Spark version).

#### 6D — Seeds & Snapshots

- Load `regions.csv` as a dbt **seed**: `dbt seed`.
- Create a **snapshot** on orders to track status changes (SCD Type 2).

---

### Module 7 — Power BI Dashboards (Week 6 Mon–Thu)

**Goal:** Build interactive dashboards connected to Snowflake Gold layer.

#### 7A — Data Connection & Import

- Connect Power BI Desktop to Snowflake using the Snowflake connector.
- Import Gold layer tables: `fact_orders`, `dim_products`, `dim_regions`, `dim_date`.
- Design the **schema** in Power BI model view (star schema relationships).

#### 7B — DAX Measures

Create the following DAX measures:
```dax
Total Revenue = SUM(fact_orders[revenue])
Total Orders = COUNTROWS(fact_orders)
Avg Order Value = DIVIDE([Total Revenue], [Total Orders])
Revenue YTD = TOTALYTD([Total Revenue], dim_date[full_date])
Cancellation Rate = 
    DIVIDE(
        CALCULATE(COUNTROWS(fact_orders), fact_orders[order_status] = "CANCELLED"),
        COUNTROWS(fact_orders)
    ) * 100
```

#### 7C — Reports & Visuals

Build the following report pages:

| Page | Visuals |
|------|---------|
| **Executive Summary** | KPI cards (revenue, orders, AOV), revenue trend line chart, status donut |
| **Product Analytics** | Top 10 products bar chart, category treemap, product revenue table |
| **Regional Performance** | Revenue by region map/bar, regional comparison matrix |
| **Data Alerts** | Conditional formatting on cancellation rate, alerts for revenue drops |

- Apply **slicers** for date range, region, category.
- Use **conditional formatting** on tables (red for high cancellation rates).
- Configure **scheduled dataset refresh** from Snowflake.

---

### Module 8 — Streamlit Dashboard (Week 6 Fri)

**Goal:** Build a live monitoring dashboard connected to Snowflake.

- Create a Streamlit app (`streamlit_app.py`) with:
  ```python
  # Key Components:
  st.metric()       # KPI cards: total revenue, orders, avg order value
  st.dataframe()    # Detailed order table with search/filter
  st.bar_chart()    # Revenue by region
  st.line_chart()   # Orders over time (hourly trend)
  st.sidebar()      # Filters: date range, region, category, order status
  ```
- **Connect to Snowflake** using `snowflake-connector-python`.
- **Outlier detection**: Flag orders with `total_amount > mean + 2σ` and highlight them.
- Auto-refresh the dashboard every 60 seconds.

---

### Module 9 — Data Quality & Testing (Week 7 Mon–Tue)

**Goal:** Implement comprehensive data quality checks.

#### 9A — dbt Tests

- **Schema tests** (in `schema.yml`):
  - `unique` and `not_null` on all primary keys
  - `accepted_values` on `order_status` (NEW, CANCELLED, RETURNED)
  - `relationships` between `fact_orders.product_key` → `dim_products.product_key`
- **Custom singular tests**:
  - `assert_positive_revenue.sql`: No orders with negative revenue
  - `assert_valid_status.sql`: No unknown order statuses
  - `assert_order_date_not_future.sql`: No future-dated orders
- Run tests: `dbt test` and review results.

#### 9B — Data Lineage

- Generate dbt documentation: `dbt docs generate` + `dbt docs serve`.
- Review the **lineage graph** showing Bronze → Silver → Gold flow.
- Document **technical lineage** (source → transformation → output).
- Describe **business lineage** (business meaning of each metric).

#### 9C — Automated Testing in Airflow

- Extend the Airflow DAG from Project 1 to include:
  - A `dbt_test` task that runs `dbt test` after `dbt run`.
  - A data quality sensor that checks Snowflake row counts.

---

### Module 10 — Data Governance & DataOps (Week 7 Wed)

**Goal:** Implement governance policies and CI/CD automation.

#### 10A — RBAC in Snowflake

- Create roles and grant permissions:
  ```sql
  -- Roles
  CREATE ROLE DATA_ENGINEER;
  CREATE ROLE DATA_ANALYST;
  CREATE ROLE DATA_STEWARD;

  -- Access control
  GRANT USAGE ON WAREHOUSE ECOMMERCE_WH TO ROLE DATA_ANALYST;
  GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE DATA_ANALYST;
  GRANT ALL PRIVILEGES ON SCHEMA BRONZE TO ROLE DATA_ENGINEER;
  GRANT ALL PRIVILEGES ON SCHEMA SILVER TO ROLE DATA_ENGINEER;
  ```

#### 10B — PII Handling & Masking

- Identify PII fields: `customer_email`.
- Apply **Dynamic Data Masking** in Snowflake:
  ```sql
  CREATE MASKING POLICY email_mask AS (val STRING)
      RETURNS STRING ->
      CASE
          WHEN CURRENT_ROLE() IN ('DATA_STEWARD') THEN val
          ELSE REGEXP_REPLACE(val, '.+@', '***@')
      END;

  ALTER TABLE GOLD.DIM_CUSTOMERS MODIFY COLUMN email
      SET MASKING POLICY email_mask;
  ```
- Document which fields are masked and for which roles.

#### 10C — CI/CD Pipeline

- Create a CI/CD pipeline (GitHub Actions or equivalent):
  ```yaml
  # .github/workflows/dbt_ci.yml
  on: [push, pull_request]
  jobs:
    dbt-ci:
      steps:
        - dbt deps
        - dbt seed --target ci
        - dbt run --target ci
        - dbt test --target ci
  ```
- Validate Airflow DAGs in CI:
  ```bash
  python -c "from airflow.models import DagBag; bag = DagBag(); assert not bag.import_errors"
  ```

#### 10D — Compliance Documentation

- Create a `GOVERNANCE.md` document covering:
  - Data classification (public, internal, confidential, restricted)
  - PII fields inventory and masking policies
  - RBAC matrix (role → schema/table access)
  - Retention policies
  - GDPR considerations (right to erasure, data portability)

---

## Deliverables

| #  | Deliverable                          | Format                  |
|----|--------------------------------------|-------------------------|
| 1  | Snowflake setup scripts              | SQL files               |
| 2  | dbt project (full)                   | dbt project directory   |
| 3  | Power BI report                      | `.pbix` file            |
| 4  | Streamlit dashboard                  | Python script           |
| 5  | dbt tests (schema + singular)        | SQL / YAML              |
| 6  | RBAC & masking policies              | SQL files               |
| 7  | CI/CD pipeline config                | YAML                    |
| 8  | `GOVERNANCE.md`                      | Markdown                |
| 9  | Extended Airflow DAG                 | Python script           |
| 10 | `README.md`                          | Setup & run guide       |

---

## Folder Structure

```
project2/
├── README.md
├── GOVERNANCE.md
├── snowflake/
│   ├── 01_setup_warehouse.sql          # DB, schemas, warehouse
│   ├── 02_create_bronze_tables.sql     # Bronze layer DDL
│   ├── 03_load_data.sql                # COPY INTO / Snowpipe
│   ├── 04_udfs.sql                     # Revenue tier UDF
│   ├── 05_streams_and_tasks.sql        # CDC stream + scheduled task
│   ├── 06_gold_star_schema.sql         # Fact & dimension tables
│   ├── 07_rbac.sql                     # Roles & permissions
│   └── 08_masking_policies.sql         # Dynamic data masking
├── dbt_ecommerce/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _staging.yml
│   │   │   ├── stg_orders.sql
│   │   │   └── stg_regions.sql
│   │   ├── marts/
│   │   │   ├── dim_products.sql
│   │   │   ├── dim_regions.sql
│   │   │   ├── dim_customers.sql
│   │   │   ├── dim_date.sql
│   │   │   ├── fact_orders.sql
│   │   │   ├── mart_hourly_sales.sql
│   │   │   └── mart_regional_revenue.sql
│   │   └── schema.yml
│   ├── seeds/
│   │   └── regions.csv
│   ├── snapshots/
│   │   └── snap_orders.sql
│   └── tests/
│       ├── assert_positive_revenue.sql
│       ├── assert_valid_status.sql
│       └── assert_order_date_not_future.sql
├── dashboards/
│   ├── streamlit_app.py
│   └── requirements.txt
├── airflow/
│   └── dags/
│       └── ecommerce_dw_dag.py
├── ci_cd/
│   └── .github/
│       └── workflows/
│           └── dbt_ci.yml
└── config/
    └── snowflake_connection.yml
```

---

## Evaluation Criteria

| Area                          | Weight | What We Look For                                                    |
|-------------------------------|:------:|---------------------------------------------------------------------|
| Snowflake Setup & Loading     | 15%    | Medallion architecture, proper DDL, Snowpipe, bulk loading          |
| dbt Models & Transformations  | 20%    | Model structure, ref() usage, staging→marts flow, macros            |
| Star Schema Design            | 10%    | Proper fact/dim separation, surrogate keys, date dimension          |
| Power BI Dashboard            | 15%    | DAX measures, interactive visuals, conditional formatting, slicers  |
| Streamlit App                 | 10%    | Snowflake connectivity, KPIs, charts, outlier detection             |
| Data Quality (dbt tests)      | 10%    | Schema tests, custom tests, data lineage documentation              |
| Governance & Security         | 10%    | RBAC, PII masking, compliance docs                                  |
| CI/CD & DataOps               | 5%     | Automated dbt pipeline, DAG validation                              |
| Code Quality & Documentation  | 5%     | Clean code, README, GOVERNANCE.md, inline comments                  |

---

## Stretch Goals (Optional)

- Implement **Snowflake Time Travel** to query historical data states.
- Build a **dbt macro** for dynamic PII masking across multiple email/phone columns.
- Add **Snowflake Alerts** for anomaly detection (e.g., revenue drop > 30% day-over-day).
- Create a **second Streamlit page** for product-level drill-down analytics.
- Implement **Great Expectations** as an alternative to dbt tests for data quality.
- Set up **Snowflake data sharing** to simulate sharing Gold layer with external partners.
- Add **dbt exposures** to document downstream Power BI / Streamlit consumers.
