# Credit Card Data Engineering & Analytics Platform

An end-to-end data engineering and analytics project built using **Databricks, Apache Spark, PySpark, Delta Lake, and Unity Catalog**. This project implements a Medallion Architecture to ingest, clean, transform, and organize credit card customer and transaction data for business intelligence, customer segmentation, and credit risk analysis.

The platform transforms raw data into structured analytical tables and supports dashboards designed for risk managers, marketing teams, and sales leadership.

## Project Overview

Financial institutions need reliable data pipelines to understand customer behavior, monitor credit risk, analyze transaction patterns, and support data-driven business decisions.

This project addresses these needs by implementing a multi-layer data pipeline in Databricks. Raw CSV datasets are ingested into the Bronze layer, cleaned and validated in the Silver layer, and transformed into analytics-ready dimension and fact tables in the Gold layer.

The project also incorporates Unity Catalog organization, SQL views, data masking, access-group configuration, and business intelligence reports.

## Architecture

The project follows the Medallion Architecture.

```text
                Source CSV Files
                       |
                       v
              Databricks Volume
                       |
                       v
              +----------------+
              |  Bronze Layer  |
              | Raw Delta Data |
              +----------------+
                       |
                       v
              +----------------+
              |  Silver Layer  |
              | Cleaned Data   |
              | Validated Data |
              +----------------+
                       |
                       v
              +----------------+
              |   Gold Layer   |
              | Dimensional    |
              | Models & Facts |
              +----------------+
                       |
              +--------+---------+
              |                  |
              v                  v
         SQL Views         BI Dashboards
              |                  |
              +--------+---------+
                       |
                       v
             Business Insights
```

## Technology Stack

- **Databricks:** Cloud-based data engineering and analytics environment.
- **Apache Spark / PySpark:** Distributed data processing and transformation.
- **Delta Lake:** Storage format for structured tables.
- **Unity Catalog:** Centralized data organization and governance.
- **SQL:** Table creation, analytical views, and business logic.
- **Power BI:** Business intelligence dashboards and reporting.

## Data Sources

The pipeline processes five source datasets:

| Dataset | Description |
|---|---|
| `Dim_Customer.csv` | Customer attributes, credit information, and customer details |
| `Dim_Merchant.csv` | Merchant information and transaction-related attributes |
| `Dim_Card_Product.csv` | Card product characteristics, fees, and rewards |
| `Dim_Date.csv` | Calendar and date-related attributes |
| `Fact_Card_Transactions.csv` | Credit card transaction records and associated measures |

The source files are read from a Databricks Unity Catalog volume:

`/Volumes/credit_card_project/bronze/raw_files/`

## Data Pipeline

### 1. Bronze Layer — Raw Data Ingestion

**Notebook:** `01_Bronze_Layer.py`

The Bronze layer ingests the source CSV files into Delta tables while retaining the original data structure as much as possible.

Key operations:

- Reads CSV files with headers and inferred schemas.
- Loads the five datasets into corresponding Bronze tables.
- Adds `_ingestion_timestamp` to record ingestion time.
- Adds `_source_file` to identify the originating file.
- Writes the resulting DataFrames in Delta format.
- Uses overwrite mode to replace the target tables during each run.

**Output tables:**

- `credit_card_project.bronze.dim_customer`
- `credit_card_project.bronze.dim_merchant`
- `credit_card_project.bronze.dim_card_product`
- `credit_card_project.bronze.dim_date`
- `credit_card_project.bronze.fact_card_transactions`

### 2. Silver Layer — Data Cleaning and Transformation

**Notebook:** `02_Silver_Layer.py`

The Silver layer standardizes the raw data and prepares it for downstream analytical processing.

Key transformations include:

- Trimming whitespace from customer identifiers and names.
- Standardizing customer name capitalization.
- Casting numerical, Boolean, and date columns to appropriate data types.
- Removing duplicate customer, merchant, card product, date, and transaction records using their respective identifiers.
- Filtering out customer records with null customer IDs.
- Filtering out transactions with missing transaction or customer IDs.
- Cleaning phone number formatting by removing trailing `.0` values.
- Standardizing transaction dates and financial measures.

**Output tables:**

- `credit_card_project.silver.dim_customer`
- `credit_card_project.silver.dim_merchant`
- `credit_card_project.silver.dim_card_product`
- `credit_card_project.silver.dim_date`
- `credit_card_project.silver.fact_card_transactions`

### 3. Gold Layer — Analytical Data Modeling

**Notebook:** `03_Gold_Layer.py`

The Gold layer transforms the cleaned Silver datasets into analytics-ready tables.

Key operations:

- Creates surrogate keys for customers, merchants, and card products.
- Preserves `Date_ID` as the date dimension key.
- Joins customer and merchant surrogate keys to the transaction fact table.
- Uses left joins to retain transaction records when corresponding dimension keys are unavailable.
- Writes the resulting dimensional and fact tables in Delta format.

**Output tables:**

- `credit_card_project.gold.dim_customer`
- `credit_card_project.gold.dim_merchant`
- `credit_card_project.gold.dim_card_product`
- `credit_card_project.gold.dim_date`
- `credit_card_project.gold.fact_card_transactions`

#### Credit Risk Classification

The Gold notebook also contains business logic for categorizing customers according to credit utilization and missed payments.

| Risk Tier | Classification rule |
|---|---|
| High Risk | Credit utilization ratio ≥ 0.80 OR at least 2 missed payments in the last 6 months |
| Medium Risk | Credit utilization ratio ≥ 0.50 OR exactly 1 missed payment in the last 6 months, provided the High Risk condition is not met |
| Low Risk | Neither the High Risk nor Medium Risk condition is met |

This classification supports customer risk segmentation and credit portfolio analysis.

## Data Governance and Security

The project includes SQL notebooks covering data organization, access management, and privacy-related controls.

### Unity Catalog

The project uses the `credit_card_project` catalog with separate `bronze`, `silver`, and `gold` schemas to organize data by processing stage.

### SQL Views

The project includes a notebook for creating SQL views to provide reusable access to analytical data. The planned analytical views include customer-level and transaction-level reporting views.

### Access Groups

A separate SQL notebook covers group creation and access-management configuration, supporting a structured approach to data access.

### Dynamic Data Masking

The project includes SQL logic for masking personally identifiable information (PII), including customer names, email addresses, and phone numbers.

These controls are intended to reduce unnecessary exposure of sensitive customer attributes. Effective enforcement depends on the masking definitions, permissions, and access policies configured in the Databricks environment.

## Business Intelligence Dashboards

The project includes three dashboard reports exported as PDF files.

### 1. Risk Manager Dashboard

**Objective:** Support credit risk monitoring and customer risk analysis.

Areas covered include:

- Customer default analysis across annual income brackets.
- Transaction amount analysis by month and credit score band.
- Exploration of customer credit profiles and default patterns.

**Business value:** Helps risk teams examine default behavior and identify customer segments that may require closer monitoring.

### 2. Marketing Dashboard

**Objective:** Understand customer spending patterns and support customer segmentation.

Areas covered include:

- Transaction amount by merchant category.
- Transaction amount by city.
- Spending patterns across credit score bands.
- Transaction amount by annual income bracket.

**Business value:** Helps marketing teams understand spending behavior across customer segments, locations, and merchant categories.

### 3. Sales Director Dashboard

**Objective:** Analyze card product usage and transaction behavior.

Areas covered include:

- Transaction amount by card type.
- Transaction amount by transaction type, including purchases, refunds, EMI, and cash advances.

**Business value:** Supports analysis of card product performance and transaction mix.

The exported reports are available in the `docs/` directory.

## Repository Structure

```text
credit-card-data-engineering/
|
|-- notebooks/
|   |-- 01_Bronze_Layer.py
|   |-- 02_Silver_Layer.py
|   |-- 03_Gold_Layer.py
|   |-- New_Notebook.py
|
|-- sql/
|   |-- Creating_views.dbquery.ipynb
|   |-- Creating_groups.dbquery.ipynb
|   |-- Masking_data.dbquery.ipynb
|
|-- docs/
|   |-- Risk_Manager_Report.pdf
|   |-- Marketing_Dashboard_Report.pdf
|   |-- Sales_Director_Report.pdf
|
|-- README.md
|-- .gitignore
```

The filenames and directory structure can be adjusted to match the final repository. Keep `New_Notebook.py` only if it contains relevant project logic, and rename it to something descriptive.

## Setup and Execution

### Prerequisites

- A Databricks workspace with a compatible compute environment.
- Unity Catalog access and the required catalog/schema privileges.
- Apache Spark and PySpark support through Databricks.
- The five source CSV files.
- Power BI Desktop, if you want to recreate or extend the dashboards.

### Step 1: Prepare the Catalog

Create the catalog and schemas if they do not already exist:

```sql
CREATE CATALOG IF NOT EXISTS credit_card_project;

CREATE SCHEMA IF NOT EXISTS
  credit_card_project.bronze;

CREATE SCHEMA IF NOT EXISTS
  credit_card_project.silver;

CREATE SCHEMA IF NOT EXISTS
  credit_card_project.gold;
```

### Step 2: Upload the Source Files

Create a Unity Catalog volume for the raw files:

```sql
CREATE VOLUME IF NOT EXISTS
  credit_card_project.bronze.raw_files;
```

Upload the five CSV files to the volume location:

`/Volumes/credit_card_project/bronze/raw_files/`

### Step 3: Run the Notebooks

Execute the notebooks in the following order:

1. `01_Bronze_Layer.py`
2. `02_Silver_Layer.py`
3. `03_Gold_Layer.py`

Ensure each layer completes successfully before running the next one.

### Step 4: Configure Governance

Run the relevant SQL scripts for view creation, access groups, and data masking after reviewing the required privileges and dependencies.

### Step 5: Connect to Power BI

Connect Power BI to the Databricks SQL warehouse or another supported Databricks SQL endpoint. Select the Gold-layer tables or analytical views required for the reports.

The existing PDF reports are reference outputs; they do not contain the original editable Power BI report definitions.

## Engineering Considerations

- **Reproducibility:** The pipeline depends on the source CSV files, catalog setup, and appropriate Databricks permissions.
- **Data quality:** The Silver layer applies explicit type casting, deduplication, and null filtering.
- **Lineage:** Bronze metadata records ingestion timestamps and source filenames.
- **Analytical modeling:** The Gold layer separates dimensions from transaction facts and introduces surrogate keys.
- **Governance:** Unity Catalog and masking logic provide a foundation for controlled data access.
- **Idempotency:** The notebooks use overwrite operations, so reruns replace existing target tables rather than incrementally appending data.
- **Future enhancements:** Add automated data-quality checks, pipeline orchestration, incremental ingestion, reconciliation metrics, and automated deployment through Git.

## Future Improvements

Potential extensions include:

- Implementing incremental data ingestion and change data capture.
- Adding automated data-quality checks and pipeline monitoring.
- Introducing orchestration with Databricks Workflows.
- Adding unit tests for transformation logic.
- Improving surrogate-key stability across reruns.
- Automating deployment and version control using GitHub integration.
- Publishing interactive dashboards with documented KPIs.
- Adding data dictionaries and architecture diagrams.

## Key Takeaways

This project demonstrates practical implementation of a layered data pipeline in Databricks, including raw data ingestion, PySpark transformations, Delta Lake storage, dimensional modeling, SQL-based governance, and business intelligence reporting.

It brings together data engineering and analytics to turn raw credit card customer and transaction data into structured datasets for business reporting and decision support.

## Disclaimer

This repository is intended for educational and portfolio purposes. Do not commit confidential data, real customer PII, credentials, access tokens, or production connection details. Use authorized, sanitized, or synthetic datasets when reproducing the pipeline.

---

**Technologies:** Databricks · Apache Spark · PySpark · Delta Lake · Unity Catalog · SQL · Power BI
