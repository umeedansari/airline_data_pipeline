# ✈️ Data Pipeline: Bronze, Silver & Gold Layers

This repository outlines the implementation of a **multi-layered data pipeline** built on **Databricks**, using **Delta Lake architecture**, **Auto Loader**, and **Delta Live Tables (DLT)**.

It processes airline-related data across domains like **Bookings**, **Flights**, **Passengers**, and **Airports**, and transforms it through the classic **Bronze → Silver → Gold** data layering pattern.

---

## 🟫 Bronze Layer – Raw Data Ingestion

The **Bronze Layer** is responsible for ingesting raw CSV files from cloud storage into Delta Lake using **Databricks Auto Loader**.

### Key Features:
- Streaming ingestion with support for **schema evolution** and **rescue mode**
- Reusable logic parameterized by domain (e.g., Flights, Bookings)
- Checkpointing enabled for fault tolerance
- Data stored as-is in Delta format, preserving original structure

### 🔁 Orchestration:

<img width="892" height="291" alt="Screenshot 2025-09-29 120921" src="https://github.com/user-attachments/assets/4e37a099-a07c-4bbe-9eb3-5bd497021810" />


- A **Databricks Job** is created to run the ingestion logic per domain.
- Uses **Databricks widgets** to dynamically pass folder names (e.g., `Flights`, `Bookings`, etc.)
- The job can be triggered **manually or scheduled**, and it processes new data in micro-batches.

---

## 🪙 Silver Layer – Cleaned & Conformed Data

The **Silver Layer** processes raw Bronze data into structured and validated datasets using **Delta Live Tables (DLT)**.

### Key Features:
- **DLT pipeline** manages transformations and automates deployment
- Implements **data quality expectations** (e.g., non-null primary keys)
- Adds **audit columns** like `modified_date` for tracking changes
- Uses **`create_auto_cdc_flow`** to manage **Type 1 Slowly Changing Dimensions (SCD1)**
- Each domain (Airports, Flights, etc.) is handled via reusable logic

### 🔁 Orchestration:

<img width="589" height="578" alt="Screenshot 2025-09-29 121139" src="https://github.com/user-attachments/assets/1b881095-eba6-42ec-904c-8266fc2f3ad4" />


- A **DLT Pipeline Job** handles all Silver transformations.
- It can be configured to run **in triggered or continuous mode**.
- Simplifies execution by encapsulating notebook logic within the DLT framework.

---

## 🥇 Gold Layer – Business-Focused Dimensions & Facts

The **Gold Layer** consists of cleaned and joined **dimension** and **fact** tables ready for analytics, BI tools, or reporting.

### 1. ⭐ Dimension Tables
- Surrogate keys generated for each dimension table
- Upsert logic handles both new and changed records using `modified_date`
- Maintains `created_date` and `updated_date` for audit purposes

### 2. 📊 Fact Table
- Bookings data acts as the main fact table
- Joins with all dimension tables using natural keys → surrogate keys
- Supports **CDC-based incremental loads**
- Handles **backdated refreshes** when needed
- Outputs a fully denormalized fact table with measures (e.g., `amount`)

## 🥇 Gold Layer – Job Orchestration

The Gold Layer job runs as a **multi-task job** in Databricks, orchestrating dynamic dimension and fact table builds.

<img width="952" height="169" alt="Screenshot 2025-09-29 120050" src="https://github.com/user-attachments/assets/3a475a0f-82d0-4281-9908-ab8b6bd2a7ba" />  

- **dimension_params:** Defines the parameters for dimension processing.
- **dimension_tables:** Loops through and builds each dimension dynamically.
- **fact_table:** Runs after dimensions to build the final fact table with surrogate keys and measures.

---

## 📊 Dashboard – Insights from Gold Layer Data
The dashboard leverages curated and fully transformed datasets from the Gold Layer to provide rich analytics and business insights. By using dimension and fact tables processed through the data pipeline.

<img width="1239" height="674" alt="image" src="https://github.com/user-attachments/assets/2d12e518-e173-41c8-a545-f272e6021e2d" />


