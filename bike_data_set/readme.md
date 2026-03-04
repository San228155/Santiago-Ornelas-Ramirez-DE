# Bike Data Set – Star Schema Project

## Client Background

Company X requested support organizing their sales data for analytical use. They require a **Star Schema data model** to enable fast query performance and improve usability for data analysts.

We developed a pipeline for Company X using the **Medallion Architecture** and the **Kimball dimensional modeling approach**. Our focus is on delivering clean, transparent, and reliable pipelines that provide meaningful business insights and actionable recommendations.

---

## Target Metrics

We designed a data model that improves data quality, computational efficiency, and storage performance:

- Implemented a Kimball-based dimensional model  
- Reduced 6 source tables into:
  - 1 fact table  
  - 2 dimension tables  
- Ensured guaranteed data quality standards  
- Created indexed Gold tables:
  - Primary keys indexed on all tables  
  - Foreign keys indexed on the fact table  
- Reduced file size by a factor of 4 using a Star Schema design  

---

## Summary

### Executive Summary

- Improved query performance  
- Reduced file size by 4x  
- Delivered a clean, analytics-ready Star Schema model  

### Customer Summary (Data Analysts)

A 3-table Star Schema was implemented:

- 1 fact table  
- 2 dimension tables  

Each table includes:

- Indexed primary keys  
- Indexed foreign keys on the fact table for optimized query performance  

The pipeline ensures:

- Standardized column formatting (trimmed, lowercase, no spaces)  
- Correct data types  
- Adjustable metadata configurations  
- Guaranteed Second Normal Form (2NF) compliance  

---

## How to Use

1. Create a Databricks Repo in Databricks.
2. Clone the repository:  
   **Santiago-Ornelas-Ramirez-De**
3. Deploy the `databricks.yml` file using either:
   - Databricks CLI  
   - Databricks UI  

Deployment will automatically create a Databricks Job.

4. Run the job to execute transformations and create the required tables.

### Required Setup

Create:

- **Catalog:** `bike_data_lakehouse`
- **Schema:** `raw_data`
- **Volumes:**
  - `source_crm`
  - `source_erp`

Upload the necessary data from the GitHub repository below into the corresponding volumes (upload only the files, not the folders):

https://github.com/DataWithBaraa/databricks_bootcamp_2026/tree/main/datasets/engineering

---

## Structure & Execution

All transformations are executed via **Databricks Jobs**.

The job is created through deployment of a **Databricks Asset Bundle**.  
The job executes Python files packaged within a prebuilt Python wheel.

All future commits to the `main` branch automatically trigger a CI/CD pipeline that rebuilds the wheel to incorporate any changes.

---

# Architecture Overview

This project follows the **Medallion Architecture**, consisting of:

- Bronze Layer  
- Silver Layer  
- Gold Layer  

Each layer is executed through a Python **driver file**, which calls a **runner module** responsible for grouping transformation functions.

---

## Core Components

### Driver

The file executed by the job.  
It orchestrates table-by-table transformations for a specific layer.

### Runner

Called by the driver.  
Defines the execution order of transformation functions for each table within a layer.

### Transformation Files

Contain modularized Python functions.  
These define the transformation logic for each processing step.

### Table Metadata

Configuration files that define:

- File names  
- Data types  
- Column names  
- Required transformations  
- Target table names  

Each medallion layer has its own metadata configuration file.

---

# Medallion Architecture Layer Responsibilities

## Bronze Layer

Metadata requirements:

- Incoming table name  
- File type (CSV only supported)  
- Bronze table name  
- Overwrite configuration  

Processing behavior:

- Ingests raw data without transformation  
- Converts file type to Delta table format  
- Schema evolution allowed  
- Tables overwritten by default (unless specified otherwise)  
- All columns ingested as strings  

---

## Silver Layer

Metadata requirements:

- Bronze table name  
- All incoming columns explicitly specified  
- Transformation configurations  
- Final Silver table name  
- Final column names  

Processing behavior:

- Trim and lowercase all values  
- Enforce strict column specification (job fails if column missing)  
- Rename columns  
- Replace nulls and empty strings with datatype-appropriate defaults  
- Replace hyphens and spaces with underscores  
  - Multiple consecutive special characters reduced to a single underscore  
- Cast columns to correct data types  
- Apply specified column augmentations  
- Separate intelligent keys  
- Generate surrogate keys for Gold layer  
- Ensure Second Normal Form (2NF) compliance  

---

## Gold Layer

Metadata requirements:

- Silver table name  
- Columns required in Gold table  
- Gold table name  
- Join conditions for aggregation  

Processing behavior:

- Create aggregated dimension and fact tables  
- Drop surrogate keys not required in final model  
- Remove intelligent key components not part of primary keys  

---

# Output & Data Contracts

Final analytics-ready tables are published in the **Gold layer**.

Downstream consumers should rely exclusively on Gold tables.

The primary users are Data Analysts, as the final structure is optimized for visualization and reporting.

All table creation is internally managed for transparency and governance.

Any structural changes must be made in the transformation metadata files.  
If a table or column name changes, it must be updated across all metadata configurations where referenced.

---

# Monitoring & Troubleshooting

- Execution status and logs are available in **Databricks Job Runs**
- Failed tasks prevent downstream execution
- Individual drivers may be rerun independently for remediation

---

# Technology Used

- Databricks  
- PySpark  
- SQL  
- Beautiful Soup  

---

# Notes

This dataset and project were designed following **Data With Baraa’s Databricks Bootcamp**:

https://candle-gosling-511.notion.site/Databricks-Bootcamp-2e734b251f1280208697c641df833373?p=2e734b251f1280ab8dadc269e033cc38&pm=s



