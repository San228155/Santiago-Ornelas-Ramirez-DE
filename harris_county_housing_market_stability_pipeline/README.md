# Housing Market Stability Analysis Pipeline in Harris County from 2017 to 2025

## Client Background
Harris County has asked us to explore the stability of the Housing Market in Harris County from 2017 to 2025, specifically for the population between the 25th and 50th percentile of earners. Harris County wants to ensure that this population has adequeate access to housing and that market conditions remain stable enough to prevent ongoing affordability challenges.

Harris County has determined they need 500 thousand houses for this population<sup><a href="#note1">1</a></sup>. The County has determined that an affordable house is 4 times the yearly salary of a household. We look only at houses registerd to be single family residentials ("a1" category<sup><a href="#note2">2</a></sup>) or mobile homes ("a2" category), we do not count multi-family residentials ("b1") for this analysis

We provide an pipeline from Harris County's Property data <sup><a href="#note2">3</a></sup> using the medallion architecture. We emphasize providing reliable pipelines with clean data and visibility througt. The key insight and recommendations focus on teh follwoing areas.
### Target Metrics
- Supply of Houses vs Demand of Houses - Amount of availabe houses vs amount houses needed in **2025** for the target population
- Stability of the Market — We assess the stability category of the median home based on how long it has been since its most recent affordability‑bracket change. Stability categories are defined by whether properties have remained in the same 2025 affordability bracket over the past eight years, four years, and two years (counting backward from 2025), as well as by the number of properties that have shifted to a different bracket within the last two years.
- Number of times a property classified as affordable, for the target population, in 2025 has shifted to a different affordability bracket over the past eight years.


## Summary
### Executive summary
Metrics:
The 2025 analysis of the Harris County housing market indicates that housing supply is sufficient and pricing dynamics are stable for households in the second income quartile.

Housing availability exceeds expectations, with over 600,000 homes meeting eligibility criteria against a target of 500,000, signaling adequate inventory to support current population needs.

Price stability metrics further support this assessment. Homes in the second income quartile exhibit moderate price consistency, with a median of 4–8 years since the last bracket change. While the first quartile remains the most stable segment, the second quartile demonstrates meaningfully greater stability than higher-income segments, where prices shift more frequently.

Overall market volatility is limited, as the median number of bracket changes per home is one, indicating that most properties experience minimal reclassification over time.

Conclusion:
The Harris County housing market for second-quartile earners in 2025 is stable and well-supplied. Although not the most static segment of the market, it offers availability and price predictability, positioning suitable housing options for this income group.

## Project Decisions
We will explore the following decisions
### Data Model:
The OBT data model was chosen as we prioritize the analysis end goal and storage is not a main concern. Since we want the tables to be analyst-friendly, we want easily queryable tables that avoid joins. Storage is also not a concern as data duplication is not an issue, we use Parquet and columnar compression, and the tables are designed to not be wide.

### SCD Type 2:
We chose to make the source of truth an SCD Type 2 table as it is more storage efficient than storing a complete historical row for every record at every point in time. Our analysis also requires historical data where only changes in the dimensions matter, since we measure stability, otherwise thought of as how stagnant the data is, perfect for SCD Type 2.

### Orchestration:
We use a combination of a pipeline that lives inside a job. Pipelines give us access to declarative tables and easily configurable SCD Type 2 tables, as well as additional observability tooling such as DAGs that help us understand the functioning of the pipeline, particularly when the input data has over 21 tables that get reduced to a handful of output tables. We also use expectations to further add observability to our data. We use jobs in order to orchestrate the creation of our infrastructure, data fetching (which a pipeline should not do), and the execution of one or more pipelines.

### Tools Used in Each Pipeline Step:
We use three purposely different tools in each step of the medallion architecture:

Bronze: We use Autoloader as it is idempotent, assuming each file contains unique data. It also allows us to explicitly express our intent with schema evolution.
Silver: We use classes to define all transformations. This centralizes our maintainability to one class per table and gives us a consistent way to apply transformations, which reduces the effort required to understand what differs from one table to the next.
Gold: The tables are targeted to be analyst-friendly, hence we use SQL, more specifically PySpark SQL. Since these are more ad hoc requests, we want a flexible structure that can be easily modified without compromising other tables.

### Configuration-Driven Pipeline:
Pipelines should be as observable and understandable as possible. Using configuration tables provides a high degree of observability, as all configuration lives in queryable tables and any transformation should be abundantly clear. It also allows for quick, programmatic, and difficult-to-break update capacity.

### Idempotency:
Bronze tables (properties and owners) are fully idempotent, with the caveat that they use Autoloader.
Silver and Gold tables are managed by Databricks and therefore would not be recalculated unnecessarily. Most importantly, the SCD Type 2 table is not altered incorrectly, as Spark Declarative Pipelines use sequence_by to disregard conflicting or duplicate data.
The infrastructure uses CREATE [object] IF NOT EXISTS logic in its DDL.
The zip code bronze table uses overwrite logic to allow for updates and is safe to recalculate multiple times, as the table is very small.

## How to Use
This project implements an end-to-end Databricks pipeline that processes Harris County property data using a medallion architecture (Bronze, Silver, Gold). Pipeline execution is orchestrated via Databricks Jobs, with notebooks executed in a predefined order.

Detailed schemas, metrics, and pipeline flow diagrams are documented in the Design Specification, which serves as the authoritative reference for this project.

### Input Data Requirements

Raw source data is recuperated from two sources, the Harris County Property Data Website <sup><a href="#note2">3</a></sup> and from https://www.zip-codes.com/county/tx-harris.asp

Raw source data from the Harris County Property Data Website is expected to be delivered as uncompressed files (.txt files) to the configured landing location (this is the natural format from the Harris County Property Data Website)

Raw source data from www.zip-codes.com is recovered automatically through web scrapping

Raw files are treated as immutable inputs and are first registered in the Bronze layer

Note: Raw data files are not included in this repository. The Bronze layer defines the ingestion contract for all source data

### Structure & Execution

Deploying will:
- Build and upload the Python wheel from pyproject.toml using setuptools
- Upload all project files to your Databricks workspace
- Create the job and pipeline resources in Databricks automatically

Run the pipeline
- databricks bundle run harris_county_job
- Or go to your Databricks workspace → Workflows → harris_county_etl → Run now.

What happens when it runs
The job executes three tasks automatically in this order:
1. bootstrap    → creates the entire catalog structure and seeds all config data
2. scraper      → scrapes zip code reference data into the catalog
3. pipeline     → runs bronze → silver → gold transformations via DLT
Each task must succeed before the next one starts. Everything is created automatically — no manual setup in Databricks is needed.

Catalog Structure
Created automatically by the bootstrap task on first run.
harris_county_catalog/
│
├── config/                          ← pipeline configuration  
│   ├── bronze_ingestion_config  
│   ├── pipeline_column_names  
│   ├── pipeline_transformations  
│   ├── pipeline_output  
│   └── pipeline_value_maps  
│  
└── etl/                             ← pipeline output  
    ├── bronze/                      ← raw ingested data  
    ├── silver/                      ← cleaned and transformed  
    │   ├── owners  
    │   ├── zip  
    │   └── property  
    └── gold/                        ← analysis-ready data  

Notes

.env contains sensitive credentials — never commit it, it is in .gitignore
Bootstrap is safe to re-run — all operations are idempotent
To deploy to a different catalog, change BUNDLE_VAR_harris_catalog and HARRIS_CATALOG in .env

  Comments:
  - the files in raw_data are .txt files (with delimiter /t)
  - The files with an input {year} are parameters with range [2017,2018...,2025]. If this needs to be changed, the user must add the necessary data and change the inputs in resources/pipelines/h_c_pipeline.yml

Medallion Architecture Rules
- Bronze:
  - Ingest all copy without transformations. Only change the incoming file type to delta table
  - We do not allow shcema evolution. If a column is not in the previously specified schema, it is dropped
- Silver
  - Clean Data
    - No dupicates or nulls in primary keys
    - Minimum volume of valid data (specifics in tests in design spec)
    - No values out of range (specifics in tests in design spec)
  - Schema enforcment
    - All columns of schema must exist
    - Columns must comply with predetermined data type
  - Business rules are not enforced
- Gold
  - Business rules enforced
  - Aggregate tables
    - SCD
    - types of other tables (update)  


### How to run this project

1. In Databricks, go to Repos → Add Repo → paste this GitHub URL.


### Output & Data Contracts 

Final, analytics-ready tables are published in the Gold layer

Table structures, relationships, and metrics are defined in the Design Specification

Downstream consumers should rely on Gold tables only

The intended customer are Data Analysts as the final product are tables with simple data structures, ideal to use for further visualization

All required table creation is managed internally as to allow visibility of the data throught the project

### Monitoring & Troubleshooting

Execution status and logs are available in Databricks Job Runs

Failed tasks prevent downstream execution

Individual notebooks may be rerun independently if remediation is required

All runs are logged in three different ways
- Pipeline runs - Logs the execution of all notebooks, independently, most importantly noting if the execution was succesful or not, in which case also noting the error
- Data quality runs- Logs the execution of unit tests for a notebook, independently, noting if the test was successful or not, including the error. This registers the most important data quality checks and will shut down the notebook and pipeline if it finds any
- Metrics - Logs metrics accumulated thorught the process of the pipeline. This does not shut down a notebook or pipeline and all metric descriptions are given in the design spec.

## Technology
- Databricks
- Pyspark
- Sql
- Beautiful Soup

#Notes

<a id="note1"></a>1. Harris County has a population of 5 million, the target percentile has a quarter of that population and the average household in the county is 2.5. Hence we have 5,000,000/(4*2.5) = 500,000  
<a href="#note2"></a>2. a1 or a2 is the naming convention given in directly from the property information from Harris County  
<a href="#note2"></a>3. https://hcad.org/pdata/pdata-property-downloads.html
