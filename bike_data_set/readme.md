Bike Data Set Star Schema Project
Client Background
Company X has asked us to organize their sales data for their data analysts. They want a Star Schema data model for fast queries and ease of understanding.

We provide an pipeline for Company X using the medallion architecture and the Kimball approach. We emphasize providing clean, visible, and correct pipelines to assist on providing key insight and recommendations to the business.

Target Metrics
Provide a data model fitting data quality needs and imporoving computational and storage efficiency:
- Provided a data model using the Kimball approach. Reduced 6 tables into 1 fact table and 2 dimension tables
- Provided data with guaranteed data quality
- Provided gold table data with indexed columns to ensure fast queries [on primary keys and foreign keys]
- Used the Star Schema to reduce file size by 4

Summary
Executive summary:
- Provided data with improved query speed and reduced file size (by 4)

Customer summary (analyst):
Installed a Star Schema data model with 3 tables, 1 fact and 2 dimension tables. Each table has an indexed primary key and the fact table has 2 indexed foreign keys for fast query performance. 

This pipeline provides data that without spaces and lower, as well as correct data types, adjustable meta data, and guaranteed 2nd NF configuration. 

**How to Use**

This projects requires the user create a Databricks Repo in Databricks and paste the Santiago-Ornelas-Ramirez-De repo. In this repo, one will find a databricks.yml file that needs to be deployed either through Databricks Cli or through the UI which will create a job in Databricks jobs. One needs to run the job and the transformations will be processed and the tables will be automatically created. 

One needs to create a **catalog** with name: bike_data_lakehouse and **schema**: raw_data and two volumes with names: **source_crm**, **source_erp**. The necessary data can be found here https://github.com/DataWithBaraa/databricks_bootcamp_2026/tree/main/datasets/engineering. There are two folders with names corresponding to the two volumes created, upload only the files to the corresponding folder.

**Structure & Execution**
All the transformations are executed through Databricks Jobs. 

The job is created through the deployment of a Databricks Asset Bundle. The job calls python files extracted from a prebuilt python wheel. All future commits to the main branch of the repo automatically get put through a CI/CD process in which the wheel is rebuilt to accomodate any changess.

This project is constructed with a Medallion Architecture in mind, hence it is devided into Bronze, Silver, and Gold layers.
Each layer gets executed by a python file called the driver each executing a single runner which groups functions from transformation files. 

All tables have meta data specified in the configs folder, one file for each medallion step. This file defines the file names, types, column names, types, needed transformations, etc.

Driver:
File called on by the job. It orchestrates a table by table transformation for a secific layer. 

Runner:
File called by the driver. It organizes the order in which the transformation functions must be called to transform a table for each layer.

Transformation files:
These are files in which modularized python functions live. These files are responsible for defining the logic behind each transformation function.

Table Metadata:
Specify important information needed for each step in the medallion architecture.

**Medallion Architecture Layer Responsabilities**
- Table Metadata  
  - Bronze

**Bronze:**  
Ingest all copy without transformations. Only change the incoming file type to delta table.
Schema evolution is allowed as the incoming table is always overwritten, unless otherwise specified

**Silver**  
Clean all incoming data from bronze table
All values are set to be trimmed and lowered
If a column from bronze is not specified in the silver
No dupicates or nulls in primary keys
Minimum volume of valid data (specifics in tests in design spec)
No values out of range (specifics in tests in design spec)
Schema enforcment
All columns of schema must exist
Columns must comply with predetermined data type
Business rules are not enforced
**Gold**  
Business rules enforced
Aggregate tables
SCD
types of other tables (update)
How to run this project
In Databricks, go to Repos → Add Repo → paste this GitHub URL.
Open Jobs → Create Job → Import → select jobs.json (found in jobs folder in this repo)
The job will automatically reference the notebooks inside the repo
This file uses a for_each_task for loop with parameters [2017,2018,...,2025]. These parameters are already included inside the jobs/jobs.json file for all 4 "for each" loops.

Output & Data Contracts (update)
Final, analytics-ready tables are published in the Gold layer

Table structures, relationships, and metrics are defined in the Design Specification

Downstream consumers should rely on Gold tables only

The intended customer are Data Analysts as the final product are tables with simple data structures, ideal to use for further visualization

All required table creation is managed internally as to allow visibility of the data throught the project

Monitoring & Troubleshooting
Execution status and logs are available in Databricks Job Runs

Failed tasks prevent downstream execution

Individual notebooks may be rerun independently if remediation is required

All runs are logged in three different ways

Pipeline runs - Logs the execution of all notebooks, independently, most importantly noting if the execution was succesful or not, in which case also noting the error
Data quality runs- Logs the execution of unit tests for a notebook, independently, noting if the test was successful or not, including the error. This registers the most important data quality checks and will shut down the notebook and pipeline if it finds any
Metrics - Logs metrics accumulated thorught the process of the pipeline. This does not shut down a notebook or pipeline and all metric descriptions are given in the design spec.
Technology
Databricks
Pyspark
Sql
Beautiful Soup
#Notes

1. Harris County has a population of 5 million, the target percentile has a quarter of that population and the average household in the county is 2.5. Hence we have 5,000,000/(4*2.5) = 500,000
2. a1 or a2 is the naming convention given in directly from the property information from Harris County
3. https://hcad.org/pdata/pdata-property-downloads.html
