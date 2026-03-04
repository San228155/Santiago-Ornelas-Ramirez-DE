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
    - Expects the incoming table name
    - Expects file type, only csv is enabled
    - Specifies bronze table name
    - Specifies if overwrite is allowed
  - Silver
    - Expects bronze table name
    - Expects configurations to execute correct transformations
    - Expects all columns from incoming table to be specified
    - Specifies name of final silver table
    - Specifies name of columns in final silver table
  - Gold
    - Expects silver table name
    - Expects columns desired in gold table
    - Specifies gold table name
    - Specifies join conditions necessary for aggregation
  
**Bronze:**  
- Ingest all copy without transformations. Only change the incoming file type to delta table.
- Schema evolution is allowed as the incoming table is always overwritten, unless otherwise specified
- All columns are ingested as strings

**Silver**  
- All values are set to be trimmed and lowered
- If a column from bronze is not specified in the silver, the program will stop 
- Columns are renamed
- nulls and empty strings are changed to a specified value depending on the datatype of column
- hyphones and spaces are removed and replaced by underscore (if multiple of these are together, they are replaced by a single hyphon)
- All columns that should not be strings are casted to the appropriate data type
- Specified columns are augmented when possible
- Intelligent keys are separated and surrogate keys are applied for gold transformation
- All tables are put into second normal form
**Gold**  
- Dimension and fact tables are aggregated with predefiend columns
- Surrogate keys and columns extracted from intelligent keys that were not part of the primary key are dropped.

Output & Data Contracts 
Final, analytics-ready tables are published in the Gold layer

Downstream consumers should rely on Gold tables only

The intended customer are Data Analysts as the final product are tables with simple data structures, ideal to use for further visualization

All required table creation is managed internally as to allow visibility of the data throught the project. Any changes should be made to the transformation metadata. If a table name or column name is changed, it will need to be changed in each individual metadata table where the name is present. 

Monitoring & Troubleshooting
Execution status and logs are available in Databricks Job Runs

Failed tasks prevent downstream execution

Individual drivers may be rerun independently if remediation is required


**Technology used**
Databricks
Pyspark
Sql
Beautiful Soup

#Notes
The data set and project was designed after Data with Barra 's Databricks Bootcamp. Link below:
https://candle-gosling-511.notion.site/Databricks-Bootcamp-2e734b251f1280208697c641df833373?p=2e734b251f1280ab8dadc269e033cc38&pm=s





