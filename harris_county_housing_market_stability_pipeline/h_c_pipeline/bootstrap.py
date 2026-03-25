"""
harris_county/bootstrap.py
--------------------------
Idempotent bootstrap for harris_county_catalog.
 
Execution order (enforced by DAB jobs.yml):
    1. bootstrap.py (catalog, schema, tables, seed data)
    2. scraper.py (land raw data)
    3. SDP pipeline (bronze → silver → gold)
 
Run locally (Databricks Connect configured):
    python -m harris_county.bootstrap
"""

import logging
import os
 
from pyspark.sql import SparkSession
 
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)
 

# Config — swap via env vars for dev / staging

CATALOG = os.getenv("HARRIS_CATALOG", "harris_county_catalog")
SCHEMA  = os.getenv("HARRIS_SCHEMA",  "config")
FQN     = f"{CATALOG}.{SCHEMA}"
 
 

# Helpers functions

def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()
 
 
def sql(spark: SparkSession, stmt: str) -> None:
    spark.sql(stmt)
 
 
def section(title: str) -> None:
    log.info("\n%s\n  %s\n%s", "─" * 60, title, "─" * 60)
 
 
def seed(spark: SparkSession, table: str, scope: str, insert_body: str) -> None:
    """Delete rows matching `scope` then re-insert — safe to re-run."""
    fqn = f"{FQN}.{table}"
    sql(spark, f"DELETE FROM {fqn} WHERE {scope}")
    sql(spark, f"INSERT INTO {fqn} {insert_body}")
 

# 1. Catalog & Schema

def create_catalog_and_schema(spark: SparkSession) -> None:
    section("Catalog & Schema")
    sql(spark, f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    log.info("catalog: %s", CATALOG)
    sql(spark, f"CREATE SCHEMA IF NOT EXISTS {FQN}")
    log.info("schema: %s", FQN)
 
 
# 2. Tables (DDL)
TABLES: dict[str, str] = {
    "bronze_ingestion_config": f"""
        CREATE TABLE IF NOT EXISTS {FQN}.bronze_ingestion_config (
            source_path         STRING,
            target_table        STRING,
            source_format       STRING,
            cloud_files_options MAP<STRING, STRING>,
            target_catalog      STRING,
            target_schema       STRING,
            load_type           STRING,
            table_year          DATE
        )
    """,
 
    "pipeline_column_names": f"""
        CREATE TABLE IF NOT EXISTS {FQN}.pipeline_column_names (
            pipeline_id  INT,
            medallion    STRING,
            table_name   STRING,
            source_col   STRING,
            target_col   STRING
        )
    """,
 
    "pipeline_transformations": f"""
        CREATE TABLE IF NOT EXISTS {FQN}.pipeline_transformations (
            pipeline_id  STRING,
            medallion    STRING,
            table_name   STRING,
            step_order   INT,
            op           STRING,
            cols         ARRAY<STRING>,
            val          STRING,
            extra        MAP<STRING, STRING>
        )
    """,
 
    "pipeline_output": f"""
        CREATE TABLE IF NOT EXISTS {FQN}.pipeline_output (
            pipeline_id  INT,
            medallion    STRING,
            table_name   STRING,
            col_order    INT,
            col          STRING
        )
    """,
 
    "pipeline_value_maps": f"""
        CREATE TABLE IF NOT EXISTS {FQN}.pipeline_value_maps (
            pipeline_id  INT,
            medallion    STRING,
            table_name   STRING,
            col          STRING,
            source_val   STRING,
            target_val   STRING
        )
    """,
}
 
 
def create_tables(spark: SparkSession) -> None:
    section("Tables (DDL)")
    for name, ddl in TABLES.items():
        sql(spark, ddl)
        log.info("%s", name)
 
 
# 3. Seed: pipeline_column_names
def seed_pipeline_column_names(spark: SparkSession) -> None:
    section("Seed → pipeline_column_names")
 
    COLS = "(pipeline_id, medallion, table_name, source_col, target_col)"
 
    seed(spark, "pipeline_column_names",
         "pipeline_id = 1 AND table_name = 'owners'",
         f"""{COLS} VALUES
           (1, 'silver', 'owners', 'acct', 'dim_account_number'),
           (1, 'silver', 'owners', 'ln_num', 'dim_owner_number'),
           (1, 'silver', 'owners', 'name', 'dim_owner_name'),
           (1, 'silver', 'owners', 'aka', 'dim_owner_alias'),
           (1, 'silver', 'owners', 'pct_own', 'm_percent_ownership')
         """)
 
    seed(spark, "pipeline_column_names",
         "pipeline_id = 2 AND table_name = 'zip'",
         f"""{COLS} VALUES
           (2, 'silver', 'zip', 'ZIP Code', 'dim_zip_code'),
           (2, 'silver', 'zip', 'Classification', 'dim_classification'),
           (2, 'silver', 'zip', 'City', 'dim_city'),
           (2, 'silver', 'zip', 'Population', 'm_population'),
           (2, 'silver', 'zip', '% of Population', 'm_percent_of_population')
         """)
 
    seed(spark, "pipeline_column_names",
         "pipeline_id = 3 AND table_name = 'property'",
         f"""{COLS} VALUES
           (3, 'silver', 'property', 'acct', 'dim_account_number'),
           (3, 'silver', 'property', 'site_addr_1', 'dim_street'),
           (3, 'silver', 'property', 'site_addr_2', 'dim_city'),
           (3, 'silver', 'property', 'site_addr_3', 'dim_zip_code'),
           (3, 'silver', 'property', 'state_class', 'dim_state_class'),
           (3, 'silver', 'property', 'bldr_ar', 'm_building_area'),
           (3, 'silver', 'property', 'land_ar', 'm_land_area'),
           (3, 'silver', 'property', 'tot_mkt_val', 'm_total_market_value'),
           (3, 'silver', 'property', 'yr', 'dim_year_date')
         """)
 
 
# 4. Seed: pipeline_transformations
def seed_pipeline_transformations(spark: SparkSession) -> None:
    section("Seed → pipeline_transformations")
 
    COLS = "(pipeline_id, medallion, table_name, step_order, op, cols, val, extra)"
 
    # pipeline 1 - owners
    seed(spark, "pipeline_transformations",
         "pipeline_id = '1' AND table_name = 'owners'",
         f"""{COLS} VALUES
           ('1', 'silver', 'owners', 1, 'fill_null',
               ARRAY('dim_owner_name'), '', MAP()),
           ('1', 'silver', 'owners', 2, 'fill_null',
               ARRAY('m_percent_ownership'), '0', MAP()),
           ('1', 'silver', 'owners', 3, 'cast_col',
               ARRAY('dim_account_number', 'dim_owner_name', 'm_percent_ownership'),
               NULL, MAP('type', 'string')),
           ('1', 'silver', 'owners', 4, 'filter',
               ARRAY('m_percent_ownership'), '1.1', MAP('func', '<=')),
           ('1', 'silver', 'owners', 5, 'filter',
               ARRAY('m_percent_ownership'), '0.9', MAP('func', '>=')),
           ('1', 'silver', 'owners', 6, 'groupby_agg',
               ARRAY('dim_account_number'), NULL,
               MAP('agg_exprs',
                   'collect_set(dim_owner_name) AS dim_name_list|round(sum(m_percent_ownership), 2) AS m_total_ownership_percentage'))
         """)
 
    # pipeline 2 - zip
    seed(spark, "pipeline_transformations",
         "pipeline_id = '2' AND table_name = 'zip'",
         f"""{COLS} VALUES
           ('2', 'silver', 'zip', 1, 'regex_replace',
               ARRAY('dim_classification'), '_', MAP('pattern', '[\\\\s]+')),
           ('2', 'silver', 'zip', 2, 'cast_col',
               ARRAY('m_population', 'm_percent_of_population'),
               NULL, MAP('type', 'DOUBLE')),
           ('2', 'silver', 'zip', 3, 'filter',
               ARRAY('dim_zip_code'), '^[0-9]{{5}}$', MAP('func', 'rlike'))
         """)
 
    # pipeline 3 — property
    seed(spark, "pipeline_transformations",
         "pipeline_id = '3' AND table_name = 'property'",
         f"""{COLS} VALUES
           ('3', 'silver', 'property',  1, 'fill_null',
               ARRAY('acct', 'site_addr_1', 'site_addr_2', 'state_class'),
               'unknown', MAP()),
           ('3', 'silver', 'property',  2, 'fill_null',
               ARRAY('bld_ar', 'land_ar', 'tot_mkt_val'), '0', MAP()),
           ('3', 'silver', 'property',  3, 'fill_null',
               ARRAY('site_addr_3'), '00000', MAP()),
           ('3', 'silver', 'property',  4, 'cast_col',
               ARRAY('dim_account_number', 'dim_street', 'dim_city', 'dim_zip_code', 'dim_state_class'),
               NULL, MAP('type', 'string')),
           ('3', 'silver', 'property',  5, 'cast_col',
               ARRAY('m_building_area', 'm_land_area', 'm_total_market_value'),
               NULL, MAP('type', 'bigint')),
           ('3', 'silver', 'property',  6, 'cast_col',
               ARRAY('dim_year_date'),
               NULL, MAP('type', 'date', 'format', 'yyyy')),
           ('3', 'silver', 'property',  7, 'replace_val',
               ARRAY('dim_account_number', 'dim_street', 'dim_city', 'dim_state_class'),
               NULL, MAP('replacement', 'unknown')),
           ('3', 'silver', 'property',  8, 'replace_val',
               ARRAY('dim_zip_code'), NULL, MAP('replacement', '00000')),
           ('3', 'silver', 'property',  9, 'filter',
               ARRAY('dim_state_class'), NULL,
               MAP('func', 'isin', 'vals', 'single_family_residential|mobile_home')),
           ('3', 'silver', 'property', 10, 'filter',
               ARRAY('dim_account_number'), NULL, MAP('func', 'isnotnull')),
           ('3', 'silver', 'property', 11, 'filter',
               ARRAY('dim_zip_code'), '^[0-9]{{5}}$', MAP('func', 'rlike')),
           ('3', 'silver', 'property', 12, 'regex_replace',
               ARRAY('dim_street'), '_', MAP('pattern', ' ')),
           ('3', 'silver', 'property', 13, 'value_map',
               ARRAY('dim_state_class'), NULL, MAP())
         """)
 
 
# 5. Seed: pipeline_output
def seed_pipeline_output(spark: SparkSession) -> None:
    section("Seed → pipeline_output")
 
    COLS = "(pipeline_id, medallion, table_name, col_order, col)"
 
    seed(spark, "pipeline_output",
         "pipeline_id = 1 AND table_name = 'owners'",
         f"""{COLS} VALUES
           (1, 'silver', 'owners', 1, 'dim_account_number'),
           (1, 'silver', 'owners', 2, 'dim_name_list')
         """)
 
    seed(spark, "pipeline_output",
         "pipeline_id = 2 AND table_name = 'zip'",
         f"""{COLS} VALUES
           (2, 'silver', 'zip', 1, 'dim_zip_code'),
           (2, 'silver', 'zip', 2, 'dim_classification'),
           (2, 'silver', 'zip', 3, 'dim_city'),
           (2, 'silver', 'zip', 4, 'm_population'),
           (2, 'silver', 'zip', 5, 'm_percent_of_population')
         """)
 
    seed(spark, "pipeline_output",
         "pipeline_id = 3 AND table_name = 'property'",
         f"""{COLS} VALUES
           (3, 'silver', 'property', 1, 'dim_account_number'),
           (3, 'silver', 'property', 2, 'dim_street'),
           (3, 'silver', 'property', 3, 'dim_city'),
           (3, 'silver', 'property', 4, 'dim_zip_code'),
           (3, 'silver', 'property', 5, 'dim_state_class'),
           (3, 'silver', 'property', 6, 'm_building_area'),
           (3, 'silver', 'property', 7, 'm_land_area'),
           (3, 'silver', 'property', 8, 'm_total_market_value'),
           (3, 'silver', 'property', 9, 'dim_year_date')
         """)
 
# 6. Seed: pipeline_value_maps
def seed_pipeline_value_maps(spark: SparkSession) -> None:
    section("Seed → pipeline_value_maps")
 
    COLS = "(pipeline_id, medallion, table_name, col, source_val, target_val)"
 
    seed(spark, "pipeline_value_maps",
         "pipeline_id = 3 AND table_name = 'property' AND col = 'dim_state_class'",
         f"""{COLS} VALUES
           (3, 'silver', 'property', 'dim_state_class', 'A1', 'single_family_residential'),
           (3, 'silver', 'property', 'dim_state_class', 'A2', 'mobile_home')
         """)
    
# Entrypoint

def main() -> None:
    spark = get_spark()
 
    log.info("\n%s", "=" * 60)
    log.info("Harris County Catalog - Bootstrap")
    log.info("Catalog : %s", CATALOG)
    log.info("Schema  : %s", SCHEMA)
    log.info("%s", "=" * 60)
 
    create_catalog_and_schema(spark)
    create_tables(spark)
    seed_pipeline_column_names(spark)
    seed_pipeline_transformations(spark)
    seed_pipeline_output(spark)
    seed_pipeline_value_maps(spark)
 
    section("Bootstrap complete")
    log.info("All objects created and seed data loaded.\n")
 
 
if __name__ == "__main__":
    main()