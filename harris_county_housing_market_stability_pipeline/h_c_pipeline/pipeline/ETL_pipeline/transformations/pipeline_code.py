"""
This file computes all steps from bronze to gold. 
Requirements: Needs bootstrap.py and read_zip.py to be ran beforehand

It will transform all files from the specified years (this is a string inserted into pipeline configs as {years: "20xx, 20xx,..."})
It process each year independently, transforming the owners and property table and saving them into silver schema
The file with zip_code information is loaded and transformed and the appropriate joins are made per year and unioned into one table
An SCD type 2 table is formed from the unioned table
The gold tables are created from the scd type 2 table
"""


from functools import reduce

from silver_table_class import SilverTable
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import struct
from pyspark.sql import Window

years_raw = spark.conf.get("years", "2025") 
years     = [y.strip() for y in years_raw.split(",")]

raw_path      = "/Volumes/harris_county_catalog/raw_data/"
bronze_schema = "harris_county_catalog.bronze"
silver_schema = "harris_county_catalog.silver"

_schema_base     = "/Volumes/harris_county_catalog/etl/landing/schema"
_checkpoint_base = "/Volumes/harris_county_catalog/etl/landing/checkpoint"

tables = {
    "property": {"raw_path": f"{raw_path}property/"},
    "owners":   {"raw_path": f"{raw_path}owners/"},
}

dp.create_streaming_table(name="harris_county_catalog.gold.property_scd2")

for year in years:

    @dp.table(
        name=f"{bronze_schema}.owners_{year}",
        comment=f"Bronze owners data for {year}"
    )
    def owners_bronze(y=year):
        return (
            spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.schemaLocation", f"{_schema_base}/bronze/owners/{y}/")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("pathGlobFilter", f"*{y}*.txt")
                .option("mergeSchema", "true")
                .option("rescuedDataColumn", "_rescued_data")
                .option("header", "true")
                .option("delimiter", "\t")
                .load(tables["owners"]["raw_path"])
        )

    @dp.table(
        name=f"{bronze_schema}.property_{year}",
        comment=f"Bronze property data for {year}"
    )
    def property_bronze(y=year):
        return (
            spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.schemaLocation", f"{_schema_base}/bronze/property/{y}/")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("pathGlobFilter", f"*{y}*.txt")
                .option("mergeSchema", "true")
                .option("rescuedDataColumn", "_rescued_data")
                .option("header", "true")
                .option("delimiter", "\t")
                .load(tables["property"]["raw_path"])
        )

    @dp.materialized_view(
        name=f"{silver_schema}.owners_{year}",
        comment=f"Silver owners data for {year}"
    )
    def silver_owners(y=year):
        df = spark.read.table(f"{bronze_schema}.owners_{y}")
        table = SilverTable.from_catalog(1, "silver", "owners", spark, df, int(y))
        table.df = df
        return table.run()

    @dp.materialized_view(
        name=f"{silver_schema}.property_{year}",
        comment=f"Silver property data for {year}"
    )
    def silver_property(y=year):
        df = spark.read.table(f"{bronze_schema}.property_{y}")
        table = SilverTable.from_catalog(3, "silver", "property", spark, df, int(y))
        table.df = df
        return table.run()

@dp.materialized_view(name=f"{silver_schema}.zip", comment="Zip reference data")
def bronze_to_silver_zip():
    df = spark.read.table(f"{bronze_schema}.zip")
    table = SilverTable.from_catalog(2, "silver", "zip", spark, df)
    table.df = df
    return table.run()

@dp.materialized_view(
    name=f"{silver_schema}.joined_union",
    comment="All years unioned — single source for CDC flow"
)
def joined_union():
    zip_df = spark.read.table(f"{silver_schema}.zip")
    frames = []

    for year in years:
        property_df = spark.read.table(f"{silver_schema}.property_{year}")
        owners_df   = spark.read.table(f"{silver_schema}.owners_{year}")

        property_zip = zip_df.alias("z").join(
            property_df.alias("p"),
            F.col("z.dim_zip_code") == F.col("p.dim_zip_code"),
            how="left"
        ).withColumn("dim_zip",          F.coalesce(F.col("z.dim_zip_code"), F.lit("00000"))) \
         .withColumn("dim_property_city", F.coalesce(F.col("z.dim_city"),     F.lit("unknown")))

        joined = property_zip.alias("pz").join(
            owners_df.alias("o"),
            F.col("pz.dim_account_number") == F.col("o.dim_account_number"),
            how="left"
        ).select(
            "pz.dim_account_number", "pz.dim_zip", "pz.dim_property_city",
            "pz.dim_street", "pz.m_building_area", "pz.m_land_area",
            "pz.m_total_market_value", "o.dim_name_list",
            "pz.dim_state_class", "pz.dim_year_date"
        ).where(F.col("m_total_market_value") != 0) \
         .withColumn(
            "quartile",
            F.when(F.col("m_total_market_value") < 41401 * 4,  "<25%")
             .when(F.col("m_total_market_value") < 83592 * 4,  "25-50%")
             .when(F.col("m_total_market_value") < 153000 * 4, "50-75%")
             .otherwise("75%>")
        )
        frames.append(joined)

    return reduce(lambda a, b: a.unionAll(b), frames)

dp.create_auto_cdc_flow(
    target                    = "harris_county_catalog.gold.property_scd2",
    source                    = f"{silver_schema}.joined_union",
    keys                      = ["dim_account_number"],
    stored_as_scd_type        = 2,
    track_history_column_list = ["quartile"],
    sequence_by               = F.col("dim_year_date")
)


# ----- GOLD -------

@dp.materialized_view(
    name = "harris_county_catalog.gold.properties_in_quartile"
)
def properties_in_quartile():
    df = spark.read.table('harris_county_catalog.gold.property_scd2')
    df_properties_in_quartile = df.groupBy('quartile').agg(F.count("*").alias('amount_of_properties_in_quartile'))
    return df_properties_in_quartile


@dp.materialized_view(
    name = "harris_county_catalog.gold.stability_categorization"
)
def stability_categorization():
    df = spark.read.table("harris_county_catalog.gold.property_scd2")

    df = df.withColumn("interval", (F.col("__END_AT") - F.col("__START_AT")).cast("int"))

    df = df.filter(F.col("__END_AT") == "2025-01-01") \
        .withColumn(
            "stability_categorization",
            F.when(F.col("interval") > 2920, "Very Very Stable Price")
            .when(F.col("interval") > 1460, "Very Stable Price")
            .when(F.col("interval") > 730,  "Stable Price")
            .otherwise("Variable Price")
        )

    result = df.groupBy("stability_categorization") \
        .agg(
            F.count(F.when(F.col("quartile") == "<25%",   True)).alias("q_0_25"),
            F.count(F.when(F.col("quartile") == "25-50%", True)).alias("q_25_50"),
            F.count(F.when(F.col("quartile") == "50-75%", True)).alias("q_50_75"),
            F.count(F.when(F.col("quartile") == "75%>",   True)).alias("q_75_100"),
        )

    stability_order = F.when(F.col("stability_categorization") == "Very Very Stable Price", 1) \
                    .when(F.col("stability_categorization") == "Very Stable Price",       2) \
                    .when(F.col("stability_categorization") == "Stable Price",            3) \
                    .when(F.col("stability_categorization") == "Variable Price",          4) \
                    .otherwise(5)

    result = result.orderBy(stability_order)
    
    return result

@dp.materialized_view(
    name = "harris_county_catalog.gold.amount_of_quartile_changes"
)
def amount_of_quartile_changes():
    df = spark.read.table("harris_county_catalog.gold.property_scd2")

    window_spec = Window.partitionBy("dim_account_number").orderBy("__END_AT")

    df = df.withColumn("changes_in_quartiles", F.row_number().over(window_spec))

    df = df.filter(F.col("__END_AT") == "2025-01-01") \
        .withColumn("quartile_changes", F.col("changes_in_quartiles") - 1)

    result = df.groupBy("quartile_changes") \
        .agg(
            F.count(F.when(F.col("quartile") == "<25%",   True)).alias("q_0_25"),
            F.count(F.when(F.col("quartile") == "25-50%", True)).alias("q_25_50"),
            F.count(F.when(F.col("quartile") == "50-75%", True)).alias("q_50_75"),
            F.count(F.when(F.col("quartile") == "75%>",   True)).alias("q_75_100"),
        ) \
        .orderBy("quartile_changes")

    return result




