from pyspark import pipelines as dp
from dataclasses import dataclass, field
from functools import partial
from pyspark.sql import functions as F, DataFrame
import logging 


"""
This file describes the class for silver tables.
To create an object, one must pass the user definitions and make sure the table information is populated in the following table: pipeline_column_names, pipeline_transformations, and pipeline_output
The pipeline_transformations table carries the function names in string form that are translated in the OPS
Run triggers reading, renaming, transforming, and selecting the output tables
"""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SilverTable:
    # user definitions 
    pipeline_id: int
    medallion: str
    table_name: str
    spark: object
    df: object
    year: int    = None

    # populated by _load_config
    column_names: dict   = field(default_factory=dict,  repr=False)
    transformations: list   = field(default_factory=list,  repr=False)
    output_cols: list   = field(default_factory=list,  repr=False)
    value_maps: dict   = field(default_factory=dict,  repr=False)


    # ------------- named constructor --------------
    @classmethod
    def from_catalog(cls, pipeline_id: int, medallion: str, table_name: str, spark, df, year: int = None):
        instance = cls(
            pipeline_id = pipeline_id,
            medallion = medallion,
            table_name = table_name,
            spark = spark,
            df = df,
            year = year,
        )
        instance._load_config()
        return instance

    """
    Transformations used on self.df
    Each function returns self for chaining
    """
    def lower_and_trim(self, cols:list, **_) -> "SilverTable":
        for column in cols:
            self.df = self.df.select(*[F.trim(F.lower(F.col(c))).alias(c) for c in cols])
        return self

    def fill_null(self, cols: list, val: str, **_) -> "SilverTable":
        logger.info("fill_null called [57]")
        self.df = self.df.fillna(val, subset=cols)
        return self

    def replace_val(self, cols: list, val: str, extra: dict, **_) -> "SilverTable":
        logger.info("replace_val called [62]")
        self.df = self.df.replace({val: extra["replacement"]}, subset=cols)
        return self

    def cast_col(self, cols: list, extra: dict, **_) -> "SilverTable":
        logger.info("cast_col called [67]")
        def _cast(c):
            if extra.get("format") and c in cols: # format option only exists for dates
                return F.to_date(F.col(c), extra["format"]).alias(c)
            elif c in cols:
                return F.col(c).cast(extra["type"]).alias(c)
            else:
                return F.col(c)

        self.df = self.df.select([_cast(c) for c in self.df.columns])
        return self

    def regex_replace(self, cols: list, val: str, extra: dict, **_) -> "SilverTable":
        logger.info("regex replace called [80]")
        self.df = self.df.select([
            F.regexp_replace(F.col(c), extra["pattern"], val).alias(c)
            if c in cols
            else F.col(c)
            for c in self.df.columns
        ])
        return self

    def filter(self, cols: list, val: str, extra: dict, **_) -> "SilverTable":
        logger.info("filter called [90]")
        func = extra["func"]
        col  = F.col(cols[0])
        dispatch = {
            "<=":        lambda: col <= float(val),
            ">=":        lambda: col >= float(val),
            "==":        lambda: col == val,
            "rlike":     lambda: col.rlike(val),
            "isin":      lambda: col.isin(val.split("|")),
            "isnotnull": lambda: col.isNotNull(),
        }
        self.df = self.df.filter(dispatch[func]())
        return self

    def groupby_agg(self, cols: list, extra: dict, **_) -> "SilverTable":
        logger.info("groupby called [102]")
        self.df = (
            self.df
            .groupBy(cols)
            .agg(*[F.expr(e.strip()) for e in extra["agg_exprs"].split("|")])
        )
        return self

    def value_map(self, cols: list, extra: dict, **_) -> "SilverTable":
        logger.info("value map called 111")
        for col in cols:
            self.df = self.df.replace(extra, subset=[col])

        return self

    """
    OPS REGISTRY
    maps op name to a method
    defined as @property so self is always the current instance
    """

    @property
    def OPS(self) -> dict:
        return {
            "trim_and_lower":self.lower_and_trim,
            "fill_null":     self.fill_null,
            "replace_val":   self.replace_val,
            "cast_col":      self.cast_col,
            "regex_replace": self.regex_replace,
            "filter":        self.filter,
            "groupby_agg":   self.groupby_agg,
            "value_map":     self.value_map,
        }


    """
    Config loader
    Extracts the information from tables:
    """

    def _load_config(self):
        pid, med, tbl = self.pipeline_id, self.medallion, self.table_name
        year_filter   = f"AND table_year = {self.year}" if self.year else "AND table_year IS NULL"
        base_filter   = f"pipeline_id = {pid} AND medallion = '{med}' AND table_name = '{tbl}'"
        base_filter_with_year   = f"pipeline_id = {pid} AND medallion = '{med}' AND table_name = '{tbl}' {year_filter}"
        logger.info(f"_load_config called with {base_filter} [145]")

        # column renaming map 
        self.column_names = {
            r["source_col"]: r["target_col"]
            for r in (
                self.spark.read.table("harris_county_catalog.config.pipeline_column_names")
                .filter(base_filter)
                .collect()
            )
        }

        self.value_maps = {}
        for r in self.spark.read.table("harris_county_catalog.config.pipeline_value_maps").filter(base_filter).collect():
            if r["col"] not in self.value_maps:
                self.value_maps[r["col"]] = {}
            self.value_maps[r["col"]][r["source_val"]] = r["target_val"]

        # transformation steps - build partials bound to each method
        steps = (
            self.spark.read.table("harris_county_catalog.config.pipeline_transformations")
            .filter(base_filter)
            # .filter(F.col("step_order")<=3)
            .orderBy("step_order")
            .collect()
        )

        self.transformations = [
            partial(
                self.OPS[r["op"]],
                cols  = list(r["cols"])  if r["cols"]  else [],
                val   = r["val"],
                extra = dict(r["extra"]) if r["extra"] else {},
            )
            for r in steps
        ]

        # output column selection
        self.output_cols = [
            r["col"]
            for r in (
                self.spark.read.table("harris_county_catalog.config.pipeline_output")
                .filter(base_filter)
                .orderBy("col_order")
                .collect()
            )
        ]


    """
    Defines execution
    """

    def run(self) -> DataFrame:
        logger.info("run called [209]")

        self.df = self._rename_cols(self.df)

        for t in self.transformations:
            t()

        if self.output_cols:
            self.df = self.df.select(self.output_cols)

        return self.df


    """
    Helper functions
    """

    def _rename_cols(self, df: DataFrame) -> DataFrame:
        return df.toDF(*[self.column_names.get(c, c) for c in df.columns])

    def preview(self, n: int = 20) -> "SilverTable":
        if self.df is not None:
            self.df.show(n, truncate=False)
        return self