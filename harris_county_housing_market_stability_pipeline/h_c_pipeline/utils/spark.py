"""
Single point of entry for the SparkSession.
Databricks supplies a live session automatically; this wrapper
makes local / test usage explicit and mockable.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "harris_county_pipeline") -> SparkSession:
    """
    Return the active SparkSession.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )