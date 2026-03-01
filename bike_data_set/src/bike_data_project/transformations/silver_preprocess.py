"""
Definitions of function used in the cleaning part of the Silver Transformations for the pipeline: apply enums, aliasing, and validation, data augmentation, and intelligent key separation
The logic behind these functions can change overtime. For example, logic behind data augmentation
"""

from typing import Any
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql import DataFrame




def apply_enum_aliases(df: DataFrame, table_configs: dict[str, Any]) -> DataFrame:
    """
    Applies mappings to make abbreviations and aliases into a standard name
    Validates all values are in an acceptable list (enum)
    """

    exprs = []
    validation_conditions = []

    for col_key, col_val in table_configs["columns"].items():
        # we use a when condition chain to express the mappings
        # We do not use a join or broadcast join as the number of possible values is small < 20
        if "map" in col_val:
            when_condition_chain = None

            for desired_val, possible_vals in col_val["map"].items():
                cond = F.col(col_key).isin(possible_vals)
                when_condition_chain = F.when(cond, F.lit(desired_val)) if when_condition_chain is None else when_condition_chain.when(cond, F.lit(desired_val))

            normalized_col = when_condition_chain.otherwise(F.col(col_key))  

        else:
            normalized_col = F.col(col_key)

        exprs.append(normalized_col.alias(col_key))

        # optional check to ensure only allowed values are in the column
        if "validation" in col_val and "enum" in col_val["validation"]:
            valid_values = col_val["validation"]["enum"]
            
            validation_conditions.append(normalized_col.isin(valid_values))

    if validation_conditions:
        filter_condition = reduce(lambda x,y: x & y, validation_conditions)

    normalized_df = df.select(*exprs)

    normalized_df = normalized_df.withColumn(
        "is_valid",
        filter_condition
    )

    valid_df = normalized_df.filter(F.col("is_valid"))
    # quarantine_df = normalized_df.filter(~F.col("is_valid"))

    return valid_df

def data_augmentation(df: DataFrame, table_name:str, table_configs:dict[str, Any]):
    """
    Augments missing data from preexisting data. Only used for sales
    """

    expr_list = []
    if augmented_cols_list := table_configs.get("data_augmentation_columns", []):
        ops = {
            "+": lambda a, b: a + b,
            "-": lambda a, b: a - b,
            "*": lambda a, b: a * b,
            "/": lambda a, b: a / b,
            }

        for cols in df.columns:
            if cols in augmented_cols_list:
                operation = table_configs.get("columns").get(cols).get("expression")

                left  = F.col(operation[0])
                right = F.col(operation[2])
                op = operation[1]

                expr = F.when((left == 0) | (right == 0), F.col(cols))\
                    .otherwise(F.round(ops[op](left, right))).cast("int")
                
                expr_list.append(
                    F.when(
                        F.col(cols) == 0,
                        expr
                    ).otherwise(F.col(cols))\
                    .alias(cols)
                )
            else:
                expr_list.append(F.col(cols))
        return df.select(*expr_list)
    return df

def intelligent_key_preparation(df: DataFrame, table_name:str, table_configs:dict[str, Any]):
    """
    Separates intelligent keys
    """
    
    if table_configs.get("intelligent_key"):
        expr_list = []

        intelligent_key_mapping = table_configs.get("intelligent_key", {})

        derived_cols = intelligent_key_mapping.get("intelligent_key")["source"]

        untouched_cols = [
            F.col(col)
            for col in df.columns
            if col != derived_cols
        ]

        for new_column_name, column_values in intelligent_key_mapping.items():
            source_column = column_values["source"]
            start, length = column_values["substr"]

            expr_list.append(
                    F.substring(F.col(source_column), start, length).alias(new_column_name)
            )

        expr_list.extend(untouched_cols)
        
        int_key_removed_df = df.select(*expr_list)

        return int_key_removed_df
    else: 
        return df








