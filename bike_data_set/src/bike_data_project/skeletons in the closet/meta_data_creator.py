new_rows = [
    ["customer_id", "customer_birthdate", "customer_gender"],
    ["customer_id", "country"],
    ["id", "category", "subcategory", "maintenence"]
]

tables = spark.sql("SHOW TABLES IN bike_data_lakehouse.bronze")

table_lines = []

for i, tb in enumerate(tables.collect()[3:6]): # should be zipped
    df = spark.read.table(f"bike_data_lakehouse.bronze.{tb.tableName}")

    block = [
        f"'silver_{tb.tableName}':{{",
        f"  'source': '{tb.tableName}',",
        "  'defaults': ['trim', 'lower'],",
        "  'columns':{"
    ]

    col_lines = []
    for j, col in enumerate(df.columns):
        col_lines.append(
            f"    'dim_{new_rows[i][j]}': {{\n"
            f"      'source': '{col}'\n"
            f"    }}"
        )

    block.append(",\n".join(col_lines))
    block.append("  }")
    block.append("}")

    table_lines.append("\n".join(block))

print(",\n".join(table_lines))