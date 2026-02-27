"""
Meta data for dim_customer_gold table
"""


# final table name, tables to be joined, key/columns
customer_gold_metadata = {
    "name": "dim_customers",
    "join_key": "dim_customer_sk",
    "join_type": "full_outer",
    "coalesce_columns": {
        "dim_customer_id", 
        "dim_customer_gender"
    },
    "tables": {
        "silver_crm_customer_info": {
            "dim_customer_id", 
            "dim_customer_first_name", 
            "dim_customer_last_name", 
            "dim_customer_marital_status", 
            "dim_customer_gender", 
            "dim_customer_create_date"}, 
        "silver_erp_customer_az_12": {
            "dim_customer_id",
            "dim_customer_birthdate",
            "dim_customer_gender"
        }, 
        "silver_erp_location_a101": {
            "dim_customer_id",
            "dim_country"
        }
    }
}

product_gold_metadata = {
    "name": "dim_products",
    "join_key": "dim_category_sk",
    "join_type": "full_outer",
    "coalesce_columns": {
        "dim_category_id",
    },
    "tables": {
        "silver_crm_product_info": {
            "dim_product_key",
            "dim_category_id",
            "dim_product_name",
            "m_product_cost",
            "dim_product_line",
            "dim_product_start_date"
        }, 
        "silver_erp_product_category_g1_v2": {
            "dim_category_id",
            "dim_category",
            "dim_subcategory",
            "dim_maintenence"
        }
    }
}

sales_gold_meta_data = {
    "name": "fact_sales",
    "tables": {
        "silver_crm_sales_info": {
            "dim_product_key",
            "dim_customer_id",
            "dim_sales_order_number",
            "dim_sales_order_id",
            "dim_sales_ship_date",
            "dim_sales_due_date",
            "m_sale_value",
            "m_sales_quantity",
            "m_sales_price"
        }
    }
}
