# We assume were in the bike_data_lakehouse Catalog and in the raw_data Schema
INGESTION = {
    # Volume
    "source_crm": {
        "crm_customer_info":{ 
                "source":"cust_info", #file_name
                "table_type": "CSV",
                "allow_overwrite": True
             },
        "crm_product_info": {
                "source": "prd_info", #file_name
                "table_type": "CSV",
                "allow_overwrite": True
            },
        "crm_sales_info": {
                "source": "sales_details", #file_name
                "table_type": "CSV",
                "allow_overwrite": True
            }
    },
    # Volume
    "source_erp": {
            "erp_customer_az_12": {
                "source": "CUST_AZ12", #file_name
                "table_type": "CSV",
                "allow_overwrite": True
            },
            "erp_location_a101": {
                "source": "LOC_A101", #file_name
                "table_type": "CSV",
                "allow_overwrite": True
            },
            "erp_product_category_g1_v2": {
                "source": "PX_CAT_G1V2", #file_name
                "table_type": "CSV",
                "allow_overwrite": True
            }
        }
}

# we want to ingest data
# rename
# pass it as a csv