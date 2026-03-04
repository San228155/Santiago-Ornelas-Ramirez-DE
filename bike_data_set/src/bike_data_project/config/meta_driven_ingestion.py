INGESTION = {
    "source_crm": {
        "crm_customer_info":{
                "source":"cust_info",
                "table_type": "CSV",
                "allow_overwrite": True
             },
        "crm_product_info": {
                "source": "prd_info",
                "table_type": "CSV",
                "allow_overwrite": True
            },
        "crm_sales_info": {
                "source": "sales_details",
                "table_type": "CSV",
                "allow_overwrite": True
            }
    },
    "source_erp": {
            "erp_customer_az_12": {
                "source": "CUST_AZ12",
                "table_type": "CSV",
                "allow_overwrite": True
            },
            "erp_location_a101": {
                "source": "LOC_A101",
                "table_type": "CSV",
                "allow_overwrite": True
            },
            "erp_product_category_g1_v2": {
                "source": "PX_CAT_G1V2",
                "table_type": "CSV",
                "allow_overwrite": True
            }
        }
}
