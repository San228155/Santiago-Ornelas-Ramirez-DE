from pyspark.sql import functions as F

TRANSFORMATION = {
    "silver_crm_customer_info": {
        "source": "crm_customer_info",

        "defaults": ["trim", "lower"],

        "second_nf_configuration": True,

        "primary_key": {
            "dim_customer_id": 5
        },

        "surrogate_key": {
            "dim_customer_sk": "dim_customer_id"
        },

        "columns": {
            "dim_customer_id": {
                "source": "cst_id",
            },
            "dim_customer_key": {
                "source": "cst_key"
            },
            "dim_customer_first_name": {
                "source": "cst_firstname"
            },
            "dim_customer_last_name": {
                "source": "cst_lastname"
            },
            "dim_customer_marital_status": {
                "source": "cst_marital_status",
                "map": {
                    "single": ["s"],
                    "married": ["m"]
                },
                "validate": {
                    "enum": ["single", "married"],
                    "enforce": True
                }
            },
            "dim_customer_gender": {
                "source": "cst_gndr",
                "map": {
                    "male": ["m"],
                    "female": ["f"]
                },
                "validate": {
                    "enum": ["male", "female"],
                    "enforce": True
                }
            },
            "dim_customer_create_date": {
                "source": "cst_create_date",
                "cast": {
                    "date": "yyyy-MM-dd",
                    "default_value": "0001-01-01"
                }
            }
        }
    },
    "silver_crm_product_info": {
      "source": "crm_product_info",
      "defaults": ["trim", "lower"],
      "primary_key": {
          "dim_product_id": 5
        },
      "intelligent_key": {
        "intelligent_key": {
            "source": "dim_product_key",
            "substr": [1, 100]
        },
        "dim_category_id": {
            "source": "dim_product_key",
            "substr": [1, 5]
        },
        "dim_product_key": {
            "source": "dim_product_key",
            "substr": [7, 100]
        }
      },
      "surrogate_key": {
            "dim_category_sk": "dim_category_id",
             "dim_product_sk": "dim_product_key"
        },
      "columns": {
        "dim_product_id": {
          "source": "prd_id"
        },
        "dim_product_key": {
          "source": "prd_key",
          "replace_hyphon": True
        },
        "dim_product_name": {
          "source": "prd_nm",
          "replace_hyphon": True
        },
        "m_product_cost": {
          "source": "prd_cost",
          "cast": {
            "int": {}
          }
        },
        "dim_product_line": {
          "source": "prd_line",
          "map": {
            "road" : ["r"], 
            "standard": ["s"],
            "mountain": ["m"],
            "touring": ["t"]
          },
          "validation": {
            "enum": ["road", "standard", "mountain", "touring"],
            "enforce": True
          }
        },
        "dim_product_start_date": {
          "source": "prd_start_dt",
          "cast": {
            "date": "yyyy-MM-dd",
            "default_value": "0001-01-01"
          }
        },
        "dim_product_end_date":{
          "source": "prd_end_dt",
          "cast": {
            "date": "yyyy-MM-dd",
            "default_value": "0001-01-01"
          }
        }
      }
    },
    "silver_crm_sales_info": {
        "source": "crm_sales_info",
        "defaults": ["trim", "lower"],
        "primary_key": {
            "dim_sales_order_number": 7,
            "dim_product_key": 5
            }, 
        "surrogate_key": {
            "dim_customer_sk": "dim_customer_id",
            "dim_product_sk": "dim_product_key"
        },
        "data_augmentation_columns": ["m_sale_value", "m_sales_quanitity", "m_sales_price"],
        "columns": {
            "dim_sales_order_number": {
                "source": "sls_ord_num"
            },
            "dim_product_key": {
                "source": "sls_prd_key",
                "replace_hyphon": True
            },
            "dim_customer_id": {
                "source": "sls_cust_id"
            },
            "dim_sales_order_id": {
                "source": "sls_order_dt",
                "cast": {
                    "date": "yyyyMMdd",
                    "default_value": "00010101"
                },
            },
            "dim_sales_ship_date": {
                "source": "sls_ship_dt",
                "cast": {
                    "date": "yyyyMMdd",
                    "default_value": "00010101"
                }
            },
            "dim_sales_due_date": {
                "source": "sls_due_dt",
                "cast": {
                    "date": "yyyyMMdd",
                    "default_value": "00010101"
                }
            },
            "m_sale_value": {
                "source": "sls_sales",
                "cast": {
                    "int": {}
                },
                "expression": ["m_sales_price", "*", "m_sales_quantity"]

            },
            "m_sales_quantity": {
                "source": "sls_quantity",
                "cast": {
                    "int": {}
                },
                "expression": ["m_sale_value", "/", "m_sales_price"]
            },
            "m_sales_price": {
                "source": "sls_price",
                "cast": {
                    "int": {}
                },
                "expression": ["m_sale_value", "/", "m_sales_quantity"]
            }
        }
    },
    "silver_erp_customer_az_12": {
        "source": "erp_customer_az_12",
        "defaults": ["trim", "lower"],
        "primary_key": {
            "dim_customer_id": 5
        },
        "intelligent_key": {
            "intelligent_key": {
                "source": "dim_customer_id",
                "substr": [1, 100]
            },
            "dim_customer_id_prefix": {
                "source": "dim_customer_id",
                "substr": [1, F.length("dim_customer_id") - 5]
            },
            "dim_customer_id": {
                "source": "dim_customer_id",
                "substr": [F.length("dim_customer_id") - 4, 5]
            }
        },
        "surrogate_key": {
            "dim_customer_sk": "dim_customer_id"
        },
        "columns": {
            "dim_customer_id": {
                "source": "CID",
            },
            "dim_customer_birthdate": {
                "source": "BDATE",
                "cast": {
                    "date": "yyyy-MM-dd",
                    "default_value": "0001-01-01"
                }
            },
            "dim_customer_gender": {
                "source": "GEN"
            }
        }
    },
    "silver_erp_location_a101": {
        "source": "erp_location_a101",
        "defaults": ["trim", "lower"],
        "primary_key": {
            "dim_customer_id": 5
        },
        "surrogate_key": {
            "dim_customer_sk": "dim_customer_id"
        },
        "intelligent_key": {
            "intelligent_key": {
                "source": "dim_customer_id",
                "substr": [1, 100]
            },
            "dim_customer_id_prefix": {
                "source": "dim_customer_id",
                "substr": [1, F.length("dim_customer_id") - 5]
            },
            "dim_customer_id": {
                "source": "dim_customer_id",
                "substr": [F.length("dim_customer_id") - 4, 5]
            }
        },
        "columns": {
            "dim_customer_id": {
                "source": "CID",
                "remove_hyphon": True
            },
            "dim_country": {
                "source": "CNTRY",
                "map": {
                    "au": ["australia"],
                    "us": ["us", "usa", "united states"],
                    "ca": ["canada"],
                    "de": ["deutschland", "de", "germany"],
                    "uk": ["united kingdom"],
                    "france": ["france"],
                },
                "validation": {
                    "enum": ["au", "us", "ca", "de", "uk", "france", "unknown"],
                    "enforce": True
                }
            }
        }
    },
    "silver_erp_product_category_g1_v2": {
        "source": "erp_product_category_g1_v2",
        "defaults": ["trim", "lower"],
        "primary_key": {
            "dim_category_id": 5
        },
        "surrogate_key": {
            "dim_category_sk": "dim_category_id"
        },
        "columns": {
            "dim_category_id": {
                "source": "ID"
            },
            "dim_category": {
                "source": "CAT",
                "validation": {
                    "enum": ["accessories", "bikes", "clothing", "components"],
                    "enforce": True
                }

            },
            # we do not use an enum, too many categories, we live with the results
            "dim_subcategory": {
                "source": "SUBCAT",
                "replace_hyphon": True
            },
            "dim_maintenence": {
                "source": "MAINTENANCE",
                "map": {
                    "true": ["yes"],
                    "false": ["no"]
                },
                "validation": {
                    "enum": ["true", "false"],
                    "enforce": True
                }
            }
        }
    }
}