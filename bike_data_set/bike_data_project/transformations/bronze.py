"""
Definitions of functions used in the bronze step of the pipeline
"""

def missing_configs(config_dict: dict) -> None: 
    """
    Check meta_data file has all minimum configurations for each table
    """
    REQUIRED_FIELDS: list[str] = ["source", "table_type", "allow_overwrite"]

    missing_configs:list[str] = [field for field in REQUIRED_FIELDS if field not in config_dict]

    if missing_configs:
        raise ValueError(f"Missing required metadata fields: {missing_configs}")

    return None

def overwrite_allowed(original_table_name: str, config_dict: dict) -> Bool:
    """
    Check if table is allowed to be overwritten
    """
    if not config_dict.get("allow_overwrite"):
        # raise ValueError(f"{original_table_name} does not allow table overwrite")
        return False
    return True


def valid_file_type(original_table_name: str, config_dict: dict)-> None:
    """
    Check if incoming file has the correct file type to be processed
    """
    supported_types = {'CSV'} #has to be in upper
    if config_dict['table_type'].upper() not in supported_types:
        raise ValueError(f"ERROR: Unsupported file {original_table_name}")

    return None

def path_checker(path: str)-> bool:
    """
    # Check if file path to be read exists in volume
    """
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False
    
def empty_table(row_count: int, original_table_name: str)-> None:
    """
    Check if the read table is empty
    """
    if row_count < 1:
        raise ValueError(f"Data frame is empty: {original_table_name}")







