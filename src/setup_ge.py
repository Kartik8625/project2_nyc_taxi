import great_expectations as ge
from great_expectations.data_context import DataContext
import os

context = DataContext()

# Use absolute path
base_dir = os.path.abspath("data/raw")
print(f"Looking in: {base_dir}")

datasource_config = {
    "name": "nyc_taxi_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SparkDFExecutionEngine",
    },
    "data_connectors": {
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": base_dir,
            "default_regex": {
                "group_names": ["data_asset_name"],
                "pattern": "(.*)\\.parquet",
            },
        },
    },
}

context.add_or_update_datasource(**datasource_config)
print("✅ Datasource updated with absolute path!")