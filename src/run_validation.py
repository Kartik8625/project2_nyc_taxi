import great_expectations as ge
from great_expectations.data_context import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler

context = DataContext()

yaml = YAMLHandler()

checkpoint_config = {
    "name": "nyc_taxi_checkpoint",
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": {
                "datasource_name": "nyc_taxi_datasource",
                "data_connector_name": "default_inferred_data_connector_name",
                "data_asset_name": "yellow_tripdata_2024-01",
            },
            "expectation_suite_name": "nyc_taxi_quality_suite",
        }
    ],
}

context.add_or_update_checkpoint(**checkpoint_config)
print("✅ Checkpoint created!")

results = context.run_checkpoint(checkpoint_name="nyc_taxi_checkpoint")

print("\n" + "="*50)
print("VALIDATION RESULTS SUMMARY")
print("="*50)
print(f"Overall Success: {results.success}")

for result in results.run_results.values():
    stats = result["validation_result"]["statistics"]
    print(f"\nTotal Expectations : {stats['evaluated_expectations']}")
    print(f"✅ Passed          : {stats['successful_expectations']}")
    print(f"❌ Failed          : {stats['unsuccessful_expectations']}")
    print(f"Success Rate       : {stats['success_percent']:.1f}%")

print("\n" + "="*50)
if results.success:
    print("🎉 DATA QUALITY CHECK PASSED!")
else:
    print("⚠️  DATA QUALITY ISSUES FOUND!")
print("="*50)