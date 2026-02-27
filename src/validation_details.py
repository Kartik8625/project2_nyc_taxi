from great_expectations.data_context import DataContext
from great_expectations.core.batch import BatchRequest

context = DataContext()

batch_request = BatchRequest(
    datasource_name="nyc_taxi_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="yellow_tripdata_2024-01",
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="nyc_taxi_quality_suite"
)

results = validator.validate()

print("\n" + "="*60)
print("DETAILED VALIDATION RESULTS — NYC TAXI DATA")
print("="*60)

for r in results.results:
    exp_type = r.expectation_config.expectation_type
    column = r.expectation_config.kwargs.get("column", "N/A")
    success = "✅ PASS" if r.success else "❌ FAIL"
    print(f"\n{success} | {column} | {exp_type}")
    if not r.success and r.result:
        print(f"         Details: {r.result}")