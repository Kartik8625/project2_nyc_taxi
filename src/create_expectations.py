import great_expectations as ge
from great_expectations.data_context import DataContext
from great_expectations.core.batch import BatchRequest

# Load context
context = DataContext()

# Create expectation suite
suite_name = "nyc_taxi_quality_suite"
context.add_or_update_expectation_suite(
    expectation_suite_name=suite_name
)

# Build batch request pointing to our parquet file
batch_request = BatchRequest(
    datasource_name="nyc_taxi_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="yellow_tripdata_2024-01",
)

# Create validator
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

print("✅ Validator created! Columns:", validator.columns())

# ── EXPECTATION 1 ──────────────────────────────
# fare_amount must be between 0 and 500
validator.expect_column_values_to_be_between(
    column="fare_amount",
    min_value=0,
    max_value=500
)
print("✅ Expectation 1 added: fare_amount between 0-500")

# ── EXPECTATION 2 ──────────────────────────────
# passenger_count must be between 1 and 6
validator.expect_column_values_to_be_between(
    column="passenger_count",
    min_value=1,
    max_value=6
)
print("✅ Expectation 2 added: passenger_count between 1-6")

# ── EXPECTATION 3 ──────────────────────────────
# trip_distance must be greater than 0
validator.expect_column_values_to_be_between(
    column="trip_distance",
    min_value=0.01,
    max_value=None
)
print("✅ Expectation 3 added: trip_distance > 0")

# ── EXPECTATION 4 ──────────────────────────────
# pickup datetime must never be null
validator.expect_column_values_to_not_be_null(
    column="tpep_pickup_datetime"
)
print("✅ Expectation 4 added: pickup_datetime not null")

# ── EXPECTATION 5 ──────────────────────────────
# payment_type must be in known values (1-6)
validator.expect_column_values_to_be_in_set(
    column="payment_type",
    value_set=[1, 2, 3, 4, 5, 6]
)
print("✅ Expectation 5 added: payment_type in valid set")

# Save the suite
validator.save_expectation_suite(discard_failed_expectations=False)
print("\n🎯 Expectation Suite saved successfully!")
print(f"Suite name: {suite_name}")