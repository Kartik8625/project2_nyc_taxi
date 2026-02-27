import great_expectations as ge
from great_expectations.data_context import DataContext

context = DataContext()

available_assets = context.get_available_data_asset_names()
print("✅ Available assets:", available_assets)