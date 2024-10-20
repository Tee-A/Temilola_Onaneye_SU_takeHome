from faker import Faker
import pandas as pd

fake = Faker()

# Generate 25 million log records
log_records = [{
    "timestamp": fake.date_time_this_year(),
    "level": fake.random_element(elements=("INFO", "ERROR", "WARNING")),
    "message": fake.sentence()
} for _ in range(25000000)]

# Create DataFrame
df_logs = pd.DataFrame(log_records)

# Optionally save to CSV
df_logs.to_csv('./pyspark/data/sample_logs.csv', index=False)
