import pandas as pd
import numpy as np
from pathlib import Path

# -----------------------------
# Configuration
# -----------------------------

NUM_DEVICES = 1000
PINGS_PER_DEVICE = 100

# Approximate Pune city center
CENTER_LAT = 18.5204
CENTER_LON = 73.8567

START_TIME = "2026-01-01 06:00:00"

# Reproducible random numbers
np.random.seed(42)


# -----------------------------
# Generate device IDs
# -----------------------------

device_ids = [
    f"D{i:05d}"
    for i in range(1, NUM_DEVICES + 1)
]


# -----------------------------
# Generate timestamps
# -----------------------------

timestamps = pd.date_range(
    start=START_TIME,
    periods=PINGS_PER_DEVICE,
    freq="15min"
)


records = []


# -----------------------------
# Generate movement
# -----------------------------

for device_id in device_ids:

    # Give every device a slightly different starting location
    latitude = CENTER_LAT + np.random.normal(0, 0.03)
    longitude = CENTER_LON + np.random.normal(0, 0.03)

    for timestamp in timestamps:

        # Small movement from previous location
        latitude += np.random.normal(0, 0.001)
        longitude += np.random.normal(0, 0.001)

        records.append({
            "DeviceID": device_id,
            "Latitude": round(latitude, 6),
            "Longitude": round(longitude, 6),
            "Timestamp": timestamp
        })


# -----------------------------
# Create DataFrame
# -----------------------------

df = pd.DataFrame(records)


# -----------------------------
# Save CSV
# -----------------------------

output_dir = Path("data")
output_dir.mkdir(exist_ok=True)

output_file = output_dir / "mobile_pings.csv"

df.to_csv(output_file, index=False)


# -----------------------------
# Print information
# -----------------------------

print("Dataset generated successfully!")
print(f"Rows: {len(df):,}")
print(f"Devices: {df['DeviceID'].nunique():,}")
print(f"File: {output_file}")

print("\nFirst 5 rows:")
print(df.head())