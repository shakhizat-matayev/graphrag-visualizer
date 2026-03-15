from pathlib import Path

import pandas as pd


artifacts_dir = Path("public/artifacts")
parquet_files = sorted(artifacts_dir.glob("*.parquet"))

if not parquet_files:
	print(f"No parquet files found in {artifacts_dir}")
else:
	for parquet_file in parquet_files:
		csv_file = parquet_file.with_suffix(".csv")
		df = pd.read_parquet(parquet_file, engine="pyarrow")
		df.to_csv(csv_file, index=False)
		print(f"Converted {parquet_file} -> {csv_file}")
