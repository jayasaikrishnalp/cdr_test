#!/usr/bin/env python
"""
Check column names in tower dump files
"""

import pandas as pd
from pathlib import Path

# Load first tower dump file
file_path = Path("../../Tower Dumps/All Data_Tuni.xlsx")
print(f"Loading {file_path.name}...")

df = pd.read_excel(file_path, nrows=5)
print(f"\nColumns in tower dump file:")
for col in df.columns:
    print(f"  - {col}")

print(f"\nFirst few rows:")
print(df.head())

print(f"\nData types:")
print(df.dtypes)