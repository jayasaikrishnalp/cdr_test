#!/usr/bin/env python
"""
Debug tower dump timestamp formats
"""

import pandas as pd
from pathlib import Path

# Load first tower dump file
file_path = Path("../../Tower Dumps/All Data_Tuni.xlsx")
print(f"Loading {file_path.name}...")

df = pd.read_excel(file_path, nrows=10)

print(f"\nColumns in tower dump file:")
for col in df.columns:
    print(f"  - {col}")

print(f"\nData types:")
print(df.dtypes)

# Check DATE column
if 'DATE' in df.columns:
    print(f"\nDATE column samples:")
    print(df['DATE'].head())
    print(f"DATE type: {df['DATE'].dtype}")
    
# Check TIME column  
if 'TIME' in df.columns:
    print(f"\nTIME column samples:")
    print(df['TIME'].head())
    print(f"TIME type: {df['TIME'].dtype}")

# Try combining
if 'DATE' in df.columns and 'TIME' in df.columns:
    print(f"\nTrying to combine DATE and TIME...")
    
    # Method 1: Direct string concatenation
    df['combined1'] = df['DATE'].astype(str) + ' ' + df['TIME'].astype(str)
    print(f"\nCombined strings:")
    print(df['combined1'].head())
    
    # Method 2: Try parsing
    df['timestamp'] = pd.to_datetime(df['combined1'], errors='coerce')
    print(f"\nParsed timestamps:")
    print(df['timestamp'].head())
    
    # Check for nulls
    null_count = df['timestamp'].isna().sum()
    print(f"\nNull timestamps: {null_count}/{len(df)}")
    
    # If all null, show why
    if null_count == len(df):
        print("\nAll timestamps failed to parse. Raw values:")
        for i in range(min(5, len(df))):
            print(f"  Row {i}: DATE='{df.iloc[i]['DATE']}', TIME='{df.iloc[i]['TIME']}'")