#!/usr/bin/env python
"""
Debug CDR number extraction
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from processors.cdr_loader import CDRLoader

# Load CDR data
cdr_loader = CDRLoader()
cdr_data = cdr_loader.load_cdrs()

print("CDR Data Structure:")
for name, df in list(cdr_data.items())[:2]:
    print(f"\n{name}:")
    print(f"  Columns: {list(df.columns)}")
    
    # Check a_party column
    if 'a_party' in df.columns:
        print(f"  a_party samples: {df['a_party'].head(3).tolist()}")
        print(f"  a_party unique: {df['a_party'].nunique()}")
        
        # Extract phone from suspect name
        import re
        match = re.search(r'(\d{10})', name)
        if match:
            phone = match.group(1)
            print(f"  Phone from name: {phone}")
            
            # Check if phone matches a_party
            if phone in df['a_party'].values:
                print(f"  ✓ Phone found in a_party column")
            else:
                print(f"  ✗ Phone NOT found in a_party column")
                print(f"  First a_party value: {df['a_party'].iloc[0]}")