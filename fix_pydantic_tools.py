#!/usr/bin/env python
"""
Fix Pydantic compatibility issues in all tower analysis tools
"""

import os
from pathlib import Path

# Tower analysis tools directory
tools_dir = Path("tower_analysis_tools")

# Files to fix
tool_files = [
    "behavior_pattern_tool.py",
    "device_identity_tool.py",
    "movement_analysis_tool.py",
    "geofencing_tool.py",
    "cross_reference_tool.py",
    "network_analysis_tool.py"
]

# Fix pattern: replace `name = "..."` with `name: str = "..."`
# and `description = """..."""` with `description: str = """..."""`

for tool_file in tool_files:
    file_path = tools_dir / tool_file
    
    if file_path.exists():
        print(f"Fixing {tool_file}...")
        
        # Read the file
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Apply fixes
        # Fix name field
        if 'name = "' in content and 'name: str = "' not in content:
            content = content.replace('name = "', 'name: str = "')
        
        # Fix description field
        if 'description = """' in content and 'description: str = """' not in content:
            content = content.replace('description = """', 'description: str = """')
        
        # Write back
        with open(file_path, 'w') as f:
            f.write(content)
        
        print(f"✅ Fixed {tool_file}")
    else:
        print(f"❌ File not found: {tool_file}")

print("\nAll files fixed!")
print("\nNow updating requirements.txt to ensure compatibility...")

# Update requirements.txt to ensure we have compatible versions
requirements_path = Path("requirements.txt")
if requirements_path.exists():
    with open(requirements_path, 'r') as f:
        requirements = f.read()
    
    # Check if pydantic version is specified
    if 'pydantic' not in requirements:
        print("Adding pydantic to requirements.txt")
        with open(requirements_path, 'a') as f:
            f.write("\npydantic>=2.0.0,<3.0.0\n")

print("\nDone! You can now run the integrated analysis.")