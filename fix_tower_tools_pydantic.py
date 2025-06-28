#!/usr/bin/env python
"""
Fix tower analysis tools for Pydantic v2 compatibility
"""

import os
from pathlib import Path

# Tower analysis tools directory
tools_dir = Path("tower_analysis_tools")

# Files to fix
tool_files = [
    "time_filter_tool.py",
    "behavior_pattern_tool.py",
    "device_identity_tool.py",
    "movement_analysis_tool.py",
    "geofencing_tool.py",
    "cross_reference_tool.py",
    "network_analysis_tool.py"
]

# For each tool, we need to move instance attributes to class attributes
fixes = {
    "time_filter_tool.py": {
        "after_class": """    \"\"\"Tool for filtering tower dump data by time windows\"\"\"
    
    name: str = "time_window_filter"
    description: str = \"\"\"Filter tower dump data by time window. Use this tool to:
    - Extract records during crime time window
    - Find activity before/after incidents
    - Analyze odd-hour patterns
    - Identify temporal anomalies
    
    Input format: "filter [time_start] to [time_end]" or "analyze odd hours" or "show activity around [time]"
    Example: "filter 2024-01-15 02:00:00 to 2024-01-15 03:00:00"
    \"\"\"
    
    # Define attributes as class variables for Pydantic v2
    tower_dump_data: Dict[str, Any] = {}
    params: Dict[str, Any] = {}""",
        "in_init": """        super().__init__()
        # Initialize parameters
        self.params = {
            'odd_hours_start': 0,      # Midnight
            'odd_hours_end': 5,        # 5 AM
            'crime_window_before': 60,  # Minutes before incident
            'crime_window_after': 60,   # Minutes after incident
            'burst_threshold': 10,      # Calls in 5 minutes
            'silence_threshold': 120    # Minutes of inactivity
        }"""
    }
}

# Generic fix for all tools
def fix_tool_file(file_path):
    """Add typing imports and fix Pydantic compatibility"""
    
    print(f"Fixing {file_path.name}...")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Check if already has typing import
    if 'from typing import' not in content:
        # Add after other imports
        content = content.replace(
            'from langchain.tools import BaseTool',
            'from langchain.tools import BaseTool\nfrom typing import Dict, Any, List, Optional'
        )
    
    # Find the class definition and add attribute definitions
    lines = content.split('\n')
    new_lines = []
    in_class = False
    class_name = None
    init_found = False
    
    for i, line in enumerate(lines):
        if line and line.strip().startswith('class ') and 'BaseTool' in line:
            in_class = True
            class_name = line.split('(')[0].replace('class ', '').strip()
            new_lines.append(line)
            
            # Look ahead to find where to insert attributes
            j = i + 1
            while j < len(lines) and (lines[j].strip().startswith('"""') or lines[j].strip() == ''):
                new_lines.append(lines[j])
                j += 1
                if j < len(lines) and lines[j].strip().endswith('"""'):
                    new_lines.append(lines[j])
                    j += 1
                    break
            
            # Add class attributes after docstring
            if file_path.name in ["behavior_pattern_tool.py", "device_identity_tool.py", 
                                 "movement_analysis_tool.py", "geofencing_tool.py",
                                 "cross_reference_tool.py", "network_analysis_tool.py"]:
                new_lines.append("    ")
                new_lines.append("    # Class attributes for Pydantic v2")
                new_lines.append("    tower_dump_data: Dict[str, Any] = {}")
                
                # Add specific attributes based on tool
                if "geofencing" in file_path.name:
                    new_lines.append("    tower_locations: Dict[str, Any] = {}")
                    new_lines.append("    geofences: Dict[str, Any] = {}")
                    new_lines.append("    params: Dict[str, Any] = {}")
                elif "cross_reference" in file_path.name:
                    new_lines.append("    cdr_data: Dict[str, Any] = {}")
                    new_lines.append("    ipdr_data: Dict[str, Any] = {}")
                    new_lines.append("    params: Dict[str, Any] = {}")
                elif "network_analysis" in file_path.name:
                    new_lines.append("    cdr_data: Dict[str, Any] = {}")
                    new_lines.append("    params: Dict[str, Any] = {}")
                else:
                    new_lines.append("    params: Dict[str, Any] = {}")
            
            # Skip the lines we already processed
            for k in range(i + 1, j):
                if k < len(lines):
                    lines[k] = None
            
        elif line is not None and isinstance(line, str):
            # Fix __init__ methods to not create new attributes
            if '    def __init__(self):' in line:
                init_found = True
            
            # Skip lines that try to create instance attributes in __init__
            if init_found and in_class:
                if 'self.tower_dump_data = {}' in line:
                    continue
                elif 'self.tower_locations = {}' in line:
                    continue
                elif 'self.geofences = {}' in line:
                    continue
                elif 'self.cdr_data = {}' in line:
                    continue
                elif 'self.ipdr_data = {}' in line:
                    continue
                elif 'self.params = {' in line and not any(x in line for x in ['crime_window', 'odd_hours', 'co_location']):
                    # Skip simple params initialization
                    continue
            
            new_lines.append(line)
    
    # Write back
    with open(file_path, 'w') as f:
        f.write('\n'.join(new_lines))
    
    print(f"✅ Fixed {file_path.name}")

# Main execution
print("Fixing tower analysis tools for Pydantic v2 compatibility...\n")

for tool_file in tool_files:
    file_path = tools_dir / tool_file
    if file_path.exists():
        fix_tool_file(file_path)
    else:
        print(f"❌ File not found: {tool_file}")

print("\nDone! All tower tools should now be Pydantic v2 compatible.")