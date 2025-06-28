#!/usr/bin/env python
"""
Simple fix for tower analysis tools Pydantic v2 compatibility
"""

import os
from pathlib import Path

# Tower analysis tools directory
tools_dir = Path("tower_analysis_tools")

# For TimeWindowFilterTool
time_filter_path = tools_dir / "time_filter_tool.py"
if time_filter_path.exists():
    print("Fixing time_filter_tool.py...")
    
    with open(time_filter_path, 'r') as f:
        content = f.read()
    
    # Add Dict import if not present
    if 'from typing import Dict' not in content:
        content = content.replace(
            'from typing import Dict, Any, List, Optional, Union',
            'from typing import Dict, Any, List, Optional, Union'
        )
    
    # Add class attributes after description
    old_section = '''    name: str = "time_window_filter"
    description: str = """Filter tower dump data by time window. Use this tool to:
    - Extract records during crime time window
    - Find activity before/after incidents
    - Analyze odd-hour patterns
    - Identify temporal anomalies
    
    Input format: "filter [time_start] to [time_end]" or "analyze odd hours" or "show activity around [time]"
    Example: "filter 2024-01-15 02:00:00 to 2024-01-15 03:00:00"
    """'''
    
    new_section = '''    name: str = "time_window_filter"
    description: str = """Filter tower dump data by time window. Use this tool to:
    - Extract records during crime time window
    - Find activity before/after incidents
    - Analyze odd-hour patterns
    - Identify temporal anomalies
    
    Input format: "filter [time_start] to [time_end]" or "analyze odd hours" or "show activity around [time]"
    Example: "filter 2024-01-15 02:00:00 to 2024-01-15 03:00:00"
    """
    
    # Class attributes for Pydantic v2
    tower_dump_data: Dict[str, Any] = {}
    params: Dict[str, Any] = {}'''
    
    content = content.replace(old_section, new_section)
    
    # Remove the self.tower_dump_data = {} line in __init__
    content = content.replace('        self.tower_dump_data = {}', '')
    
    with open(time_filter_path, 'w') as f:
        f.write(content)
    
    print("✅ Fixed time_filter_tool.py")

# Fix other tools
other_tools = [
    "behavior_pattern_tool.py",
    "device_identity_tool.py", 
    "movement_analysis_tool.py",
    "geofencing_tool.py",
    "cross_reference_tool.py",
    "network_analysis_tool.py"
]

for tool_name in other_tools:
    tool_path = tools_dir / tool_name
    if tool_path.exists():
        print(f"Fixing {tool_name}...")
        
        with open(tool_path, 'r') as f:
            content = f.read()
        
        # Find the class definition
        lines = content.split('\n')
        new_lines = []
        
        for i, line in enumerate(lines):
            new_lines.append(line)
            
            # After the description field, add class attributes
            if 'description: str = """' in line and i+10 < len(lines):
                # Find the end of the description
                j = i + 1
                while j < len(lines) and '"""' not in lines[j]:
                    new_lines.append(lines[j])
                    j += 1
                if j < len(lines):
                    new_lines.append(lines[j])  # Add the closing """
                    
                    # Add class attributes
                    new_lines.append("    ")
                    new_lines.append("    # Class attributes for Pydantic v2")
                    new_lines.append("    tower_dump_data: Dict[str, Any] = {}")
                    
                    if "geofencing" in tool_name:
                        new_lines.append("    tower_locations: Dict[str, Any] = {}")
                        new_lines.append("    geofences: Dict[str, Any] = {}")
                    elif "cross_reference" in tool_name:
                        new_lines.append("    cdr_data: Dict[str, Any] = {}")
                        new_lines.append("    ipdr_data: Dict[str, Any] = {}")
                    elif "network_analysis" in tool_name:
                        new_lines.append("    cdr_data: Dict[str, Any] = {}")
                    
                    new_lines.append("    params: Dict[str, Any] = {}")
                    
                    # Skip the lines we already processed
                    for k in range(i+1, j+1):
                        lines[k] = None
        
        # Reconstruct content
        final_lines = []
        for line in new_lines:
            if line is not None:
                # Skip lines that create instance attributes
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
                final_lines.append(line)
        
        with open(tool_path, 'w') as f:
            f.write('\n'.join(final_lines))
        
        print(f"✅ Fixed {tool_name}")

print("\nAll tower tools fixed for Pydantic v2!")