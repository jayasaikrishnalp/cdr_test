#!/usr/bin/env python
"""
Fix remaining tower analysis tools for Pydantic v2
"""

import os
from pathlib import Path

# Define fixes for each file
fixes = {
    "device_identity_tool.py": {
        "after": '    Input examples: "analyze device switching patterns", "check IMEI changes", "find switched devices"\n    """',
        "add": '\n    \n    # Class attributes for Pydantic v2\n    tower_dump_data: Dict[str, Any] = {}\n    device_profiles: Dict[str, Any] = {}'
    },
    "movement_analysis_tool.py": {
        "after": '    Input examples: "analyze movement patterns", "find rapid movements", "detect suspicious routes", "analyze travel patterns"\n    """',
        "add": '\n    \n    # Class attributes for Pydantic v2\n    tower_dump_data: Dict[str, Any] = {}\n    tower_locations: Dict[str, Any] = {}\n    movement_patterns: Dict[str, Any] = {}'
    },
    "geofencing_tool.py": {
        "after": '    Input examples: "geofence crime scene", "who was at location X", "analyze area around tower Y", "triangulate suspect location"\n    """',
        "add": '\n    \n    # Class attributes for Pydantic v2\n    tower_dump_data: Dict[str, Any] = {}\n    tower_locations: Dict[str, Any] = {}\n    geofences: Dict[str, Any] = {}\n    params: Dict[str, Any] = {}'
    },
    "cross_reference_tool.py": {
        "after": '    Input examples: "cross-reference with CDR", "find silent devices", "link tower and calls", "build suspect profile"\n    """',
        "add": '\n    \n    # Class attributes for Pydantic v2\n    tower_dump_data: Dict[str, Any] = {}\n    cdr_data: Dict[str, Any] = {}\n    ipdr_data: Dict[str, Any] = {}\n    params: Dict[str, Any] = {}'
    },
    "network_analysis_tool.py": {
        "after": '    Input examples: "analyze device network", "find network hubs", "identify clusters", "network evolution"\n    """',
        "add": '\n    \n    # Class attributes for Pydantic v2\n    tower_dump_data: Dict[str, Any] = {}\n    cdr_data: Dict[str, Any] = {}\n    params: Dict[str, Any] = {}'
    }
}

# Process each file
tools_dir = Path("tower_analysis_tools")

for filename, fix_data in fixes.items():
    file_path = tools_dir / filename
    
    if file_path.exists():
        print(f"Fixing {filename}...")
        
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Add the class attributes after the description
        if fix_data["after"] in content and "# Class attributes for Pydantic v2" not in content:
            content = content.replace(fix_data["after"], fix_data["after"] + fix_data["add"])
        
        # Remove duplicate initializations in __init__
        lines = content.split('\n')
        new_lines = []
        skip_next = False
        
        for i, line in enumerate(lines):
            if skip_next:
                skip_next = False
                continue
                
            if 'self.tower_dump_data = {}' in line and '__init__' in '\n'.join(lines[max(0,i-5):i]):
                continue
            elif 'self.tower_locations = {}' in line and '__init__' in '\n'.join(lines[max(0,i-5):i]):
                continue
            elif 'self.geofences = {}' in line and '__init__' in '\n'.join(lines[max(0,i-5):i]):
                continue
            elif 'self.cdr_data = {}' in line and '__init__' in '\n'.join(lines[max(0,i-5):i]):
                continue
            elif 'self.ipdr_data = {}' in line and '__init__' in '\n'.join(lines[max(0,i-5):i]):
                continue
            elif 'self.device_profiles = {}' in line and '__init__' in '\n'.join(lines[max(0,i-5):i]):
                continue
            elif 'self.movement_patterns = {}' in line and '__init__' in '\n'.join(lines[max(0,i-5):i]):
                continue
            else:
                new_lines.append(line)
        
        with open(file_path, 'w') as f:
            f.write('\n'.join(new_lines))
        
        print(f"✅ Fixed {filename}")
    else:
        print(f"❌ File not found: {filename}")

print("\nDone! All tower tools should now be Pydantic v2 compatible.")