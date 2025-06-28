#!/usr/bin/env python3
"""
Safe runner for integrated analysis with proper encoding
"""

import os
import sys
import subprocess

# Set encoding environment variables
os.environ['PYTHONIOENCODING'] = 'utf-8'
os.environ['LC_ALL'] = 'en_US.UTF-8'
os.environ['LANG'] = 'en_US.UTF-8'

# Run the main script with proper encoding
print("Running integrated analysis with UTF-8 encoding...")
subprocess.run([sys.executable, 'main.py', 'integrated-analyze'], 
               env=os.environ)