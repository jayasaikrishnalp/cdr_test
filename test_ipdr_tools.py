"""
Test IPDR tools directly without agent
"""

import os
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).parent))

from ipdr_processors.ipdr_loader import IPDRLoader
from ipdr_agent.ipdr_tools import EncryptionAnalysisTool, DataPatternAnalysisTool

def test_ipdr_tools():
    """Test IPDR tools directly"""
    
    # Load IPDR data
    print("Loading IPDR data...")
    loader = IPDRLoader()
    ipdr_data = loader.load_ipdrs()
    
    print(f"Loaded {len(ipdr_data)} IPDR files")
    
    if not ipdr_data:
        print("No IPDR data found!")
        return
    
    # Test encryption analysis tool
    print("\n" + "="*50)
    print("Testing Encryption Analysis Tool")
    print("="*50)
    
    encryption_tool = EncryptionAnalysisTool()
    encryption_tool.ipdr_data = ipdr_data
    
    result = encryption_tool._run("analyze all encryption patterns")
    print(result)
    
    # Test data pattern analysis tool
    print("\n" + "="*50)
    print("Testing Data Pattern Analysis Tool")
    print("="*50)
    
    data_tool = DataPatternAnalysisTool()
    data_tool.ipdr_data = ipdr_data
    
    result = data_tool._run("find large uploads")
    print(result)

if __name__ == "__main__":
    test_ipdr_tools()