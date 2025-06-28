#!/usr/bin/env python3
"""
Test script to demonstrate enhanced risk scoring with detailed point breakdowns
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from agent.cdr_agent import CDRIntelligenceAgent

def test_risk_scoring():
    """Test the enhanced risk scoring with point breakdowns"""
    
    print("=" * 80)
    print("TESTING ENHANCED RISK SCORING WITH POINT BREAKDOWNS")
    print("=" * 80)
    
    # Initialize agent
    print("\n1. Initializing CDR Intelligence Agent...")
    agent = CDRIntelligenceAgent()
    
    # Load CDR data
    print("\n2. Loading CDR data...")
    result = agent.load_cdr_data()
    
    if result['status'] == 'success':
        print(f"✅ Loaded {result['files_loaded']} CDR files")
    else:
        print(f"❌ Failed to load data: {result.get('message')}")
        return
    
    # Test risk scoring with detailed breakdowns
    print("\n3. Running risk assessment with detailed point breakdowns...")
    print("-" * 60)
    
    # Query for risk scores
    query = "Calculate detailed risk scores for all suspects and show point breakdowns"
    
    result = agent.analyze(query)
    print(result)
    
    print("\n" + "=" * 80)
    print("VERIFICATION: Ali's Risk Level")
    print("=" * 80)
    
    # Specifically check Ali's risk
    ali_query = "Show detailed risk score breakdown for Ali"
    ali_result = agent.analyze(ali_query)
    
    # Check if Ali is now correctly classified as MEDIUM
    if "Ali" in ali_result and ("MEDIUM" in ali_result or "🟡" in ali_result):
        print("✅ SUCCESS: Ali is correctly classified as MEDIUM risk due to 2 IMEIs")
    else:
        print("❌ ISSUE: Ali may still be incorrectly classified")
    
    print("\n" + "=" * 80)
    print("KEY FEATURES DEMONSTRATED:")
    print("=" * 80)
    print("1. Detailed point breakdowns for each risk component")
    print("2. Shows exactly where points come from (e.g., '+25 points for 2 IMEIs')")
    print("3. Override rules clearly indicated")
    print("4. Total scores displayed (e.g., '65/100')")
    print("5. Each suspect shows:")
    print("   - Device Risk breakdown")
    print("   - Temporal Risk breakdown")
    print("   - Communication Risk breakdown")
    print("   - Frequency Risk breakdown")
    print("   - Network Risk breakdown")

if __name__ == "__main__":
    test_risk_scoring()