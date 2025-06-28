"""
Test script for IPDR Intelligence Agent
Tests all IPDR analysis capabilities
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

# Add parent directory to path
import sys
sys.path.append(str(Path(__file__).parent))

from ipdr_agent import IPDRAgent

def test_ipdr_agent():
    """Test IPDR agent functionality"""
    
    # Load environment variables
    load_dotenv()
    
    # Check for API key
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        print("❌ Error: OPENROUTER_API_KEY not found in environment variables")
        print("Please set your OpenRouter API key:")
        print("export OPENROUTER_API_KEY='your-api-key-here'")
        return
    
    print("🚀 Testing IPDR Intelligence Agent")
    print("=" * 50)
    
    try:
        # Initialize IPDR agent
        print("\n1️⃣ Initializing IPDR Agent...")
        ipdr_agent = IPDRAgent(api_key=api_key)
        print("✅ IPDR Agent initialized successfully")
        
        # Load IPDR data
        print("\n2️⃣ Loading IPDR data...")
        result = ipdr_agent.load_ipdr_data()
        
        if result['status'] == 'success':
            print(f"✅ Loaded IPDR data for {result['loaded_count']} suspects:")
            for suspect in result['suspects']:
                print(f"   • {suspect}")
            
            # Show data summary
            print("\n📊 IPDR Data Summary:")
            for summary in result['summary']:
                print(f"\n{summary['suspect']}:")
                print(f"  • Total Sessions: {summary['total_sessions']}")
                print(f"  • Total Data: {summary['total_data_mb']} MB")
                print(f"  • Encrypted Sessions: {summary['encrypted_sessions']} ({summary['encrypted_percentage']}%)")
                print(f"  • Date Range: {summary['date_range']}")
        else:
            print(f"❌ Error loading IPDR data: {result.get('message', 'Unknown error')}")
            print("\nPlease ensure IPDR files are in the ../IPDRs directory")
            return
        
        # Test natural language queries
        print("\n3️⃣ Testing Natural Language Analysis...")
        
        test_queries = [
            "Show me high-risk suspects based on IPDR analysis",
            "Find suspects using encrypted apps at odd hours",
            "Analyze large file uploads and pattern day activity",
            "Detect marathon sessions and concurrent connections",
            "Identify unknown apps and P2P behavior"
        ]
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n📝 Query {i}: {query}")
            print("-" * 50)
            try:
                response = ipdr_agent.analyze(query)
                print(response)
            except Exception as e:
                print(f"❌ Error: {str(e)}")
        
        # Test specific analysis functions
        print("\n4️⃣ Testing Specific Analysis Functions...")
        
        # Risk Summary
        print("\n🎯 Risk Summary:")
        print("-" * 50)
        risk_summary = ipdr_agent.get_risk_summary()
        print(risk_summary)
        
        # Encryption Analysis
        print("\n🔐 Encryption Analysis:")
        print("-" * 50)
        encryption_analysis = ipdr_agent.analyze_encryption_patterns()
        print(encryption_analysis)
        
        # Data Pattern Analysis
        print("\n📊 Data Pattern Analysis:")
        print("-" * 50)
        data_analysis = ipdr_agent.analyze_data_patterns()
        print(data_analysis)
        
        # Session Analysis
        print("\n⏱️ Session Analysis:")
        print("-" * 50)
        session_analysis = ipdr_agent.analyze_sessions()
        print(session_analysis)
        
        # App Fingerprinting
        print("\n📱 App Fingerprinting:")
        print("-" * 50)
        app_analysis = ipdr_agent.analyze_apps()
        print(app_analysis)
        
        # Generate comprehensive report
        print("\n5️⃣ Generating Comprehensive Report...")
        report_path = Path("ipdr_test_report.md")
        report = ipdr_agent.generate_report(output_file=report_path)
        print(f"✅ Report generated and saved to: {report_path}")
        
        # Test suspect-specific analysis
        if result['suspects']:
            suspect = result['suspects'][0]
            print(f"\n6️⃣ Testing Suspect-Specific Analysis for: {suspect}")
            
            specific_queries = [
                f"Analyze encryption patterns for {suspect}",
                f"Show me data usage patterns for {suspect}",
                f"Find session anomalies for {suspect}"
            ]
            
            for query in specific_queries:
                print(f"\n📝 Query: {query}")
                print("-" * 50)
                try:
                    response = ipdr_agent.analyze(query)
                    print(response)
                except Exception as e:
                    print(f"❌ Error: {str(e)}")
        
        print("\n✅ IPDR Agent testing completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Fatal error during testing: {str(e)}")
        logger.exception("Test failed")

def create_sample_ipdr_data():
    """Create sample IPDR data for testing if none exists"""
    
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    
    print("\n📁 Creating sample IPDR data for testing...")
    
    ipdr_dir = Path("../IPDRs")
    ipdr_dir.mkdir(exist_ok=True)
    
    # Sample suspects
    suspects = ["Sonu", "Peerbasha", "Ravi Kumar"]
    
    for suspect in suspects:
        # Generate sample data
        num_records = np.random.randint(100, 300)
        
        # Generate timestamps
        base_date = datetime.now() - timedelta(days=30)
        timestamps = [base_date + timedelta(
            days=np.random.randint(0, 30),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60)
        ) for _ in range(num_records)]
        
        # Generate IPDR data
        data = {
            'subscriber_id': [f"+91{np.random.randint(7000000000, 9999999999)}" for _ in range(num_records)],
            'start_time': timestamps,
            'end_time': [t + timedelta(seconds=np.random.randint(60, 3600)) for t in timestamps],
            'destination_ip': [f"192.168.{np.random.randint(1, 255)}.{np.random.randint(1, 255)}" for _ in range(num_records)],
            'destination_port': np.random.choice([443, 80, 8080, 5222, 5223, 9001, 6667, 
                                                np.random.randint(10000, 65535)], num_records),
            'data_volume_up': np.random.exponential(1000000, num_records),  # Bytes
            'data_volume_down': np.random.exponential(5000000, num_records),  # Bytes
            'protocol': np.random.choice(['TCP', 'UDP'], num_records)
        }
        
        # Add some WhatsApp/Telegram sessions
        whatsapp_indices = np.random.choice(num_records, size=int(num_records * 0.3), replace=False)
        for idx in whatsapp_indices:
            data['destination_port'][idx] = np.random.choice([5222, 5223])
        
        # Add some large uploads
        large_upload_indices = np.random.choice(num_records, size=int(num_records * 0.1), replace=False)
        for idx in large_upload_indices:
            data['data_volume_up'][idx] = np.random.randint(10000000, 100000000)  # 10-100 MB
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Save to Excel
        file_path = ipdr_dir / f"{suspect}.xlsx"
        df.to_excel(file_path, index=False)
        print(f"✅ Created sample IPDR data for {suspect}: {file_path}")
    
    print("\n✅ Sample IPDR data created successfully!")

if __name__ == "__main__":
    # Check if IPDR data exists
    ipdr_dir = Path("../IPDRs")
    if not ipdr_dir.exists() or not list(ipdr_dir.glob("*.xlsx")) and not list(ipdr_dir.glob("*.csv")):
        print("⚠️ No IPDR data found. Creating sample data...")
        create_sample_ipdr_data()
    
    # Run tests
    test_ipdr_agent()