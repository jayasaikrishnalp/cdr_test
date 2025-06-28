#!/usr/bin/env python
"""
Quick test script for integrated CDR-IPDR analysis
"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from integrated_agent.integrated_agent_v2 import EnhancedIntegratedAgent
from loguru import logger

def main():
    """Run integrated analysis test"""
    
    # Check API key
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        print("❌ Error: Please set OPENROUTER_API_KEY environment variable")
        print("Run: export OPENROUTER_API_KEY='your-api-key-here'")
        return
    
    print("CDR-IPDR-Tower Dump Integrated Intelligence System")
    print("=" * 60)
    
    try:
        # Initialize enhanced agent
        print("\n📊 Initializing Enhanced Integrated Agent...")
        agent = EnhancedIntegratedAgent(api_key=api_key)
        print("✅ Agent initialized successfully")
        
        # Load CDR and IPDR data
        print("\n📁 Loading data...")
        
        # Get available files
        cdr_dir = Path("../CDRs")
        ipdr_dir = Path("../IPDR")
        
        cdr_files = list(cdr_dir.glob("*.xlsx"))[:3]  # Load first 3
        ipdr_files = list(ipdr_dir.glob("*.xlsx"))[:3]  # Load first 3
        
        if not cdr_files:
            print("❌ No CDR files found in ../CDRs/")
            return
            
        if not ipdr_files:
            print("❌ No IPDR files found in ../IPDR/")
            return
        
        # Load data
        result = agent.load_all_data(
            cdr_files=[str(f) for f in cdr_files],
            ipdr_files=[str(f) for f in ipdr_files]
        )
        
        print(f"\n✅ Data loaded successfully:")
        print(f"   - CDR suspects: {result['summary']['cdr_suspects']}")
        print(f"   - IPDR suspects: {result['summary']['ipdr_suspects']}")
        print(f"   - Common suspects: {result['summary']['common_cdr_ipdr']}")
        
        # Correlate data
        print("\n🔗 Correlating CDR and IPDR data...")
        correlation = agent.correlate_all_data()
        
        if 'cdr_ipdr_correlation' in correlation and correlation['cdr_ipdr_correlation']:
            print(f"✅ Correlation complete:")
            corr_data = correlation['cdr_ipdr_correlation']
            print(f"   - Correlated suspects: {corr_data.get('correlated_suspects', 0)}")
            print(f"   - Cross patterns: {corr_data.get('cross_patterns', 0)}")
            print(f"   - Evidence chains: {corr_data.get('evidence_chains', 0)}")
        
        # Run analyses
        print("\n" + "=" * 60)
        print("RUNNING INTEGRATED ANALYSES")
        print("=" * 60)
        
        analyses = [
            ("🎯 Risk Assessment", 
             "Provide a risk assessment of all suspects based on CDR and IPDR data. "
             "Focus on: 1) Device switching patterns, 2) Encrypted communications, "
             "3) Odd hour activities, 4) Suspicious coordination"),
            
            ("🔍 Surveillance Evasion Detection",
             "Identify surveillance evasion techniques including: "
             "1) Device switching (multiple IMEIs), 2) Encrypted app usage, "
             "3) Silent periods, 4) Burner phone patterns"),
            
            ("🔗 Evidence Chains",
             "Find evidence chains linking voice calls to data activities. "
             "Focus on: calls followed by encryption, data during silence periods, "
             "and coordinated activities between suspects."),
            
            ("📱 Top Priority Suspects",
             "Identify the top 3 highest priority suspects with specific evidence "
             "from both CDR and IPDR data. Provide actionable intelligence.")
        ]
        
        for title, query in analyses:
            print(f"\n{title}")
            print("-" * 60)
            try:
                result = agent.analyze(query)
                print(result)
            except Exception as e:
                print(f"Error: {str(e)}")
        
        # Generate report
        print("\n" + "=" * 60)
        print("GENERATING COMPREHENSIVE REPORT")
        print("=" * 60)
        
        report_dir = Path("reports")
        report_dir.mkdir(exist_ok=True)
        report_file = report_dir / "integrated_analysis_report.md"
        
        print("\n📝 Generating comprehensive report...")
        report = agent.generate_comprehensive_report(output_file=report_file)
        print(f"✅ Report saved to: {report_file}")
        
        print("\n" + "=" * 60)
        print("ANALYSIS COMPLETE")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Review the generated report in reports/integrated_analysis_report.md")
        print("2. Run interactive mode for specific queries")
        print("3. Load tower dump data when available for location analysis")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        logger.exception("Error in integrated analysis")

if __name__ == "__main__":
    main()