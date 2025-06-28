#!/usr/bin/env python
"""
Run integrated CDR-IPDR-Tower Dump analysis
This script uses the enhanced integrated agent with better error handling
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from integrated_agent.integrated_agent_v2 import EnhancedIntegratedAgent
from loguru import logger

# Configure logging
logger.add("integrated_analysis.log", rotation="10 MB")

def main():
    """Run integrated analysis with enhanced agent"""
    
    print("=" * 80)
    print("CDR-IPDR-Tower Dump Integrated Intelligence System v2.0")
    print("=" * 80)
    print(f"Analysis started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 80)
    
    # Check API key
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        print("\n❌ ERROR: OpenRouter API key not found!")
        print("\nPlease set your API key using one of these methods:")
        print("1. Export environment variable:")
        print("   export OPENROUTER_API_KEY='your-api-key-here'")
        print("\n2. Create .env file:")
        print("   echo \"OPENROUTER_API_KEY=your-api-key-here\" > .env")
        return
    
    print(f"\n✅ API Key detected (length: {len(api_key)} chars)")
    
    try:
        # Initialize enhanced agent
        print("\n📊 Initializing Enhanced Integrated Agent...")
        agent = EnhancedIntegratedAgent(api_key=api_key)
        print("✅ Agent initialized successfully")
        
        # Prepare data files
        cdr_dir = Path("../CDRs")
        ipdr_dir = Path("../IPDR")
        
        # Get all available files
        cdr_files = list(cdr_dir.glob("*.xlsx"))
        ipdr_files = list(ipdr_dir.glob("*.xlsx"))
        
        print(f"\n📁 Found {len(cdr_files)} CDR files and {len(ipdr_files)} IPDR files")
        
        if not cdr_files and not ipdr_files:
            print("❌ No data files found!")
            print(f"   CDR directory: {cdr_dir.absolute()}")
            print(f"   IPDR directory: {ipdr_dir.absolute()}")
            return
        
        # Load data (limit to first 3 of each for demo)
        print("\n📥 Loading data...")
        result = agent.load_all_data(
            cdr_files=[str(f) for f in cdr_files[:3]] if cdr_files else None,
            ipdr_files=[str(f) for f in ipdr_files[:3]] if ipdr_files else None
        )
        
        # Display loading summary
        summary = result.get('summary', {})
        print(f"\n✅ Data Loading Complete:")
        print(f"   • CDR Suspects: {summary.get('cdr_suspects', 0)}")
        print(f"   • IPDR Suspects: {summary.get('ipdr_suspects', 0)}")
        print(f"   • Common Suspects: {summary.get('common_cdr_ipdr', 0)}")
        print(f"   • Tower Dumps: {summary.get('tower_dumps', 0)}")
        
        # Perform correlation
        if summary.get('cdr_suspects', 0) > 0 and summary.get('ipdr_suspects', 0) > 0:
            print("\n🔗 Correlating data sources...")
            correlation = agent.correlate_all_data()
            
            if correlation.get('cdr_ipdr_correlation'):
                corr_data = correlation['cdr_ipdr_correlation']
                print(f"✅ Correlation Complete:")
                print(f"   • Correlated Suspects: {corr_data.get('correlated_suspects', 0)}")
                print(f"   • Cross Patterns: {corr_data.get('cross_patterns', 0)}")
                print(f"   • Evidence Chains: {corr_data.get('evidence_chains', 0)}")
        
        # Run key analyses
        print("\n" + "=" * 80)
        print("RUNNING KEY ANALYSES")
        print("=" * 80)
        
        analyses = [
            {
                'title': '🎯 RISK ASSESSMENT',
                'query': (
                    "Analyze all suspects and provide a risk assessment. "
                    "Include: 1) Top 3 highest risk suspects, "
                    "2) Key risk indicators from CDR and IPDR, "
                    "3) Evidence summary for each high-risk suspect"
                )
            },
            {
                'title': '🔍 SURVEILLANCE EVASION',
                'query': (
                    "Detect surveillance evasion techniques including: "
                    "1) Device switching (multiple IMEIs), "
                    "2) Encrypted communication usage, "
                    "3) Communication silence periods, "
                    "4) Unusual patterns indicating counter-surveillance"
                )
            },
            {
                'title': '🔗 COMMUNICATION PATTERNS',
                'query': (
                    "Analyze communication patterns across CDR and IPDR. Find: "
                    "1) Voice calls followed by encrypted app usage, "
                    "2) Data activity during call silence periods, "
                    "3) Coordination patterns between suspects, "
                    "4) Unusual timing patterns"
                )
            },
            {
                'title': '📱 DEVICE INTELLIGENCE',
                'query': (
                    "Analyze device-related intelligence including: "
                    "1) Device switching patterns (IMEI changes), "
                    "2) Burner phone indicators, "
                    "3) Device sharing or cloning, "
                    "4) Unusual device behaviors"
                )
            }
        ]
        
        # Run each analysis
        for analysis in analyses:
            print(f"\n{analysis['title']}")
            print("-" * 60)
            
            try:
                result = agent.analyze(analysis['query'])
                print(result)
            except Exception as e:
                print(f"❌ Error in analysis: {str(e)}")
                logger.error(f"Analysis error: {str(e)}", exc_info=True)
        
        # Crime scene simulation (without actual tower dumps)
        print("\n" + "=" * 80)
        print("CRIME SCENE SIMULATION")
        print("=" * 80)
        
        print("\n🚨 Simulating Bank Robbery Analysis")
        print("-" * 60)
        
        crime_result = agent.analyze(
            "Simulate a crime scene analysis for a bank robbery. "
            "Based on available CDR and IPDR data, identify: "
            "1) Suspects with suspicious activity patterns, "
            "2) Communication bursts that could indicate coordination, "
            "3) Encrypted communications before/after suspicious times, "
            "4) Evidence chains that could link suspects to criminal activity"
        )
        print(crime_result)
        
        # Generate comprehensive report
        print("\n" + "=" * 80)
        print("GENERATING REPORT")
        print("=" * 80)
        
        # Create reports directory
        reports_dir = Path("reports")
        reports_dir.mkdir(exist_ok=True)
        
        # Generate timestamp for unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = reports_dir / f"integrated_analysis_{timestamp}.md"
        
        print(f"\n📝 Generating comprehensive report...")
        try:
            report = agent.generate_comprehensive_report(output_file=report_file)
            print(f"✅ Report saved to: {report_file}")
            print(f"   File size: {report_file.stat().st_size:,} bytes")
        except Exception as e:
            print(f"❌ Error generating report: {str(e)}")
            logger.error(f"Report generation error: {str(e)}", exc_info=True)
        
        # Summary
        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE")
        print("=" * 80)
        print(f"Analysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\n📋 Next Steps:")
        print("1. Review the generated report in the reports/ directory")
        print("2. Run interactive mode for specific queries:")
        print("   python interactive_integrated.py")
        print("3. Load actual tower dump data when available for location analysis")
        print("4. Use individual component tests for detailed analysis")
        
    except Exception as e:
        print(f"\n❌ Critical Error: {str(e)}")
        logger.exception("Critical error in integrated analysis")
        print("\n💡 Troubleshooting:")
        print("1. Check if API key is valid")
        print("2. Verify data files exist in ../CDRs and ../IPDR directories")
        print("3. Check integrated_analysis.log for detailed error information")

if __name__ == "__main__":
    main()