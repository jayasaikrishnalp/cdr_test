"""
Test script for integrated CDR-IPDR-Tower Dump analysis
"""

import os
import sys
from pathlib import Path
from loguru import logger

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from integrated_agent.integrated_agent_v2 import EnhancedIntegratedAgent
from tower_dump_agent import TowerDumpAgent

def test_tower_dump_integration():
    """Test the integrated system with tower dump analysis"""
    
    # Initialize agent
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        logger.error("Please set OPENROUTER_API_KEY environment variable")
        return
    
    logger.info("Initializing Enhanced Integrated Agent...")
    agent = EnhancedIntegratedAgent(api_key=api_key)
    
    # Load sample data
    logger.info("\n" + "="*80)
    logger.info("LOADING DATA")
    logger.info("="*80)
    
    # Load CDR data
    cdr_dir = Path("../CDRs")
    cdr_files = list(cdr_dir.glob("*.xlsx"))[:3]  # Load first 3 CDR files
    
    # Load IPDR data
    ipdr_dir = Path("../IPDR")
    ipdr_files = list(ipdr_dir.glob("*.xlsx"))[:3]  # Load first 3 IPDR files
    
    # For demo, we'll simulate tower dump data since we don't have actual files
    # In real usage, you would load actual tower dump CSV/Excel files
    tower_dump_files = []  # Would be list of tower dump file paths
    
    # Load all data
    load_result = agent.load_all_data(
        cdr_files=[str(f) for f in cdr_files] if cdr_files else None,
        ipdr_files=[str(f) for f in ipdr_files] if ipdr_files else None,
        tower_dump_files=tower_dump_files if tower_dump_files else None
    )
    
    logger.info(f"\nData Loading Summary: {load_result['summary']}")
    
    # Perform correlation
    logger.info("\n" + "="*80)
    logger.info("CORRELATING DATA")
    logger.info("="*80)
    
    correlation_result = agent.correlate_all_data()
    logger.info(f"Correlation Results: {correlation_result}")
    
    # Example analyses
    logger.info("\n" + "="*80)
    logger.info("EXAMPLE ANALYSES")
    logger.info("="*80)
    
    # 1. Crime scene analysis (simulated)
    logger.info("\n1. Crime Scene Analysis")
    logger.info("-" * 40)
    crime_analysis = agent.analyze_crime_scene(
        crime_location="Bank of India, MG Road",
        crime_time="2024-01-15 14:30:00",
        radius_km=1.0
    )
    logger.info(crime_analysis)
    
    # 2. Cross-data pattern analysis
    logger.info("\n2. Cross-Data Pattern Analysis")
    logger.info("-" * 40)
    pattern_analysis = agent.analyze(
        "Identify patterns across CDR and IPDR data. "
        "Focus on encrypted communications after voice calls, "
        "data usage during odd hours, and suspicious coordination patterns."
    )
    logger.info(pattern_analysis)
    
    # 3. Surveillance evasion detection
    logger.info("\n3. Surveillance Evasion Detection")
    logger.info("-" * 40)
    evasion_analysis = agent.detect_surveillance_evasion()
    logger.info(evasion_analysis)
    
    # 4. Network analysis for specific suspect
    if agent.cdr_data:
        suspect = list(agent.cdr_data.keys())[0]
        logger.info(f"\n4. Network Analysis for {suspect}")
        logger.info("-" * 40)
        network_analysis = agent.find_network_connections(suspect)
        logger.info(network_analysis)
    
    # 5. Integrated risk assessment
    logger.info("\n5. Integrated Risk Assessment")
    logger.info("-" * 40)
    risk_assessment = agent.analyze(
        "Provide integrated risk assessment of all suspects. "
        "Combine CDR patterns, IPDR behaviors, and any location intelligence. "
        "Rank top 5 suspects with evidence summary."
    )
    logger.info(risk_assessment)
    
    # Generate comprehensive report
    logger.info("\n" + "="*80)
    logger.info("GENERATING COMPREHENSIVE REPORT")
    logger.info("="*80)
    
    report_path = Path("reports/integrated_intelligence_report.md")
    report_path.parent.mkdir(exist_ok=True)
    
    report = agent.generate_comprehensive_report(output_file=report_path)
    logger.info(f"Report generated at: {report_path}")
    
    # Demo tower dump specific analysis (if we had tower data)
    logger.info("\n" + "="*80)
    logger.info("TOWER DUMP SPECIFIC FEATURES (Demo)")
    logger.info("="*80)
    
    # These would work with actual tower dump data
    demo_queries = [
        "What devices were present at crime scenes based on tower connections?",
        "Identify one-time visitors during suspicious time windows",
        "Detect impossible travel patterns indicating device cloning",
        "Find devices that were present but made no calls (silent devices)",
        "Analyze movement patterns and identify vehicle-based movements"
    ]
    
    for query in demo_queries[:2]:  # Run first 2 queries
        logger.info(f"\nQuery: {query}")
        logger.info("-" * 40)
        result = agent.analyze(query)
        logger.info(result)
    
    logger.info("\n" + "="*80)
    logger.info("INTEGRATION TEST COMPLETE")
    logger.info("="*80)

def test_tower_dump_standalone():
    """Test tower dump agent standalone"""
    
    logger.info("\n" + "="*80)
    logger.info("TESTING TOWER DUMP AGENT STANDALONE")
    logger.info("="*80)
    
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        logger.error("Please set OPENROUTER_API_KEY environment variable")
        return
    
    # Initialize tower dump agent
    tower_agent = TowerDumpAgent(api_key=api_key)
    
    # Example queries (would work with actual tower dump data)
    queries = [
        "Explain how tower dump analysis helps in criminal investigations",
        "What patterns should investigators look for in tower dump data?",
        "How can tower dumps help identify suspects at crime scenes?",
        "What is the significance of one-time visitors in tower dump analysis?",
        "How do you detect device cloning using tower dump data?"
    ]
    
    for query in queries[:3]:  # Run first 3 queries
        logger.info(f"\nQuery: {query}")
        logger.info("-" * 40)
        result = tower_agent.analyze(query)
        logger.info(result)

if __name__ == "__main__":
    # Test integrated system
    test_tower_dump_integration()
    
    # Test tower dump standalone
    test_tower_dump_standalone()