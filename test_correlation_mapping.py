"""
Test CDR-IPDR correlation with name mapping
"""

import os
from dotenv import load_dotenv
from integrated_agent.integrated_agent import IntegratedIntelligenceAgent
from integrated_agent.correlation_tools.cdr_ipdr_correlator import CDRIPDRCorrelator
from rich.console import Console
import re

# Load environment variables
load_dotenv()

console = Console()

def extract_phone_number(name: str) -> str:
    """Extract phone number from suspect name"""
    # Match 10-digit phone numbers
    match = re.search(r'\d{10}', name)
    return match.group() if match else None

def map_cdr_to_ipdr(cdr_data: dict, ipdr_data: dict) -> dict:
    """Map CDR suspects to IPDR suspects based on phone numbers"""
    mapping = {}
    
    # Extract phone numbers from CDR suspects
    cdr_phones = {}
    for cdr_name in cdr_data.keys():
        phone = extract_phone_number(cdr_name)
        if phone:
            cdr_phones[phone] = cdr_name
            console.print(f"[yellow]CDR: {cdr_name} → {phone}[/yellow]")
    
    # Match with IPDR suspects (IPDR has 91 prefix)
    for ipdr_name in ipdr_data.keys():
        # Extract full number including 91 prefix
        full_match = re.search(r'91(\d{10})', ipdr_name)
        if full_match:
            phone = full_match.group(1)  # Get just the 10-digit number
            if phone in cdr_phones:
                mapping[cdr_phones[phone]] = ipdr_name
                console.print(f"[green]Mapped: {cdr_phones[phone]} → {ipdr_name}[/green]")
    
    return mapping

def test_correlation_with_mapping():
    """Test correlation with proper name mapping"""
    
    try:
        # Initialize integrated agent
        console.print("[yellow]Initializing integrated agent...[/yellow]")
        agent = IntegratedIntelligenceAgent()
        
        # Load data
        console.print("\n[yellow]Loading CDR and IPDR data...[/yellow]")
        load_result = agent.load_all_data()
        
        # Create mapping
        console.print("\n[yellow]Creating CDR-IPDR name mapping...[/yellow]")
        mapping = map_cdr_to_ipdr(agent.cdr_data, agent.ipdr_data)
        
        console.print(f"\n[cyan]Mapped {len(mapping)} suspects[/cyan]")
        
        if mapping:
            # Create normalized data for correlation
            console.print("\n[yellow]Normalizing data for correlation...[/yellow]")
            
            # Create IPDR data with CDR names
            normalized_ipdr = {}
            for cdr_name, ipdr_name in mapping.items():
                normalized_ipdr[cdr_name] = agent.ipdr_data[ipdr_name]
            
            # Run correlation with normalized data
            console.print("\n[yellow]Running CDR-IPDR correlation...[/yellow]")
            correlator = CDRIPDRCorrelator()
            correlation_results = correlator.correlate_suspects(
                agent.cdr_data,
                normalized_ipdr
            )
            
            # Display results
            console.print("\n[bold]Correlation Results:[/bold]")
            console.print(f"Correlated suspects: {len(correlation_results['suspect_correlations'])}")
            
            # Show some correlation details
            for suspect, patterns in list(correlation_results['suspect_correlations'].items())[:3]:
                console.print(f"\n[cyan]{suspect}:[/cyan]")
                if patterns.get('call_to_data'):
                    console.print(f"  - Call-to-data patterns: {len(patterns['call_to_data'])}")
                if patterns.get('data_during_silence'):
                    console.print(f"  - Data during silence: {len(patterns['data_during_silence'])}")
            
            # Generate correlation report
            report = correlator.generate_correlation_report(correlation_results)
            console.print("\n[bold]Correlation Summary:[/bold]")
            console.print(report[:500] + "..." if len(report) > 500 else report)
            
        console.print("\n[green]✓ Correlation test completed![/green]")
        
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_correlation_with_mapping()