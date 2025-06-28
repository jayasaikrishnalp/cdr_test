"""
Test CDR-IPDR correlation with fixed data preparation
"""

import os
from dotenv import load_dotenv
from integrated_agent.integrated_agent import IntegratedIntelligenceAgent
from integrated_agent.correlation_tools.cdr_ipdr_correlator import CDRIPDRCorrelator
from rich.console import Console
import pandas as pd
import re

# Load environment variables
load_dotenv()

console = Console()

def fix_ipdr_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Fix IPDR datetime columns for correlation"""
    df = df.copy()
    
    # Create start_time from date and time columns
    if 'Start Date of Public IP Address allocation (dd/mm/yyyy)' in df.columns:
        date_col = 'Start Date of Public IP Address allocation (dd/mm/yyyy)'
        time_col = 'IST Start Time of Public IP address allocation (hh:mm:ss)'
        
        # Combine date and time
        df['start_datetime_str'] = df[date_col].astype(str) + ' ' + df[time_col].astype(str)
        df['start_time'] = pd.to_datetime(df['start_datetime_str'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        
    # Create end_time from date and time columns
    if 'End Date of Public IP address allocation (dd/mm/yyyy)' in df.columns:
        date_col = 'End Date of Public IP address allocation (dd/mm/yyyy)'
        time_col = 'IST End Time of Public IP address allocation (hh:mm:ss)'
        
        # Combine date and time
        df['end_datetime_str'] = df[date_col].astype(str) + ' ' + df[time_col].astype(str)
        df['end_time'] = pd.to_datetime(df['end_datetime_str'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    
    return df

def map_cdr_to_ipdr(cdr_data: dict, ipdr_data: dict) -> dict:
    """Map CDR suspects to IPDR suspects based on phone numbers"""
    mapping = {}
    
    # Extract phone numbers from CDR suspects
    cdr_phones = {}
    for cdr_name in cdr_data.keys():
        match = re.search(r'\d{10}', cdr_name)
        if match:
            cdr_phones[match.group()] = cdr_name
    
    # Match with IPDR suspects (IPDR has 91 prefix)
    for ipdr_name in ipdr_data.keys():
        full_match = re.search(r'91(\d{10})', ipdr_name)
        if full_match:
            phone = full_match.group(1)
            if phone in cdr_phones:
                mapping[cdr_phones[phone]] = ipdr_name
                console.print(f"[green]Mapped: {cdr_phones[phone]} → {ipdr_name}[/green]")
    
    return mapping

def test_correlation_fixed():
    """Test correlation with fixed datetime columns"""
    
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
            # Fix IPDR data and create normalized data for correlation
            console.print("\n[yellow]Fixing IPDR datetime columns...[/yellow]")
            
            normalized_ipdr = {}
            for cdr_name, ipdr_name in mapping.items():
                # Fix datetime columns
                fixed_df = fix_ipdr_datetime_columns(agent.ipdr_data[ipdr_name])
                normalized_ipdr[cdr_name] = fixed_df
            
            # Run correlation with fixed data
            console.print("\n[yellow]Running CDR-IPDR correlation...[/yellow]")
            correlator = CDRIPDRCorrelator()
            
            # Only correlate suspects that have both CDR and IPDR data
            cdr_subset = {k: v for k, v in agent.cdr_data.items() if k in normalized_ipdr}
            
            correlation_results = correlator.correlate_suspects(
                cdr_subset,
                normalized_ipdr
            )
            
            # Display results
            console.print("\n[bold]Correlation Results:[/bold]")
            console.print(f"Correlated suspects: {len(correlation_results['suspect_correlations'])}")
            console.print(f"Cross-suspect patterns: {len(correlation_results['cross_suspect_patterns'])}")
            console.print(f"Evidence chains: {len(correlation_results['evidence_chains'])}")
            
            # Show some correlation details
            for suspect, patterns in list(correlation_results['suspect_correlations'].items())[:2]:
                console.print(f"\n[cyan]{suspect}:[/cyan]")
                if patterns.get('call_to_data'):
                    console.print(f"  - Call-to-data patterns: {len(patterns['call_to_data'])}")
                    # Show first pattern
                    if patterns['call_to_data']:
                        p = patterns['call_to_data'][0]
                        console.print(f"    Example: Call at {p['call_time']} → {p['data_app']} after {p['gap_seconds']:.0f}s")
                        
                if patterns.get('data_during_silence'):
                    console.print(f"  - Data during silence: {len(patterns['data_during_silence'])}")
                    if patterns['data_during_silence']:
                        p = patterns['data_during_silence'][0]
                        console.print(f"    Example: {p['silence_hours']:.1f}h silence with {p['data_volume_mb']:.1f}MB data")
            
            # Show evidence chains
            if correlation_results['evidence_chains']:
                console.print("\n[bold]Evidence Chains Found:[/bold]")
                for chain in correlation_results['evidence_chains'][:2]:
                    console.print(f"  - {chain['description']}")
                    console.print(f"    Risk: {chain['risk_level']}")
            
            # Generate brief report
            report = correlator.generate_correlation_report(correlation_results)
            console.print("\n[bold]Correlation Summary:[/bold]")
            console.print(report[:800] + "..." if len(report) > 800 else report)
            
        console.print("\n[green]✓ Correlation test completed successfully![/green]")
        
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_correlation_fixed()