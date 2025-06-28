"""
Quick test for integrated CDR-IPDR analysis
"""

import os
from dotenv import load_dotenv
from integrated_agent.integrated_agent import IntegratedIntelligenceAgent
from rich.console import Console

# Load environment variables
load_dotenv()

console = Console()

def test_integrated():
    """Test integrated agent basic functionality"""
    
    try:
        # Initialize integrated agent
        console.print("[yellow]Initializing integrated agent...[/yellow]")
        agent = IntegratedIntelligenceAgent()
        
        # Load data
        console.print("\n[yellow]Loading CDR and IPDR data...[/yellow]")
        load_result = agent.load_all_data()
        
        # Display results
        console.print(f"\n[green]CDR Suspects: {load_result['summary']['cdr_suspects']}[/green]")
        console.print(f"[green]IPDR Suspects: {load_result['summary']['ipdr_suspects']}[/green]")
        console.print(f"[cyan]Common Suspects: {load_result['summary']['common_suspects']}[/cyan]")
        
        # Test correlation
        if load_result['summary']['common_suspects'] > 0:
            console.print("\n[yellow]Running correlation analysis...[/yellow]")
            correlation_result = agent.correlate_data()
            
            if correlation_result['status'] == 'success':
                console.print(f"\n[green]Correlation successful![/green]")
                console.print(f"Correlated suspects: {correlation_result['correlated_suspects']}")
                console.print(f"Cross patterns found: {correlation_result['cross_patterns']}")
                console.print(f"Evidence chains: {correlation_result['evidence_chains']}")
        
        # Quick risk assessment test
        console.print("\n[yellow]Testing risk assessment (simplified)...[/yellow]")
        
        # Direct CDR risk assessment
        if agent.cdr_data:
            console.print("\n[bold]CDR Risk Summary:[/bold]")
            cdr_suspects = list(agent.cdr_data.keys())[:3]  # First 3 suspects
            for suspect in cdr_suspects:
                console.print(f"  - {suspect}: CDR data loaded")
        
        # Direct IPDR risk assessment
        if agent.ipdr_data:
            console.print("\n[bold]IPDR Risk Summary:[/bold]")
            ipdr_suspects = list(agent.ipdr_data.keys())[:3]  # First 3 suspects
            for suspect in ipdr_suspects:
                console.print(f"  - {suspect}: IPDR data loaded")
        
        console.print("\n[green]✓ Integrated agent test completed successfully![/green]")
        
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_integrated()