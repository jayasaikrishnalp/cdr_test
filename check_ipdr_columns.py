"""
Check IPDR column names
"""

import os
from dotenv import load_dotenv
from ipdr_processors.ipdr_loader import IPDRLoader
from rich.console import Console

# Load environment variables
load_dotenv()

console = Console()

def check_ipdr_columns():
    """Check actual IPDR column names"""
    
    try:
        # Load IPDR data
        console.print("[yellow]Loading IPDR data...[/yellow]")
        loader = IPDRLoader()
        ipdr_data = loader.load_ipdrs()
        
        # Check columns for first suspect
        for suspect, df in list(ipdr_data.items())[:1]:
            console.print(f"\n[cyan]Columns for {suspect}:[/cyan]")
            console.print(list(df.columns))
            
            console.print(f"\n[cyan]Data types:[/cyan]")
            console.print(df.dtypes)
            
            console.print(f"\n[cyan]First few rows:[/cyan]")
            console.print(df.head(3))
            
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_ipdr_columns()