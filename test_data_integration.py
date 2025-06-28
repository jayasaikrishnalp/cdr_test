#!/usr/bin/env python
"""
Test data integration between CDR, IPDR, and Tower Dump
"""

import os
import sys
from pathlib import Path
from rich.console import Console
from rich.table import Table
from loguru import logger
import pandas as pd

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from processors.cdr_loader import CDRLoader
from ipdr_processors.ipdr_loader import IPDRLoader
from tower_dump_processors.tower_dump_loader import TowerDumpLoader
from tower_analysis_tools.cross_reference_tool import CrossReferenceTool

console = Console()

def test_data_integration():
    """Test integration of CDR, IPDR, and Tower Dump data"""
    
    console.print("[bold cyan]DATA INTEGRATION TEST[/bold cyan]")
    console.print("=" * 80)
    
    try:
        # Load CDR data
        console.print("\n[yellow]Loading CDR data...[/yellow]")
        cdr_loader = CDRLoader()
        cdr_data = cdr_loader.load_cdrs()
        console.print(f"[green]✓ Loaded {len(cdr_data)} CDR suspects[/green]")
        
        # Show CDR files
        for name, df in cdr_data.items():
            console.print(f"  {name}: {len(df)} records")
        
        # Load IPDR data
        console.print("\n[yellow]Loading IPDR data...[/yellow]")
        ipdr_loader = IPDRLoader()
        ipdr_data = ipdr_loader.load_ipdrs()
        console.print(f"[green]✓ Loaded {len(ipdr_data)} IPDR suspects[/green]")
        
        # Show IPDR files
        for name, df in ipdr_data.items():
            console.print(f"  {name}: {len(df)} records")
        
        # Load Tower Dump data
        console.print("\n[yellow]Loading Tower Dump data...[/yellow]")
        tower_loader = TowerDumpLoader()
        tower_files_dir = Path("../../Tower Dumps")
        tower_data = {}
        
        for file_path in list(tower_files_dir.glob("*.xlsx"))[:3]:  # First 3 files
            console.print(f"Loading Tower Dump: {file_path.name}")
            df = tower_loader.load_tower_dump(str(file_path))
            tower_data[file_path.stem] = df
            console.print(f"  Loaded {len(df)} records")
        
        # Extract unique numbers from each source
        console.print("\n[bold]DATA SOURCE ANALYSIS:[/bold]")
        
        # CDR numbers
        cdr_numbers = set()
        for name, df in cdr_data.items():
            if 'a_party' in df.columns:
                cdr_numbers.update(df['a_party'].unique())
        
        # IPDR numbers (from suspect names)
        ipdr_numbers = set()
        for name in ipdr_data.keys():
            # Extract number from name like "Srinivas-9505341205"
            import re
            match = re.search(r'(\d{10})', name)
            if match:
                ipdr_numbers.add(match.group(1))
        
        # Tower dump numbers
        tower_numbers = set()
        for name, df in tower_data.items():
            if 'mobile_number' in df.columns:
                tower_numbers.update(df['mobile_number'].unique())
        
        console.print(f"\nUnique numbers found:")
        console.print(f"  CDR: {len(cdr_numbers)} numbers")
        console.print(f"  IPDR: {len(ipdr_numbers)} numbers")
        console.print(f"  Tower Dump: {len(tower_numbers)} numbers")
        
        # Find overlaps
        console.print(f"\n[bold]CROSS-REFERENCE ANALYSIS:[/bold]")
        
        # All three sources
        all_three = cdr_numbers & ipdr_numbers & tower_numbers
        console.print(f"  In all three sources: {len(all_three)} numbers")
        
        # Two sources
        cdr_ipdr = (cdr_numbers & ipdr_numbers) - all_three
        cdr_tower = (cdr_numbers & tower_numbers) - all_three
        ipdr_tower = (ipdr_numbers & tower_numbers) - all_three
        
        console.print(f"  CDR + IPDR only: {len(cdr_ipdr)} numbers")
        console.print(f"  CDR + Tower only: {len(cdr_tower)} numbers")
        console.print(f"  IPDR + Tower only: {len(ipdr_tower)} numbers")
        
        # Single source only
        cdr_only = cdr_numbers - ipdr_numbers - tower_numbers
        ipdr_only = ipdr_numbers - cdr_numbers - tower_numbers
        tower_only = tower_numbers - cdr_numbers - ipdr_numbers
        
        console.print(f"  CDR only: {len(cdr_only)} numbers")
        console.print(f"  IPDR only: {len(ipdr_only)} numbers")
        console.print(f"  Tower only: {len(tower_only)} numbers")
        
        # Show some examples from each category
        if all_three:
            console.print(f"\n[green]Numbers in all three sources:[/green]")
            for num in list(all_three)[:5]:
                console.print(f"  {num}")
        
        if tower_only:
            console.print(f"\n[yellow]Silent devices (Tower only):[/yellow]")
            for num in list(tower_only)[:5]:
                console.print(f"  {num}")
        
        # Analyze specific suspect if found in all sources
        if all_three:
            suspect_number = list(all_three)[0]
            console.print(f"\n[bold]DETAILED ANALYSIS FOR {suspect_number}:[/bold]")
            
            # CDR analysis
            for name, df in cdr_data.items():
                if 'a_party' in df.columns:
                    suspect_cdr = df[df['a_party'] == suspect_number]
                    if len(suspect_cdr) > 0:
                        console.print(f"\nCDR Activity:")
                        console.print(f"  Total calls: {len(suspect_cdr)}")
                        if 'datetime' in suspect_cdr.columns:
                            console.print(f"  Date range: {suspect_cdr['datetime'].min()} to {suspect_cdr['datetime'].max()}")
                        if 'b_party' in suspect_cdr.columns:
                            console.print(f"  Unique contacts: {suspect_cdr['b_party'].nunique()}")
                        break
            
            # Tower dump analysis
            for name, df in tower_data.items():
                if 'mobile_number' in df.columns:
                    suspect_tower = df[df['mobile_number'] == suspect_number]
                    if len(suspect_tower) > 0:
                        console.print(f"\nTower Dump Activity:")
                        console.print(f"  Total tower pings: {len(suspect_tower)}")
                        if 'tower_id' in suspect_tower.columns:
                            console.print(f"  Unique towers: {suspect_tower['tower_id'].nunique()}")
                        if 'timestamp' in suspect_tower.columns:
                            console.print(f"  Date range: {suspect_tower['timestamp'].min()} to {suspect_tower['timestamp'].max()}")
                        break
            
            # IPDR analysis
            for name, df in ipdr_data.items():
                if suspect_number in name:
                    console.print(f"\nIPDR Activity:")
                    console.print(f"  Total sessions: {len(df)}")
                    if 'total_data_volume' in df.columns:
                        total_data = df['total_data_volume'].sum() / (1024*1024)
                        console.print(f"  Total data: {total_data:.2f} MB")
                    if 'detected_app' in df.columns:
                        apps = df['detected_app'].value_counts()
                        console.print(f"  Top apps: {', '.join(apps.head(3).index.tolist())}")
                    break
        
        # Test cross-reference tool
        console.print("\n[bold]TESTING CROSS-REFERENCE TOOL:[/bold]")
        cross_ref_tool = CrossReferenceTool()
        cross_ref_tool.tower_dump_data = tower_data
        cross_ref_tool.cdr_data = cdr_data
        cross_ref_tool.ipdr_data = ipdr_data
        
        # Update cross-reference tool CDR data to fix column names
        fixed_cdr_data = {}
        for name, df in cdr_data.items():
            # Create a copy with standardized column names
            df_copy = df.copy()
            if 'A PARTY' in df.columns and 'a_party' not in df.columns:
                df_copy['a_party'] = df_copy['A PARTY'].astype(str)
            if 'B PARTY' in df.columns and 'b_party' not in df.columns:
                df_copy['b_party'] = df_copy['B PARTY'].astype(str)
            fixed_cdr_data[name] = df_copy
        
        cross_ref_tool.cdr_data = fixed_cdr_data
        
        # Find silent devices
        result = cross_ref_tool._run("find silent devices")
        console.print("\n[cyan]Silent Device Analysis:[/cyan]")
        console.print(result)
        
        console.print("\n[green]✓ Data integration test completed successfully![/green]")
        
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_data_integration()