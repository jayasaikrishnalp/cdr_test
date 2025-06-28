#!/usr/bin/env python3
"""
Simple integrated analysis test that bypasses agent and shows raw data
"""

import os
import sys
from pathlib import Path
from rich.console import Console
from rich.table import Table

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from processors.cdr_loader import CDRLoader
from ipdr_processors.ipdr_loader import IPDRLoader

console = Console()

def main():
    """Run simple integrated analysis"""
    
    console.print("[bold cyan]Integrated CDR-IPDR Analysis (Direct)[/bold cyan]")
    console.print("=" * 60)
    
    # Load CDR data
    console.print("\n[yellow]Loading CDR data...[/yellow]")
    cdr_loader = CDRLoader()
    cdr_data = cdr_loader.load_cdrs()
    
    console.print(f"[green]✓ Loaded {len(cdr_data)} CDR suspects[/green]")
    
    # Load IPDR data
    console.print("\n[yellow]Loading IPDR data...[/yellow]")
    ipdr_loader = IPDRLoader()
    ipdr_data = ipdr_loader.load_ipdrs()
    
    console.print(f"[green]✓ Loaded {len(ipdr_data)} IPDR suspects[/green]")
    
    # Create mapping between CDR and IPDR
    console.print("\n[yellow]Creating CDR-IPDR mapping...[/yellow]")
    
    # Extract phone numbers from CDR names
    cdr_phones = {}
    for cdr_name in cdr_data.keys():
        # Extract 10-digit phone number
        import re
        match = re.search(r'\d{10}', cdr_name)
        if match:
            phone = match.group()
            cdr_phones[phone] = cdr_name
    
    # Map to IPDR (which has 91 prefix)
    mapping = {}
    for phone, cdr_name in cdr_phones.items():
        ipdr_prefix = f"91{phone}"
        for ipdr_name in ipdr_data.keys():
            if ipdr_prefix in ipdr_name:
                mapping[cdr_name] = ipdr_name
                break
    
    console.print(f"[cyan]Mapped {len(mapping)} suspects between CDR and IPDR[/cyan]")
    
    # Display mapping
    if mapping:
        table = Table(title="CDR-IPDR Mapping")
        table.add_column("CDR Suspect", style="cyan")
        table.add_column("IPDR Suspect", style="green")
        
        for cdr, ipdr in list(mapping.items())[:5]:
            table.add_row(cdr, ipdr)
        
        console.print(table)
    
    # Basic risk analysis
    console.print("\n[bold]Risk Summary:[/bold]")
    
    # CDR Analysis
    console.print("\n[yellow]CDR Risk Indicators:[/yellow]")
    for suspect, df in list(cdr_data.items())[:3]:
        total_calls = len(df)
        
        # Check for voice-only behavior
        voice_calls = len(df[df['call_type'] == 'Voice']) if 'call_type' in df.columns else 0
        voice_pct = (voice_calls / total_calls * 100) if total_calls > 0 else 0
        
        # Check IMEIs
        imei_count = df['imei'].nunique() if 'imei' in df.columns else 0
        
        console.print(f"\n[cyan]{suspect}:[/cyan]")
        console.print(f"  Total Communications: {total_calls}")
        console.print(f"  Voice Percentage: {voice_pct:.1f}%")
        console.print(f"  Unique IMEIs: {imei_count}")
        
        if voice_pct == 100:
            console.print("  [red]⚠️ 100% Voice-only communication[/red]")
        if imei_count >= 2:
            console.print(f"  [red]⚠️ Device switching detected ({imei_count} IMEIs)[/red]")
    
    # IPDR Analysis
    console.print("\n[yellow]IPDR Risk Indicators:[/yellow]")
    for suspect, df in list(ipdr_data.items())[:3]:
        total_sessions = len(df)
        
        # Check encryption
        encrypted = df[df['is_encrypted'] == True] if 'is_encrypted' in df.columns else pd.DataFrame()
        encryption_pct = (len(encrypted) / total_sessions * 100) if total_sessions > 0 else 0
        
        # Check apps
        if 'detected_app' in df.columns:
            apps = df['detected_app'].value_counts().head(3)
        else:
            apps = {}
        
        console.print(f"\n[cyan]{suspect}:[/cyan]")
        console.print(f"  Total Sessions: {total_sessions}")
        console.print(f"  Encryption Rate: {encryption_pct:.1f}%")
        
        if apps is not None and len(apps) > 0:
            console.print("  Top Apps:")
            for app, count in apps.items():
                console.print(f"    - {app}: {count} sessions")
    
    console.print("\n[green]✓ Analysis complete![/green]")

if __name__ == "__main__":
    import pandas as pd
    main()