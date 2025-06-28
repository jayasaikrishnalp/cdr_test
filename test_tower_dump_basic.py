#!/usr/bin/env python
"""
Basic test for tower dump functionality without requiring API key
"""

import os
import sys
from pathlib import Path
from rich.console import Console
from rich.table import Table
from loguru import logger

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from tower_dump_processors.tower_dump_loader import TowerDumpLoader
from tower_dump_processors.data_validator import TowerDumpValidator
from tower_dump_processors.tower_database import TowerDatabase

console = Console()

def test_tower_dump_loading():
    """Test tower dump data loading and validation"""
    
    console.print("[bold cyan]Tower Dump Basic Functionality Test[/bold cyan]")
    console.print("=" * 60)
    
    try:
        # Initialize components
        console.print("\n[yellow]Initializing tower dump components...[/yellow]")
        loader = TowerDumpLoader()
        validator = TowerDumpValidator()
        tower_db = TowerDatabase()
        
        # Load tower dump data
        console.print("\n[yellow]Loading tower dump data...[/yellow]")
        tower_files_dir = Path("../../Tower Dumps")
        
        if not tower_files_dir.exists():
            console.print("[red]Tower Dumps directory not found![/red]")
            return
        
        # Get all Excel files
        tower_files = list(tower_files_dir.glob("*.xlsx"))
        console.print(f"Found {len(tower_files)} tower dump files")
        
        # Load files
        loaded_data = {}
        for file_path in tower_files[:3]:  # Load first 3 files for testing
            console.print(f"\nLoading: {file_path.name}")
            
            try:
                df = loader.load_tower_dump(str(file_path))
                loaded_data[file_path.stem] = df
                console.print(f"[green]✓ Loaded {len(df)} records[/green]")
                
                # Validate data
                validation = validator.validate_dump(df)
                if validation['is_valid']:
                    console.print(f"[green]✓ Data validation passed[/green]")
                else:
                    console.print(f"[yellow]⚠ Validation issues: {validation['issues']}[/yellow]")
            except Exception as e:
                console.print(f"[red]✗ Failed to load: {str(e)}[/red]")
        
        # Display summary
        if loaded_data:
            console.print("\n[bold]TOWER DUMP SUMMARY:[/bold]")
            
            # Create summary table
            table = Table(title="Loaded Tower Dumps")
            table.add_column("Location", style="cyan")
            table.add_column("Records", style="green")
            table.add_column("Devices", style="yellow")
            table.add_column("Date Range", style="magenta")
            
            for location, df in loaded_data.items():
                records = len(df)
                devices = df['mobile_number'].nunique() if 'mobile_number' in df.columns else 0
                
                if 'timestamp' in df.columns:
                    date_range = f"{df['timestamp'].min()} to {df['timestamp'].max()}"
                else:
                    date_range = "N/A"
                
                table.add_row(location, str(records), str(devices), date_range)
            
            console.print(table)
            
            # Analyze first tower dump
            first_location = list(loaded_data.keys())[0]
            first_df = loaded_data[first_location]
            
            console.print(f"\n[bold]DETAILED ANALYSIS - {first_location}:[/bold]")
            
            # Device analysis
            if 'mobile_number' in first_df.columns:
                device_counts = first_df['mobile_number'].value_counts()
                console.print(f"\n[yellow]Top 5 Most Active Devices:[/yellow]")
                for device, count in device_counts.head().items():
                    console.print(f"  {device}: {count} records")
            
            # Tower analysis
            if 'tower_id' in first_df.columns:
                tower_counts = first_df['tower_id'].value_counts()
                console.print(f"\n[yellow]Top 5 Most Active Towers:[/yellow]")
                for tower, count in tower_counts.head().items():
                    console.print(f"  Tower {tower}: {count} connections")
            
            # Time analysis
            if 'timestamp' in first_df.columns:
                first_df['hour'] = first_df['timestamp'].dt.hour
                hourly = first_df['hour'].value_counts().sort_index()
                
                console.print(f"\n[yellow]Activity by Hour:[/yellow]")
                peak_hour = hourly.idxmax()
                console.print(f"  Peak hour: {peak_hour}:00 ({hourly[peak_hour]} records)")
                
                # Odd hour activity
                odd_hours = first_df[first_df['hour'].between(0, 5)]
                if len(odd_hours) > 0:
                    odd_devices = odd_hours['mobile_number'].nunique()
                    console.print(f"  [red]Odd hour activity: {len(odd_hours)} records from {odd_devices} devices[/red]")
            
            # Device switching detection
            if 'imei' in first_df.columns:
                multi_imei = first_df.groupby('mobile_number')['imei'].nunique()
                switchers = multi_imei[multi_imei > 1]
                if len(switchers) > 0:
                    console.print(f"\n[red]⚠️ Device Switching Detected:[/red]")
                    for device, imei_count in switchers.head().items():
                        console.print(f"  {device}: {imei_count} different IMEIs")
            
            console.print("\n[green]✓ Tower dump analysis completed successfully![/green]")
            
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_tower_dump_loading()