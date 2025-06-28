#!/usr/bin/env python3
"""
CDR Intelligence Agent - Main Entry Point
Command-line interface for criminal pattern analysis in CDR data
"""

import click
import sys
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from rich.markdown import Markdown
from loguru import logger
import warnings
from dotenv import load_dotenv
import os
import locale

# Set UTF-8 encoding for better Unicode handling
try:
    locale.setlocale(locale.LC_ALL, '')
except:
    pass
os.environ['PYTHONIOENCODING'] = 'utf-8'

# Load environment variables from .env file
load_dotenv()

# Suppress warnings
warnings.filterwarnings("ignore")

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from agent.cdr_agent import CDRIntelligenceAgent
from report.report_generator import ReportGenerator
from config import settings
from ipdr_agent import IPDRAgent
from integrated_agent.integrated_agent import IntegratedIntelligenceAgent

# Initialize Rich console
console = Console()

@click.group()
def cli():
    """Intelligence Agent - Criminal Pattern Analysis System for CDR and IPDR"""
    pass

@cli.command()
@click.option('--data-path', '-d', 
              default=None,
              help='Path to CDR Excel files directory')
@click.option('--files', '-f',
              default=None,
              help='Comma-separated list of CDR Excel files to analyze')
@click.option('--output', '-o', 
              default=None,
              help='Output file for report')
def analyze(data_path, files, output):
    """Run comprehensive CDR analysis on all suspects"""
    
    console.print("\n[bold cyan]CDR Intelligence Agent[/bold cyan]")
    console.print("Criminal Pattern Analysis System\n")
    
    try:
        # Initialize agent
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Initializing agent...", total=None)
            agent = CDRIntelligenceAgent()
            progress.update(task, description="Agent initialized")
        
        # Load CDR data
        console.print("\n[yellow]Loading CDR data...[/yellow]")
        
        # Parse file list if provided
        file_list = None
        if files:
            file_list = [f.strip() for f in files.split(',')]
            console.print(f"Loading specific files: {', '.join(file_list)}")
        
        load_result = agent.load_cdr_data(data_path, file_list)
        
        if load_result['status'] == 'error':
            console.print(f"[red]Error: {load_result['message']}[/red]")
            return
        
        # Display loaded files
        console.print(f"\n[green]✓ Loaded {load_result['files_loaded']} CDR files[/green]")
        
        # Show data summary
        if load_result.get('summary'):
            table = Table(title="CDR Data Summary")
            table.add_column("Suspect", style="cyan")
            table.add_column("Records", justify="right")
            table.add_column("Contacts", justify="right")
            table.add_column("Duration (s)", justify="right")
            
            for summary in load_result['summary']:
                table.add_row(
                    summary['suspect'],
                    str(summary['total_records']),
                    str(summary['unique_contacts']),
                    f"{summary['total_duration']:.0f}"
                )
            
            console.print(table)
        
        # Run analysis
        console.print("\n[yellow]Analyzing criminal patterns...[/yellow]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Running comprehensive analysis...", total=None)
            
            # Generate report using direct pattern analysis
            report_gen = ReportGenerator()
            report = report_gen.generate_comprehensive_report(agent.cdr_data)
            
            progress.update(task, description="Analysis complete")
        
        # Display report
        console.print("\n" + "="*80)
        console.print(report)
        console.print("="*80 + "\n")
        
        # Save report if output specified
        if output:
            report_path = report_gen.save_report(report, output)
            console.print(f"[green]✓ Report saved to: {report_path}[/green]")
        
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        logger.exception("Analysis failed")

@cli.command()
@click.option('--data-path', '-d', 
              default=None,
              help='Path to CDR Excel files directory')
@click.option('--files', '-f',
              default=None,
              help='Comma-separated list of CDR Excel files to analyze')
def interactive(data_path, files):
    """Start interactive analysis session"""
    
    console.print("\n[bold cyan]CDR Intelligence Agent - Interactive Mode[/bold cyan]\n")
    
    try:
        # Initialize agent
        console.print("[yellow]Initializing agent...[/yellow]")
        agent = CDRIntelligenceAgent()
        
        # Load CDR data
        # Parse file list if provided
        file_list = None
        if files:
            file_list = [f.strip() for f in files.split(',')]
            console.print(f"Loading specific files: {', '.join(file_list)}")
        
        load_result = agent.load_cdr_data(data_path, file_list)
        
        if load_result['status'] == 'error':
            console.print(f"[red]Error: {load_result['message']}[/red]")
            return
        
        console.print(f"[green]✓ Loaded {load_result['files_loaded']} CDR files[/green]")
        console.print(f"Suspects: {', '.join(load_result['suspects'])}\n")
        
        # Interactive loop
        console.print("[cyan]Enter queries to analyze CDR data. Type 'help' for examples or 'exit' to quit.[/cyan]\n")
        
        while True:
            try:
                query = console.input("[bold]Query>[/bold] ")
                
                if query.lower() in ['exit', 'quit', 'q']:
                    break
                
                if query.lower() == 'help':
                    show_help()
                    continue
                
                if query.lower() == 'clear':
                    agent.reset_memory()
                    console.print("[green]Memory cleared[/green]")
                    continue
                
                # Run analysis
                console.print("\n[yellow]Analyzing...[/yellow]")
                result = agent.analyze(query)
                
                # Display result
                console.print("\n[bold]Analysis Result:[/bold]")
                console.print(Panel(result, title="CDR Intelligence", border_style="blue"))
                console.print()
                
            except KeyboardInterrupt:
                console.print("\n[yellow]Interrupted[/yellow]")
                break
            except Exception as e:
                console.print(f"[red]Error: {str(e)}[/red]")
        
        console.print("\n[cyan]Thank you for using CDR Intelligence Agent![/cyan]")
        
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        logger.exception("Interactive mode failed")

def show_help():
    """Show help information"""
    help_text = """
## Example Queries:

### Device Analysis
- "Analyze device switching patterns for all suspects"
- "Check IMEI count for Peer basha"
- "Find suspects with multiple devices"

### Temporal Analysis
- "Analyze odd hour activity for all suspects"
- "Check for call bursts"
- "Find pattern day activity"

### Communication Analysis
- "Analyze voice vs SMS patterns"
- "Find voice-only suspects"
- "Check contact frequencies"

### Network Analysis
- "Find connections between suspects"
- "Analyze common contacts"
- "Identify network hierarchy"

### Risk Assessment
- "Calculate risk scores for all suspects"
- "Rank suspects by risk level"
- "Generate comprehensive report"

### Other Commands
- `clear` - Clear conversation memory
- `help` - Show this help
- `exit` - Exit interactive mode
"""
    console.print(Markdown(help_text))

@cli.command()
def validate():
    """Validate CDR files against specification"""
    
    console.print("\n[bold cyan]CDR Data Validation[/bold cyan]\n")
    
    try:
        from processors.data_validator import CDRValidator
        from processors.cdr_loader import CDRLoader
        
        loader = CDRLoader()
        validator = CDRValidator()
        
        # Load files
        cdr_data = loader.load_all_cdrs()
        
        if not cdr_data:
            console.print("[red]No CDR files found[/red]")
            return
        
        # Validate each file
        for suspect, df in cdr_data.items():
            console.print(f"\n[yellow]Validating {suspect}...[/yellow]")
            
            validation = validator.validate_dataframe(df, suspect)
            
            if validation['is_valid']:
                console.print(f"[green]✓ Valid[/green] - Quality Score: {validation['data_quality_score']:.1f}%")
            else:
                console.print(f"[red]✗ Invalid[/red] - {', '.join(validation['errors'])}")
            
            # Show field validation details
            if 'field_validation' in validation:
                for field, details in validation['field_validation'].items():
                    if 'validity_percentage' in details and details['validity_percentage'] < 100:
                        console.print(f"  {field}: {details['validity_percentage']:.1f}% valid")
        
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        logger.exception("Validation failed")

@cli.command()
def config():
    """Show current configuration"""
    
    console.print("\n[bold cyan]CDR Intelligence Agent Configuration[/bold cyan]\n")
    
    table = Table(show_header=False)
    table.add_column("Setting", style="cyan")
    table.add_column("Value")
    
    # Show key configuration values
    table.add_row("Data Path", str(settings.cdr_data_path))
    table.add_row("Output Path", str(settings.output_path))
    table.add_row("OpenRouter Model", settings.openrouter_model)
    table.add_row("Odd Hour Range", f"{settings.odd_hour_start}:00 - {settings.odd_hour_end}:00")
    table.add_row("High Risk IMEI Count", str(settings.high_risk_imei_count))
    table.add_row("Pattern Days", ", ".join(settings.pattern_days))
    
    console.print(table)
    
    # Show risk weights
    console.print("\n[bold]Risk Scoring Weights:[/bold]")
    for factor, weight in settings.risk_weights.items():
        console.print(f"  {factor}: {weight*100:.0f}%")

@cli.command('ipdr-analyze')
@click.option('--data-path', '-d', 
              default=None,
              help='Path to IPDR Excel files directory')
@click.option('--files', '-f',
              default=None,
              help='Comma-separated list of IPDR Excel files to analyze')
@click.option('--output', '-o', 
              default=None,
              help='Output file for report')
def ipdr_analyze(data_path, files, output):
    """Run comprehensive IPDR analysis on suspects"""
    
    console.print("\n[bold cyan]IPDR Intelligence Agent[/bold cyan]")
    console.print("Internet Protocol Detail Record Analysis\n")
    
    try:
        # Initialize agent
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Initializing IPDR agent...", total=None)
            agent = IPDRAgent()
            progress.update(task, description="IPDR agent initialized")
        
        # Load IPDR data
        console.print("\n[yellow]Loading IPDR data...[/yellow]")
        
        # Parse file list if provided
        file_list = None
        if files:
            file_list = [f.strip() for f in files.split(',')]
            console.print(f"Loading specific files: {', '.join(file_list)}")
        
        load_result = agent.load_ipdr_data(file_list)
        
        if load_result['status'] == 'error':
            console.print(f"[red]Error: {load_result['message']}[/red]")
            return
        
        # Display loaded files
        console.print(f"\n[green]✓ Loaded {load_result['loaded_count']} IPDR files[/green]")
        console.print(f"Suspects: {', '.join(load_result['suspects'])}")
        
        # Show summary
        if load_result.get('summary'):
            table = Table(title="IPDR Data Summary")
            table.add_column("Suspect", style="cyan")
            table.add_column("Sessions", justify="right")
            table.add_column("Data (MB)", justify="right")
            table.add_column("Encrypted %", justify="right")
            
            for summary in load_result['summary']:
                table.add_row(
                    summary['suspect'],
                    str(summary['total_sessions']),
                    f"{summary['total_data_mb']:.1f}",
                    f"{summary['encrypted_percentage']:.1f}%"
                )
            
            console.print(table)
        
        # Run analysis
        console.print("\n[yellow]Analyzing IPDR patterns...[/yellow]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Running IPDR analysis...", total=None)
            
            # Generate risk assessment
            risk_assessment = agent.get_risk_summary()
            
            progress.update(task, description="Analysis complete")
        
        # Display results
        console.print("\n" + "="*80)
        console.print(risk_assessment)
        console.print("="*80 + "\n")
        
        # Generate full report
        if output:
            report_path = Path(output)
            report = agent.generate_report(output_file=report_path)
            console.print(f"[green]✓ Report saved to: {report_path}[/green]")
        
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        logger.exception("IPDR analysis failed")

@cli.command('integrated-analyze')
@click.option('--cdr-files', '-c',
              default=None,
              help='Comma-separated list of CDR files')
@click.option('--ipdr-files', '-i',
              default=None,
              help='Comma-separated list of IPDR files')
@click.option('--output', '-o', 
              default=None,
              help='Output file for integrated report')
@click.option('--correlate/--no-correlate', 
              default=True,
              help='Perform CDR-IPDR correlation')
def integrated_analyze(cdr_files, ipdr_files, output, correlate):
    """Run integrated CDR-IPDR analysis with correlation"""
    
    console.print("\n[bold cyan]Integrated Intelligence Agent[/bold cyan]")
    console.print("CDR + IPDR Correlation Analysis\n")
    
    try:
        # Initialize integrated agent
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Initializing integrated agent...", total=None)
            agent = IntegratedIntelligenceAgent()
            progress.update(task, description="Integrated agent initialized")
        
        # Parse file lists
        cdr_list = [f.strip() for f in cdr_files.split(',')] if cdr_files else None
        ipdr_list = [f.strip() for f in ipdr_files.split(',')] if ipdr_files else None
        
        # Load all data
        console.print("\n[yellow]Loading CDR and IPDR data...[/yellow]")
        
        load_result = agent.load_all_data(cdr_files=cdr_list, ipdr_files=ipdr_list)
        
        # Display loading results
        if load_result['cdr_load']:
            cdr_status = load_result['cdr_load'].get('status', 'unknown')
            if cdr_status == 'success':
                console.print(f"[green]✓ CDR: Loaded {load_result['summary']['cdr_suspects']} suspects[/green]")
            else:
                console.print(f"[red]✗ CDR: {load_result['cdr_load'].get('message', 'Failed')}")
        
        if load_result['ipdr_load']:
            ipdr_status = load_result['ipdr_load'].get('status', 'unknown')
            if ipdr_status == 'success':
                console.print(f"[green]✓ IPDR: Loaded {load_result['summary']['ipdr_suspects']} suspects[/green]")
            else:
                console.print(f"[red]✗ IPDR: {load_result['ipdr_load'].get('message', 'Failed')}")
        
        console.print(f"\n[cyan]Common suspects: {load_result['summary']['common_suspects']}[/cyan]")
        
        # Perform correlation if requested
        if correlate and load_result['summary']['common_suspects'] > 0:
            console.print("\n[yellow]Performing CDR-IPDR correlation...[/yellow]")
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                task = progress.add_task("Correlating CDR and IPDR data...", total=None)
                correlation_result = agent.correlate_data()
                progress.update(task, description="Correlation complete")
            
            if correlation_result['status'] == 'success':
                console.print("\n[bold]Correlation Results:[/bold]")
                console.print(Panel(correlation_result['report'], 
                               title="CDR-IPDR Correlation", 
                               border_style="green"))
        
        # Run integrated analysis
        console.print("\n[yellow]Running integrated analysis...[/yellow]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Analyzing integrated intelligence...", total=None)
            
            # Get integrated risk assessment
            risk_assessment = agent.get_integrated_risk_assessment()
            
            progress.update(task, description="Analysis complete")
        
        # Display results
        console.print("\n" + "="*80)
        # Handle potential encoding issues
        if isinstance(risk_assessment, str):
            # Encode and decode to handle Unicode characters
            risk_assessment = risk_assessment.encode('utf-8', errors='replace').decode('utf-8')
        console.print(risk_assessment)
        console.print("="*80 + "\n")
        
        # Generate full report if requested
        if output:
            console.print("\n[yellow]Generating integrated report...[/yellow]")
            report_path = Path(output)
            report = agent.generate_integrated_report(output_file=report_path)
            console.print(f"[green]✓ Integrated report saved to: {report_path}[/green]")
        
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        logger.exception("Integrated analysis failed")

@cli.command('ipdr-interactive')
@click.option('--data-path', '-d', 
              default=None,
              help='Path to IPDR Excel files directory')
@click.option('--files', '-f',
              default=None,
              help='Comma-separated list of IPDR Excel files to analyze')
def ipdr_interactive(data_path, files):
    """Start interactive IPDR analysis session"""
    
    console.print("\n[bold cyan]IPDR Intelligence Agent - Interactive Mode[/bold cyan]\n")
    
    try:
        # Initialize agent
        console.print("[yellow]Initializing IPDR agent...[/yellow]")
        agent = IPDRAgent()
        
        # Load IPDR data
        file_list = None
        if files:
            file_list = [f.strip() for f in files.split(',')]
            console.print(f"Loading specific files: {', '.join(file_list)}")
        
        load_result = agent.load_ipdr_data(file_list)
        
        if load_result['status'] == 'error':
            console.print(f"[red]Error: {load_result['message']}[/red]")
            return
        
        console.print(f"[green]✓ Loaded {load_result['loaded_count']} IPDR files[/green]")
        console.print(f"Suspects: {', '.join(load_result['suspects'])}\n")
        
        # Interactive loop
        console.print("[cyan]Enter queries to analyze IPDR data. Type 'help' for examples or 'exit' to quit.[/cyan]\n")
        
        while True:
            try:
                query = console.input("[bold]IPDR Query>[/bold] ")
                
                if query.lower() in ['exit', 'quit', 'q']:
                    break
                
                if query.lower() == 'help':
                    show_ipdr_help()
                    continue
                
                # Run analysis
                console.print("\n[yellow]Analyzing...[/yellow]")
                result = agent.analyze(query)
                
                # Display result
                console.print("\n[bold]Analysis Result:[/bold]")
                console.print(Panel(result, title="IPDR Intelligence", border_style="blue"))
                console.print()
                
            except KeyboardInterrupt:
                console.print("\n[yellow]Interrupted[/yellow]")
                break
            except Exception as e:
                console.print(f"[red]Error: {str(e)}[/red]")
        
        console.print("\n[cyan]Thank you for using IPDR Intelligence Agent![/cyan]")
        
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        logger.exception("IPDR interactive mode failed")

def show_ipdr_help():
    """Show IPDR help information"""
    help_text = """
## IPDR Analysis Example Queries:

### Encryption Analysis
- "Find suspects using WhatsApp at odd hours"
- "Analyze encrypted communication patterns"
- "Who has the most encrypted sessions?"

### Data Pattern Analysis
- "Find large uploads over 50MB"
- "Analyze Tuesday and Friday data patterns"
- "Show data usage spikes"

### Session Analysis
- "Find marathon sessions over 3 hours"
- "Detect concurrent sessions"
- "Analyze night-time activity"

### App Fingerprinting
- "Identify unknown apps"
- "Find P2P application usage"
- "Analyze apps on non-standard ports"

### Risk Assessment
- "Calculate IPDR risk scores"
- "Show high-risk suspects"
- "Generate risk report"
"""
    console.print(Markdown(help_text))

if __name__ == "__main__":
    # Setup logging
    logger.remove()
    logger.add(
        settings.log_file,
        rotation="10 MB",
        level=settings.log_level
    )
    
    # Add console logging for errors
    logger.add(
        sys.stderr,
        level="ERROR",
        format="<red>{message}</red>"
    )
    
    # Run CLI
    cli()