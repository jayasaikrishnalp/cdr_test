#!/usr/bin/env python3
"""
Interactive IPDR Intelligence Agent
Allows interactive querying of IPDR data
"""

import os
from pathlib import Path
from dotenv import load_dotenv
import readline  # For better input handling

# Add parent directory to path
import sys
sys.path.append(str(Path(__file__).parent))

from ipdr_agent import IPDRAgent

def print_banner():
    """Print welcome banner"""
    print("\n" + "="*60)
    print("🔍 IPDR INTELLIGENCE AGENT - INTERACTIVE MODE")
    print("="*60)
    print("Type 'help' for available commands or 'quit' to exit")
    print("="*60 + "\n")

def print_help():
    """Print help information"""
    print("\n📚 Available Commands:")
    print("-" * 40)
    print("help                  - Show this help message")
    print("load                  - Load IPDR data files")
    print("load <file1,file2>    - Load specific IPDR files")
    print("summary               - Show loaded data summary")
    print("risk                  - Generate risk assessment")
    print("encryption            - Analyze encryption patterns")
    print("data                  - Analyze data patterns")
    print("sessions              - Analyze session patterns")
    print("apps                  - Analyze app usage")
    print("report                - Generate full report")
    print("query <question>      - Ask a natural language question")
    print("quit/exit             - Exit the program")
    print("\nExamples:")
    print("  query find suspects using WhatsApp at night")
    print("  query who has the most encrypted sessions?")
    print("  query analyze large uploads on Tuesdays")
    print("-" * 40 + "\n")

def main():
    """Main interactive loop"""
    
    # Load environment variables
    load_dotenv()
    
    # Check for API key
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        print("❌ Error: OPENROUTER_API_KEY not found")
        print("Please set your OpenRouter API key:")
        print("export OPENROUTER_API_KEY='your-api-key-here'")
        return
    
    print_banner()
    
    # Initialize agent
    print("🚀 Initializing IPDR Intelligence Agent...")
    try:
        agent = IPDRAgent(api_key=api_key)
        print("✅ Agent initialized successfully\n")
    except Exception as e:
        print(f"❌ Failed to initialize agent: {str(e)}")
        return
    
    # Interactive loop
    while True:
        try:
            # Get user input
            user_input = input("IPDR> ").strip()
            
            if not user_input:
                continue
            
            # Parse command
            parts = user_input.split(maxsplit=1)
            command = parts[0].lower()
            args = parts[1] if len(parts) > 1 else ""
            
            # Handle commands
            if command in ['quit', 'exit']:
                print("\n👋 Goodbye!")
                break
            
            elif command == 'help':
                print_help()
            
            elif command == 'load':
                print("\n📂 Loading IPDR data...")
                if args:
                    # Load specific files
                    file_list = [f.strip() for f in args.split(',')]
                    result = agent.load_ipdr_data(file_list)
                else:
                    # Load all files
                    result = agent.load_ipdr_data()
                
                if result['status'] == 'success':
                    print(f"✅ Loaded {result['loaded_count']} IPDR files")
                    print("Suspects:", ", ".join(result['suspects']))
                else:
                    print(f"❌ Error: {result.get('message', 'Failed to load data')}")
            
            elif command == 'summary':
                if not agent.ipdr_data:
                    print("⚠️ No IPDR data loaded. Use 'load' command first.")
                else:
                    print("\n📊 IPDR Data Summary:")
                    print("-" * 40)
                    summary = agent.ipdr_loader.get_suspect_summary(agent.ipdr_data)
                    print(summary.to_string())
            
            elif command == 'risk':
                print("\n🎯 Generating Risk Assessment...")
                result = agent.get_risk_summary()
                print(result)
            
            elif command == 'encryption':
                print("\n🔐 Analyzing Encryption Patterns...")
                result = agent.analyze_encryption_patterns()
                print(result)
            
            elif command == 'data':
                print("\n📊 Analyzing Data Patterns...")
                result = agent.analyze_data_patterns()
                print(result)
            
            elif command == 'sessions':
                print("\n⏱️ Analyzing Session Patterns...")
                result = agent.analyze_sessions()
                print(result)
            
            elif command == 'apps':
                print("\n📱 Analyzing App Usage...")
                result = agent.analyze_apps()
                print(result)
            
            elif command == 'report':
                print("\n📄 Generating Comprehensive Report...")
                report_path = Path("ipdr_analysis_report.md")
                report = agent.generate_report(output_file=report_path)
                print(f"✅ Report saved to: {report_path}")
                print("\nReport Preview:")
                print("-" * 40)
                print(report[:1000] + "...\n")
            
            elif command == 'query':
                if not args:
                    print("⚠️ Please provide a question. Example: query who uses WhatsApp the most?")
                else:
                    print(f"\n🤔 Analyzing: {args}")
                    result = agent.analyze(args)
                    print(result)
            
            else:
                print(f"❌ Unknown command: {command}")
                print("Type 'help' for available commands")
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
            print("Type 'help' for available commands")

if __name__ == "__main__":
    main()