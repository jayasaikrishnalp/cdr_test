# Comprehensive Testing Commands for CDR-IPDR-Tower Dump Intelligence System

## Prerequisites

### 1. Set Environment Variable
```bash
export OPENROUTER_API_KEY="your-api-key-here"
```

Or create `.env` file:
```bash
echo "OPENROUTER_API_KEY=your-api-key-here" > .env
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Verify Installation
```bash
python -c "
from agent.cdr_agent import CDRIntelligenceAgent
from ipdr_agent.ipdr_agent import IPDRAgent
from tower_dump_agent.tower_dump_agent import TowerDumpAgent
from integrated_agent.integrated_agent_v2 import EnhancedIntegratedAgent
print('✅ All components imported successfully')
"
```

## 🧪 Testing Commands

### 1. Individual Component Tests

#### A. CDR Analysis Only
```bash
# Analyze all CDR files
python main.py analyze

# Analyze specific files
python main.py analyze -f "9042720423_Ali.xlsx,9391251134_Peer basha.xlsx"

# Save report
python main.py analyze -o cdr_report.md

# Interactive mode
python main.py interactive
```

#### B. IPDR Analysis Only
```bash
# Run IPDR interactive analysis
python ipdr_interactive.py

# Test IPDR tools directly
python test_ipdr_tools.py

# Test IPDR agent
python test_ipdr_agent.py
```

#### C. Tower Dump Analysis Only
Create `test_tower_dump.py`:
```python
from tower_dump_agent import TowerDumpAgent

# Initialize agent
agent = TowerDumpAgent()

# Test queries (no data needed)
queries = [
    "What patterns should I look for in tower dump data?",
    "How do tower dumps help identify crime scene suspects?",
    "Explain one-time visitors and their significance",
    "What is impossible travel and how to detect it?",
    "How to identify device cloning from tower dumps?"
]

for query in queries:
    print(f"\nQ: {query}")
    print(f"A: {agent.analyze(query)}")
    print("-" * 80)
```

Run:
```bash
python test_tower_dump.py
```

### 2. Integrated System Test

#### A. Quick Integration Test
```bash
# Run pre-built integration test
python test_integrated_tower_dump.py
```

#### B. Full Integration Test
Create `test_full_integration.py`:
```python
from integrated_agent.integrated_agent_v2 import EnhancedIntegratedAgent
from pathlib import Path

# Initialize enhanced agent
print("Initializing Enhanced Integrated Agent...")
agent = EnhancedIntegratedAgent()

# Load all data sources
print("\nLoading data...")
result = agent.load_all_data(
    cdr_files=[
        "../CDRs/9042720423_Ali.xlsx",
        "../CDRs/9391251134_Peer basha.xlsx",
        "../CDRs/9309347633_Danial Srikakulam.xlsx"
    ],
    ipdr_files=[
        "../IPDR/919042720423_IPV6_IPDRNT_ALI.xlsx",
        "../IPDR/919391251134_IPV6_IPDRNT_Peer Bhasa.xlsx",
        "../IPDR/919309347633_IPV6_IPDRNT_Danial.xlsx"
    ]
    # Note: Add tower_dump_files when available
)
print(f"Data loaded: {result['summary']}")

# Correlate data
print("\nCorrelating data...")
correlation = agent.correlate_all_data()
print(f"Correlation complete: {correlation}")

# Run comprehensive analyses
analyses = [
    ("Surveillance Evasion", agent.detect_surveillance_evasion()),
    ("High Risk Suspects", agent.analyze("Identify top 3 highest risk suspects with evidence")),
    ("Encryption Patterns", agent.analyze("Find encrypted communications after voice calls")),
    ("Network Analysis", agent.find_network_connections("9042720423"))
]

for title, result in analyses:
    print(f"\n{'='*60}")
    print(f"{title}")
    print('='*60)
    print(result)

# Generate report
print("\nGenerating comprehensive report...")
report = agent.generate_comprehensive_report(
    output_file=Path("reports/full_integration_report.md")
)
print("✅ Report saved to reports/full_integration_report.md")
```

Run:
```bash
python test_full_integration.py
```

### 3. Crime Scene Analysis Test
Create `test_crime_scene.py`:
```python
from integrated_agent.integrated_agent_v2 import EnhancedIntegratedAgent

agent = EnhancedIntegratedAgent()

# Load data
agent.load_all_data(
    cdr_files=["../CDRs/9042720423_Ali.xlsx", "../CDRs/9391251134_Peer basha.xlsx"],
    ipdr_files=["../IPDR/919042720423_IPV6_IPDRNT_ALI.xlsx"]
)

# Simulate crime scene analysis
print("CRIME SCENE ANALYSIS SIMULATION")
print("="*80)
print("Scenario: Bank Robbery at MG Road Branch")
print("Date/Time: January 15, 2024 at 14:30")
print("="*80)

# Analysis 1: Time-based
result = agent.analyze(
    "Analyze all communication patterns between 14:00 and 15:00 on any available dates. "
    "Look for: 1) Call bursts indicating coordination, "
    "2) Encrypted app usage after calls, "
    "3) Suspicious patterns like voice-only communication"
)
print("\n1. TIME WINDOW ANALYSIS:")
print(result)

# Analysis 2: Suspect behavior
result = agent.analyze(
    "For suspects Ali (9042720423) and Peer basha (9391251134), analyze: "
    "1) Their communication patterns, "
    "2) Any encrypted app usage, "
    "3) Signs of coordination between them, "
    "4) Risk assessment"
)
print("\n2. SUSPECT BEHAVIOR ANALYSIS:")
print(result)

# Analysis 3: Evidence chains
result = agent.analyze(
    "Build evidence chains showing: "
    "1) Voice calls followed by data activity, "
    "2) Periods of communication silence, "
    "3) Unusual patterns that could indicate criminal activity"
)
print("\n3. EVIDENCE CHAIN ANALYSIS:")
print(result)
```

Run:
```bash
python test_crime_scene.py
```

### 4. Interactive Testing Session
Create `interactive_integrated.py`:
```python
from integrated_agent.integrated_agent_v2 import EnhancedIntegratedAgent
import sys

print("CDR-IPDR-Tower Dump Intelligence System")
print("="*50)

# Initialize
agent = EnhancedIntegratedAgent()

# Load data
print("Loading data...")
agent.load_all_data(
    cdr_files=["../CDRs/9042720423_Ali.xlsx"],
    ipdr_files=["../IPDR/919042720423_IPV6_IPDRNT_ALI.xlsx"]
)

print("\nData loaded. Enter queries (type 'help' for examples, 'quit' to exit)")
print("-"*50)

# Example queries
examples = [
    "Analyze suspect 9042720423 comprehensively",
    "Find encrypted app usage patterns",
    "Detect device switching behaviors",
    "Identify odd hour activities",
    "Show risk assessment for all suspects",
    "Find coordination patterns between suspects",
    "Analyze calls followed by encryption",
    "Generate investigation priorities"
]

while True:
    query = input("\n> ").strip()
    
    if query.lower() == 'quit':
        break
    elif query.lower() == 'help':
        print("\nExample queries:")
        for i, ex in enumerate(examples, 1):
            print(f"{i}. {ex}")
        continue
    elif query.isdigit() and 1 <= int(query) <= len(examples):
        query = examples[int(query)-1]
        print(f"Running: {query}")
    
    try:
        result = agent.analyze(query)
        print("\nResult:")
        print("-"*50)
        print(result)
    except Exception as e:
        print(f"Error: {e}")
```

Run:
```bash
python interactive_integrated.py
```

### 5. Batch Analysis
```bash
# Create batch_analyze.py
cat > batch_analyze.py << 'EOF'
from integrated_agent.integrated_agent_v2 import EnhancedIntegratedAgent
from pathlib import Path
import glob

agent = EnhancedIntegratedAgent()

# Load all files
cdr_files = glob.glob("../CDRs/*.xlsx")
ipdr_files = glob.glob("../IPDR/*.xlsx")

print(f"Loading {len(cdr_files)} CDR and {len(ipdr_files)} IPDR files...")
agent.load_all_data(cdr_files=cdr_files, ipdr_files=ipdr_files)

# Run analyses
analyses = {
    "risk_assessment": "Generate risk assessment for all suspects",
    "encryption_patterns": "Find all encrypted communication patterns",
    "device_switching": "Identify all device switching behaviors",
    "coordination": "Detect coordination patterns between suspects",
    "evasion": "Find surveillance evasion techniques"
}

results_dir = Path("batch_results")
results_dir.mkdir(exist_ok=True)

for name, query in analyses.items():
    print(f"\nRunning: {name}")
    result = agent.analyze(query)
    
    # Save result
    output_file = results_dir / f"{name}.md"
    output_file.write_text(f"# {query}\n\n{result}")
    print(f"Saved to: {output_file}")

# Generate master report
print("\nGenerating master report...")
report = agent.generate_comprehensive_report(
    output_file=results_dir / "master_report.md"
)
print("✅ All analyses complete. Check batch_results/ directory")
EOF

python batch_analyze.py
```

### 6. Pattern Detection Tests
```bash
# Test specific patterns
python -c "
from integrated_agent.integrated_agent_v2 import EnhancedIntegratedAgent

agent = EnhancedIntegratedAgent()
agent.load_all_data(
    cdr_files=['../CDRs/9042720423_Ali.xlsx'],
    ipdr_files=['../IPDR/919042720423_IPV6_IPDRNT_ALI.xlsx']
)

patterns = [
    'Find voice calls immediately followed by WhatsApp usage',
    'Detect periods of CDR silence with active IPDR data',
    'Identify midnight to 5 AM activities in both CDR and IPDR',
    'Find large data uploads after voice calls',
    'Detect VPN or proxy usage patterns'
]

for pattern in patterns:
    print(f'\n{"="*60}')
    print(f'Pattern: {pattern}')
    print("="*60)
    result = agent.analyze(pattern)
    print(result)
"
```

### 7. Performance Test
```bash
# Test performance
python -c "
import time
from integrated_agent.integrated_agent_v2 import EnhancedIntegratedAgent

agent = EnhancedIntegratedAgent()

# Time data loading
start = time.time()
agent.load_all_data(
    cdr_files=['../CDRs/9042720423_Ali.xlsx'],
    ipdr_files=['../IPDR/919042720423_IPV6_IPDRNT_ALI.xlsx']
)
print(f'Data loading: {time.time()-start:.2f}s')

# Time analysis
queries = [
    'Analyze all suspects',
    'Find high risk patterns',
    'Generate risk scores'
]

for query in queries:
    start = time.time()
    result = agent.analyze(query)
    print(f'{query}: {time.time()-start:.2f}s')
"
```

### 8. Tower Dump Simulation
```bash
# Since we don't have actual tower dump files
python -c "
from integrated_agent.integrated_agent_v2 import EnhancedIntegratedAgent

agent = EnhancedIntegratedAgent()
agent.load_all_data(
    cdr_files=['../CDRs/9042720423_Ali.xlsx'],
    ipdr_files=['../IPDR/919042720423_IPV6_IPDRNT_ALI.xlsx']
)

# Simulate tower dump analysis
print('TOWER DUMP SIMULATION')
print('='*60)

result = agent.analyze('''
Based on the CDR location data (cell towers), simulate a tower dump analysis:
1. Identify which suspects were in same locations
2. Find movement patterns from cell tower changes
3. Detect suspicious location behaviors
4. Identify devices that might have been at crime scenes
''')
print(result)
"
```

## 🔧 Troubleshooting

### Check API Key
```bash
python -c "
import os
key = os.getenv('OPENROUTER_API_KEY')
print(f'API Key set: {bool(key)}')
print(f'First 10 chars: {key[:10]}...' if key else 'No key found')
"
```

### Enable Debug Mode
```bash
export LOGURU_LEVEL=DEBUG
python test_integrated_tower_dump.py
```

### Test Individual Imports
```bash
# Test each component
python -c "from agent.cdr_agent import CDRIntelligenceAgent; print('✅ CDR Agent OK')"
python -c "from ipdr_agent.ipdr_agent import IPDRAgent; print('✅ IPDR Agent OK')"
python -c "from tower_dump_agent import TowerDumpAgent; print('✅ Tower Agent OK')"
python -c "from integrated_agent.integrated_agent_v2 import EnhancedIntegratedAgent; print('✅ Integrated Agent OK')"
```

### Check Data Files
```bash
# List available data files
echo "CDR Files:"
ls -la ../CDRs/*.xlsx 2>/dev/null | wc -l
echo "IPDR Files:"
ls -la ../IPDR/*.xlsx 2>/dev/null | wc -l
```

## 📊 Expected Outputs

### Successful Data Load
```
Data loaded: {'cdr_suspects': 3, 'ipdr_suspects': 3, 'common_suspects': 3}
```

### Risk Assessment Output
```
🔴 HIGH RISK: Ali (9042720423)
- Multiple IMEIs detected
- WhatsApp usage at odd hours
- Voice-only communication pattern

🟡 MEDIUM RISK: Peer basha (9391251134)
- Encrypted app usage
- Device switching detected
```

### Evidence Chain Output
```
EVIDENCE CHAIN: Ali (9042720423)
1. Voice call at 14:25 → WhatsApp at 14:32
2. Location: Cell Tower A → Data usage pattern
3. Risk indicators: Device switching + Encryption
```

## 📝 Notes

- System works without actual tower dump files
- Tower dump features simulate using CDR cell tower data
- All analyses use natural language processing
- Results formatted for law enforcement use