# CDR-IPDR-Tower Intelligence Platform

A comprehensive LangChain-based intelligence platform for analyzing Call Detail Records (CDRs), Internet Protocol Detail Records (IPDRs), and Tower Dump data to detect criminal patterns and generate actionable intelligence reports.

## 🚀 New: Tower Dump Analysis Integration

The platform now includes full Tower Dump analysis capabilities, enabling law enforcement to:
- Identify all devices present at crime scenes
- Track movement patterns and trajectories
- Detect device identity changes (IMEI/IMSI swapping)
- Cross-reference physical presence with communication data
- Build comprehensive suspect profiles

## Features

### CDR Analysis
- **Multi-Pattern Detection**: Device switching, temporal patterns, communication behavior
- **Risk Assessment**: Automated suspect ranking with color-coded risk levels
- **Network Analysis**: Identifies connections and criminal network structures
- **Natural Language Interface**: Query CDR data using plain English

### IPDR Analysis
- **Encryption Detection**: Identifies usage of encrypted apps (WhatsApp, Telegram, Signal)
- **Data Pattern Analysis**: Unusual data usage patterns and volumes
- **Session Behavior**: Connection timing and duration analysis
- **App Fingerprinting**: Identifies specific applications used

### Tower Dump Analysis
- **Crime Scene Presence**: Identify all devices at specific locations and times
- **Movement Tracking**: Analyze device trajectories and speed patterns
- **Device Identity**: Track IMEI/IMSI changes and SIM swapping
- **Behavioral Patterns**: Detect one-time visitors, frequent visitors, reconnaissance
- **Geofencing**: Define and analyze geographic boundaries

### Integrated Intelligence
- **Cross-Reference Analysis**: Link tower presence with CDR/IPDR activity
- **Silent Device Detection**: Find devices present but not communicating
- **Evidence Chains**: Build connections between physical presence and digital activity
- **Comprehensive Reports**: Unified intelligence reports from all data sources

## Installation

1. Clone the repository and navigate to the project directory:
```bash
cd cdr_intelligence_agent
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env and add your OpenRouter API key
```

## Usage

### 1. Integrated Analysis (Recommended)
Analyze CDR, IPDR, and Tower Dump data together:

```bash
python test_integrated_tower_dump.py
```

Or use the enhanced integrated agent directly:

```python
from integrated_agent.integrated_agent_v2 import EnhancedIntegratedAgent

# Initialize agent
agent = EnhancedIntegratedAgent(api_key="your-api-key")

# Load all data sources
agent.load_all_data(
    cdr_files=["cdr1.xlsx", "cdr2.xlsx"],
    ipdr_files=["ipdr1.xlsx", "ipdr2.xlsx"],
    tower_dump_files=["tower_dump1.csv"]
)

# Analyze crime scene
result = agent.analyze_crime_scene(
    crime_location="Bank of India, MG Road",
    crime_time="2024-01-15 14:30:00",
    radius_km=1.0
)
```

### 2. CDR Analysis
Analyze CDR files and generate intelligence report:
```bash
python main.py analyze
```

Analyze specific files:
```bash
python main.py analyze -f "9042720423_Ali.xlsx,9391251134_Peer basha.xlsx"
```

### 3. IPDR Analysis
Run IPDR analysis interactively:
```bash
python ipdr_interactive.py
```

### 4. Tower Dump Analysis
Use the tower dump agent for specific analysis:

```python
from tower_dump_agent import TowerDumpAgent

agent = TowerDumpAgent(api_key="your-api-key")
agent.load_tower_dump("tower_dump.csv")

# Analyze crime scene
result = agent.analyze_crime_scene(
    crime_time="2024-01-15 14:30:00",
    duration_hours=2
)
```

### 5. Interactive Mode
Query data interactively:
```bash
python main.py interactive
```

Example queries:
- "Who was present at the crime scene based on tower dumps?"
- "Find devices that were present but made no calls"
- "Analyze movement patterns for vehicle detection"
- "Cross-reference tower presence with CDR activity"

## Data Formats

### CDR Data Format
Excel files with columns:
- A PARTY (caller number)
- B PARTY (receiver number)
- DATE, TIME, DURATION
- CALL TYPE (CALL-IN, CALL-OUT, SMS-IN, SMS-OUT)
- CELL ID, IMEI, IMSI
- LATITUDE, LONGITUDE

### IPDR Data Format
Excel files with columns:
- Mobile Number
- Source/Destination IP
- URL/Domain
- Upload/Download Volume
- Session Duration
- Protocol, Port

### Tower Dump Format
CSV/Excel files with columns:
- Mobile Number
- IMEI, IMSI
- Tower ID
- Tower Latitude/Longitude
- Timestamp
- Call Type

## Risk Indicators

### Integrated Risk Assessment
- 🔴 **CRITICAL**: Present at crime scene + encrypted comms + device switching
- 🟡 **HIGH**: Multiple risk factors across data sources
- 🟢 **MEDIUM**: Some suspicious patterns
- ⚪ **LOW**: Normal behavior patterns

### Pattern Detection
1. **Physical Presence**: Crime scene presence from tower dumps
2. **Device Manipulation**: IMEI/IMSI changes, SIM swapping
3. **Communication Evasion**: Silent periods, encrypted apps
4. **Movement Patterns**: Vehicle use, impossible travel
5. **Network Coordination**: Group movements, synchronized activity

## Key Use Cases

### 1. Crime Scene Investigation
```python
# Identify all suspects at crime scene
result = agent.analyze_crime_scene(
    crime_location="Bank Branch",
    crime_time="2024-01-15 14:30:00"
)
```

### 2. Suspect Tracking
```python
# Track complete journey
journey = agent.track_suspect_journey(
    suspect="9876543210",
    date="2024-01-15"
)
```

### 3. Network Mapping
```python
# Find all connections
network = agent.find_network_connections("9876543210")
```

### 4. Surveillance Detection
```python
# Detect evasion techniques
evasion = agent.detect_surveillance_evasion()
```

## Output Examples

### Crime Scene Analysis
```
🚨 CRIME SCENE ANALYSIS - Bank Robbery 14:30

TOWER DUMP FINDINGS:
- 47 devices present during crime window
- 12 one-time visitors (never seen before/after)
- 3 devices left immediately after incident

CDR ANALYSIS:
- 5 devices made calls during robbery
- 2 devices called same number after leaving

IPDR CORRELATION:
- 3 devices used encrypted apps post-incident
- Heavy data usage indicating media transfer

PRIORITY SUSPECTS:
1. 9876543210 - One-time visitor, encrypted comms after
2. 9876543211 - Rapid exit, called coordinator
```

### Integrated Report
```
📊 INTEGRATED INTELLIGENCE REPORT

SUSPECT: 9876543210
Risk Level: 🔴 CRITICAL

EVIDENCE SUMMARY:
✓ Present at 3 crime scenes (Tower Dump)
✓ No calls during presence (CDR - Silent)
✓ WhatsApp usage immediately after (IPDR)
✓ 4 different IMEIs used (Device Switching)
✓ Impossible travel detected (Cloning)

RECOMMENDATION: Immediate apprehension
```

## Advanced Features

- **Real-time Alerts**: Monitor specific devices
- **Pattern Learning**: Identify new criminal patterns
- **Evidence Chains**: Court-ready evidence compilation
- **Visualization**: Heat maps, movement trajectories
- **API Integration**: RESTful API for system integration

## Architecture

```
CDR-IPDR-Tower Intelligence Platform
├── CDR Intelligence Agent
│   ├── Pattern Detection Tools
│   ├── Risk Assessment
│   └── Network Analysis
├── IPDR Agent  
│   ├── Encryption Detection
│   ├── Data Pattern Analysis
│   └── App Fingerprinting
├── Tower Dump Agent
│   ├── Time Window Analysis
│   ├── Movement Tracking
│   ├── Device Identity Analysis
│   └── Geofencing Tools
└── Integrated Analysis Engine
    ├── Cross-Reference Tools
    ├── Correlation Engine
    └── Report Generator
```

## Security & Compliance

- **Law Enforcement Use Only**: Designed for authorized criminal investigations
- **Data Privacy**: All analyses performed locally
- **Audit Trail**: Complete logging of all operations
- **Legal Compliance**: Follows telecom data handling regulations

## Troubleshooting

1. **API Key Issues**: Ensure OPENROUTER_API_KEY is set
2. **Data Format Errors**: Check column names match specifications
3. **Memory Issues**: Process large datasets in chunks
4. **Integration Errors**: Verify all data sources are loaded

## Documentation

- [Tower Dump Analysis Guide](TOWER_DUMP_ANALYSIS_DOCUMENTATION.md)
- [Integration Summary](TOWER_DUMP_INTEGRATION_SUMMARY.md)
- [IPDR Testing Guide](IPDR_TESTING_GUIDE.md)
- [API Documentation](API_DOCUMENTATION.md)

## Support

For issues or questions:
1. Check documentation in the docs/ folder
2. Review example scripts in tests/
3. Enable debug logging with `--debug` flag

---

**Version**: 2.0 (with Tower Dump Integration)  
**License**: Law Enforcement Use Only  
**Last Updated**: January 2024