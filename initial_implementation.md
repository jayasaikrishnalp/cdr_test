# CDR Intelligence Agent - Initial Implementation

## Overview
This document captures the complete implementation of the CDR Intelligence Agent, a LangChain-based system for analyzing Call Detail Records (CDRs) to detect criminal patterns and generate actionable intelligence reports.

## Implementation Date
June 28, 2024

## Key Features Implemented

### 1. Project Structure
Created a modular architecture with clear separation of concerns:

```
cdr_intelligence_agent/
├── agent/                    # LangChain agent core
│   ├── __init__.py
│   ├── cdr_agent.py         # Main agent implementation
│   └── openrouter_llm.py    # OpenRouter integration
├── processors/              # Data processing layer
│   ├── __init__.py
│   ├── cdr_loader.py        # Excel file loading
│   ├── data_validator.py    # CDR validation
│   └── pattern_detector.py  # Criminal pattern detection
├── tools/                   # Analysis tools
│   ├── __init__.py
│   ├── base_tool.py         # Base tool class
│   ├── device_analysis.py   # IMEI/IMSI tracking
│   ├── temporal_analysis.py # Time-based patterns
│   ├── communication_analysis.py # Voice/SMS analysis
│   ├── network_analysis.py  # Relationship mapping
│   └── risk_scorer.py       # Risk assessment
├── report/                  # Report generation
│   ├── __init__.py
│   └── report_generator.py  # Formatted output
├── config.py               # Configuration management
├── main.py                 # CLI entry point
├── requirements.txt        # Dependencies
├── README.md              # Documentation
└── .env.example           # Environment template
```

### 2. Technology Stack
- **LangChain 0.3+** with Pydantic 2 support
- **OpenRouter** for multi-LLM access (Claude, GPT-4, Gemini)
- **Pandas** for data processing
- **Click** for CLI interface
- **Rich** for terminal UI
- **NetworkX** for network analysis

### 3. Data Processing Layer

#### CDR Loader (`processors/cdr_loader.py`)
- Loads Excel files from CDRs directory
- **NEW**: Supports comma-separated file input
- Validates against CDR Data Description.docx specification
- Filters provider messages (AA-AIRTEL, JZ-REGINF, etc.)
- Preprocesses data (combines DATE+TIME, cleans phone numbers)

#### Data Validator (`processors/data_validator.py`)
- Validates all CDR fields against specification
- Checks data types and formats
- Calculates data quality scores
- Provides detailed validation reports

#### Pattern Detector (`processors/pattern_detector.py`)
- Implements criminal pattern detection from YAML analysis
- Detects device switching, temporal patterns, communication anomalies
- Calculates risk scores based on multiple factors

### 4. Analysis Tools

#### Device Analysis Tool
- Tracks IMEI/IMSI changes
- Detects device switching (3+ IMEIs = HIGH RISK)
- Identifies SIM swapping patterns
- Timeline tracking of device changes

#### Temporal Analysis Tool
- Odd hour activity detection (midnight-5AM)
- Call burst identification
- Pattern day analysis (Tuesday/Friday for narcotics)
- Hourly distribution analysis

#### Communication Analysis Tool
- Voice vs SMS ratio analysis
- 100% voice communication detection
- Contact frequency analysis
- Call duration pattern detection

#### Network Analysis Tool
- Inter-suspect connection detection
- Common contact identification
- Network hierarchy analysis
- Potential intermediary detection

#### Risk Scoring Tool
- Multi-factor risk assessment
- Weighted scoring algorithm:
  - Device switching: 40%
  - Odd hours: 25%
  - Voice-only: 20%
  - High frequency: 15%
- Risk level categorization (HIGH/MEDIUM/LOW)

### 5. LangChain Agent Implementation

#### OpenRouter Integration (`agent/openrouter_llm.py`)
- Custom ChatOpenRouter class extending ChatOpenAI
- API-compatible with OpenAI format
- Fallback model support
- Tiktoken integration for token counting

#### CDR Agent (`agent/cdr_agent.py`)
- ReAct pattern implementation
- Natural language query interface
- Tool orchestration
- Memory management
- **FIXED**: Updated to use `invoke()` instead of deprecated `run()`

### 6. Report Generation
- Formatted criminal intelligence reports
- Emoji indicators (🔴 HIGH, 🟡 MEDIUM, 🟢 LOW)
- Risk-ranked suspect tables
- Network observations
- Investigation priorities
- Matches the provided example format exactly

### 7. CLI Interface (`main.py`)

#### Commands Implemented
1. **analyze** - Comprehensive CDR analysis
   - **NEW**: `-f` flag for comma-separated file input
   - `-o` flag for output file
   - `-d` flag for data directory

2. **interactive** - Interactive query mode
   - **NEW**: `-f` flag for specific file selection
   - Natural language queries
   - Real-time analysis

3. **validate** - Data validation
4. **config** - Show configuration

### 8. Key Enhancements and Fixes

#### Comma-Separated File Input (NEW)
```python
# Updated CDRLoader.load_cdrs() to accept file_list parameter
# Supports both with and without .xlsx extension
# Can use full paths or just filenames
```

#### Import Path Fixes
- Added `sys.path.append()` to all modules for proper imports
- Fixed circular import issues

#### Type Annotation Fixes
- Changed `type[BaseModel]` to `Type[BaseModel]`
- Added proper Type imports

#### LangChain 0.3 Compatibility
- Updated agent executor to use `invoke()` method
- Fixed memory configuration
- Added proper output handling

#### Pydantic 2 Compatibility
- Updated field definitions
- Fixed attribute handling in tools

### 9. Configuration Management
- Environment-based configuration
- Customizable risk weights
- Pattern detection thresholds
- Provider message patterns
- OpenRouter API settings

### 10. Criminal Patterns Detected

#### Device Patterns
- Multiple IMEIs (burner phones)
- SIM swapping
- Device switching timeline

#### Temporal Patterns
- Odd hour activity (covert operations)
- Call bursts (coordination)
- Pattern days (Tuesday/Friday)

#### Communication Patterns
- Voice-only behavior (avoiding SMS traces)
- High frequency contacts
- Repeated call durations (coded communication)

#### Network Patterns
- Inter-suspect connections
- Common contacts (intermediaries)
- Hierarchical structures

### 11. Usage Examples

#### Analyze All Files
```bash
python main.py analyze
```

#### Analyze Specific Files (NEW)
```bash
# Without extension
python main.py analyze -f "9042720423_Ali,9391251134_Peer basha"

# With extension
python main.py analyze -f "9042720423_Ali.xlsx,9391251134_Peer basha.xlsx"

# With full paths
python main.py analyze -f "../CDRs/9042720423_Ali.xlsx"
```

#### Interactive Mode
```bash
# All files
python main.py interactive

# Specific files
python main.py interactive -f "9042720423_Ali,9391251134_Peer basha"
```

#### Example Queries
- "Who made the most calls to Ali?"
- "Analyze device switching patterns"
- "Find odd hour activity"
- "Check voice vs SMS patterns"
- "Calculate risk scores for all suspects"

### 12. Output Format
Generates reports matching the provided example:
```
🚨 CRITICAL ANOMALIES DETECTED
1. Device Switching Pattern (High Risk)
   🔴 Peer basha: 3 different IMEIs detected

2. Odd Hour Activity (Covert Operations)
   🔴 Danial: 4.2% of calls between midnight-5AM

📊 SUSPECT RISK RANKING
| Suspect | Risk Level | Primary Indicators |
|---------|------------|-------------------|
| Peer basha | 🔴 HIGH | 3 IMEIs, Voice-only |

🎯 INVESTIGATION PRIORITIES
1. Deep investigation on Peer basha
2. Monitor Danial's midnight activities
```

### 13. Security Considerations
- Defensive security analysis only
- No malicious code generation
- Data privacy controls
- Secure API key management

### 14. Testing Results
Successfully tested with:
- All 6 CDR files
- Individual file analysis
- Comma-separated file lists
- Interactive queries
- Report generation

### 15. Future Enhancements
- IPDR integration
- Bank statement correlation
- Real-time monitoring
- Web interface
- Advanced visualizations

## Summary
The CDR Intelligence Agent provides law enforcement with a powerful, AI-driven tool for analyzing call detail records and detecting criminal patterns. The system combines modern LLM capabilities with traditional data analysis to generate actionable intelligence reports.

## Version
Initial Implementation v1.0
Date: June 28, 2024