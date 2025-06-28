# IPDR Intelligence Agent Testing Guide

## Prerequisites

1. **Set OpenRouter API Key**:
   ```bash
   export OPENROUTER_API_KEY='your-api-key-here'
   ```

2. **Ensure IPDR Data Files**:
   - Place IPDR Excel/CSV files in `../IPDRs/` directory
   - Files should be named after suspects (e.g., `Sonu.xlsx`, `Peerbasha.xlsx`)

## Testing Methods

### Method 1: Interactive Mode (Recommended)

```bash
python ipdr_interactive.py
```

Interactive commands:
- `load` - Load all IPDR files
- `load Sonu,Peerbasha` - Load specific files
- `summary` - View data summary
- `risk` - Generate risk assessment
- `encryption` - Analyze encryption patterns
- `data` - Analyze data patterns
- `sessions` - Analyze session patterns
- `apps` - Analyze app usage
- `report` - Generate full report
- `query <question>` - Ask natural language questions

Example queries:
```
IPDR> load
IPDR> query who is using WhatsApp at odd hours?
IPDR> query find large uploads on pattern days
IPDR> query show me high-risk suspects
IPDR> query analyze marathon sessions
IPDR> report
```

### Method 2: Automated Test Script

```bash
python test_ipdr_agent.py
```

This will:
1. Load all IPDR data
2. Run predefined test queries
3. Test all analysis functions
4. Generate a comprehensive report

### Method 3: Direct Python Usage

```python
from ipdr_agent import IPDRAgent

# Initialize agent
agent = IPDRAgent(api_key="your-key")

# Load data
agent.load_ipdr_data()

# Natural language queries
result = agent.analyze("Find suspects with encrypted large uploads")
print(result)

# Specific analyses
encryption = agent.analyze_encryption_patterns()
data_patterns = agent.analyze_data_patterns()
sessions = agent.analyze_sessions()
apps = agent.analyze_apps()
risk = agent.get_risk_summary()

# Generate report
report = agent.generate_report()
```

## Sample Natural Language Queries

### Risk Assessment
- "Show me all high-risk suspects"
- "Calculate risk scores for all suspects"
- "Who are the priority investigation targets?"

### Encryption Analysis
- "Find suspects using WhatsApp at night"
- "Who has the most encrypted sessions?"
- "Analyze Telegram usage patterns"
- "Show encryption spikes"

### Data Pattern Analysis
- "Find large uploads over 50MB"
- "Analyze Tuesday and Friday data patterns"
- "Who has suspicious upload/download ratios?"
- "Show data usage spikes"

### Session Analysis
- "Find marathon sessions over 3 hours"
- "Detect concurrent sessions"
- "Who has rapid session switching?"
- "Analyze night-time activity"

### App Fingerprinting
- "Identify unknown apps"
- "Find P2P application usage"
- "Show high-risk app combinations"
- "Analyze apps on non-standard ports"

### Combined Queries
- "Find suspects using encrypted apps for large uploads"
- "Who has marathon sessions with WhatsApp?"
- "Show pattern day encryption activity"
- "Analyze suspicious app switching patterns"

## Creating Sample IPDR Data

If you don't have IPDR data, the test script can create sample data:

```bash
python test_ipdr_agent.py
```

This will create sample IPDR files in `../IPDRs/` with:
- Random session data
- WhatsApp/Telegram sessions
- Large uploads
- Various temporal patterns

## Expected IPDR File Format

IPDR files should contain these columns:
- `subscriber_id` - Phone number
- `start_time` - Session start timestamp
- `end_time` - Session end timestamp
- `destination_ip` - Destination IP address
- `destination_port` - Destination port (for app identification)
- `data_volume_up` - Upload bytes
- `data_volume_down` - Download bytes
- `protocol` - TCP/UDP

Optional columns:
- `imei` - Device identifier
- `imsi` - SIM identifier
- `source_ip` - Source IP
- `source_port` - Source port

## Troubleshooting

1. **No API Key Error**:
   - Ensure `OPENROUTER_API_KEY` is set in environment
   - Check `.env` file in the project directory

2. **No Data Loaded**:
   - Verify IPDR files exist in `../IPDRs/`
   - Check file format (Excel or CSV)
   - Ensure proper column names

3. **Analysis Errors**:
   - Check data quality (timestamps, numeric values)
   - Verify sufficient data for analysis
   - Review error messages for specific issues

## Output Files

- `ipdr_analysis_report.md` - Comprehensive analysis report
- `ipdr_test_report.md` - Test script output
- Console output for interactive queries