# Integrated CDR-IPDR Intelligence Solution Guide

## 🎯 Overview

The Integrated Intelligence Agent combines Call Detail Records (CDR) and Internet Protocol Detail Records (IPDR) analysis to provide comprehensive criminal intelligence. This solution detects patterns that would be invisible when analyzing either data source alone.

## 🚀 Quick Start

### 1. Set API Key
```bash
export OPENROUTER_API_KEY='your-api-key-here'
```

### 2. Run Integrated Analysis
```bash
# Analyze all CDR and IPDR files with correlation
python main.py integrated-analyze --output integrated_report.md

# Analyze specific suspects
python main.py integrated-analyze \
  --cdr-files "9042720423_Ali.xlsx,9391251134_Peer basha.xlsx" \
  --ipdr-files "919042720423_IPV6_IPDRNT_ALI.xlsx,919391251134_IPV6_IPDRNT_Peer Bhasa.xlsx" \
  --output suspect_analysis.md
```

## 📊 Available Commands

### CDR Analysis (Original)
```bash
# Comprehensive CDR analysis
python main.py analyze

# Interactive CDR analysis
python main.py interactive
```

### IPDR Analysis (New)
```bash
# Comprehensive IPDR analysis
python main.py ipdr-analyze

# Interactive IPDR analysis
python main.py ipdr-interactive

# Specific IPDR files
python main.py ipdr-analyze -f "919042720423_IPV6_IPDRNT_ALI.xlsx,919391251134_IPV6_IPDRNT_Peer Bhasa.xlsx"
```

### Integrated Analysis (New)
```bash
# Full integrated analysis with correlation
python main.py integrated-analyze

# Without correlation (faster)
python main.py integrated-analyze --no-correlate

# Specific files
python main.py integrated-analyze \
  -c "Ali.xlsx,Peer basha.xlsx" \
  -i "ALI_IPDR.xlsx,Peer_IPDR.xlsx"
```

## 🔍 Key Features

### 1. CDR Analysis
- **Device Switching**: Multiple IMEI detection
- **Temporal Patterns**: Odd hours, pattern days (Tuesday/Friday)
- **Communication Analysis**: Voice-only patterns, call bursts
- **Network Analysis**: Common contacts, hierarchy detection

### 2. IPDR Analysis
- **Encryption Detection**: WhatsApp, Telegram, Signal usage
- **Data Patterns**: Large uploads, pattern day transfers
- **Session Analysis**: Marathon sessions, concurrent connections
- **App Fingerprinting**: Unknown apps, P2P detection

### 3. CDR-IPDR Correlation
- **Call-to-Data Patterns**: Voice calls followed by encrypted data
- **Silence Analysis**: IPDR activity during CDR silence
- **Evidence Chains**: Linking voice and data activities
- **Cross-Suspect Coordination**: Synchronized patterns

## 🎯 Criminal Pattern Detection

### 1. Evidence Coordination Pattern
```
Voice Call → Immediate WhatsApp → Large Upload
```
Indicates: Discussing evidence followed by sharing

### 2. Operational Security Pattern
```
CDR Silence + Heavy Encryption + Unknown Apps
```
Indicates: Covert operations awareness

### 3. Drug Transport Pattern
```
Tuesday/Friday Calls + Large Data Transfers + Pattern Concentration
```
Indicates: Narcotics transport coordination

### 4. Money Laundering Pattern
```
Multiple IMEIs + Banking Apps + Rapid Sessions
```
Indicates: Financial crime activities

## 📈 Risk Scoring

### Integrated Risk Calculation
```
Total Risk = (CDR Risk × 0.5) + (IPDR Risk × 0.4) + (Correlation Score × 0.1)
```

### Risk Levels
- **CRITICAL** (80-100): Immediate action required
- **HIGH** (60-79): Priority investigation
- **MEDIUM** (40-59): Enhanced monitoring
- **LOW** (0-39): Routine surveillance

## 🔗 Correlation Analysis

### Key Correlation Patterns

1. **Immediate Encryption After Call**
   - Gap < 60 seconds = CRITICAL
   - Gap < 300 seconds = HIGH
   - Indicates coordinated activity

2. **Data During CDR Silence**
   - No calls but heavy IPDR activity
   - Indicates channel switching for security

3. **Behavioral Shifts**
   - Voice → Data communication shift
   - Indicates operational security awareness

## 📝 Natural Language Queries

### CDR Queries
```
"Find suspects with multiple IMEIs"
"Analyze odd hour activity"
"Check for pattern day calls"
"Find voice-only suspects"
```

### IPDR Queries
```
"Who uses WhatsApp at night?"
"Find large uploads over 50MB"
"Detect marathon sessions"
"Identify unknown apps"
```

### Integrated Queries
```
"Find calls followed by encryption"
"Analyze complete communication patterns for Ali"
"Who shows evidence coordination patterns?"
"Find cross-suspect coordination"
```

## 📊 Output Files

### Reports Generated
- `cdr_analysis_report.md`: CDR-only analysis
- `ipdr_analysis_report.md`: IPDR-only analysis
- `integrated_report.md`: Complete CDR+IPDR analysis
- `correlation_report.md`: Correlation findings

### Report Sections
1. Executive Summary
2. CDR Analysis
3. IPDR Analysis
4. Correlation Findings
5. Risk Assessment
6. Evidence Chains
7. Investigation Recommendations

## 🛠️ Configuration

### Data Paths
- CDR Files: `../../CDRs/`
- IPDR Files: `../../IPDRs/IPDR/`

### Key Thresholds
- Large Upload: 10MB
- Marathon Session: 2 hours
- Odd Hours: 00:00-05:00
- Pattern Days: Tuesday, Friday

## 🚨 High-Priority Indicators

### Critical Patterns
1. **2+ IMEIs + Encrypted Apps**: Device switching with secure comms
2. **Large Uploads on Pattern Days**: Evidence/contraband sharing
3. **Call → Immediate Encryption**: Coordinated criminal activity
4. **Cross-Suspect Sync**: Network coordination

### Investigation Priorities
1. Suspects with CRITICAL correlation scores
2. Evidence chains linking voice to data
3. Pattern day concentrations
4. Coordinated encryption usage

## 📚 Example Analysis Flow

```bash
# 1. Load and correlate all data
python main.py integrated-analyze

# 2. Focus on high-risk suspects
python main.py integrated-analyze -c "Ali.xlsx" -i "ALI_IPDR.xlsx"

# 3. Interactive investigation
python main.py ipdr-interactive
> query find encrypted sessions after calls
> query analyze Ali's WhatsApp usage
> query show evidence chains

# 4. Generate comprehensive report
python main.py integrated-analyze -o investigation_report.md
```

## 🔐 Security Notes

- All analysis is for law enforcement purposes only
- Maintain chain of custody for digital evidence
- Follow legal procedures for data access
- Document all findings for court admissibility

## 📞 Support

For issues or questions:
- Check error logs in `cdr_analysis.log`
- Verify data file formats match specifications
- Ensure API key is properly set
- Review sample data in test scripts

This integrated solution provides law enforcement with powerful capabilities to analyze both traditional call records and modern digital communications, revealing criminal patterns that span multiple communication channels.