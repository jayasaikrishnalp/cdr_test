# CDR Intelligence Agent - Anomaly Points Distribution

## Overview
This document details the point distribution for each anomaly pattern detected by the CDR Intelligence Agent. Total possible score: 100 points.

## 1. Device Anomalies (Max: 25 points)

### 1.1 IMEI/Device Switching
| Pattern | Points | Detection Criteria | Risk Level |
|---------|--------|-------------------|------------|
| 3+ IMEIs | 25 | Multiple burner phones detected | HIGH |
| 2 IMEIs | 25 | Device switching/SIM swapping | MEDIUM (Override) |
| SIM swapping | 10 | SIM change patterns detected | LOW-MEDIUM |
| 1 IMEI | 0 | Normal behavior | LOW |

**Note**: Anyone with 2+ IMEIs is automatically elevated to MEDIUM risk minimum.

---

## 2. Temporal Anomalies (Max: 25 points)

### 2.1 Odd Hour Activity (Midnight-5AM)
| Pattern | Points | Detection Criteria | Risk Level |
|---------|--------|-------------------|------------|
| >4% odd hour calls | 20 | Very high covert activity | HIGH |
| 2-4% odd hour calls | 15 | Significant covert activity | MEDIUM |
| 1-2% odd hour calls | 10 | Elevated late-night activity | LOW-MEDIUM |
| <1% odd hour calls | 0 | Normal patterns | LOW |

### 2.2 Call Bursts
| Pattern | Points | Detection Criteria | Risk Level |
|---------|--------|-------------------|------------|
| >3 call bursts | +5 | Rapid coordination (5+ calls in 15 min) | MEDIUM |
| 1-3 call bursts | 0 | Some rapid activity | LOW |

### 2.3 Silent Periods (Phase 1 Addition)
| Pattern | Points | Detection Criteria | Risk Level |
|---------|--------|-------------------|------------|
| >72 hour gaps | +5 | Extended silence (HIGH severity) | HIGH |
| 48-72 hour gaps | +3 | Suspicious gaps (MEDIUM severity) | MEDIUM |
| <48 hour gaps | 0 | Normal communication flow | LOW |

---

## 3. Communication Anomalies (Max: 25 points)

### 3.1 Voice vs SMS Patterns
| Pattern | Points | Detection Criteria | Risk Level |
|---------|--------|-------------------|------------|
| 100% voice (0 SMS) | 20 | Complete avoidance of text traces | HIGH |
| >90% voice calls | 15 | Heavy voice preference | MEDIUM |
| 70-90% voice | 5 | Moderate voice preference | LOW |
| <70% voice | 0 | Normal mixed communication | LOW |

### 3.2 Additional Communication Patterns
| Pattern | Points | Detection Criteria | Risk Level |
|---------|--------|-------------------|------------|
| Repeated call durations | +5 | Same duration 5+ times (coded) | MEDIUM |
| Circular loops detected | +5 | A→B→C→A patterns (Phase 1) | HIGH |
| One-ring signaling | +5 | 3+ calls ≤3 seconds (Phase 1) | MEDIUM |
| Dominant contact (>30%) | +5 | Single contact dominates | MEDIUM |

---

## 4. Frequency Anomalies (Max: 15 points)

### 4.1 High Frequency Contacts
| Pattern | Points | Detection Criteria | Risk Level |
|---------|--------|-------------------|------------|
| >3 high-freq contacts | 15 | Intensive network activity | HIGH |
| 1-3 high-freq contacts | 10 | Moderate network activity | MEDIUM |
| 0 high-freq contacts | 0 | Normal call distribution | LOW |

**High-frequency**: Contact called >20 times or >10% of total calls

---

## 5. Network Anomalies (Max: 10 points)

### 5.1 Inter-Suspect Connections
| Pattern | Points | Detection Criteria | Risk Level |
|---------|--------|-------------------|------------|
| Direct connections | 10 | Suspects calling each other | HIGH |
| No direct connections | 0 | Compartmentalized network | LOW |

### 5.2 Common Contacts
| Pattern | Points | Detection Criteria | Risk Level |
|---------|--------|-------------------|------------|
| 3+ common contacts | +5 | Shared network members | MEDIUM |
| 1-2 common contacts | +3 | Some shared contacts | LOW |
| 0 common contacts | 0 | Isolated operations | LOW |

### 5.3 Synchronized Calling (Phase 1 Addition)
| Pattern | Points | Detection Criteria | Risk Level |
|---------|--------|-------------------|------------|
| 3+ suspects synchronized | +5 | Coordinated operations | HIGH |
| 2 suspects synchronized | +3 | Paired activity | MEDIUM |
| No synchronization | 0 | Independent operations | LOW |

---

## 6. Location Anomalies (Phase 1 - Separate Tool)

Location anomalies don't add to the base risk score but can trigger automatic HIGH risk flags:

| Pattern | Action | Detection Criteria | Risk Level |
|---------|--------|-------------------|------------|
| Impossible travel | Auto HIGH flag | >200 km/h speed | HIGH |
| Border activity | +10 to total | Within 50km of border | HIGH |
| Location clustering | Risk indicator | 5+ calls from same spot | MEDIUM |
| Wide area operations | Risk indicator | High location variance | MEDIUM |

---

## Risk Score Calculation Formula

```
Total Risk Score = Device Risk + Temporal Risk + Communication Risk + Frequency Risk + Network Risk

Where:
- Device Risk: 0-25 points
- Temporal Risk: 0-25 points (base + bursts + silent periods)
- Communication Risk: 0-25 points (voice/SMS + patterns)
- Frequency Risk: 0-15 points
- Network Risk: 0-10 points (connections + common + sync)

Maximum Possible: 100 points
```

## Risk Level Thresholds

| Score Range | Risk Level | Emoji | Action Required |
|-------------|------------|-------|-----------------|
| 70-100 | CRITICAL | 🔴 | Immediate investigation |
| 50-69 | HIGH | 🔴 | Priority investigation |
| 30-49 | MEDIUM | 🟡 | Enhanced monitoring |
| 0-29 | LOW | 🟢 | Routine monitoring |

## Override Rules

1. **Device Override**: 2+ IMEIs = Automatic MEDIUM minimum
2. **Voice-Only Override**: 100% voice with 10+ calls = Automatic review
3. **Location Override**: Impossible travel = Automatic HIGH flag

---

## Example Calculations

### Example 1: Peer basha (HIGH RISK - 65 points)
```
Device Risk: 25 points (2 IMEIs)
Temporal Risk: 20 points (13.4% odd hours)
Communication Risk: 5 points (high voice %)
Frequency Risk: 10 points (2 high-freq contacts)
Network Risk: 5 points (common contacts)
Total: 65/100 = HIGH RISK 🔴
```

### Example 2: Ali (MEDIUM RISK - 35 points)
```
Device Risk: 25 points (2 IMEIs)
Temporal Risk: 0 points
Communication Risk: 0 points
Frequency Risk: 10 points (1 high-freq contact)
Network Risk: 0 points
Total: 35/100 = MEDIUM 🟡 (Override applied)
```

### Example 3: Danial (MEDIUM RISK - 45 points)
```
Device Risk: 0 points (1 IMEI)
Temporal Risk: 15 points (4.5% odd hours)
Communication Risk: 20 points (100% voice)
Frequency Risk: 5 points
Network Risk: 5 points (4 common contacts)
Total: 45/100 = MEDIUM 🟡
```

---

## Phase 1 Pattern Additions

### New Patterns Added (December 2024)
1. **Circular Communication Loops**: +5 points to Communication Risk
2. **One-Ring Signaling**: +5 points to Communication Risk
3. **Silent Periods**: +3-5 points to Temporal Risk
4. **Synchronized Calling**: +3-5 points to Network Risk
5. **Location Analysis**: Separate tool with override capabilities

### Impact on Scoring
- Maximum Communication Risk increased potential
- Maximum Temporal Risk increased potential
- Maximum Network Risk increased potential
- More granular detection of sophisticated criminal patterns

---

## Future Enhancements (Phase 2-4)

### Planned Anomaly Additions
1. **Ghost Numbers** (+10 points): Calls to non-existent numbers
2. **Conference Calls** (+5 points): Multi-party coordination
3. **Financial Patterns** (+15 points): 48-hour payment cycles
4. **Encrypted App Signals** (+10 points): WhatsApp/Telegram indicators
5. **International Red Zones** (+20 points): Calls to high-risk countries

### Machine Learning Enhancement
- Dynamic point adjustment based on confirmed cases
- Pattern combination scoring
- Regional variation adaptation
- Temporal pattern learning

---

## Usage Guidelines

1. **Point Stacking**: Multiple patterns in same category stack (e.g., voice-only + circular loops)
2. **Maximum Caps**: Each category has maximum possible points
3. **Override Priority**: Override rules supersede calculated scores
4. **Evidence Weight**: Higher points = stronger evidence for investigation

This point distribution system ensures balanced, evidence-based risk assessment for criminal intelligence analysis.