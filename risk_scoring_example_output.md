# Enhanced Risk Scoring - Example Output

## What's New in the Analyze Command

The `analyze` command now shows detailed point breakdowns for each suspect, explaining exactly where their risk score comes from.

## Example Output

```
🚨 CRIMINAL INTELLIGENCE REPORT - CDR ANALYSIS
============================================================

📊 SUSPECT RISK RANKING

| Suspect | Risk Level | Score | Primary Indicators |
|---------|------------|-------|-------------------|
| Peer basha_9391251134 | 🔴 HIGH | 65/100 | 2 IMEIs detected, 13.4% odd hours |
| Danial Srikakulam_9309347633 | 🟡 MEDIUM | 45/100 | 4.5% odd hours, Voice-only communication |
| Ali_9042720423 | 🟡 MEDIUM | 35/100 | 2 IMEIs detected, High frequency patterns |
| Kiran Kakinada_9542311767 | 🟡 MEDIUM | 30/100 | Elevated odd hour activity |
| Srujan_9959473744 | 🟢 LOW | 25/100 | High voice preference |
| Varshith_9704444251 | 🟢 LOW | 25/100 | High frequency patterns |

🎯 DETAILED RISK SCORING BREAKDOWN
------------------------------------------------------------

🔴 Peer basha_9391251134
   Total Risk Score: 65/100 (HIGH)
   ──────────────────────────────────────────────────
   📱 Device Risk: 25/25 points
      • 2 IMEIs detected: +25 points (device switching)
   ⏰ Temporal Risk: 20/25 points
      • 13.4% odd hour calls: +20 points (VERY HIGH)
   📞 Communication Risk: 5/25 points
      • 95% voice calls: +5 points
   📊 Frequency Risk: 10/15 points
      • 2 high frequency contacts: +10 points
   🌐 Network Risk: 5/10 points
      • 6 common contacts: +5 points

🟡 Ali_9042720423
   Total Risk Score: 35/100 (MEDIUM)
   ──────────────────────────────────────────────────
   📱 Device Risk: 25/25 points
      • 2 IMEIs detected: +25 points (device switching)
   ⏰ Temporal Risk: 0/25 points
   📞 Communication Risk: 0/25 points
   📊 Frequency Risk: 10/15 points
      • 1 high frequency contact: +10 points
   🌐 Network Risk: 0/10 points
   ⚠️ Override Applied: Elevated to MEDIUM due to 2+ IMEIs

🟡 Danial Srikakulam_9309347633
   Total Risk Score: 45/100 (MEDIUM)
   ──────────────────────────────────────────────────
   📱 Device Risk: 0/25 points
   ⏰ Temporal Risk: 15/25 points
      • 4.5% odd hour calls: +15 points
   📞 Communication Risk: 20/25 points
      • 100% voice calls (no SMS): +20 points (avoiding traces)
   📊 Frequency Risk: 5/15 points
   🌐 Network Risk: 5/10 points
      • 4 common contacts: +5 points

🎯 INVESTIGATION PRIORITIES
------------------------------------------------------------

🔴 Peer basha_9391251134 - IMMEDIATE ACTION REQUIRED
   • 2 IMEIs detected - device switching (MEDIUM RISK)
   • 13.4% odd hour calls - VERY HIGH
   • 95% voice calls

⚠️ RECOMMENDATIONS:
1. IMMEDIATE ACTION REQUIRED:
   - Priority surveillance on Peer basha (device switching + high odd hours)
   - Monitor Ali's device switching patterns
   - Investigate Danial's voice-only behavior
```

## Key Benefits

1. **Transparency**: Law enforcement can see exactly why each suspect is scored
2. **Justification**: Every point is explained with the specific pattern detected
3. **Override Visibility**: Shows when rules like "2+ IMEIs = MEDIUM minimum" are applied
4. **Actionable Intelligence**: Clear breakdown helps prioritize investigation resources
5. **Evidence Trail**: Point sources can be used in investigation reports

## How It Works

### Risk Components Breakdown:
- **Device Risk** (0-25 points): IMEIs, SIM swapping
- **Temporal Risk** (0-25 points): Odd hours, call bursts
- **Communication Risk** (0-25 points): Voice-only, patterns
- **Frequency Risk** (0-15 points): High frequency contacts
- **Network Risk** (0-10 points): Inter-suspect connections

### Example Scoring:
```
Ali's Score Calculation:
- Device Risk: 25 points (2 IMEIs)
- Temporal Risk: 0 points (low odd hours)
- Communication Risk: 0 points (mixed voice/SMS)
- Frequency Risk: 10 points (1 high freq contact)
- Network Risk: 0 points (no direct connections)
Total: 35/100 = MEDIUM (override applied due to 2 IMEIs)
```

This enhancement makes the CDR Intelligence Agent's risk assessment completely transparent and auditable for law enforcement use.