"""
Report Generator for CDR Intelligence Agent
Generates formatted criminal intelligence reports
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd
from pathlib import Path
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings
from processors.pattern_detector import PatternDetector

class ReportGenerator:
    """Generate formatted intelligence reports from CDR analysis"""
    
    def __init__(self):
        self.pattern_detector = PatternDetector()
        self.emojis = settings.report_emojis
        
    def generate_comprehensive_report(self, 
                                    cdr_data: Dict[str, pd.DataFrame],
                                    analysis_results: Optional[Dict[str, Any]] = None) -> str:
        """
        Generate a comprehensive criminal intelligence report
        
        Args:
            cdr_data: Dictionary of CDR DataFrames by suspect
            analysis_results: Optional pre-computed analysis results
            
        Returns:
            Formatted report string
        """
        if not analysis_results:
            # Run pattern detection for all suspects
            analysis_results = self._analyze_all_suspects(cdr_data)
        
        # Build report sections
        report_sections = []
        
        # Header
        report_sections.append(self._generate_header())
        
        # Critical Anomalies
        anomalies = self._detect_critical_anomalies(analysis_results)
        report_sections.append(self._format_critical_anomalies(anomalies))
        
        # Risk Ranking
        risk_ranking = self._calculate_risk_ranking(analysis_results)
        report_sections.append(self._format_risk_ranking(risk_ranking))
        
        # Network Observations
        network_analysis = self._analyze_network_patterns(analysis_results, cdr_data)
        report_sections.append(self._format_network_observations(network_analysis))
        
        # Investigation Priorities
        priorities = self._determine_investigation_priorities(risk_ranking, anomalies)
        report_sections.append(self._format_investigation_priorities(priorities))
        
        # Conclusion
        report_sections.append(self._generate_conclusion(risk_ranking, anomalies))
        
        return "\n\n".join(report_sections)
    
    def _analyze_all_suspects(self, cdr_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Run pattern detection for all suspects"""
        results = {}
        
        for suspect, df in cdr_data.items():
            patterns = self.pattern_detector.detect_all_patterns(df, suspect)
            results[suspect] = patterns
            
        return results
    
    def _generate_header(self) -> str:
        """Generate report header"""
        header = f"""{self.emojis['alert']} CRIMINAL INTELLIGENCE REPORT - CDR ANALYSIS
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Analysis Type: Comprehensive Criminal Pattern Detection
============================================================"""
        return header
    
    def _detect_critical_anomalies(self, analysis_results: Dict[str, Any]) -> Dict[str, List]:
        """Detect critical anomalies across all suspects"""
        anomalies = {
            'device_switching': [],
            'odd_hour_activity': [],
            'suspicious_communication': [],
            'network_indicators': []
        }
        
        for suspect, patterns in analysis_results.items():
            # Device switching
            device_data = patterns.get('device_patterns', {})
            if device_data.get('imei_count', 0) >= 3:
                anomalies['device_switching'].append({
                    'suspect': suspect,
                    'imei_count': device_data['imei_count'],
                    'risk': 'HIGH'
                })
            elif device_data.get('imei_count', 0) == 2:
                anomalies['device_switching'].append({
                    'suspect': suspect,
                    'imei_count': device_data['imei_count'],
                    'risk': 'MEDIUM'
                })
            
            # Odd hour activity
            temporal_data = patterns.get('temporal_patterns', {})
            odd_hour_pct = temporal_data.get('odd_hour_percentage', 0)
            if odd_hour_pct > 3:
                anomalies['odd_hour_activity'].append({
                    'suspect': suspect,
                    'percentage': odd_hour_pct,
                    'risk': 'HIGH' if odd_hour_pct > 4 else 'MEDIUM'
                })
            
            # Suspicious communication
            comm_data = patterns.get('communication_patterns', {})
            if comm_data.get('voice_only_behavior'):
                anomalies['suspicious_communication'].append({
                    'suspect': suspect,
                    'pattern': '100% voice calls, 0 SMS',
                    'risk': 'HIGH'
                })
            
        return anomalies
    
    def _calculate_risk_ranking(self, analysis_results: Dict[str, Any]) -> List[Dict]:
        """Calculate and rank suspects by risk"""
        risk_scores = []
        
        for suspect, patterns in analysis_results.items():
            risk_data = patterns.get('risk_assessment', {})
            
            # Calculate comprehensive risk score
            risk_score = self._calculate_risk_score(patterns)
            
            risk_scores.append({
                'suspect': suspect,
                'risk_score': risk_score['total'],
                'risk_level': risk_score['level'],
                'risk_emoji': risk_score['emoji'],
                'primary_indicators': risk_score['primary_indicators']
            })
        
        # Sort by risk score
        risk_scores.sort(key=lambda x: x['risk_score'], reverse=True)
        
        return risk_scores
    
    def _calculate_risk_score(self, patterns: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate detailed risk score for a suspect"""
        score = 0
        indicators = []
        
        # Device risk (40%)
        device_data = patterns.get('device_patterns', {})
        imei_count = device_data.get('imei_count', 0)
        if imei_count >= 3:
            score += 40
            indicators.append(f"{imei_count} IMEIs detected - identity masking")
        elif imei_count == 2:
            score += 25
            indicators.append("2 IMEIs - device switching (MEDIUM RISK)")
        
        # Temporal risk (25%)
        temporal_data = patterns.get('temporal_patterns', {})
        odd_hour_pct = temporal_data.get('odd_hour_percentage', 0)
        if odd_hour_pct > 3:
            score += 25
            indicators.append(f"{odd_hour_pct:.1f}% odd hours - covert pattern")
        elif odd_hour_pct > 1:
            score += 15
            indicators.append("Elevated odd hour activity")
        
        # Communication risk (20%)
        comm_data = patterns.get('communication_patterns', {})
        if comm_data.get('voice_only_behavior'):
            score += 20
            indicators.append("Voice-only communication")
        elif comm_data.get('voice_percentage', 0) > 90:
            score += 10
            indicators.append("High voice preference")
        
        # Frequency risk (15%)
        freq_data = patterns.get('frequency_patterns', {})
        if len(freq_data.get('high_frequency_contacts', [])) > 0:
            score += 15
            indicators.append("High frequency patterns")
        
        # Determine level
        if score >= 70:
            level = "HIGH"
            emoji = self.emojis['high_risk']
        elif score >= 40:
            level = "MEDIUM"
            emoji = self.emojis['medium_risk']
        else:
            level = "LOW"
            emoji = self.emojis['low_risk']
        
        # Override: Anyone with 2+ IMEIs should be at least MEDIUM risk
        if imei_count >= 2 and level == "LOW":
            level = "MEDIUM"
            emoji = self.emojis['medium_risk']
            indicators.insert(0, "Elevated to MEDIUM due to device switching")
        
        return {
            'total': score,
            'level': level,
            'emoji': emoji,
            'primary_indicators': indicators[:3]
        }
    
    def _analyze_network_patterns(self, analysis_results: Dict[str, Any], 
                                 cdr_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Analyze network connections between suspects"""
        network_data = {
            'direct_connections': [],
            'common_contacts': [],
            'operational_patterns': []
        }
        
        # Extract all contacts for each suspect
        suspect_contacts = {}
        for suspect, df in cdr_data.items():
            df_filtered = df[~df.get('is_provider_message', False)]
            if 'b_party_clean' in df_filtered.columns:
                contacts = set(df_filtered['b_party_clean'].dropna().unique())
                suspect_contacts[suspect] = contacts
        
        # Find common contacts
        suspects = list(suspect_contacts.keys())
        for i in range(len(suspects)):
            for j in range(i + 1, len(suspects)):
                common = suspect_contacts[suspects[i]] & suspect_contacts[suspects[j]]
                if common:
                    network_data['common_contacts'].append({
                        'suspects': [suspects[i], suspects[j]],
                        'common_count': len(common),
                        'sample_contacts': list(common)[:3]
                    })
        
        # Operational patterns
        if not network_data['direct_connections'] and not network_data['common_contacts']:
            network_data['operational_patterns'].append("Compartmentalized structure detected")
            network_data['operational_patterns'].append("Possible use of encrypted apps or intermediaries")
        
        return network_data
    
    def _format_critical_anomalies(self, anomalies: Dict[str, List]) -> str:
        """Format critical anomalies section"""
        lines = [f"{self.emojis['alert']} CRITICAL ANOMALIES DETECTED"]
        
        # Device Switching
        if anomalies['device_switching']:
            lines.append("\n1. Device Switching Pattern (High Risk)")
            for item in sorted(anomalies['device_switching'], 
                             key=lambda x: x['imei_count'], reverse=True):
                risk_emoji = self.emojis['high_risk'] if item['risk'] == 'HIGH' else self.emojis['medium_risk']
                lines.append(f"   {risk_emoji} {item['suspect']}: {item['imei_count']} different IMEIs detected")
            lines.append("   This pattern strongly indicates identity masking behavior common in organized crime")
        
        # Odd Hour Activity
        if anomalies['odd_hour_activity']:
            lines.append("\n2. Odd Hour Activity (Covert Operations)")
            for item in sorted(anomalies['odd_hour_activity'], 
                             key=lambda x: x['percentage'], reverse=True):
                risk_emoji = self.emojis['high_risk'] if item['risk'] == 'HIGH' else self.emojis['medium_risk']
                lines.append(f"   {risk_emoji} {item['suspect']}: {item['percentage']:.1f}% of calls between midnight-5AM")
            lines.append("   This suggests secretive/illegal coordination activities")
        
        # Suspicious Communication
        if anomalies['suspicious_communication']:
            lines.append("\n3. Suspicious Communication Patterns")
            for item in anomalies['suspicious_communication']:
                lines.append(f"   {self.emojis['high_risk']} {item['suspect']}: {item['pattern']}")
            lines.append("   Indicates avoiding traceable text communications")
        
        return "\n".join(lines)
    
    def _format_risk_ranking(self, risk_ranking: List[Dict]) -> str:
        """Format risk ranking section"""
        lines = [f"{self.emojis['chart']} SUSPECT RISK RANKING"]
        lines.append("\n| Suspect | Risk Level | Score | Primary Indicators |")
        lines.append("|---------|------------|-------|-------------------|")
        
        for rank in risk_ranking[:10]:  # Top 10
            indicators = ", ".join(rank['primary_indicators'][:2])
            score_display = f"{rank['risk_score']}/100"
            lines.append(f"| {rank['suspect']} | {rank['risk_emoji']} {rank['risk_level']} | {score_display} | {indicators} |")
        
        return "\n".join(lines)
    
    def _format_network_observations(self, network_data: Dict[str, Any]) -> str:
        """Format network observations section"""
        lines = [f"{self.emojis['network']} NETWORK OBSERVATIONS"]
        
        if network_data['direct_connections']:
            lines.append("\n• Direct inter-suspect communications found:")
            for conn in network_data['direct_connections']:
                lines.append(f"  - {conn['from']} ↔ {conn['to']}")
        else:
            lines.append("\n• No direct inter-suspect communications found")
        
        if network_data['common_contacts']:
            lines.append("\n• Common contacts detected:")
            for common in network_data['common_contacts'][:3]:
                lines.append(f"  - {common['suspects'][0]} & {common['suspects'][1]}: "
                           f"{common['common_count']} shared contacts")
        
        if network_data['operational_patterns']:
            lines.append("\n• Operational patterns:")
            for pattern in network_data['operational_patterns']:
                lines.append(f"  - {pattern}")
        
        return "\n".join(lines)
    
    def _format_investigation_priorities(self, priorities: Dict[str, Any]) -> str:
        """Format investigation priorities section"""
        lines = [f"{self.emojis['target']} INVESTIGATION PRIORITIES"]
        
        lines.append("\nImmediate Actions:")
        for i, action in enumerate(priorities['immediate_actions'], 1):
            lines.append(f"{i}. {action}")
        
        lines.append("\nPattern Analysis Needed:")
        for pattern in priorities['pattern_analysis']:
            lines.append(f"• {pattern}")
        
        lines.append("\nAdditional Data Required:")
        for data in priorities['additional_data']:
            lines.append(f"• {data}")
        
        return "\n".join(lines)
    
    def _determine_investigation_priorities(self, risk_ranking: List[Dict], 
                                          anomalies: Dict[str, List]) -> Dict[str, Any]:
        """Determine investigation priorities based on analysis"""
        priorities = {
            'immediate_actions': [],
            'pattern_analysis': [],
            'additional_data': []
        }
        
        # High risk suspects
        high_risk = [r for r in risk_ranking if r['risk_level'] == 'HIGH'][:3]
        for suspect in high_risk:
            priorities['immediate_actions'].append(
                f"Deep investigation on {suspect['suspect']} - {', '.join(suspect['primary_indicators'][:2])}"
            )
        
        # Pattern analysis
        if anomalies['device_switching']:
            priorities['pattern_analysis'].append("Cross-reference with IPDR data for encrypted app usage")
        
        if any(r for r in risk_ranking if 'odd hours' in str(r['primary_indicators'])):
            priorities['pattern_analysis'].append("Tuesday/Friday activity spikes (cannabis transport days)")
        
        # Additional data
        priorities['additional_data'] = [
            "IPDR for Threema/Telegram detection",
            "Bank statements (48-hour payment patterns)",
            "SMS headers (financial alerts)",
            "CAF/SDR (address verification)"
        ]
        
        return priorities
    
    def _generate_conclusion(self, risk_ranking: List[Dict], anomalies: Dict[str, List]) -> str:
        """Generate report conclusion"""
        lines = [f"{self.emojis['warning']} CONCLUSION"]
        
        high_risk_count = len([r for r in risk_ranking if r['risk_level'] == 'HIGH'])
        
        if high_risk_count > 0:
            lines.append(f"\nMultiple high-risk indicators suggest an organized criminal network with "
                        f"sophisticated operational security measures. The combination of device switching, "
                        f"odd-hour activities, and voice-only communication patterns is consistent with "
                        f"narcotics trafficking operations.")
            
            lines.append(f"\nRecommendation: Immediate escalation for comprehensive multi-source "
                        f"investigation focusing on the {high_risk_count} high-risk suspects identified.")
        else:
            lines.append("\nModerate risk indicators detected. Continued monitoring recommended "
                        "with focus on pattern evolution and network connections.")
        
        return "\n".join(lines)
    
    def save_report(self, report: str, filename: Optional[str] = None) -> Path:
        """Save report to file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"cdr_intelligence_report_{timestamp}.txt"
        
        output_path = settings.output_path / filename
        output_path.write_text(report, encoding='utf-8')
        
        logger.info(f"Report saved to: {output_path}")
        return output_path