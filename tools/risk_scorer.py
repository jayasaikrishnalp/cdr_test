"""
Risk Scoring Tool for CDR Intelligence
Calculates comprehensive risk scores for suspects based on multiple factors
"""

from typing import Dict, Optional, Any, List, Type
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
import pandas as pd
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings
from processors.pattern_detector import PatternDetector

class RiskScoringInput(BaseModel):
    """Input for risk scoring tool"""
    query: str = Field(description="Risk assessment request (e.g., 'calculate risk scores', 'rank all suspects')")
    suspect_name: Optional[str] = Field(default=None, description="Specific suspect to assess")

class RiskScoringTool(BaseTool):
    """Tool for calculating risk scores and ranking suspects"""
    
    name: str = "risk_scoring"
    description: str = """Calculate comprehensive risk scores for suspects based on device switching, 
    temporal patterns, communication behavior, and other criminal indicators. 
    Use this to prioritize investigations.
    Examples: 'calculate risk scores for all suspects', 'rank suspects by risk level'"""
    
    args_schema: Type[BaseModel] = RiskScoringInput
    cdr_data: Dict[str, pd.DataFrame] = {}
    pattern_detector: Optional[Any] = None
    
    def __init__(self):
        super().__init__()
        self.pattern_detector = PatternDetector()
    
    def _run(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Run risk scoring analysis"""
        try:
            if not self.cdr_data:
                return "No CDR data loaded. Please load data first."
            
            analyze_all = "all" in query.lower() or not suspect_name
            results = []
            suspects_to_analyze = self.cdr_data.keys() if analyze_all else [suspect_name]
            
            for suspect in suspects_to_analyze:
                if suspect in self.cdr_data:
                    # Run comprehensive pattern detection
                    patterns = self.pattern_detector.detect_all_patterns(
                        self.cdr_data[suspect], 
                        suspect
                    )
                    
                    # Calculate enhanced risk score
                    risk_assessment = self._calculate_comprehensive_risk(patterns)
                    risk_assessment['suspect'] = suspect
                    results.append(risk_assessment)
            
            if not results:
                return "No suspects found for risk assessment."
            
            # Sort by risk score
            results.sort(key=lambda x: x['total_risk_score'], reverse=True)
            
            response = self._format_risk_assessment(results, query)
            return response
            
        except Exception as e:
            logger.error(f"Error in risk scoring: {str(e)}")
            return f"Error calculating risk scores: {str(e)}"
    
    async def _arun(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Async not implemented"""
        raise NotImplementedError("Async execution not supported")
    
    def _calculate_comprehensive_risk(self, patterns: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate comprehensive risk score with detailed breakdown"""
        risk_components = {
            'device_risk': 0,
            'temporal_risk': 0,
            'communication_risk': 0,
            'frequency_risk': 0,
            'location_risk': 0,
            'behavioral_risk': 0,
            'network_risk': 0
        }
        
        risk_factors = []
        
        # 1. Device Risk (0-25 points)
        device_data = patterns.get('device_patterns', {})
        imei_count = device_data.get('imei_count', 0)
        
        if imei_count >= 3:
            risk_components['device_risk'] = 25
            risk_factors.append(f"{imei_count} IMEIs detected - HIGH RISK")
        elif imei_count == 2:
            risk_components['device_risk'] = 25
            risk_factors.append(f"2 IMEIs detected - device switching (MEDIUM RISK)")
        elif device_data.get('sim_swapping_detected'):
            risk_components['device_risk'] = 10
            risk_factors.append("SIM swapping detected")
        
        # 2. Temporal Risk (0-20 points)
        temporal_data = patterns.get('temporal_patterns', {})
        odd_hour_pct = temporal_data.get('odd_hour_percentage', 0)
        
        if odd_hour_pct > 4:
            risk_components['temporal_risk'] = 20
            risk_factors.append(f"{odd_hour_pct:.1f}% odd hour activity - VERY HIGH")
        elif odd_hour_pct > 2:
            risk_components['temporal_risk'] = 15
            risk_factors.append(f"{odd_hour_pct:.1f}% odd hour activity")
        elif odd_hour_pct > 1:
            risk_components['temporal_risk'] = 10
            risk_factors.append("Elevated odd hour activity")
        
        # Add burst pattern risk
        burst_count = len(temporal_data.get('call_bursts', []))
        if burst_count > 3:
            risk_components['temporal_risk'] += 5
            risk_factors.append(f"{burst_count} call bursts detected")
        
        # 3. Communication Risk (0-20 points)
        comm_data = patterns.get('communication_patterns', {})
        
        if comm_data.get('voice_only_behavior'):
            risk_components['communication_risk'] = 20
            risk_factors.append("100% voice communication - NO SMS")
        elif comm_data.get('voice_percentage', 0) > 90:
            risk_components['communication_risk'] = 15
            risk_factors.append(f"{comm_data['voice_percentage']}% voice-heavy communication")
        
        if comm_data.get('repeated_durations'):
            risk_components['communication_risk'] += 5
            risk_factors.append("Repeated call durations (coded communication)")
        
        # 4. Frequency Risk (0-15 points)
        freq_data = patterns.get('frequency_patterns', {})
        high_freq_contacts = len(freq_data.get('high_frequency_contacts', []))
        
        if high_freq_contacts > 3:
            risk_components['frequency_risk'] = 15
            risk_factors.append(f"{high_freq_contacts} very high frequency contacts")
        elif high_freq_contacts > 0:
            risk_components['frequency_risk'] = 10
            risk_factors.append("High frequency contact patterns")
        
        # 5. Location Risk (0-10 points)
        location_data = patterns.get('location_patterns', {})
        
        if location_data.get('tower_hopping_detected'):
            risk_components['location_risk'] = 10
            risk_factors.append("Tower hopping detected (rapid movement)")
        elif len(location_data.get('rapid_movements', [])) > 2:
            risk_components['location_risk'] = 8
            risk_factors.append("Multiple rapid tower changes")
        
        # 6. Behavioral Risk (0-10 points)
        behavioral_data = patterns.get('behavioral_indicators', {})
        suspicious_patterns = behavioral_data.get('suspicious_patterns', [])
        
        if suspicious_patterns:
            risk_components['behavioral_risk'] = 10
            risk_factors.extend(suspicious_patterns)
        
        # 7. Network Risk (0-10 points)
        network_patterns = patterns.get('network_patterns', {})
        if network_patterns.get('direct_connections'):
            risk_components['network_risk'] = 10
            risk_factors.append("Direct connections to other suspects")
        elif network_patterns.get('common_contacts', 0) >= 3:
            risk_components['network_risk'] = 5
            risk_factors.append(f"{network_patterns.get('common_contacts', 0)} common contacts with suspects")
        
        # Calculate total risk score
        total_risk = sum(risk_components.values())
        
        # Determine risk level
        if total_risk >= 70:
            risk_level = "CRITICAL"
            risk_emoji = "🔴"
        elif total_risk >= 50:
            risk_level = "HIGH"
            risk_emoji = "🔴"
        elif total_risk >= 30:
            risk_level = "MEDIUM"
            risk_emoji = "🟡"
        else:
            risk_level = "LOW"
            risk_emoji = "🟢"
        
        # Override: Anyone with 2+ IMEIs should be at least MEDIUM risk
        device_data = patterns.get('device_patterns', {})
        imei_count = device_data.get('imei_count', 0)
        if imei_count >= 2 and risk_level == "LOW":
            risk_level = "MEDIUM"
            risk_emoji = "🟡"
            risk_factors.insert(0, "Elevated to MEDIUM due to device switching")
        
        return {
            'total_risk_score': total_risk,
            'risk_level': risk_level,
            'risk_emoji': risk_emoji,
            'risk_components': risk_components,
            'risk_factors': risk_factors,
            'primary_indicators': risk_factors[:3] if risk_factors else [],
            'patterns': patterns  # Include full pattern data
        }
    
    def _format_risk_assessment(self, results: List[Dict], query: str) -> str:
        """Format risk assessment results"""
        output = []
        
        output.append("🚨 CRIMINAL RISK ASSESSMENT REPORT")
        output.append("=" * 60)
        output.append(f"Analyzed {len(results)} suspects")
        
        # Group by risk level
        critical = [r for r in results if r['risk_level'] == 'CRITICAL']
        high = [r for r in results if r['risk_level'] == 'HIGH']
        medium = [r for r in results if r['risk_level'] == 'MEDIUM']
        low = [r for r in results if r['risk_level'] == 'LOW']
        
        # Summary
        output.append(f"\n📊 RISK DISTRIBUTION:")
        if critical:
            output.append(f"   🔴 CRITICAL RISK: {len(critical)} suspects")
        if high:
            output.append(f"   🔴 HIGH RISK: {len(high)} suspects")
        if medium:
            output.append(f"   🟡 MEDIUM RISK: {len(medium)} suspects")
        if low:
            output.append(f"   🟢 LOW RISK: {len(low)} suspects")
        
        # Detailed rankings
        output.append(f"\n📋 SUSPECT RISK RANKING")
        output.append("-" * 60)
        
        # Create table-like output
        output.append(f"{'Rank':<5} {'Suspect':<25} {'Risk':<10} {'Score':<7} {'Primary Indicators'}")
        output.append("-" * 80)
        
        for i, result in enumerate(results[:10], 1):  # Top 10
            suspect_name = result['suspect'][:24]  # Truncate if needed
            risk_display = f"{result['risk_emoji']} {result['risk_level']}"
            score_display = f"{result['total_risk_score']}/100"
            
            # Primary indicators
            indicators = result['primary_indicators']
            if indicators:
                indicator_str = indicators[0][:40]  # First indicator, truncated
            else:
                indicator_str = "No significant risks"
            
            output.append(f"{i:<5} {suspect_name:<25} {risk_display:<10} {score_display:<7} {indicator_str}")
        
        # Detailed breakdown for top suspects
        output.append(f"\n🎯 DETAILED RISK SCORING BREAKDOWN")
        output.append("-" * 60)
        
        # Show top suspects AND medium risk with details
        detailed_suspects = sorted(results, key=lambda x: x['total_risk_score'], reverse=True)
        detailed_suspects = [r for r in detailed_suspects if r['total_risk_score'] >= 30 or r['risk_level'] != 'LOW'][:5]
        
        for result in detailed_suspects:
            output.append(f"\n{result['risk_emoji']} {result['suspect']}")
            output.append(f"   Total Risk Score: {result['total_risk_score']}/100 ({result['risk_level']})")
            output.append("   " + "─" * 50)
            
            # Detailed point breakdown
            components = result['risk_components']
            patterns = result.get('patterns', {})
            
            # Device Risk Breakdown
            output.append(f"   📱 Device Risk: {components['device_risk']}/25 points")
            if components['device_risk'] > 0:
                device_patterns = patterns.get('device_patterns', {})
                imei_count = device_patterns.get('imei_count', 1)
                if imei_count >= 3:
                    output.append(f"      • {imei_count} IMEIs detected: +25 points (HIGH RISK)")
                elif imei_count == 2:
                    output.append(f"      • 2 IMEIs detected: +25 points (device switching)")
                if device_patterns.get('sim_swapping_detected'):
                    output.append(f"      • SIM swapping detected: +10 points")
            
            # Temporal Risk Breakdown  
            output.append(f"   ⏰ Temporal Risk: {components['temporal_risk']}/25 points")
            if components['temporal_risk'] > 0:
                temporal_patterns = patterns.get('temporal_patterns', {})
                odd_hour_pct = temporal_patterns.get('odd_hour_percentage', 0)
                if odd_hour_pct > 4:
                    output.append(f"      • {odd_hour_pct:.1f}% odd hour calls: +20 points (VERY HIGH)")
                elif odd_hour_pct > 2:
                    output.append(f"      • {odd_hour_pct:.1f}% odd hour calls: +15 points")
                elif odd_hour_pct > 1:
                    output.append(f"      • Elevated odd hour activity: +10 points")
                burst_count = len(temporal_patterns.get('call_bursts', []))
                if burst_count > 3:
                    output.append(f"      • {burst_count} call bursts: +5 points")
            
            # Communication Risk Breakdown
            output.append(f"   📞 Communication Risk: {components['communication_risk']}/25 points")
            if components['communication_risk'] > 0:
                comm_patterns = patterns.get('communication_patterns', {})
                if comm_patterns.get('voice_only_behavior'):
                    output.append(f"      • 100% voice calls (no SMS): +20 points (avoiding traces)")
                elif comm_patterns.get('voice_percentage', 0) > 90:
                    output.append(f"      • {comm_patterns['voice_percentage']}% voice calls: +15 points")
                if comm_patterns.get('repeated_durations'):
                    output.append(f"      • Repeated call durations: +5 points (coded communication)")
                if comm_patterns.get('circular_loops'):
                    output.append(f"      • Circular communication loops: +5 points")
                if comm_patterns.get('one_ring_patterns', {}).get('signaling_detected'):
                    output.append(f"      • One-ring signaling detected: +5 points")
            
            # Frequency Risk Breakdown
            output.append(f"   📊 Frequency Risk: {components['frequency_risk']}/15 points")
            if components['frequency_risk'] > 0:
                freq_patterns = patterns.get('frequency_patterns', {})
                high_freq = len(freq_patterns.get('high_frequency_contacts', []))
                if high_freq > 3:
                    output.append(f"      • {high_freq} high frequency contacts: +15 points")
                elif high_freq > 0:
                    output.append(f"      • {high_freq} high frequency contacts: +10 points")
            
            # Network Risk Breakdown
            output.append(f"   🌐 Network Risk: {components['network_risk']}/10 points")
            if components['network_risk'] > 0:
                network_patterns = patterns.get('network_patterns', {})
                if network_patterns.get('direct_connections'):
                    output.append(f"      • Direct suspect connections: +10 points")
                if network_patterns.get('common_contacts', 0) >= 3:
                    output.append(f"      • {network_patterns.get('common_contacts', 0)} common contacts: +5 points")
            
            # Override rules applied
            if "Elevated to MEDIUM due to device switching" in result.get('risk_factors', []):
                output.append("   ⚠️ Override Applied: Elevated to MEDIUM due to 2+ IMEIs")
            
            output.append("")  # Blank line between suspects
        
        # Investigation priorities for high/critical risk
        output.append(f"\n🎯 INVESTIGATION PRIORITIES")
        output.append("-" * 60)
        
        high_priority = [r for r in results if r['total_risk_score'] >= 50]
        if high_priority:
            for result in high_priority[:3]:
                output.append(f"\n{result['risk_emoji']} {result['suspect']} - IMMEDIATE ACTION REQUIRED")
                for factor in result['risk_factors'][:3]:
                    output.append(f"   • {factor}")
        
        # Recommendations
        output.append(f"\n⚠️ RECOMMENDATIONS:")
        
        if critical or high:
            output.append("1. IMMEDIATE ACTION REQUIRED:")
            for r in (critical + high)[:3]:
                output.append(f"   • Deep investigation on {r['suspect']}")
        
        output.append("\n2. ADDITIONAL DATA NEEDED:")
        output.append("   • IPDR data for encrypted app detection")
        output.append("   • Bank records for financial correlation")
        output.append("   • Tower dumps for meeting locations")
        
        # Pattern summary
        if any(r['patterns'].get('device_patterns', {}).get('imei_count', 0) >= 3 for r in results):
            output.append("\n3. ORGANIZED CRIME INDICATORS:")
            output.append("   • Multiple device switching patterns detected")
            output.append("   • Sophisticated operational security measures")
        
        return "\n".join(output)