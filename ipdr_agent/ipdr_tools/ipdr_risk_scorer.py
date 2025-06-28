"""
IPDR Risk Scorer Tool
Comprehensive risk assessment based on all IPDR analysis components
"""

from typing import Dict, Optional, Any, List, Type
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
import pandas as pd
import numpy as np
from datetime import datetime
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config import settings
from ipdr_agent.ipdr_tools.encryption_analysis import EncryptionAnalysisTool
from ipdr_agent.ipdr_tools.data_pattern_analysis import DataPatternAnalysisTool
from ipdr_agent.ipdr_tools.session_analysis import SessionAnalysisTool
from ipdr_agent.ipdr_tools.app_fingerprinting import AppFingerprintingTool

class IPDRRiskScorerInput(BaseModel):
    """Input for IPDR risk scorer tool"""
    query: str = Field(description="Risk assessment query (e.g., 'calculate risk scores', 'high-risk suspects', 'risk summary')")
    suspect_name: Optional[str] = Field(default=None, description="Specific suspect to assess")

class IPDRRiskScorerTool(BaseTool):
    """Tool for comprehensive IPDR risk assessment"""
    
    name: str = "ipdr_risk_scorer"
    description: str = """Calculate comprehensive risk scores for suspects based on IPDR data analysis including 
    encryption patterns, data usage, session anomalies, and app fingerprinting. Provides overall risk assessment 
    and prioritization. Examples: 'calculate risk scores', 'find high-risk suspects', 'generate risk report'"""
    
    args_schema: Type[BaseModel] = IPDRRiskScorerInput
    ipdr_data: Dict[str, pd.DataFrame] = {}
    
    def _run(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Run comprehensive IPDR risk assessment"""
        try:
            if not self.ipdr_data:
                return "No IPDR data loaded. Please load IPDR data first."
            
            # Initialize sub-tools
            encryption_tool = EncryptionAnalysisTool()
            data_pattern_tool = DataPatternAnalysisTool()
            session_tool = SessionAnalysisTool()
            app_fingerprint_tool = AppFingerprintingTool()
            
            # Share data with sub-tools
            encryption_tool.ipdr_data = self.ipdr_data
            data_pattern_tool.ipdr_data = self.ipdr_data
            session_tool.ipdr_data = self.ipdr_data
            app_fingerprint_tool.ipdr_data = self.ipdr_data
            
            analyze_all = "all" in query.lower() or not suspect_name
            results = []
            suspects_to_analyze = self.ipdr_data.keys() if analyze_all else [suspect_name]
            
            for suspect in suspects_to_analyze:
                if suspect in self.ipdr_data:
                    risk_assessment = self._assess_suspect_risk(
                        suspect, 
                        self.ipdr_data[suspect],
                        encryption_tool,
                        data_pattern_tool,
                        session_tool,
                        app_fingerprint_tool
                    )
                    results.append(risk_assessment)
            
            if not results:
                return "No suspects found for risk assessment."
            
            # Sort by overall risk score
            results.sort(key=lambda x: x['overall_risk_score'], reverse=True)
            
            response = self._format_risk_assessment(results, query)
            return response
            
        except Exception as e:
            logger.error(f"Error in IPDR risk assessment: {str(e)}")
            return f"Error calculating risk scores: {str(e)}"
    
    async def _arun(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Async not implemented"""
        raise NotImplementedError("Async execution not supported")
    
    def _assess_suspect_risk(self, suspect: str, df: pd.DataFrame,
                           encryption_tool, data_pattern_tool, 
                           session_tool, app_fingerprint_tool) -> Dict[str, Any]:
        """Comprehensive risk assessment for a single suspect"""
        
        risk_assessment = {
            'suspect': suspect,
            'total_sessions': len(df),
            'risk_components': {},
            'risk_factors': [],
            'overall_risk_level': 'LOW',
            'overall_risk_score': 0,
            'priority_rank': 0,
            'investigation_notes': []
        }
        
        # Get individual analysis results
        encryption_analysis = self._get_encryption_score(suspect, encryption_tool)
        data_analysis = self._get_data_pattern_score(suspect, data_pattern_tool)
        session_analysis = self._get_session_score(suspect, session_tool)
        app_analysis = self._get_app_fingerprint_score(suspect, app_fingerprint_tool)
        
        # Aggregate risk components
        risk_assessment['risk_components'] = {
            'encryption_risk': {
                'score': encryption_analysis['score'],
                'level': encryption_analysis['level'],
                'factors': encryption_analysis['factors']
            },
            'data_pattern_risk': {
                'score': data_analysis['score'],
                'level': data_analysis['level'],
                'factors': data_analysis['factors']
            },
            'session_risk': {
                'score': session_analysis['score'],
                'level': session_analysis['level'],
                'factors': session_analysis['factors']
            },
            'app_risk': {
                'score': app_analysis['score'],
                'level': app_analysis['level'],
                'factors': app_analysis['factors']
            }
        }
        
        # Calculate weighted overall score
        weights = settings.ipdr_risk_weights
        
        weighted_score = (
            encryption_analysis['score'] * weights.get('encryption', 0.3) +
            data_analysis['score'] * weights.get('data_patterns', 0.25) +
            session_analysis['score'] * weights.get('session_anomalies', 0.25) +
            app_analysis['score'] * weights.get('app_behavior', 0.2)
        )
        
        risk_assessment['overall_risk_score'] = int(weighted_score)
        
        # Apply risk multipliers for critical patterns
        multipliers = self._calculate_risk_multipliers(risk_assessment)
        
        if multipliers['multiplier'] > 1.0:
            risk_assessment['overall_risk_score'] = min(
                int(risk_assessment['overall_risk_score'] * multipliers['multiplier']), 
                100
            )
            risk_assessment['risk_factors'].extend(multipliers['reasons'])
        
        # Determine overall risk level
        thresholds = settings.ipdr_risk_thresholds
        if risk_assessment['overall_risk_score'] >= thresholds['high']:
            risk_assessment['overall_risk_level'] = 'CRITICAL'
        elif risk_assessment['overall_risk_score'] >= thresholds['medium']:
            risk_assessment['overall_risk_level'] = 'HIGH'
        elif risk_assessment['overall_risk_score'] >= thresholds['low']:
            risk_assessment['overall_risk_level'] = 'MEDIUM'
        else:
            risk_assessment['overall_risk_level'] = 'LOW'
        
        # Generate investigation notes
        risk_assessment['investigation_notes'] = self._generate_investigation_notes(risk_assessment)
        
        return risk_assessment
    
    def _get_encryption_score(self, suspect: str, encryption_tool) -> Dict[str, Any]:
        """Get encryption analysis score"""
        
        try:
            # Run encryption analysis for specific suspect
            result = encryption_tool._analyze_suspect_encryption(
                suspect, 
                self.ipdr_data[suspect]
            )
            
            factors = []
            if result['encryption_percentage'] > 60:
                factors.append(f"{result['encryption_percentage']}% encrypted sessions")
            if result['odd_hour_encryption'] > 20:
                factors.append(f"{result['odd_hour_encryption']} odd-hour encrypted sessions")
            
            return {
                'score': result['encryption_score'],
                'level': result['encryption_risk'],
                'factors': factors[:3]
            }
        except Exception as e:
            logger.error(f"Error getting encryption score: {e}")
            return {'score': 0, 'level': 'LOW', 'factors': []}
    
    def _get_data_pattern_score(self, suspect: str, data_pattern_tool) -> Dict[str, Any]:
        """Get data pattern analysis score"""
        
        try:
            result = data_pattern_tool._analyze_suspect_data_patterns(
                suspect,
                self.ipdr_data[suspect]
            )
            
            factors = []
            if len(result['large_uploads']) > 5:
                factors.append(f"{len(result['large_uploads'])} large uploads detected")
            if result['upload_download_ratio'] > 2.0:
                factors.append(f"Upload/Download ratio: {result['upload_download_ratio']}")
            
            return {
                'score': result['data_anomaly_score'],
                'level': result['data_risk'],
                'factors': factors[:3]
            }
        except Exception as e:
            logger.error(f"Error getting data pattern score: {e}")
            return {'score': 0, 'level': 'LOW', 'factors': []}
    
    def _get_session_score(self, suspect: str, session_tool) -> Dict[str, Any]:
        """Get session analysis score"""
        
        try:
            result = session_tool._analyze_suspect_sessions(
                suspect,
                self.ipdr_data[suspect]
            )
            
            factors = []
            if len(result['marathon_sessions']) > 5:
                factors.append(f"{len(result['marathon_sessions'])} marathon sessions")
            if len(result['concurrent_sessions']) > 5:
                factors.append(f"{len(result['concurrent_sessions'])} concurrent sessions")
            
            return {
                'score': result['session_anomaly_score'],
                'level': result['session_risk'],
                'factors': factors[:3]
            }
        except Exception as e:
            logger.error(f"Error getting session score: {e}")
            return {'score': 0, 'level': 'LOW', 'factors': []}
    
    def _get_app_fingerprint_score(self, suspect: str, app_fingerprint_tool) -> Dict[str, Any]:
        """Get app fingerprinting score"""
        
        try:
            result = app_fingerprint_tool._analyze_suspect_apps(
                suspect,
                self.ipdr_data[suspect]
            )
            
            factors = []
            if len(result['high_risk_apps']) > 0:
                factors.append(f"{len(result['high_risk_apps'])} high-risk apps")
            if len(result['unknown_apps']) > 10:
                factors.append(f"{len(result['unknown_apps'])} unknown apps")
            
            return {
                'score': result['app_risk_score'],
                'level': result['app_risk'],
                'factors': factors[:3]
            }
        except Exception as e:
            logger.error(f"Error getting app fingerprint score: {e}")
            return {'score': 0, 'level': 'LOW', 'factors': []}
    
    def _calculate_risk_multipliers(self, assessment: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate risk multipliers for critical patterns"""
        
        multiplier = 1.0
        reasons = []
        
        # Check for critical combinations
        components = assessment['risk_components']
        
        # High encryption + High data patterns = Evidence sharing
        if (components['encryption_risk']['level'] == 'HIGH' and 
            components['data_pattern_risk']['level'] == 'HIGH'):
            multiplier *= 1.3
            reasons.append("High encryption + Large uploads (evidence sharing pattern)")
        
        # High session anomalies + High app risk = Operational security
        if (components['session_risk']['level'] == 'HIGH' and 
            components['app_risk']['level'] == 'HIGH'):
            multiplier *= 1.2
            reasons.append("Session anomalies + High-risk apps (OpSec pattern)")
        
        # All components elevated = Highly suspicious
        elevated_count = sum(1 for comp in components.values() 
                           if comp['level'] in ['MEDIUM', 'HIGH'])
        if elevated_count >= 3:
            multiplier *= 1.25
            reasons.append("Multiple elevated risk components")
        
        return {
            'multiplier': multiplier,
            'reasons': reasons
        }
    
    def _generate_investigation_notes(self, assessment: Dict[str, Any]) -> List[str]:
        """Generate specific investigation recommendations"""
        
        notes = []
        components = assessment['risk_components']
        
        # Critical risk recommendations
        if assessment['overall_risk_level'] == 'CRITICAL':
            notes.append("🚨 IMMEDIATE ACTION REQUIRED - High probability of criminal activity")
            notes.append("Recommend immediate surveillance and warrant consideration")
        
        # Component-specific recommendations
        if components['encryption_risk']['level'] == 'HIGH':
            notes.append("Focus on encrypted communication patterns - possible evidence coordination")
        
        if components['data_pattern_risk']['level'] == 'HIGH':
            notes.append("Investigate large uploads - potential evidence/contraband sharing")
        
        if components['session_risk']['level'] == 'HIGH':
            notes.append("Analyze marathon sessions for planning/review activities")
        
        if components['app_risk']['level'] == 'HIGH':
            notes.append("Monitor high-risk app usage and unknown services")
        
        # Pattern-specific notes
        for factor in assessment['risk_factors']:
            if "evidence sharing" in factor.lower():
                notes.append("Cross-reference upload times with criminal activity dates")
            elif "opsec pattern" in factor.lower():
                notes.append("Subject showing advanced operational security awareness")
        
        return notes[:5]  # Limit to top 5 recommendations
    
    def _format_risk_assessment(self, results: List[Dict], query: str) -> str:
        """Format comprehensive risk assessment results"""
        
        output = []
        output.append("🎯 IPDR COMPREHENSIVE RISK ASSESSMENT")
        output.append("=" * 50)
        output.append(f"Assessment Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output.append(f"Total Suspects Analyzed: {len(results)}")
        
        # Risk distribution
        critical = sum(1 for r in results if r['overall_risk_level'] == 'CRITICAL')
        high = sum(1 for r in results if r['overall_risk_level'] == 'HIGH')
        medium = sum(1 for r in results if r['overall_risk_level'] == 'MEDIUM')
        low = sum(1 for r in results if r['overall_risk_level'] == 'LOW')
        
        output.append(f"\nRisk Distribution:")
        output.append(f"   🔴 CRITICAL: {critical}")
        output.append(f"   🟠 HIGH: {high}")
        output.append(f"   🟡 MEDIUM: {medium}")
        output.append(f"   🟢 LOW: {low}")
        
        # Priority targets
        priority_targets = [r for r in results if r['overall_risk_level'] in ['CRITICAL', 'HIGH']]
        
        if priority_targets:
            output.append("\n🎯 PRIORITY INVESTIGATION TARGETS")
            output.append("-" * 40)
            
            for idx, target in enumerate(priority_targets, 1):
                emoji = "🔴" if target['overall_risk_level'] == 'CRITICAL' else "🟠"
                output.append(f"\n{emoji} Priority #{idx}: {target['suspect']}")
                output.append(f"   Overall Risk Score: {target['overall_risk_score']}/100 ({target['overall_risk_level']})")
                output.append("   Risk Components:")
                
                for comp_name, comp_data in target['risk_components'].items():
                    if comp_data['level'] != 'LOW':
                        output.append(f"     • {comp_name.replace('_', ' ').title()}: {comp_data['score']}/100 ({comp_data['level']})")
                        for factor in comp_data['factors'][:2]:
                            output.append(f"       - {factor}")
                
                if target['risk_factors']:
                    output.append("   Critical Patterns:")
                    for factor in target['risk_factors'][:2]:
                        output.append(f"     ⚠️ {factor}")
                
                if target['investigation_notes']:
                    output.append("   Investigation Priority:")
                    for note in target['investigation_notes'][:2]:
                        output.append(f"     → {note}")
        
        # Risk component analysis
        output.append("\n📊 RISK COMPONENT ANALYSIS")
        output.append("-" * 40)
        
        # Average scores by component
        component_names = ['encryption_risk', 'data_pattern_risk', 'session_risk', 'app_risk']
        component_averages = {}
        
        for comp in component_names:
            scores = [r['risk_components'][comp]['score'] for r in results]
            component_averages[comp] = np.mean(scores)
        
        output.append("Average Component Scores:")
        for comp, avg in sorted(component_averages.items(), key=lambda x: x[1], reverse=True):
            output.append(f"   • {comp.replace('_', ' ').title()}: {avg:.1f}/100")
        
        # Pattern detection summary
        output.append("\n🔍 DETECTED PATTERNS SUMMARY")
        output.append("-" * 40)
        
        pattern_counts = {
            'encryption_evidence': 0,
            'large_uploads': 0,
            'session_anomalies': 0,
            'high_risk_apps': 0
        }
        
        for result in results:
            if result['risk_components']['encryption_risk']['level'] == 'HIGH':
                pattern_counts['encryption_evidence'] += 1
            if result['risk_components']['data_pattern_risk']['level'] == 'HIGH':
                pattern_counts['large_uploads'] += 1
            if result['risk_components']['session_risk']['level'] == 'HIGH':
                pattern_counts['session_anomalies'] += 1
            if result['risk_components']['app_risk']['level'] == 'HIGH':
                pattern_counts['high_risk_apps'] += 1
        
        for pattern, count in sorted(pattern_counts.items(), key=lambda x: x[1], reverse=True):
            if count > 0:
                output.append(f"   • {pattern.replace('_', ' ').title()}: {count} suspects")
        
        # Operational recommendations
        output.append("\n💡 OPERATIONAL RECOMMENDATIONS")
        output.append("-" * 40)
        
        if critical > 0:
            output.append("1. 🚨 Immediate action required for CRITICAL risk suspects")
            output.append("2. Consider warrant applications for priority targets")
            output.append("3. Coordinate with cybercrime unit for digital evidence")
        elif high > 0:
            output.append("1. Initiate enhanced monitoring of HIGH risk suspects")
            output.append("2. Correlate IPDR patterns with physical surveillance")
            output.append("3. Prepare digital evidence preservation protocols")
        else:
            output.append("1. Continue routine monitoring")
            output.append("2. Watch for escalation in risk indicators")
        
        # Risk trend analysis
        output.append("\n📈 RISK INDICATORS TO MONITOR")
        output.append("-" * 40)
        output.append("• Sudden increase in encrypted app usage")
        output.append("• Large file uploads during odd hours")
        output.append("• Marathon sessions followed by communication bursts")
        output.append("• New unknown apps on non-standard ports")
        output.append("• Coordinated activity patterns across suspects")
        
        return "\n".join(output)