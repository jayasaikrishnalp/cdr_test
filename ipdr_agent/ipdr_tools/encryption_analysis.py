"""
Encryption Analysis Tool for IPDR Intelligence
Analyzes encrypted application usage patterns and suspicious encryption behaviors
"""

from typing import Dict, Optional, Any, List, Type
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config import settings

class EncryptionAnalysisInput(BaseModel):
    """Input for encryption analysis tool"""
    query: str = Field(description="What encryption patterns to analyze (e.g., 'whatsapp usage', 'encrypted sessions', 'all encryption')")
    suspect_name: Optional[str] = Field(default=None, description="Specific suspect to analyze")

class EncryptionAnalysisTool(BaseTool):
    """Tool for analyzing encryption patterns in IPDR data"""
    
    name: str = "ipdr_encryption_analysis"
    description: str = """Analyze encrypted application usage including WhatsApp, Telegram, Signal, and other 
    encrypted communication apps. Detects patterns like high encryption usage, odd-hour encrypted sessions, 
    and suspicious encryption behaviors. Examples: 'analyze WhatsApp usage', 'find encrypted sessions at night', 
    'check encryption patterns for all suspects'"""
    
    args_schema: Type[BaseModel] = EncryptionAnalysisInput
    ipdr_data: Dict[str, pd.DataFrame] = {}
    
    def _run(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Run encryption analysis on IPDR data"""
        try:
            if not self.ipdr_data:
                return "No IPDR data loaded. Please load IPDR data first."
            
            analyze_all = "all" in query.lower() or not suspect_name
            results = []
            suspects_to_analyze = self.ipdr_data.keys() if analyze_all else [suspect_name]
            
            for suspect in suspects_to_analyze:
                if suspect in self.ipdr_data:
                    analysis = self._analyze_suspect_encryption(suspect, self.ipdr_data[suspect])
                    results.append(analysis)
            
            if not results:
                return "No suspects found for encryption analysis."
            
            # Sort by encryption risk (highest first)
            results.sort(key=lambda x: (x['encryption_score'], x['total_encrypted_sessions']), reverse=True)
            
            response = self._format_encryption_analysis(results, query)
            return response
            
        except Exception as e:
            logger.error(f"Error in encryption analysis: {str(e)}")
            return f"Error analyzing encryption patterns: {str(e)}"
    
    async def _arun(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Async not implemented"""
        raise NotImplementedError("Async execution not supported")
    
    def _analyze_suspect_encryption(self, suspect: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze encryption patterns for a single suspect"""
        
        analysis = {
            'suspect': suspect,
            'total_sessions': len(df),
            'total_encrypted_sessions': 0,
            'encryption_percentage': 0.0,
            'encrypted_apps': {},
            'encryption_timeline': {},
            'odd_hour_encryption': 0,
            'encryption_risk': 'LOW',
            'encryption_score': 0,
            'suspicious_patterns': []
        }
        
        # Filter encrypted sessions
        if 'is_encrypted' in df.columns:
            encrypted_df = df[df['is_encrypted'] == True].copy()
            analysis['total_encrypted_sessions'] = len(encrypted_df)
            
            if len(df) > 0:
                analysis['encryption_percentage'] = round((len(encrypted_df) / len(df)) * 100, 2)
            
            if len(encrypted_df) > 0:
                # Analyze by app
                if 'detected_app' in encrypted_df.columns:
                    app_counts = encrypted_df['detected_app'].value_counts()
                    for app, count in app_counts.items():
                        if app:  # Skip None values
                            analysis['encrypted_apps'][app] = {
                                'sessions': int(count),
                                'percentage': round((count / len(encrypted_df)) * 100, 2),
                                'risk': settings.app_signatures.get(app, {}).get('risk', 'UNKNOWN')
                            }
                
                # Temporal analysis
                if 'start_time' in encrypted_df.columns:
                    # Odd hour encryption
                    if 'is_odd_hour' in encrypted_df.columns:
                        analysis['odd_hour_encryption'] = encrypted_df['is_odd_hour'].sum()
                        odd_hour_pct = (analysis['odd_hour_encryption'] / len(encrypted_df)) * 100
                        
                        if odd_hour_pct > 30:
                            analysis['suspicious_patterns'].append({
                                'pattern': 'HIGH_ODD_HOUR_ENCRYPTION',
                                'value': f"{odd_hour_pct:.1f}% encrypted sessions at odd hours",
                                'severity': 'HIGH'
                            })
                    
                    # Daily patterns
                    encrypted_df['date'] = encrypted_df['start_time'].dt.date
                    daily_encryption = encrypted_df.groupby('date').size()
                    
                    # Detect spikes
                    if len(daily_encryption) > 3:
                        mean_daily = daily_encryption.mean()
                        std_daily = daily_encryption.std()
                        
                        for date, count in daily_encryption.items():
                            if count > mean_daily + (2 * std_daily):
                                analysis['suspicious_patterns'].append({
                                    'pattern': 'ENCRYPTION_SPIKE',
                                    'value': f"{count} encrypted sessions on {date}",
                                    'severity': 'MEDIUM'
                                })
                
                # Data volume analysis for encrypted sessions
                if 'total_data_volume' in encrypted_df.columns:
                    total_encrypted_data = encrypted_df['total_data_volume'].sum()
                    if total_encrypted_data > settings.large_upload_threshold * 100:  # 1GB
                        analysis['suspicious_patterns'].append({
                            'pattern': 'LARGE_ENCRYPTED_TRANSFER',
                            'value': f"{total_encrypted_data / 1073741824:.2f} GB encrypted data",
                            'severity': 'HIGH'
                        })
                
                # Session duration patterns
                if 'session_duration' in encrypted_df.columns:
                    avg_duration = encrypted_df['session_duration'].mean()
                    long_sessions = (encrypted_df['session_duration'] > 3600).sum()  # >1 hour
                    
                    if long_sessions > 10:
                        analysis['suspicious_patterns'].append({
                            'pattern': 'LONG_ENCRYPTED_SESSIONS',
                            'value': f"{long_sessions} sessions >1 hour",
                            'severity': 'MEDIUM'
                        })
        
        # Calculate encryption risk score
        analysis['encryption_score'] = self._calculate_encryption_score(analysis)
        
        # Determine risk level
        if analysis['encryption_score'] >= 70:
            analysis['encryption_risk'] = 'HIGH'
        elif analysis['encryption_score'] >= 40:
            analysis['encryption_risk'] = 'MEDIUM'
        else:
            analysis['encryption_risk'] = 'LOW'
        
        return analysis
    
    def _calculate_encryption_score(self, analysis: Dict[str, Any]) -> int:
        """Calculate encryption risk score (0-100)"""
        
        score = 0
        
        # Base score from encryption percentage
        enc_pct = analysis['encryption_percentage']
        if enc_pct >= 80:
            score += 30
        elif enc_pct >= 60:
            score += 20
        elif enc_pct >= 40:
            score += 10
        
        # High-risk apps
        high_risk_apps = sum(1 for app_data in analysis['encrypted_apps'].values() 
                           if app_data['risk'] in ['HIGH', 'CRITICAL'])
        score += min(high_risk_apps * 10, 20)
        
        # Odd hour encryption
        if analysis['total_encrypted_sessions'] > 0:
            odd_hour_pct = (analysis['odd_hour_encryption'] / analysis['total_encrypted_sessions']) * 100
            if odd_hour_pct > 30:
                score += 20
            elif odd_hour_pct > 15:
                score += 10
        
        # Suspicious patterns
        high_severity = sum(1 for p in analysis['suspicious_patterns'] if p['severity'] == 'HIGH')
        score += min(high_severity * 15, 30)
        
        return min(score, 100)
    
    def _format_encryption_analysis(self, results: List[Dict], query: str) -> str:
        """Format encryption analysis results"""
        
        output = []
        output.append("🔐 IPDR ENCRYPTION ANALYSIS")
        output.append("=" * 50)
        
        # High-risk suspects
        high_risk = [r for r in results if r['encryption_risk'] == 'HIGH']
        
        if high_risk:
            output.append("\n🚨 HIGH ENCRYPTION RISK SUSPECTS")
            output.append("-" * 40)
            
            for result in high_risk:
                output.append(f"\n🔴 {result['suspect']}")
                output.append(f"   Encryption Score: {result['encryption_score']}/100")
                output.append(f"   Total Encrypted Sessions: {result['total_encrypted_sessions']}")
                output.append(f"   Encryption Rate: {result['encryption_percentage']}%")
                
                if result['encrypted_apps']:
                    output.append("   Encrypted Apps Used:")
                    for app, data in sorted(result['encrypted_apps'].items(), 
                                          key=lambda x: x[1]['sessions'], reverse=True)[:3]:
                        output.append(f"     • {app.upper()}: {data['sessions']} sessions ({data['percentage']}%)")
                
                if result['suspicious_patterns']:
                    output.append("   ⚠️ Suspicious Patterns:")
                    for pattern in result['suspicious_patterns'][:3]:
                        output.append(f"     • {pattern['value']}")
        
        # Summary statistics
        output.append("\n📊 ENCRYPTION USAGE SUMMARY")
        output.append("-" * 40)
        
        total_encrypted = sum(r['total_encrypted_sessions'] for r in results)
        total_sessions = sum(r['total_sessions'] for r in results)
        
        output.append(f"Total Encrypted Sessions: {total_encrypted}")
        output.append(f"Overall Encryption Rate: {round((total_encrypted/total_sessions)*100, 2)}%" if total_sessions > 0 else "N/A")
        
        # App usage breakdown
        all_apps = defaultdict(int)
        for result in results:
            for app, data in result['encrypted_apps'].items():
                all_apps[app] += data['sessions']
        
        if all_apps:
            output.append("\n🔍 ENCRYPTED APP USAGE (ALL SUSPECTS)")
            output.append("-" * 40)
            for app, count in sorted(all_apps.items(), key=lambda x: x[1], reverse=True):
                risk = settings.app_signatures.get(app, {}).get('risk', 'UNKNOWN')
                emoji = "🔴" if risk == "HIGH" else "🟡" if risk == "MEDIUM" else "🟢"
                output.append(f"   {emoji} {app.upper()}: {count} total sessions")
        
        # Patterns of concern
        output.append("\n⚠️ PATTERNS OF CONCERN")
        output.append("-" * 40)
        
        # Check for coordinated encryption
        suspects_with_spikes = [r['suspect'] for r in results 
                               if any(p['pattern'] == 'ENCRYPTION_SPIKE' for p in r['suspicious_patterns'])]
        if len(suspects_with_spikes) > 2:
            output.append(f"   🔄 Coordinated encryption spikes: {', '.join(suspects_with_spikes)}")
        
        # Check for heavy night encryption
        night_encryptors = [r for r in results if r['odd_hour_encryption'] > 20]
        if night_encryptors:
            output.append(f"   🌙 Heavy night-time encryption: {', '.join([r['suspect'] for r in night_encryptors[:3]])}")
        
        # Recommendations
        output.append("\n💡 INVESTIGATION RECOMMENDATIONS")
        output.append("-" * 40)
        
        if high_risk:
            output.append("1. Priority surveillance on high-encryption suspects")
            output.append("2. Correlate encryption spikes with CDR silence periods")
            output.append("3. Check for encryption immediately after voice calls")
        else:
            output.append("1. Continue monitoring encryption patterns")
            output.append("2. Watch for sudden increases in encrypted app usage")
        
        return "\n".join(output)