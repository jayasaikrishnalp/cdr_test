"""
App Fingerprinting Tool for IPDR Intelligence
Advanced application identification and behavioral analysis
"""

from typing import Dict, Optional, Any, List, Type
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
import pandas as pd
import numpy as np
from collections import defaultdict
from datetime import datetime
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config import settings

class AppFingerprintingInput(BaseModel):
    """Input for app fingerprinting tool"""
    query: str = Field(description="What app patterns to analyze (e.g., 'unknown apps', 'high-risk apps', 'app behavior')")
    suspect_name: Optional[str] = Field(default=None, description="Specific suspect to analyze")

class AppFingerprintingTool(BaseTool):
    """Tool for advanced app identification and analysis in IPDR data"""
    
    name: str = "ipdr_app_fingerprinting"
    description: str = """Perform advanced application fingerprinting and behavioral analysis. Identifies unknown apps, 
    analyzes app usage patterns, detects suspicious app combinations, and profiles app-based behaviors. 
    Examples: 'identify unknown apps', 'analyze high-risk app usage', 'find suspicious app combinations'"""
    
    args_schema: Type[BaseModel] = AppFingerprintingInput
    ipdr_data: Dict[str, pd.DataFrame] = {}
    
    def _run(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Run app fingerprinting analysis on IPDR data"""
        try:
            if not self.ipdr_data:
                return "No IPDR data loaded. Please load IPDR data first."
            
            analyze_all = "all" in query.lower() or not suspect_name
            results = []
            suspects_to_analyze = self.ipdr_data.keys() if analyze_all else [suspect_name]
            
            for suspect in suspects_to_analyze:
                if suspect in self.ipdr_data:
                    analysis = self._analyze_suspect_apps(suspect, self.ipdr_data[suspect])
                    results.append(analysis)
            
            if not results:
                return "No suspects found for app fingerprinting analysis."
            
            # Sort by app risk score
            results.sort(key=lambda x: x['app_risk_score'], reverse=True)
            
            response = self._format_app_analysis(results, query)
            return response
            
        except Exception as e:
            logger.error(f"Error in app fingerprinting: {str(e)}")
            return f"Error analyzing app patterns: {str(e)}"
    
    async def _arun(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Async not implemented"""
        raise NotImplementedError("Async execution not supported")
    
    def _analyze_suspect_apps(self, suspect: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze app patterns for a single suspect"""
        
        analysis = {
            'suspect': suspect,
            'total_sessions': len(df),
            'identified_apps': {},
            'unknown_apps': [],
            'high_risk_apps': [],
            'app_combinations': {},
            'behavioral_patterns': {},
            'port_analysis': {},
            'app_risk': 'LOW',
            'app_risk_score': 0,
            'suspicious_app_patterns': []
        }
        
        # App identification statistics
        if 'detected_app' in df.columns:
            app_counts = df['detected_app'].value_counts(dropna=False)
            
            for app, count in app_counts.items():
                if pd.notna(app):
                    risk = settings.app_signatures.get(app, {}).get('risk', 'UNKNOWN')
                    analysis['identified_apps'][app] = {
                        'sessions': int(count),
                        'percentage': round((count / len(df)) * 100, 2),
                        'risk': risk
                    }
                    
                    if risk in ['HIGH', 'CRITICAL']:
                        analysis['high_risk_apps'].append({
                            'app': app,
                            'sessions': int(count),
                            'risk': risk
                        })
        
        # Unknown app analysis (sessions without app identification)
        unknown_sessions = df[df['detected_app'].isna()] if 'detected_app' in df.columns else df
        
        if len(unknown_sessions) > 0 and 'destination_port' in unknown_sessions.columns:
            unknown_ports = unknown_sessions.groupby('destination_port').size()
            
            for port, count in unknown_ports.items():
                if pd.notna(port) and count >= 5:  # Significant unknown traffic
                    port_info = {
                        'port': int(port),
                        'sessions': int(count),
                        'avg_data_mb': 0
                    }
                    
                    # Calculate average data for this port
                    port_sessions = unknown_sessions[unknown_sessions['destination_port'] == port]
                    if 'total_data_volume' in port_sessions.columns:
                        port_info['avg_data_mb'] = round(
                            port_sessions['total_data_volume'].mean() / 1048576, 2
                        )
                    
                    analysis['unknown_apps'].append(port_info)
            
            # Flag high unknown app usage
            unknown_percentage = (len(unknown_sessions) / len(df)) * 100
            if unknown_percentage > 30:
                analysis['suspicious_app_patterns'].append({
                    'pattern': 'HIGH_UNKNOWN_APP_USAGE',
                    'value': f"{unknown_percentage:.1f}% unidentified apps",
                    'severity': 'MEDIUM',
                    'description': 'Possible use of custom or covert apps'
                })
        
        # App combination analysis
        if 'detected_app' in df.columns and 'start_time' in df.columns:
            df_sorted = df.sort_values('start_time')
            
            # Find app sequences
            app_sequences = []
            for i in range(1, len(df_sorted)):
                if pd.notna(df_sorted.iloc[i-1]['detected_app']) and pd.notna(df_sorted.iloc[i]['detected_app']):
                    sequence = (df_sorted.iloc[i-1]['detected_app'], df_sorted.iloc[i]['detected_app'])
                    app_sequences.append(sequence)
            
            # Count common sequences
            sequence_counts = defaultdict(int)
            for seq in app_sequences:
                sequence_counts[seq] += 1
            
            # Identify suspicious combinations
            suspicious_combos = []
            for (app1, app2), count in sequence_counts.items():
                if count >= 5:
                    # Check for encryption → communication pattern
                    if (app1 in ['whatsapp', 'telegram', 'signal'] and 
                        app2 in ['whatsapp', 'telegram', 'signal'] and 
                        app1 != app2):
                        suspicious_combos.append({
                            'pattern': f"{app1} → {app2}",
                            'count': count,
                            'type': 'APP_SWITCHING'
                        })
                    
                    # Check for VPN/Tor usage patterns
                    if app1 in ['tor', 'vpn'] or app2 in ['tor', 'vpn']:
                        suspicious_combos.append({
                            'pattern': f"{app1} → {app2}",
                            'count': count,
                            'type': 'ANONYMIZATION'
                        })
            
            if suspicious_combos:
                analysis['app_combinations'] = suspicious_combos
                analysis['suspicious_app_patterns'].append({
                    'pattern': 'SUSPICIOUS_APP_SEQUENCES',
                    'value': f"{len(suspicious_combos)} suspicious app combinations",
                    'severity': 'HIGH',
                    'description': 'Possible operational security measures'
                })
        
        # Behavioral pattern analysis
        if 'detected_app' in df.columns and 'start_time' in df.columns:
            # Time-based app usage
            for app in analysis['identified_apps']:
                app_df = df[df['detected_app'] == app]
                
                if len(app_df) > 10:  # Enough data for pattern analysis
                    app_behavior = {
                        'primary_hours': [],
                        'avg_session_duration': 0,
                        'data_pattern': 'NORMAL'
                    }
                    
                    # Hour analysis
                    if 'hour' in app_df.columns:
                        hour_dist = app_df['hour'].value_counts()
                        top_hours = hour_dist.head(3).index.tolist()
                        app_behavior['primary_hours'] = top_hours
                        
                        # Check for odd-hour usage
                        odd_hour_usage = app_df['is_odd_hour'].sum() if 'is_odd_hour' in app_df.columns else 0
                        if odd_hour_usage / len(app_df) > 0.5:
                            app_behavior['data_pattern'] = 'ODD_HOURS'
                    
                    # Session duration
                    if 'session_duration' in app_df.columns:
                        app_behavior['avg_session_duration'] = round(
                            app_df['session_duration'].mean(), 2
                        )
                    
                    # Data volume pattern
                    if 'total_data_volume' in app_df.columns:
                        data_std = app_df['total_data_volume'].std()
                        data_mean = app_df['total_data_volume'].mean()
                        
                        if data_std > data_mean * 2:
                            app_behavior['data_pattern'] = 'HIGHLY_VARIABLE'
                    
                    analysis['behavioral_patterns'][app] = app_behavior
        
        # Port-based analysis
        if 'destination_port' in df.columns:
            port_distribution = df['destination_port'].value_counts().head(10)
            
            for port, count in port_distribution.items():
                if pd.notna(port):
                    port_int = int(port)
                    analysis['port_analysis'][port_int] = {
                        'count': int(count),
                        'percentage': round((count / len(df)) * 100, 2),
                        'known_service': self._identify_port_service(port_int)
                    }
            
            # Check for non-standard ports
            standard_ports = [80, 443, 8080, 8443, 25, 587, 110, 143, 993, 995]
            non_standard = df[~df['destination_port'].isin(standard_ports)]
            
            if len(non_standard) / len(df) > 0.5:
                analysis['suspicious_app_patterns'].append({
                    'pattern': 'NON_STANDARD_PORT_USAGE',
                    'value': f"{len(non_standard)} sessions on non-standard ports",
                    'severity': 'MEDIUM',
                    'description': 'Possible custom protocols or covert channels'
                })
        
        # P2P application detection
        p2p_indicators = self._detect_p2p_behavior(df)
        if p2p_indicators['is_p2p']:
            analysis['suspicious_app_patterns'].append({
                'pattern': 'P2P_BEHAVIOR_DETECTED',
                'value': p2p_indicators['reason'],
                'severity': 'HIGH',
                'description': 'Possible file sharing or cryptocurrency activity'
            })
        
        # Calculate risk score
        analysis['app_risk_score'] = self._calculate_app_risk_score(analysis)
        
        # Determine risk level
        if analysis['app_risk_score'] >= 70:
            analysis['app_risk'] = 'HIGH'
        elif analysis['app_risk_score'] >= 40:
            analysis['app_risk'] = 'MEDIUM'
        else:
            analysis['app_risk'] = 'LOW'
        
        return analysis
    
    def _identify_port_service(self, port: int) -> str:
        """Identify common service for a port"""
        
        common_ports = {
            80: "HTTP",
            443: "HTTPS",
            8080: "HTTP-Alt",
            8443: "HTTPS-Alt",
            25: "SMTP",
            587: "SMTP-Submission",
            110: "POP3",
            143: "IMAP",
            993: "IMAPS",
            995: "POP3S",
            22: "SSH",
            23: "Telnet",
            21: "FTP",
            3306: "MySQL",
            5432: "PostgreSQL",
            6379: "Redis",
            27017: "MongoDB",
            1194: "OpenVPN",
            9001: "Tor",
            4444: "Metasploit",
            6667: "IRC",
            5060: "SIP/VoIP"
        }
        
        return common_ports.get(port, "Unknown")
    
    def _detect_p2p_behavior(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect P2P application behavior"""
        
        p2p_indicators = {
            'is_p2p': False,
            'reason': '',
            'confidence': 0
        }
        
        if 'destination_port' not in df.columns:
            return p2p_indicators
        
        # Check for P2P port patterns
        p2p_ports = df['destination_port'].value_counts()
        
        # High port numbers (>10000) with similar frequency
        high_ports = p2p_ports[p2p_ports.index > 10000]
        
        if len(high_ports) > 20:
            # Check if ports are distributed (not concentrated)
            port_variance = high_ports.std() / high_ports.mean() if high_ports.mean() > 0 else 0
            
            if port_variance < 0.5:  # Similar frequency across many high ports
                p2p_indicators['is_p2p'] = True
                p2p_indicators['reason'] = f"Distributed traffic across {len(high_ports)} high ports"
                p2p_indicators['confidence'] = 80
                return p2p_indicators
        
        # Check for known P2P ports
        known_p2p_ports = [6881, 6882, 6883, 6884, 6885, 6886, 6887, 6888, 6889,  # BitTorrent
                          4662, 4672,  # eMule
                          1214,  # Kazaa
                          6346, 6347,  # Gnutella
                          8333,  # Bitcoin
                          30303]  # Ethereum
        
        p2p_traffic = df[df['destination_port'].isin(known_p2p_ports)]
        
        if len(p2p_traffic) > 10:
            p2p_indicators['is_p2p'] = True
            p2p_indicators['reason'] = f"{len(p2p_traffic)} sessions on known P2P ports"
            p2p_indicators['confidence'] = 90
        
        return p2p_indicators
    
    def _calculate_app_risk_score(self, analysis: Dict[str, Any]) -> int:
        """Calculate app risk score (0-100)"""
        
        score = 0
        
        # High-risk apps
        high_risk_count = len(analysis['high_risk_apps'])
        if high_risk_count >= 3:
            score += 25
        elif high_risk_count >= 1:
            score += 15
        
        # Unknown apps
        unknown_count = len(analysis['unknown_apps'])
        if unknown_count >= 10:
            score += 20
        elif unknown_count >= 5:
            score += 10
        
        # Suspicious app combinations
        if analysis['app_combinations']:
            score += min(len(analysis['app_combinations']) * 10, 20)
        
        # Behavioral anomalies
        odd_hour_apps = sum(1 for app, behavior in analysis['behavioral_patterns'].items()
                           if behavior.get('data_pattern') == 'ODD_HOURS')
        score += min(odd_hour_apps * 5, 15)
        
        # Suspicious patterns
        high_severity = sum(1 for p in analysis['suspicious_app_patterns']
                          if p['severity'] == 'HIGH')
        score += min(high_severity * 15, 30)
        
        return min(score, 100)
    
    def _format_app_analysis(self, results: List[Dict], query: str) -> str:
        """Format app fingerprinting analysis results"""
        
        output = []
        output.append("📱 IPDR APP FINGERPRINTING ANALYSIS")
        output.append("=" * 50)
        
        # High-risk suspects
        high_risk = [r for r in results if r['app_risk'] == 'HIGH']
        
        if high_risk:
            output.append("\n🚨 HIGH APP RISK SUSPECTS")
            output.append("-" * 40)
            
            for result in high_risk:
                output.append(f"\n🔴 {result['suspect']}")
                output.append(f"   App Risk Score: {result['app_risk_score']}/100")
                output.append(f"   Total Sessions: {result['total_sessions']}")
                
                if result['high_risk_apps']:
                    output.append("   High-Risk Apps Detected:")
                    for app_info in result['high_risk_apps'][:3]:
                        output.append(f"     • {app_info['app'].upper()}: {app_info['sessions']} sessions ({app_info['risk']})")
                
                if result['unknown_apps']:
                    output.append(f"   Unknown Apps: {len(result['unknown_apps'])} unidentified services")
                    top_unknown = sorted(result['unknown_apps'], key=lambda x: x['sessions'], reverse=True)[:2]
                    for unknown in top_unknown:
                        output.append(f"     • Port {unknown['port']}: {unknown['sessions']} sessions")
                
                if result['suspicious_app_patterns']:
                    output.append("   ⚠️ Suspicious Patterns:")
                    for pattern in result['suspicious_app_patterns'][:3]:
                        output.append(f"     • {pattern['value']} - {pattern['description']}")
        
        # App combination analysis
        suspects_with_combos = [r for r in results if r['app_combinations']]
        
        if suspects_with_combos:
            output.append("\n🔄 SUSPICIOUS APP COMBINATIONS")
            output.append("-" * 40)
            
            for suspect in suspects_with_combos[:3]:
                output.append(f"\n{suspect['suspect']}:")
                for combo in suspect['app_combinations'][:3]:
                    output.append(f"   • {combo['pattern']}: {combo['count']} times ({combo['type']})")
        
        # Unknown app analysis
        output.append("\n❓ UNKNOWN APP ANALYSIS")
        output.append("-" * 40)
        
        all_unknown_ports = defaultdict(int)
        for result in results:
            for unknown in result['unknown_apps']:
                all_unknown_ports[unknown['port']] += unknown['sessions']
        
        if all_unknown_ports:
            top_unknown = sorted(all_unknown_ports.items(), key=lambda x: x[1], reverse=True)[:5]
            output.append("Top Unknown Ports (across all suspects):")
            for port, count in top_unknown:
                service = self._identify_port_service(port)
                output.append(f"   • Port {port}: {count} sessions ({service})")
        
        # P2P detection
        p2p_suspects = [r for r in results 
                       if any(p['pattern'] == 'P2P_BEHAVIOR_DETECTED' 
                             for p in r['suspicious_app_patterns'])]
        
        if p2p_suspects:
            output.append("\n🌐 P2P ACTIVITY DETECTED")
            output.append("-" * 40)
            for suspect in p2p_suspects:
                pattern = next(p for p in suspect['suspicious_app_patterns'] 
                             if p['pattern'] == 'P2P_BEHAVIOR_DETECTED')
                output.append(f"   • {suspect['suspect']}: {pattern['value']}")
        
        # Behavioral patterns
        output.append("\n🔍 BEHAVIORAL INSIGHTS")
        output.append("-" * 40)
        
        # Odd-hour app usage
        odd_hour_users = []
        for result in results:
            for app, behavior in result['behavioral_patterns'].items():
                if behavior.get('data_pattern') == 'ODD_HOURS':
                    odd_hour_users.append({
                        'suspect': result['suspect'],
                        'app': app,
                        'hours': behavior['primary_hours']
                    })
        
        if odd_hour_users:
            output.append("\n🌙 Odd-Hour App Usage:")
            for user in odd_hour_users[:5]:
                output.append(f"   • {user['suspect']} uses {user['app'].upper()} primarily at hours: {user['hours']}")
        
        # Overall statistics
        output.append("\n📊 OVERALL APP STATISTICS")
        output.append("-" * 40)
        
        all_apps = defaultdict(int)
        for result in results:
            for app, data in result['identified_apps'].items():
                all_apps[app] += data['sessions']
        
        if all_apps:
            top_apps = sorted(all_apps.items(), key=lambda x: x[1], reverse=True)[:5]
            output.append("Top Apps by Usage:")
            for app, count in top_apps:
                risk = settings.app_signatures.get(app, {}).get('risk', 'UNKNOWN')
                emoji = "🔴" if risk == "HIGH" else "🟡" if risk == "MEDIUM" else "🟢"
                output.append(f"   {emoji} {app.upper()}: {count} total sessions")
        
        # Recommendations
        output.append("\n💡 INVESTIGATION RECOMMENDATIONS")
        output.append("-" * 40)
        
        if high_risk:
            output.append("1. Investigate unknown apps on non-standard ports")
            output.append("2. Analyze app switching patterns for operational security")
            output.append("3. Check P2P activity for file sharing or cryptocurrency")
            output.append("4. Correlate high-risk app usage with CDR silence periods")
        else:
            output.append("1. Continue monitoring for new app installations")
            output.append("2. Watch for increases in encrypted app usage")
        
        return "\n".join(output)