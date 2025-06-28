"""
Session Analysis Tool for IPDR Intelligence
Analyzes session timing, duration patterns, and temporal anomalies
"""

from typing import Dict, Optional, Any, List, Type
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config import settings

class SessionAnalysisInput(BaseModel):
    """Input for session analysis tool"""
    query: str = Field(description="What session patterns to analyze (e.g., 'long sessions', 'session timing', 'concurrent sessions')")
    suspect_name: Optional[str] = Field(default=None, description="Specific suspect to analyze")

class SessionAnalysisTool(BaseTool):
    """Tool for analyzing session patterns in IPDR data"""
    
    name: str = "ipdr_session_analysis"
    description: str = """Analyze session patterns including timing, duration, frequency, and concurrent sessions. 
    Detects anomalies like marathon sessions, rapid session switching, pattern day clustering, and suspicious 
    timing patterns. Examples: 'find long sessions', 'analyze session timing patterns', 'check for concurrent sessions'"""
    
    args_schema: Type[BaseModel] = SessionAnalysisInput
    ipdr_data: Dict[str, pd.DataFrame] = {}
    
    def _run(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Run session analysis on IPDR data"""
        try:
            if not self.ipdr_data:
                return "No IPDR data loaded. Please load IPDR data first."
            
            analyze_all = "all" in query.lower() or not suspect_name
            results = []
            suspects_to_analyze = self.ipdr_data.keys() if analyze_all else [suspect_name]
            
            for suspect in suspects_to_analyze:
                if suspect in self.ipdr_data:
                    analysis = self._analyze_suspect_sessions(suspect, self.ipdr_data[suspect])
                    results.append(analysis)
            
            if not results:
                return "No suspects found for session analysis."
            
            # Sort by session anomaly score
            results.sort(key=lambda x: x['session_anomaly_score'], reverse=True)
            
            response = self._format_session_analysis(results, query)
            return response
            
        except Exception as e:
            logger.error(f"Error in session analysis: {str(e)}")
            return f"Error analyzing sessions: {str(e)}"
    
    async def _arun(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Async not implemented"""
        raise NotImplementedError("Async execution not supported")
    
    def _analyze_suspect_sessions(self, suspect: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze session patterns for a single suspect"""
        
        analysis = {
            'suspect': suspect,
            'total_sessions': len(df),
            'avg_session_duration': 0,
            'max_session_duration': 0,
            'marathon_sessions': [],
            'rapid_sessions': [],
            'concurrent_sessions': [],
            'session_frequency': {},
            'temporal_patterns': {},
            'session_risk': 'LOW',
            'session_anomaly_score': 0,
            'suspicious_session_patterns': []
        }
        
        # Basic session statistics
        if 'session_duration' in df.columns:
            valid_durations = df[df['session_duration'].notna()]
            if len(valid_durations) > 0:
                analysis['avg_session_duration'] = round(valid_durations['session_duration'].mean(), 2)
                analysis['max_session_duration'] = round(valid_durations['session_duration'].max(), 2)
                
                # Marathon sessions (>2 hours)
                marathon_threshold = 7200  # 2 hours in seconds
                marathon_sessions = valid_durations[valid_durations['session_duration'] > marathon_threshold]
                
                if len(marathon_sessions) > 0:
                    for _, session in marathon_sessions.iterrows():
                        session_info = {
                            'timestamp': session.get('start_time', 'Unknown'),
                            'duration_hours': round(session['session_duration'] / 3600, 2),
                            'app': session.get('detected_app', 'Unknown'),
                            'data_mb': round(session.get('total_data_volume', 0) / 1048576, 2)
                        }
                        analysis['marathon_sessions'].append(session_info)
                    
                    if len(marathon_sessions) > 5:
                        analysis['suspicious_session_patterns'].append({
                            'pattern': 'FREQUENT_MARATHON_SESSIONS',
                            'value': f"{len(marathon_sessions)} sessions >2 hours",
                            'severity': 'HIGH',
                            'description': 'Unusual long-duration activity'
                        })
        
        # Temporal analysis
        if 'start_time' in df.columns and pd.api.types.is_datetime64_any_dtype(df['start_time']):
            df_sorted = df.sort_values('start_time')
            
            # Session frequency analysis
            df_sorted['date'] = df_sorted['start_time'].dt.date
            daily_sessions = df_sorted.groupby('date').size()
            
            if len(daily_sessions) > 0:
                analysis['session_frequency'] = {
                    'avg_daily': round(daily_sessions.mean(), 2),
                    'max_daily': int(daily_sessions.max()),
                    'min_daily': int(daily_sessions.min())
                }
                
                # Detect burst activity
                if daily_sessions.max() > daily_sessions.mean() + (2 * daily_sessions.std()):
                    burst_days = daily_sessions[daily_sessions > daily_sessions.mean() + (2 * daily_sessions.std())]
                    analysis['suspicious_session_patterns'].append({
                        'pattern': 'BURST_ACTIVITY',
                        'value': f"{len(burst_days)} days with abnormal activity",
                        'severity': 'MEDIUM',
                        'description': 'Sudden increase in session frequency'
                    })
            
            # Rapid session switching
            if 'end_time' in df.columns:
                for i in range(1, len(df_sorted)):
                    prev_end = df_sorted.iloc[i-1]['end_time']
                    curr_start = df_sorted.iloc[i]['start_time']
                    
                    if pd.notna(prev_end) and pd.notna(curr_start):
                        gap = (curr_start - prev_end).total_seconds()
                        
                        # Sessions starting within 30 seconds of previous ending
                        if 0 <= gap <= 30:
                            rapid_info = {
                                'timestamp': curr_start,
                                'gap_seconds': gap,
                                'prev_app': df_sorted.iloc[i-1].get('detected_app', 'Unknown'),
                                'curr_app': df_sorted.iloc[i].get('detected_app', 'Unknown')
                            }
                            analysis['rapid_sessions'].append(rapid_info)
                
                if len(analysis['rapid_sessions']) > 10:
                    analysis['suspicious_session_patterns'].append({
                        'pattern': 'RAPID_SESSION_SWITCHING',
                        'value': f"{len(analysis['rapid_sessions'])} rapid switches",
                        'severity': 'HIGH',
                        'description': 'Automated or scripted behavior'
                    })
            
            # Concurrent session detection
            concurrent_sessions = self._detect_concurrent_sessions(df_sorted)
            analysis['concurrent_sessions'] = concurrent_sessions
            
            if len(concurrent_sessions) > 5:
                analysis['suspicious_session_patterns'].append({
                    'pattern': 'CONCURRENT_SESSIONS',
                    'value': f"{len(concurrent_sessions)} overlapping sessions",
                    'severity': 'HIGH',
                    'description': 'Multiple simultaneous connections'
                })
            
            # Time-of-day patterns
            hourly_distribution = df_sorted['start_time'].dt.hour.value_counts()
            analysis['temporal_patterns']['hourly_distribution'] = hourly_distribution.to_dict()
            
            # Night owl pattern
            night_hours = [0, 1, 2, 3, 4, 5]
            night_sessions = sum(hourly_distribution.get(h, 0) for h in night_hours)
            night_percentage = (night_sessions / len(df_sorted)) * 100
            
            if night_percentage > 30:
                analysis['suspicious_session_patterns'].append({
                    'pattern': 'NIGHT_OWL_ACTIVITY',
                    'value': f"{night_percentage:.1f}% sessions at night (00:00-06:00)",
                    'severity': 'MEDIUM',
                    'description': 'Unusual late-night activity pattern'
                })
            
            # Pattern day analysis
            day_distribution = df_sorted['start_time'].dt.day_name().value_counts()
            analysis['temporal_patterns']['day_distribution'] = day_distribution.to_dict()
            
            # Tuesday/Friday concentration
            pattern_days = ['Tuesday', 'Friday']
            pattern_sessions = sum(day_distribution.get(day, 0) for day in pattern_days)
            pattern_percentage = (pattern_sessions / len(df_sorted)) * 100
            
            if pattern_percentage > 40:
                analysis['suspicious_session_patterns'].append({
                    'pattern': 'PATTERN_DAY_CONCENTRATION',
                    'value': f"{pattern_percentage:.1f}% on Tuesday/Friday",
                    'severity': 'HIGH',
                    'description': 'Matches narcotics transport days'
                })
        
        # Calculate anomaly score
        analysis['session_anomaly_score'] = self._calculate_session_anomaly_score(analysis)
        
        # Determine risk level
        if analysis['session_anomaly_score'] >= 70:
            analysis['session_risk'] = 'HIGH'
        elif analysis['session_anomaly_score'] >= 40:
            analysis['session_risk'] = 'MEDIUM'
        else:
            analysis['session_risk'] = 'LOW'
        
        return analysis
    
    def _detect_concurrent_sessions(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Detect overlapping/concurrent sessions"""
        
        concurrent = []
        
        if 'end_time' not in df.columns:
            return concurrent
        
        # Check each session against all others
        for i in range(len(df)):
            session1 = df.iloc[i]
            
            if pd.isna(session1['start_time']) or pd.isna(session1['end_time']):
                continue
            
            for j in range(i + 1, len(df)):
                session2 = df.iloc[j]
                
                if pd.isna(session2['start_time']) or pd.isna(session2['end_time']):
                    continue
                
                # Check if sessions overlap
                if (session1['start_time'] <= session2['end_time'] and 
                    session2['start_time'] <= session1['end_time']):
                    
                    overlap_start = max(session1['start_time'], session2['start_time'])
                    overlap_end = min(session1['end_time'], session2['end_time'])
                    overlap_duration = (overlap_end - overlap_start).total_seconds()
                    
                    if overlap_duration > 60:  # Only count significant overlaps (>1 minute)
                        concurrent.append({
                            'timestamp': overlap_start,
                            'duration_seconds': overlap_duration,
                            'session1_app': session1.get('detected_app', 'Unknown'),
                            'session2_app': session2.get('detected_app', 'Unknown')
                        })
        
        return concurrent[:10]  # Limit to top 10 for performance
    
    def _calculate_session_anomaly_score(self, analysis: Dict[str, Any]) -> int:
        """Calculate session anomaly score (0-100)"""
        
        score = 0
        
        # Marathon sessions
        marathon_count = len(analysis['marathon_sessions'])
        if marathon_count >= 10:
            score += 20
        elif marathon_count >= 5:
            score += 10
        
        # Rapid session switching
        rapid_count = len(analysis['rapid_sessions'])
        if rapid_count >= 20:
            score += 25
        elif rapid_count >= 10:
            score += 15
        
        # Concurrent sessions
        concurrent_count = len(analysis['concurrent_sessions'])
        if concurrent_count >= 10:
            score += 25
        elif concurrent_count >= 5:
            score += 15
        
        # Suspicious patterns
        high_severity = sum(1 for p in analysis['suspicious_session_patterns'] 
                          if p['severity'] == 'HIGH')
        score += min(high_severity * 15, 30)
        
        # Session frequency anomalies
        if analysis['session_frequency']:
            if analysis['session_frequency']['max_daily'] > 100:
                score += 10
        
        return min(score, 100)
    
    def _format_session_analysis(self, results: List[Dict], query: str) -> str:
        """Format session analysis results"""
        
        output = []
        output.append("⏱️ IPDR SESSION ANALYSIS")
        output.append("=" * 50)
        
        # High-risk suspects
        high_risk = [r for r in results if r['session_risk'] == 'HIGH']
        
        if high_risk:
            output.append("\n🚨 HIGH SESSION ANOMALY SUSPECTS")
            output.append("-" * 40)
            
            for result in high_risk:
                output.append(f"\n🔴 {result['suspect']}")
                output.append(f"   Session Anomaly Score: {result['session_anomaly_score']}/100")
                output.append(f"   Total Sessions: {result['total_sessions']}")
                output.append(f"   Avg Duration: {result['avg_session_duration']} seconds")
                
                if result['marathon_sessions']:
                    output.append(f"   Marathon Sessions: {len(result['marathon_sessions'])}")
                    longest = max(result['marathon_sessions'], key=lambda x: x['duration_hours'])
                    output.append(f"     • Longest: {longest['duration_hours']} hours via {longest['app']}")
                
                if result['suspicious_session_patterns']:
                    output.append("   ⚠️ Suspicious Patterns:")
                    for pattern in result['suspicious_session_patterns'][:3]:
                        output.append(f"     • {pattern['value']} - {pattern['description']}")
        
        # Marathon session summary
        all_marathons = []
        for result in results:
            for marathon in result['marathon_sessions']:
                all_marathons.append({
                    'suspect': result['suspect'],
                    'timestamp': marathon['timestamp'],
                    'duration': marathon['duration_hours'],
                    'app': marathon['app']
                })
        
        if all_marathons:
            output.append("\n🏃 MARATHON SESSIONS (>2 hours)")
            output.append("-" * 40)
            # Sort by duration
            all_marathons.sort(key=lambda x: x['duration'], reverse=True)
            for session in all_marathons[:5]:
                output.append(f"   • {session['suspect']}: {session['duration']} hours on {session['app']} at {session['timestamp']}")
        
        # Concurrent session detection
        concurrent_suspects = [r for r in results if len(r['concurrent_sessions']) > 0]
        
        if concurrent_suspects:
            output.append("\n🔄 CONCURRENT SESSION DETECTION")
            output.append("-" * 40)
            for suspect in concurrent_suspects[:3]:
                output.append(f"\n{suspect['suspect']}:")
                output.append(f"   • {len(suspect['concurrent_sessions'])} overlapping sessions detected")
                if suspect['concurrent_sessions']:
                    sample = suspect['concurrent_sessions'][0]
                    output.append(f"   • Example: {sample['session1_app']} + {sample['session2_app']} overlapped")
        
        # Temporal patterns
        output.append("\n📊 TEMPORAL PATTERNS")
        output.append("-" * 40)
        
        # Night activity
        night_owls = [r for r in results 
                     if any(p['pattern'] == 'NIGHT_OWL_ACTIVITY' 
                           for p in r['suspicious_session_patterns'])]
        
        if night_owls:
            output.append("\n🌙 Night Owl Activity:")
            for owl in night_owls[:3]:
                pattern = next(p for p in owl['suspicious_session_patterns'] 
                             if p['pattern'] == 'NIGHT_OWL_ACTIVITY')
                output.append(f"   • {owl['suspect']}: {pattern['value']}")
        
        # Pattern day activity
        pattern_day_suspects = [r for r in results 
                              if any(p['pattern'] == 'PATTERN_DAY_CONCENTRATION' 
                                    for p in r['suspicious_session_patterns'])]
        
        if pattern_day_suspects:
            output.append("\n📅 Pattern Day Concentration:")
            for suspect in pattern_day_suspects:
                pattern = next(p for p in suspect['suspicious_session_patterns'] 
                             if p['pattern'] == 'PATTERN_DAY_CONCENTRATION')
                output.append(f"   • {suspect['suspect']}: {pattern['value']}")
        
        # Overall statistics
        output.append("\n📊 OVERALL SESSION STATISTICS")
        output.append("-" * 40)
        total_sessions = sum(r['total_sessions'] for r in results)
        total_marathons = sum(len(r['marathon_sessions']) for r in results)
        total_concurrent = sum(len(r['concurrent_sessions']) for r in results)
        
        output.append(f"Total Sessions Analyzed: {total_sessions}")
        output.append(f"Marathon Sessions: {total_marathons}")
        output.append(f"Concurrent Sessions Detected: {total_concurrent}")
        
        # Recommendations
        output.append("\n💡 INVESTIGATION RECOMMENDATIONS")
        output.append("-" * 40)
        
        if high_risk:
            output.append("1. Investigate marathon sessions for evidence review/planning")
            output.append("2. Check concurrent sessions for account sharing or automation")
            output.append("3. Correlate rapid switching with criminal coordination")
            output.append("4. Focus on night activity for covert operations")
        else:
            output.append("1. Continue monitoring for session anomalies")
            output.append("2. Watch for increases in session duration or frequency")
        
        return "\n".join(output)