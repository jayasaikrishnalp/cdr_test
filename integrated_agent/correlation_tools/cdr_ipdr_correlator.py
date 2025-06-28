"""
CDR-IPDR Correlation Engine
Correlates Call Detail Records with Internet Protocol Detail Records for comprehensive analysis
"""

from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config import settings

class CDRIPDRCorrelator:
    """
    Correlates CDR and IPDR data to identify criminal patterns
    """
    
    def __init__(self):
        """Initialize correlator with configuration"""
        self.time_window = 300  # 5 minutes default correlation window
        self.pattern_thresholds = {
            'call_to_data': 300,      # Max seconds between call end and data start
            'silence_to_data': 600,   # Max seconds of CDR silence before IPDR activity
            'burst_correlation': 900   # Window for correlating burst activities
        }
    
    def correlate_suspects(self, cdr_data: Dict[str, pd.DataFrame], 
                          ipdr_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Correlate CDR and IPDR data for all suspects
        
        Args:
            cdr_data: Dictionary of CDR DataFrames by suspect
            ipdr_data: Dictionary of IPDR DataFrames by suspect
            
        Returns:
            Correlation analysis results
        """
        correlation_results = {
            'suspect_correlations': {},
            'cross_suspect_patterns': [],
            'timeline_correlations': {},
            'risk_amplifiers': {},
            'evidence_chains': []
        }
        
        # Process each suspect
        for suspect in cdr_data.keys():
            if suspect in ipdr_data:
                logger.info(f"Correlating data for {suspect}")
                suspect_correlation = self._correlate_suspect_data(
                    suspect, 
                    cdr_data[suspect], 
                    ipdr_data[suspect]
                )
                correlation_results['suspect_correlations'][suspect] = suspect_correlation
        
        # Find cross-suspect patterns
        correlation_results['cross_suspect_patterns'] = self._find_cross_suspect_patterns(
            correlation_results['suspect_correlations']
        )
        
        # Generate timeline correlations
        correlation_results['timeline_correlations'] = self._generate_timeline_correlations(
            cdr_data, ipdr_data
        )
        
        # Calculate risk amplifiers
        correlation_results['risk_amplifiers'] = self._calculate_risk_amplifiers(
            correlation_results['suspect_correlations']
        )
        
        # Build evidence chains
        correlation_results['evidence_chains'] = self._build_evidence_chains(
            correlation_results['suspect_correlations']
        )
        
        return correlation_results
    
    def _correlate_suspect_data(self, suspect: str, cdr_df: pd.DataFrame, 
                               ipdr_df: pd.DataFrame) -> Dict[str, Any]:
        """Correlate CDR and IPDR data for a single suspect"""
        
        correlation = {
            'suspect': suspect,
            'call_to_data_patterns': [],
            'silence_periods': [],
            'encrypted_after_call': [],
            'data_during_silence': [],
            'behavioral_shifts': [],
            'risk_indicators': [],
            'correlation_score': 0
        }
        
        # Ensure datetime columns
        cdr_df = self._prepare_cdr_data(cdr_df)
        ipdr_df = self._prepare_ipdr_data(ipdr_df)
        
        if cdr_df.empty or ipdr_df.empty:
            return correlation
        
        # 1. Call to Data Patterns (Voice call followed by data session)
        correlation['call_to_data_patterns'] = self._find_call_to_data_patterns(
            cdr_df, ipdr_df
        )
        
        # 2. CDR Silence Analysis (No calls but IPDR activity)
        correlation['silence_periods'] = self._find_silence_periods(cdr_df)
        correlation['data_during_silence'] = self._find_data_during_silence(
            correlation['silence_periods'], ipdr_df
        )
        
        # 3. Encrypted Communication After Calls
        correlation['encrypted_after_call'] = self._find_encrypted_after_call(
            cdr_df, ipdr_df
        )
        
        # 4. Behavioral Shifts
        correlation['behavioral_shifts'] = self._detect_behavioral_shifts(
            cdr_df, ipdr_df
        )
        
        # 5. Risk Indicators
        correlation['risk_indicators'] = self._identify_risk_indicators(correlation)
        
        # Calculate correlation score
        correlation['correlation_score'] = self._calculate_correlation_score(correlation)
        
        return correlation
    
    def _prepare_cdr_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare CDR data with datetime columns"""
        df = df.copy()
        
        # Create datetime column
        if 'date' in df.columns and 'time' in df.columns:
            df['datetime'] = pd.to_datetime(
                df['date'].astype(str) + ' ' + df['time'].astype(str),
                errors='coerce'
            )
        
        # Calculate call end time
        if 'datetime' in df.columns and 'duration' in df.columns:
            df['end_time'] = df['datetime'] + pd.to_timedelta(df['duration'], unit='s')
        
        return df.sort_values('datetime')
    
    def _prepare_ipdr_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare IPDR data with datetime columns"""
        df = df.copy()
        
        # Ensure datetime columns
        if 'start_time' in df.columns:
            df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
        if 'end_time' in df.columns:
            df['end_time'] = pd.to_datetime(df['end_time'], errors='coerce')
        
        return df.sort_values('start_time')
    
    def _find_call_to_data_patterns(self, cdr_df: pd.DataFrame, 
                                   ipdr_df: pd.DataFrame) -> List[Dict]:
        """Find patterns where calls are followed by data sessions"""
        patterns = []
        
        for _, call in cdr_df.iterrows():
            if pd.notna(call.get('end_time')):
                # Find IPDR sessions starting shortly after call ends
                time_window_start = call['end_time']
                time_window_end = call['end_time'] + timedelta(
                    seconds=self.pattern_thresholds['call_to_data']
                )
                
                subsequent_data = ipdr_df[
                    (ipdr_df['start_time'] >= time_window_start) &
                    (ipdr_df['start_time'] <= time_window_end)
                ]
                
                if not subsequent_data.empty:
                    for _, data_session in subsequent_data.iterrows():
                        pattern = {
                            'call_time': call['datetime'],
                            'call_duration': call.get('duration', 0),
                            'call_party': call.get('b_party', 'Unknown'),
                            'data_start': data_session['start_time'],
                            'gap_seconds': (data_session['start_time'] - call['end_time']).total_seconds(),
                            'data_app': data_session.get('detected_app', 'Unknown'),
                            'is_encrypted': data_session.get('is_encrypted', False),
                            'data_volume_mb': (data_session.get('total_data_volume', 0) / 1048576)
                        }
                        
                        # Flag suspicious patterns
                        if pattern['is_encrypted'] and pattern['gap_seconds'] < 60:
                            pattern['risk'] = 'HIGH'
                            pattern['description'] = 'Immediate encrypted communication after call'
                        elif pattern['data_volume_mb'] > 10:
                            pattern['risk'] = 'MEDIUM'
                            pattern['description'] = 'Large data transfer after call'
                        else:
                            pattern['risk'] = 'LOW'
                            pattern['description'] = 'Regular data usage after call'
                        
                        patterns.append(pattern)
        
        return patterns
    
    def _find_silence_periods(self, cdr_df: pd.DataFrame) -> List[Dict]:
        """Find periods of CDR silence (no calls)"""
        silence_periods = []
        
        if len(cdr_df) < 2:
            return silence_periods
        
        # Sort by datetime
        cdr_sorted = cdr_df.sort_values('datetime')
        
        for i in range(len(cdr_sorted) - 1):
            current_call = cdr_sorted.iloc[i]
            next_call = cdr_sorted.iloc[i + 1]
            
            gap = (next_call['datetime'] - current_call['end_time']).total_seconds()
            
            # Significant silence period (>2 hours)
            if gap > 7200:
                silence_periods.append({
                    'start': current_call['end_time'],
                    'end': next_call['datetime'],
                    'duration_hours': gap / 3600,
                    'last_call_before': current_call.get('b_party', 'Unknown'),
                    'first_call_after': next_call.get('b_party', 'Unknown')
                })
        
        return silence_periods
    
    def _find_data_during_silence(self, silence_periods: List[Dict], 
                                 ipdr_df: pd.DataFrame) -> List[Dict]:
        """Find IPDR activity during CDR silence periods"""
        data_during_silence = []
        
        for silence in silence_periods:
            # Find IPDR sessions during silence
            active_sessions = ipdr_df[
                (ipdr_df['start_time'] >= silence['start']) &
                (ipdr_df['start_time'] <= silence['end'])
            ]
            
            if not active_sessions.empty:
                # Aggregate activity
                total_sessions = len(active_sessions)
                encrypted_sessions = active_sessions['is_encrypted'].sum() if 'is_encrypted' in active_sessions.columns else 0
                total_data_mb = active_sessions['total_data_volume'].sum() / 1048576 if 'total_data_volume' in active_sessions.columns else 0
                
                apps_used = []
                if 'detected_app' in active_sessions.columns:
                    apps_used = active_sessions['detected_app'].dropna().unique().tolist()
                
                activity = {
                    'silence_start': silence['start'],
                    'silence_duration_hours': silence['duration_hours'],
                    'ipdr_sessions': total_sessions,
                    'encrypted_sessions': encrypted_sessions,
                    'total_data_mb': round(total_data_mb, 2),
                    'apps_used': apps_used,
                    'risk': 'HIGH' if encrypted_sessions > 5 or total_data_mb > 50 else 'MEDIUM',
                    'description': f"{total_sessions} data sessions during {silence['duration_hours']:.1f}hr call silence"
                }
                
                data_during_silence.append(activity)
        
        return data_during_silence
    
    def _find_encrypted_after_call(self, cdr_df: pd.DataFrame, 
                                   ipdr_df: pd.DataFrame) -> List[Dict]:
        """Find encrypted app usage immediately after voice calls"""
        encrypted_patterns = []
        
        # Filter encrypted sessions
        encrypted_df = ipdr_df[ipdr_df.get('is_encrypted', False) == True]
        
        for _, call in cdr_df.iterrows():
            if pd.notna(call.get('end_time')):
                # Look for encryption within 5 minutes
                window_start = call['end_time']
                window_end = call['end_time'] + timedelta(minutes=5)
                
                encrypted_after = encrypted_df[
                    (encrypted_df['start_time'] >= window_start) &
                    (encrypted_df['start_time'] <= window_end)
                ]
                
                for _, enc_session in encrypted_after.iterrows():
                    pattern = {
                        'call_time': call['datetime'],
                        'call_party': call.get('b_party', 'Unknown'),
                        'encryption_start': enc_session['start_time'],
                        'gap_minutes': (enc_session['start_time'] - call['end_time']).total_seconds() / 60,
                        'encrypted_app': enc_session.get('detected_app', 'Unknown'),
                        'session_duration': enc_session.get('session_duration', 0),
                        'data_volume_mb': enc_session.get('total_data_volume', 0) / 1048576
                    }
                    
                    # Risk assessment
                    if pattern['gap_minutes'] < 1:
                        pattern['risk'] = 'CRITICAL'
                        pattern['description'] = 'Immediate encryption after call - possible evidence coordination'
                    elif pattern['gap_minutes'] < 3:
                        pattern['risk'] = 'HIGH'
                        pattern['description'] = 'Quick shift to encrypted communication'
                    else:
                        pattern['risk'] = 'MEDIUM'
                        pattern['description'] = 'Encrypted communication following call'
                    
                    encrypted_patterns.append(pattern)
        
        return encrypted_patterns
    
    def _detect_behavioral_shifts(self, cdr_df: pd.DataFrame, 
                                 ipdr_df: pd.DataFrame) -> List[Dict]:
        """Detect shifts in communication behavior"""
        shifts = []
        
        # Get daily statistics
        if 'datetime' in cdr_df.columns:
            cdr_df['date'] = cdr_df['datetime'].dt.date
            daily_calls = cdr_df.groupby('date').size()
        
        if 'start_time' in ipdr_df.columns:
            ipdr_df['date'] = ipdr_df['start_time'].dt.date
            daily_data = ipdr_df.groupby('date').size()
        
        # Find days with inverse patterns
        all_dates = sorted(set(daily_calls.index) | set(daily_data.index))
        
        for date in all_dates:
            calls = daily_calls.get(date, 0)
            data_sessions = daily_data.get(date, 0)
            
            # Detect shifts
            if calls == 0 and data_sessions > 20:
                shifts.append({
                    'date': date,
                    'type': 'VOICE_TO_DATA_SHIFT',
                    'calls': 0,
                    'data_sessions': data_sessions,
                    'risk': 'HIGH',
                    'description': 'Complete shift from voice to data communication'
                })
            elif calls > 20 and data_sessions == 0:
                shifts.append({
                    'date': date,
                    'type': 'DATA_TO_VOICE_SHIFT',
                    'calls': calls,
                    'data_sessions': 0,
                    'risk': 'MEDIUM',
                    'description': 'Shifted to voice-only communication'
                })
        
        return shifts
    
    def _identify_risk_indicators(self, correlation: Dict) -> List[Dict]:
        """Identify key risk indicators from correlation patterns"""
        indicators = []
        
        # Call-to-encryption pattern
        encrypted_after_call = len(correlation['encrypted_after_call'])
        if encrypted_after_call > 10:
            indicators.append({
                'type': 'FREQUENT_CALL_TO_ENCRYPTION',
                'count': encrypted_after_call,
                'risk': 'HIGH',
                'description': f'{encrypted_after_call} instances of encryption after calls'
            })
        
        # Data during silence
        if correlation['data_during_silence']:
            total_silent_data = sum(d['total_data_mb'] for d in correlation['data_during_silence'])
            if total_silent_data > 100:
                indicators.append({
                    'type': 'HEAVY_DATA_DURING_SILENCE',
                    'value': f'{total_silent_data:.1f} MB',
                    'risk': 'HIGH',
                    'description': 'Large data transfers during call silence periods'
                })
        
        # Behavioral shifts
        critical_shifts = [s for s in correlation['behavioral_shifts'] 
                          if s['type'] == 'VOICE_TO_DATA_SHIFT']
        if critical_shifts:
            indicators.append({
                'type': 'COMMUNICATION_SHIFT',
                'count': len(critical_shifts),
                'risk': 'MEDIUM',
                'description': 'Shifted from voice to data communication'
            })
        
        return indicators
    
    def _calculate_correlation_score(self, correlation: Dict) -> int:
        """Calculate overall correlation risk score"""
        score = 0
        
        # Call-to-data patterns
        high_risk_patterns = [p for p in correlation['call_to_data_patterns'] 
                             if p.get('risk') in ['HIGH', 'CRITICAL']]
        score += min(len(high_risk_patterns) * 5, 25)
        
        # Encrypted after call
        critical_encryption = [e for e in correlation['encrypted_after_call'] 
                              if e.get('risk') == 'CRITICAL']
        score += min(len(critical_encryption) * 10, 30)
        
        # Data during silence
        if correlation['data_during_silence']:
            high_risk_silence = [d for d in correlation['data_during_silence'] 
                               if d.get('risk') == 'HIGH']
            score += min(len(high_risk_silence) * 8, 20)
        
        # Behavioral shifts
        score += min(len(correlation['behavioral_shifts']) * 5, 15)
        
        # Risk indicators
        high_risk_indicators = [i for i in correlation['risk_indicators'] 
                               if i.get('risk') == 'HIGH']
        score += min(len(high_risk_indicators) * 5, 10)
        
        return min(score, 100)
    
    def _find_cross_suspect_patterns(self, suspect_correlations: Dict) -> List[Dict]:
        """Find patterns across multiple suspects"""
        patterns = []
        suspects = list(suspect_correlations.keys())
        
        # Find coordinated encryption
        for i in range(len(suspects)):
            for j in range(i + 1, len(suspects)):
                suspect1 = suspects[i]
                suspect2 = suspects[j]
                
                # Get encrypted patterns
                enc1 = suspect_correlations[suspect1]['encrypted_after_call']
                enc2 = suspect_correlations[suspect2]['encrypted_after_call']
                
                # Find temporal proximity
                for e1 in enc1:
                    for e2 in enc2:
                        time_diff = abs((e1['encryption_start'] - e2['encryption_start']).total_seconds())
                        
                        if time_diff < 300:  # Within 5 minutes
                            patterns.append({
                                'type': 'COORDINATED_ENCRYPTION',
                                'suspects': [suspect1, suspect2],
                                'time_diff_seconds': time_diff,
                                'apps': [e1['encrypted_app'], e2['encrypted_app']],
                                'risk': 'CRITICAL',
                                'description': f'Coordinated encryption between {suspect1} and {suspect2}'
                            })
        
        return patterns
    
    def _generate_timeline_correlations(self, cdr_data: Dict, ipdr_data: Dict) -> Dict:
        """Generate timeline view of CDR-IPDR correlations"""
        timeline = defaultdict(list)
        
        for suspect in cdr_data.keys():
            if suspect not in ipdr_data:
                continue
            
            # Add CDR events
            for _, call in cdr_data[suspect].iterrows():
                if pd.notna(call.get('datetime')):
                    timeline[call['datetime']].append({
                        'type': 'CDR',
                        'suspect': suspect,
                        'event': 'voice_call',
                        'details': f"Call to {call.get('b_party', 'Unknown')}"
                    })
            
            # Add IPDR events
            for _, session in ipdr_data[suspect].iterrows():
                if pd.notna(session.get('start_time')):
                    event_type = 'encrypted_data' if session.get('is_encrypted') else 'data_session'
                    timeline[session['start_time']].append({
                        'type': 'IPDR',
                        'suspect': suspect,
                        'event': event_type,
                        'details': f"{session.get('detected_app', 'Unknown')} - {session.get('total_data_volume', 0)/1048576:.1f}MB"
                    })
        
        # Sort timeline
        sorted_timeline = dict(sorted(timeline.items()))
        
        return sorted_timeline
    
    def _calculate_risk_amplifiers(self, suspect_correlations: Dict) -> Dict:
        """Calculate risk amplification factors from correlations"""
        amplifiers = {}
        
        for suspect, correlation in suspect_correlations.items():
            amplifier = 1.0
            reasons = []
            
            # Immediate encryption after calls
            critical_encryption = [e for e in correlation['encrypted_after_call'] 
                                 if e.get('risk') == 'CRITICAL']
            if len(critical_encryption) > 5:
                amplifier *= 1.5
                reasons.append(f"{len(critical_encryption)} immediate encryptions after calls")
            
            # Heavy data during silence
            silent_data = correlation['data_during_silence']
            if silent_data and sum(d['total_data_mb'] for d in silent_data) > 200:
                amplifier *= 1.3
                reasons.append("Heavy data usage during call silence")
            
            # Communication shifts
            if len(correlation['behavioral_shifts']) > 3:
                amplifier *= 1.2
                reasons.append("Multiple communication channel shifts")
            
            amplifiers[suspect] = {
                'multiplier': round(amplifier, 2),
                'reasons': reasons
            }
        
        return amplifiers
    
    def _build_evidence_chains(self, suspect_correlations: Dict) -> List[Dict]:
        """Build evidence chains from correlation patterns"""
        chains = []
        
        for suspect, correlation in suspect_correlations.items():
            # Find significant patterns
            for pattern in correlation['call_to_data_patterns']:
                if pattern.get('risk') in ['HIGH', 'CRITICAL']:
                    for enc in correlation['encrypted_after_call']:
                        if abs((pattern['data_start'] - enc['encryption_start']).total_seconds()) < 60:
                            chains.append({
                                'suspect': suspect,
                                'chain': [
                                    f"Voice call to {pattern['call_party']}",
                                    f"Data session started {pattern['gap_seconds']}s later",
                                    f"Encrypted {enc['encrypted_app']} communication",
                                    f"Transferred {enc['data_volume_mb']:.1f}MB"
                                ],
                                'risk': 'CRITICAL',
                                'timestamp': pattern['call_time'],
                                'description': 'Voice call → Data transfer → Encryption pattern detected'
                            })
        
        return chains
    
    def generate_correlation_report(self, correlation_results: Dict) -> str:
        """Generate formatted correlation report"""
        lines = []
        lines.append("🔗 CDR-IPDR CORRELATION ANALYSIS")
        lines.append("=" * 60)
        
        # Summary
        total_suspects = len(correlation_results['suspect_correlations'])
        high_risk_count = sum(1 for s, c in correlation_results['suspect_correlations'].items() 
                             if c['correlation_score'] >= 70)
        
        lines.append(f"\nAnalyzed: {total_suspects} suspects")
        lines.append(f"High-risk correlations: {high_risk_count}")
        
        # Individual suspect correlations
        lines.append("\n📊 SUSPECT CORRELATION SCORES")
        lines.append("-" * 40)
        
        sorted_suspects = sorted(
            correlation_results['suspect_correlations'].items(),
            key=lambda x: x[1]['correlation_score'],
            reverse=True
        )
        
        for suspect, correlation in sorted_suspects:
            score = correlation['correlation_score']
            emoji = "🔴" if score >= 70 else "🟡" if score >= 40 else "🟢"
            lines.append(f"{emoji} {suspect}: {score}/100")
            
            # Key findings
            if correlation['encrypted_after_call']:
                lines.append(f"   • {len(correlation['encrypted_after_call'])} encrypted sessions after calls")
            if correlation['data_during_silence']:
                total_mb = sum(d['total_data_mb'] for d in correlation['data_during_silence'])
                lines.append(f"   • {total_mb:.1f}MB transferred during call silence")
        
        # Cross-suspect patterns
        if correlation_results['cross_suspect_patterns']:
            lines.append("\n🔄 CROSS-SUSPECT PATTERNS")
            lines.append("-" * 40)
            for pattern in correlation_results['cross_suspect_patterns'][:5]:
                lines.append(f"• {pattern['description']}")
        
        # Evidence chains
        if correlation_results['evidence_chains']:
            lines.append("\n⛓️ EVIDENCE CHAINS DETECTED")
            lines.append("-" * 40)
            for chain in correlation_results['evidence_chains'][:5]:
                lines.append(f"\n{chain['suspect']} - {chain['timestamp']}")
                for step in chain['chain']:
                    lines.append(f"  → {step}")
        
        # Risk amplifiers
        lines.append("\n📈 RISK AMPLIFICATION")
        lines.append("-" * 40)
        for suspect, amp in correlation_results['risk_amplifiers'].items():
            if amp['multiplier'] > 1.0:
                lines.append(f"{suspect}: {amp['multiplier']}x")
                for reason in amp['reasons']:
                    lines.append(f"  • {reason}")
        
        return "\n".join(lines)