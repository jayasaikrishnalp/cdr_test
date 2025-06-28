"""
Temporal Analysis Tool for CDR Intelligence
Analyzes time-based patterns including odd hours, call bursts, and pattern days
"""

from typing import Dict, Optional, Any, List, Type
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

class TemporalAnalysisInput(BaseModel):
    """Input for temporal analysis tool"""
    query: str = Field(description="What temporal patterns to analyze (e.g., 'odd hours', 'call bursts', 'pattern days')")
    suspect_name: Optional[str] = Field(default=None, description="Specific suspect to analyze")

class TemporalAnalysisTool(BaseTool):
    """Tool for analyzing temporal patterns in CDR data"""
    
    name: str = "temporal_analysis"
    description: str = """Analyze time-based patterns including odd hour activity (midnight-5AM), 
    call bursts, pattern days (Tuesday/Friday), and hourly distributions. 
    Use this to detect suspicious timing patterns.
    Examples: 'analyze odd hour activity', 'check for call bursts', 'pattern day analysis for all suspects'"""
    
    args_schema: Type[BaseModel] = TemporalAnalysisInput
    cdr_data: Dict[str, pd.DataFrame] = {}
    
    def _run(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Run temporal analysis"""
        try:
            if not self.cdr_data:
                return "No CDR data loaded. Please load data first."
            
            # Determine analysis type
            analyze_all = "all" in query.lower() or not suspect_name
            query_lower = query.lower()
            
            # Determine what to analyze
            analyze_odd_hours = "odd" in query_lower or "midnight" in query_lower or "night" in query_lower
            analyze_bursts = "burst" in query_lower or "rapid" in query_lower
            analyze_patterns = "pattern" in query_lower or "tuesday" in query_lower or "friday" in query_lower
            
            # Default to comprehensive analysis if not specific
            if not any([analyze_odd_hours, analyze_bursts, analyze_patterns]):
                analyze_odd_hours = analyze_bursts = analyze_patterns = True
            
            results = []
            suspects_to_analyze = self.cdr_data.keys() if analyze_all else [suspect_name]
            
            for suspect in suspects_to_analyze:
                if suspect in self.cdr_data:
                    analysis = self._analyze_suspect_temporal(
                        suspect, 
                        self.cdr_data[suspect],
                        analyze_odd_hours,
                        analyze_bursts,
                        analyze_patterns
                    )
                    results.append(analysis)
            
            if not results:
                return "No suspects found for analysis."
            
            # Sort by odd hour percentage (highest risk)
            results.sort(key=lambda x: x.get('odd_hour_percentage', 0), reverse=True)
            
            # Format response
            response = self._format_temporal_analysis(results, query)
            return response
            
        except Exception as e:
            logger.error(f"Error in temporal analysis: {str(e)}")
            return f"Error analyzing temporal patterns: {str(e)}"
    
    async def _arun(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Async not implemented"""
        raise NotImplementedError("Async execution not supported")
    
    def _analyze_suspect_temporal(self, suspect: str, df: pd.DataFrame, 
                                 analyze_odd: bool, analyze_bursts: bool, 
                                 analyze_patterns: bool) -> Dict[str, Any]:
        """Analyze temporal patterns for a single suspect"""
        # Filter out provider messages
        df_filtered = df[~df.get('is_provider_message', False)].copy()
        
        analysis = {
            'suspect': suspect,
            'total_calls': len(df_filtered),
            'silent_periods': []
        }
        
        if analyze_odd:
            # Odd hour analysis
            odd_hour_mask = (df_filtered['hour'] >= settings.odd_hour_start) & \
                           (df_filtered['hour'] < settings.odd_hour_end)
            odd_hour_calls = odd_hour_mask.sum()
            odd_hour_pct = (odd_hour_calls / len(df_filtered) * 100) if len(df_filtered) > 0 else 0
            
            analysis.update({
                'odd_hour_calls': odd_hour_calls,
                'odd_hour_percentage': round(odd_hour_pct, 2),
                'odd_hour_risk': 'HIGH' if odd_hour_pct > 3 else 'MEDIUM' if odd_hour_pct > 1 else 'LOW'
            })
        
        if analyze_bursts:
            # Call burst detection
            bursts = self._detect_call_bursts(df_filtered)
            analysis['call_bursts'] = bursts
            analysis['burst_count'] = len(bursts)
        
        if analyze_patterns:
            # Pattern day analysis
            pattern_activity = {}
            for day in settings.pattern_days:
                day_calls = df_filtered[df_filtered['day_of_week'] == day]
                pattern_activity[day] = {
                    'count': len(day_calls),
                    'percentage': round((len(day_calls) / len(df_filtered) * 100), 2) if len(df_filtered) > 0 else 0
                }
            analysis['pattern_day_activity'] = pattern_activity
            
            # Check for pattern day concentration
            total_pattern_pct = sum(d['percentage'] for d in pattern_activity.values())
            analysis['pattern_concentration'] = 'HIGH' if total_pattern_pct > 40 else 'MEDIUM' if total_pattern_pct > 25 else 'LOW'
        
        # Hourly distribution
        hourly_dist = df_filtered['hour'].value_counts().sort_index()
        peak_hours = hourly_dist.nlargest(3).index.tolist() if len(hourly_dist) > 0 else []
        analysis['peak_hours'] = peak_hours
        
        # Silent period detection
        silent_periods = self._detect_silent_periods(df_filtered)
        if silent_periods:
            analysis['silent_periods'] = silent_periods
            analysis['has_suspicious_silence'] = any(p['severity'] == 'HIGH' for p in silent_periods)
        
        return analysis
    
    def _detect_call_bursts(self, df: pd.DataFrame) -> List[Dict]:
        """Detect call bursts in the data"""
        bursts = []
        df_sorted = df.sort_values('datetime')
        
        window_minutes = settings.call_burst_window
        threshold = settings.call_burst_threshold
        
        i = 0
        while i < len(df_sorted):
            window_start = df_sorted.iloc[i]['datetime']
            window_end = window_start + timedelta(minutes=window_minutes)
            
            # Count calls in window
            calls_in_window = df_sorted[
                (df_sorted['datetime'] >= window_start) & 
                (df_sorted['datetime'] <= window_end)
            ]
            
            if len(calls_in_window) >= threshold:
                unique_numbers = calls_in_window['b_party_clean'].nunique()
                bursts.append({
                    'time': window_start.strftime('%Y-%m-%d %H:%M'),
                    'calls': len(calls_in_window),
                    'unique_contacts': unique_numbers,
                    'suspicious': unique_numbers == 1  # Multiple calls to same number
                })
                # Skip past this burst
                i += len(calls_in_window)
            else:
                i += 1
        
        return bursts[:5]  # Return top 5 bursts
    
    def _detect_silent_periods(self, df: pd.DataFrame) -> List[Dict]:
        """Detect unusual communication gaps"""
        silent_periods = []
        
        if len(df) < 2:
            return silent_periods
        
        # Sort by datetime
        df_sorted = df.sort_values('datetime')
        
        # Calculate gaps between communications
        for i in range(len(df_sorted) - 1):
            current_time = df_sorted.iloc[i]['datetime']
            next_time = df_sorted.iloc[i+1]['datetime']
            
            gap_hours = (next_time - current_time).total_seconds() / 3600
            
            # Flag gaps > 48 hours as suspicious
            if gap_hours > 48:
                silent_periods.append({
                    'start': current_time.strftime('%Y-%m-%d %H:%M'),
                    'end': next_time.strftime('%Y-%m-%d %H:%M'),
                    'duration_hours': round(gap_hours, 1),
                    'severity': 'HIGH' if gap_hours > 72 else 'MEDIUM'
                })
        
        return silent_periods[:5]  # Return top 5 silent periods
    
    def _format_temporal_analysis(self, results: list, query: str) -> str:
        """Format temporal analysis results"""
        output = []
        
        output.append("⏰ TEMPORAL PATTERN ANALYSIS")
        output.append("=" * 50)
        
        # Identify high risk suspects
        high_odd_hour = [r for r in results if r.get('odd_hour_percentage', 0) > 3]
        high_bursts = [r for r in results if r.get('burst_count', 0) > 3]
        high_pattern = [r for r in results if r.get('pattern_concentration') == 'HIGH']
        
        if high_odd_hour or high_bursts:
            output.append("\n🚨 SUSPICIOUS TEMPORAL PATTERNS DETECTED")
        
        # Detailed results for each suspect
        for result in results:
            # Skip low-activity suspects unless specifically requested
            if result['total_calls'] < 10 and "all" not in query.lower():
                continue
            
            output.append(f"\n📱 {result['suspect']}")
            
            # Odd hour analysis
            if 'odd_hour_percentage' in result:
                risk_emoji = {'HIGH': '🔴', 'MEDIUM': '🟡', 'LOW': '🟢'}[result['odd_hour_risk']]
                output.append(f"   Odd Hours: {risk_emoji} {result['odd_hour_percentage']}% ({result['odd_hour_calls']} calls)")
            
            # Call bursts
            if 'burst_count' in result and result['burst_count'] > 0:
                output.append(f"   Call Bursts: {result['burst_count']} detected")
                for burst in result['call_bursts'][:2]:  # Show first 2
                    emoji = "🚨" if burst['suspicious'] else "📞"
                    output.append(f"   {emoji} {burst['time']}: {burst['calls']} calls to {burst['unique_contacts']} numbers")
            
            # Pattern days
            if 'pattern_day_activity' in result:
                tuesday = result['pattern_day_activity'].get('Tuesday', {})
                friday = result['pattern_day_activity'].get('Friday', {})
                if tuesday.get('percentage', 0) > 20 or friday.get('percentage', 0) > 20:
                    output.append(f"   Pattern Days: Tuesday {tuesday.get('percentage', 0)}%, Friday {friday.get('percentage', 0)}%")
            
            # Peak hours
            if result.get('peak_hours'):
                output.append(f"   Peak Hours: {', '.join(map(str, result['peak_hours']))}")
            
            # Silent periods
            if result.get('silent_periods'):
                output.append(f"   🔇 Silent Periods: {len(result['silent_periods'])} detected")
                for period in result['silent_periods'][:2]:  # Show first 2
                    severity_emoji = "🚨" if period['severity'] == 'HIGH' else "⚠️"
                    output.append(f"   {severity_emoji} {period['duration_hours']}h gap: {period['start']} to {period['end']}")
        
        # Risk summary
        output.append("\n📊 TEMPORAL RISK SUMMARY:")
        if high_odd_hour:
            output.append(f"   🔴 High Odd-Hour Activity: {', '.join([r['suspect'] for r in high_odd_hour[:3]])}")
        if high_bursts:
            output.append(f"   🔴 Suspicious Call Bursts: {', '.join([r['suspect'] for r in high_bursts[:3]])}")
        if high_pattern:
            output.append(f"   🟡 Pattern Day Concentration: {', '.join([r['suspect'] for r in high_pattern[:3]])}")
        
        # Silent period suspects
        suspects_with_silence = [r for r in results if r.get('has_suspicious_silence')]
        if suspects_with_silence:
            output.append(f"   🔇 Suspicious Silent Periods: {', '.join([r['suspect'] for r in suspects_with_silence[:3]])}")
        
        # Narcotics indicator
        if high_pattern and any(r.get('pattern_concentration') == 'HIGH' for r in results):
            output.append("\n⚠️ NARCOTICS TRAFFICKING INDICATOR:")
            output.append("   High activity on Tuesday/Friday detected - common drug transport days")
        
        # Silent period indicator
        if suspects_with_silence:
            output.append("\n⚠️ POST-INCIDENT SILENCE INDICATOR:")
            output.append("   Extended communication gaps detected - possible post-raid/arrest behavior")
        
        return "\n".join(output)