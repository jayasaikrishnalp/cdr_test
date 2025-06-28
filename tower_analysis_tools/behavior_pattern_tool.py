"""
Behavior Pattern Detection Tool
Detects and analyzes behavioral patterns in tower dump data
"""

from langchain.tools import BaseTool
from typing import Dict, Any, List, Optional, Set
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
import numpy as np

class BehaviorPatternTool(BaseTool):
    """Tool for detecting behavioral patterns in tower dump data"""
    
    name: str = "behavior_pattern_detection"
    description: str = """Detect behavioral patterns in tower dump data. Use this tool to identify:
    - Frequent visitors vs one-time visitors
    - Regular patterns vs anomalies
    - Suspicious behavioral indicators
    - Group coordination patterns
    - Reconnaissance behavior
    
    Input examples: "detect frequent visitors", "find one-time visitors", "analyze suspicious behavior", "identify reconnaissance patterns"
    """
    
    # Class attributes for Pydantic v2
    tower_dump_data: Dict[str, Any] = {}
    thresholds: Dict[str, Any] = {}
    
    def __init__(self):
        super().__init__()
        
        # Pattern thresholds
        self.thresholds = {
            'frequent_visitor_days': 3,  # Days to consider frequent
            'frequent_visitor_count': 5,  # Min visits to be frequent
            'one_time_threshold': 1,  # Max visits for one-time
            'recon_pattern_days': 7,  # Days before incident for recon
            'group_time_window': 300,  # Seconds for group coordination
            'suspicious_duration': 300,  # Seconds for brief suspicious visits
            'new_sim_days': 7,  # Days to consider SIM as new
            'burner_phone_activity': 10  # Max activity for burner phone
        }
    
    def _run(self, query: str) -> str:
        """Execute behavior pattern detection"""
        
        if not self.tower_dump_data:
            return "No tower dump data loaded. Please load tower dump data first."
        
        try:
            query_lower = query.lower()
            
            if "frequent" in query_lower:
                return self._detect_frequent_visitors()
            elif "one-time" in query_lower or "one time" in query_lower:
                return self._detect_one_time_visitors()
            elif "reconnaissance" in query_lower or "recon" in query_lower:
                return self._detect_reconnaissance_patterns()
            elif "group" in query_lower or "coordination" in query_lower:
                return self._detect_group_coordination()
            elif "burner" in query_lower or "new sim" in query_lower:
                return self._detect_burner_phones()
            elif "suspicious" in query_lower:
                return self._detect_suspicious_behavior()
            else:
                return self._comprehensive_behavior_analysis()
                
        except Exception as e:
            logger.error(f"Error in behavior pattern detection: {str(e)}")
            return f"Error detecting patterns: {str(e)}"
    
    async def _arun(self, query: str) -> str:
        """Async version"""
        return self._run(query)
    
    def _detect_frequent_visitors(self) -> str:
        """Detect frequent visitors to towers"""
        
        results = []
        results.append("🔄 FREQUENT VISITOR ANALYSIS")
        results.append("=" * 80)
        
        frequent_visitors = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' not in df.columns or 'timestamp' not in df.columns:
                continue
            
            # Add date column
            df['date'] = df['timestamp'].dt.date
            
            # Analyze each tower
            if 'tower_id' in df.columns:
                for tower_id, tower_group in df.groupby('tower_id'):
                    # Count visits per number
                    number_stats = tower_group.groupby('mobile_number').agg({
                        'timestamp': 'count',
                        'date': 'nunique'
                    }).rename(columns={'timestamp': 'visit_count', 'date': 'unique_days'})
                    
                    # Filter frequent visitors
                    freq_mask = (
                        (number_stats['unique_days'] >= self.thresholds['frequent_visitor_days']) |
                        (number_stats['visit_count'] >= self.thresholds['frequent_visitor_count'])
                    )
                    
                    tower_frequent = number_stats[freq_mask]
                    
                    if len(tower_frequent) > 0:
                        if tower_id not in frequent_visitors:
                            frequent_visitors[tower_id] = []
                        
                        for number, stats in tower_frequent.iterrows():
                            frequent_visitors[tower_id].append({
                                'number': number,
                                'visits': stats['visit_count'],
                                'days': stats['unique_days'],
                                'frequency': stats['visit_count'] / stats['unique_days']
                            })
        
        # Report findings
        if frequent_visitors:
            results.append(f"\n📍 TOWERS WITH FREQUENT VISITORS: {len(frequent_visitors)}")
            
            # Sort towers by number of frequent visitors
            sorted_towers = sorted(
                frequent_visitors.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            
            for tower_id, visitors in sorted_towers[:5]:
                results.append(f"\n🏗️ Tower: {tower_id}")
                results.append(f"   Frequent Visitors: {len(visitors)}")
                
                # Top visitors for this tower
                top_visitors = sorted(visitors, key=lambda x: x['visits'], reverse=True)[:3]
                for visitor in top_visitors:
                    results.append(f"   📱 {visitor['number']}")
                    results.append(f"      Visits: {visitor['visits']} over {visitor['days']} days")
                    results.append(f"      Daily Average: {visitor['frequency']:.1f} visits/day")
        
        # Pattern analysis
        results.append("\n🎯 BEHAVIORAL PATTERNS:")
        
        # Identify different types of frequent visitors
        all_visitors = []
        for tower_visitors in frequent_visitors.values():
            all_visitors.extend(tower_visitors)
        
        if all_visitors:
            # Daily commuters (regular frequency)
            commuters = [v for v in all_visitors if 0.8 <= v['frequency'] <= 2.5]
            if commuters:
                results.append(f"\n  • Likely Commuters/Locals: {len(set(v['number'] for v in commuters))}")
                results.append("    - Regular daily presence suggests legitimate activity")
            
            # High-frequency visitors
            high_freq = [v for v in all_visitors if v['frequency'] > 5]
            if high_freq:
                results.append(f"\n  • High-Frequency Visitors: {len(set(v['number'] for v in high_freq))}")
                results.append("    ⚠️ Unusually high visit frequency - possible surveillance")
        
        return "\n".join(results)
    
    def _detect_one_time_visitors(self) -> str:
        """Detect one-time visitors"""
        
        results = []
        results.append("👤 ONE-TIME VISITOR ANALYSIS")
        results.append("=" * 80)
        
        one_time_visitors = {}
        crime_window_visitors = []
        
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' not in df.columns:
                continue
            
            # Overall visit counts
            visit_counts = df['mobile_number'].value_counts()
            one_timers = visit_counts[visit_counts <= self.thresholds['one_time_threshold']]
            
            # Analyze by tower
            if 'tower_id' in df.columns:
                for tower_id, tower_group in df.groupby('tower_id'):
                    tower_one_timers = []
                    
                    for number in one_timers.index:
                        if number in tower_group['mobile_number'].values:
                            # Get details of the visit
                            visit_data = tower_group[tower_group['mobile_number'] == number].iloc[0]
                            
                            visitor_info = {
                                'number': number,
                                'timestamp': visit_data.get('timestamp'),
                                'duration': visit_data.get('duration', 0) if 'duration' in visit_data else 0,
                                'imei': visit_data.get('imei', 'Unknown') if 'imei' in visit_data else 'Unknown'
                            }
                            
                            tower_one_timers.append(visitor_info)
                            
                            # Check if during crime window (night/odd hours)
                            if 'timestamp' in visit_data and pd.notna(visit_data['timestamp']):
                                hour = visit_data['timestamp'].hour
                                if 0 <= hour <= 5:
                                    crime_window_visitors.append(visitor_info)
                    
                    if tower_one_timers:
                        one_time_visitors[tower_id] = tower_one_timers
        
        # Report findings
        total_one_timers = sum(len(visitors) for visitors in one_time_visitors.values())
        results.append(f"\n📊 TOTAL ONE-TIME VISITORS: {total_one_timers}")
        
        if one_time_visitors:
            results.append("\n🏗️ TOWERS WITH ONE-TIME VISITORS:")
            
            # Sort by number of one-time visitors
            sorted_towers = sorted(
                one_time_visitors.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            
            for tower_id, visitors in sorted_towers[:5]:
                results.append(f"\n  Tower {tower_id}: {len(visitors)} one-time visitors")
                
                # Show recent one-timers
                recent_visitors = sorted(
                    [v for v in visitors if v['timestamp'] is not None],
                    key=lambda x: x['timestamp'],
                    reverse=True
                )[:3]
                
                for visitor in recent_visitors:
                    results.append(f"    📱 {visitor['number']}")
                    if visitor['timestamp']:
                        results.append(f"       Time: {visitor['timestamp']}")
                    if visitor['duration'] > 0:
                        results.append(f"       Duration: {visitor['duration']}s")
        
        # High-risk one-time visitors
        if crime_window_visitors:
            results.append(f"\n🚨 HIGH-RISK ONE-TIME VISITORS (Odd Hours): {len(crime_window_visitors)}")
            results.append("   ⚠️ One-time appearance during suspicious hours indicates:")
            results.append("      - Possible burner phone")
            results.append("      - External operative")
            results.append("      - Reconnaissance activity")
            
            for visitor in crime_window_visitors[:5]:
                results.append(f"\n   🔴 {visitor['number']}")
                results.append(f"      Time: {visitor['timestamp']}")
        
        return "\n".join(results)
    
    def _detect_reconnaissance_patterns(self) -> str:
        """Detect potential reconnaissance behavior"""
        
        results = []
        results.append("🔍 RECONNAISSANCE PATTERN DETECTION")
        results.append("=" * 80)
        
        recon_suspects = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                continue
            
            # Add date column
            df['date'] = df['timestamp'].dt.date
            
            # For each number, analyze visit patterns
            for number, number_group in df.groupby('mobile_number'):
                # Skip if too many records (likely not recon)
                if len(number_group) > 100:
                    continue
                
                # Analyze tower visit patterns
                tower_visits = number_group.groupby('tower_id').agg({
                    'timestamp': ['count', 'min', 'max'],
                    'date': 'nunique'
                })
                
                # Flatten column names
                tower_visits.columns = ['visit_count', 'first_visit', 'last_visit', 'unique_days']
                
                # Reconnaissance indicators
                recon_score = 0
                indicators = []
                
                # 1. Multiple towers visited briefly
                brief_visits = tower_visits[tower_visits['visit_count'] <= 3]
                if len(brief_visits) >= 3:
                    recon_score += 2
                    indicators.append(f"Brief visits to {len(brief_visits)} towers")
                
                # 2. Progressive tower exploration
                first_visits = tower_visits['first_visit'].sort_values()
                if len(first_visits) >= 3:
                    # Check if visits are spread over days
                    date_span = (first_visits.iloc[-1] - first_visits.iloc[0]).days
                    if date_span >= 3 and date_span <= self.thresholds['recon_pattern_days']:
                        recon_score += 3
                        indicators.append(f"Progressive exploration over {date_span} days")
                
                # 3. Short duration visits
                if 'duration' in number_group.columns:
                    short_visits = number_group[number_group['duration'] < self.thresholds['suspicious_duration']]
                    if len(short_visits) > len(number_group) * 0.7:
                        recon_score += 2
                        indicators.append("Majority short-duration visits")
                
                # 4. Specific time patterns
                hour_distribution = number_group['timestamp'].dt.hour.value_counts()
                if len(hour_distribution) <= 3:  # Activity in limited hours
                    recon_score += 1
                    indicators.append("Activity in specific hours only")
                
                # Store if suspicious
                if recon_score >= 3:
                    recon_suspects[number] = {
                        'score': recon_score,
                        'indicators': indicators,
                        'towers_visited': len(tower_visits),
                        'total_visits': len(number_group),
                        'date_range': f"{number_group['date'].min()} to {number_group['date'].max()}"
                    }
        
        # Report findings
        if recon_suspects:
            results.append(f"\n🎯 POTENTIAL RECONNAISSANCE SUSPECTS: {len(recon_suspects)}")
            
            # Sort by recon score
            sorted_suspects = sorted(
                recon_suspects.items(),
                key=lambda x: x[1]['score'],
                reverse=True
            )
            
            for number, data in sorted_suspects[:10]:
                results.append(f"\n📱 {number}")
                results.append(f"   Reconnaissance Score: {data['score']}/10")
                results.append(f"   Date Range: {data['date_range']}")
                results.append(f"   Towers Visited: {data['towers_visited']}")
                results.append(f"   Total Visits: {data['total_visits']}")
                results.append("   Indicators:")
                for indicator in data['indicators']:
                    results.append(f"     • {indicator}")
        
        # Pattern summary
        results.append("\n📋 RECONNAISSANCE PATTERNS IDENTIFIED:")
        results.append("  • Brief visits to multiple locations")
        results.append("  • Progressive area exploration")
        results.append("  • Limited time window activity")
        results.append("  • Short duration connections")
        
        return "\n".join(results)
    
    def _detect_group_coordination(self) -> str:
        """Detect coordinated group activities"""
        
        results = []
        results.append("👥 GROUP COORDINATION DETECTION")
        results.append("=" * 80)
        
        coordination_patterns = []
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                continue
            
            # Sort by timestamp
            df_sorted = df.sort_values('timestamp')
            
            # Group by tower and time windows
            for tower_id, tower_group in df_sorted.groupby('tower_id'):
                # Create time windows
                tower_sorted = tower_group.sort_values('timestamp')
                
                # Sliding window analysis
                for i in range(len(tower_sorted)):
                    window_start = tower_sorted.iloc[i]['timestamp']
                    window_end = window_start + timedelta(seconds=self.thresholds['group_time_window'])
                    
                    # Find all numbers in this window
                    window_mask = (
                        (tower_sorted['timestamp'] >= window_start) &
                        (tower_sorted['timestamp'] <= window_end)
                    )
                    window_data = tower_sorted[window_mask]
                    
                    unique_numbers = window_data['mobile_number'].unique()
                    
                    # Check for coordination pattern
                    if len(unique_numbers) >= 3:
                        # Check if these numbers appear together elsewhere
                        coordination_patterns.append({
                            'tower': tower_id,
                            'timestamp': window_start,
                            'numbers': list(unique_numbers),
                            'count': len(unique_numbers),
                            'duration': (window_data['timestamp'].max() - window_data['timestamp'].min()).total_seconds()
                        })
        
        # Analyze coordination patterns
        if coordination_patterns:
            # Remove duplicates and overlapping windows
            unique_patterns = []
            for pattern in coordination_patterns:
                is_duplicate = False
                for existing in unique_patterns:
                    if (existing['tower'] == pattern['tower'] and
                        abs((existing['timestamp'] - pattern['timestamp']).total_seconds()) < 300 and
                        len(set(existing['numbers']) & set(pattern['numbers'])) > len(pattern['numbers']) * 0.7):
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    unique_patterns.append(pattern)
            
            results.append(f"\n🎯 COORDINATION EVENTS DETECTED: {len(unique_patterns)}")
            
            # Sort by group size
            sorted_patterns = sorted(unique_patterns, key=lambda x: x['count'], reverse=True)
            
            for pattern in sorted_patterns[:5]:
                results.append(f"\n📍 Tower: {pattern['tower']}")
                results.append(f"   Time: {pattern['timestamp']}")
                results.append(f"   Group Size: {pattern['count']} numbers")
                results.append(f"   Duration: {pattern['duration']:.0f} seconds")
                results.append(f"   Numbers: {', '.join(pattern['numbers'][:3])}...")
            
            # Find recurring groups
            results.append("\n🔗 RECURRING GROUP ANALYSIS:")
            
            # Track which numbers appear together
            number_associations = {}
            for pattern in unique_patterns:
                for i, num1 in enumerate(pattern['numbers']):
                    for num2 in pattern['numbers'][i+1:]:
                        pair = tuple(sorted([num1, num2]))
                        number_associations[pair] = number_associations.get(pair, 0) + 1
            
            # Find strong associations
            strong_associations = {
                pair: count for pair, count in number_associations.items()
                if count >= 2
            }
            
            if strong_associations:
                results.append(f"\n  Strong Associations Found: {len(strong_associations)}")
                for pair, count in list(strong_associations.items())[:5]:
                    results.append(f"    • {pair[0]} ↔ {pair[1]}: {count} co-occurrences")
        
        return "\n".join(results)
    
    def _detect_burner_phones(self) -> str:
        """Detect potential burner phones and new SIMs"""
        
        results = []
        results.append("🔥 BURNER PHONE / NEW SIM DETECTION")
        results.append("=" * 80)
        
        burner_suspects = {}
        new_sims = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' not in df.columns or 'timestamp' not in df.columns:
                continue
            
            # Analyze each number
            for number, group in df.groupby('mobile_number'):
                # Calculate activity span
                first_seen = group['timestamp'].min()
                last_seen = group['timestamp'].max()
                activity_span = (last_seen - first_seen).days
                
                # Burner phone indicators
                burner_score = 0
                indicators = []
                
                # 1. Short activity span with limited records
                if activity_span <= self.thresholds['new_sim_days'] and len(group) <= self.thresholds['burner_phone_activity']:
                    burner_score += 3
                    indicators.append(f"Limited activity over {activity_span} days")
                
                # 2. One-way communication pattern
                # (This would need CDR integration to fully implement)
                
                # 3. Odd hour activity
                if 'timestamp' in group.columns:
                    odd_hour_activity = group[group['timestamp'].dt.hour.between(0, 5)]
                    if len(odd_hour_activity) > len(group) * 0.5:
                        burner_score += 2
                        indicators.append("Majority activity in odd hours")
                
                # 4. Limited tower usage
                if 'tower_id' in group.columns:
                    unique_towers = group['tower_id'].nunique()
                    if unique_towers <= 2:
                        burner_score += 1
                        indicators.append(f"Used only {unique_towers} tower(s)")
                
                # 5. No IMEI or changing IMEI
                if 'imei' in group.columns:
                    unique_imeis = group['imei'].dropna().nunique()
                    if unique_imeis == 0:
                        burner_score += 1
                        indicators.append("No IMEI recorded")
                    elif unique_imeis > 1:
                        burner_score += 2
                        indicators.append(f"Multiple IMEIs ({unique_imeis})")
                
                # Categorize
                if burner_score >= 4:
                    burner_suspects[number] = {
                        'score': burner_score,
                        'indicators': indicators,
                        'first_seen': first_seen,
                        'last_seen': last_seen,
                        'total_activity': len(group),
                        'activity_span': activity_span
                    }
                elif activity_span <= self.thresholds['new_sim_days']:
                    new_sims[number] = {
                        'first_seen': first_seen,
                        'activity_days': activity_span,
                        'total_activity': len(group)
                    }
        
        # Report findings
        if burner_suspects:
            results.append(f"\n🔴 SUSPECTED BURNER PHONES: {len(burner_suspects)}")
            
            sorted_suspects = sorted(
                burner_suspects.items(),
                key=lambda x: x[1]['score'],
                reverse=True
            )
            
            for number, data in sorted_suspects[:10]:
                results.append(f"\n📱 {number}")
                results.append(f"   Burner Score: {data['score']}/8")
                results.append(f"   Active: {data['first_seen'].date()} to {data['last_seen'].date()}")
                results.append(f"   Total Activity: {data['total_activity']} records")
                results.append("   Indicators:")
                for indicator in data['indicators']:
                    results.append(f"     • {indicator}")
        
        if new_sims:
            results.append(f"\n🆕 NEWLY ACTIVATED SIMS: {len(new_sims)}")
            results.append("   (Active less than 7 days)")
            
            # Group by activation date
            activation_dates = {}
            for number, data in new_sims.items():
                date = data['first_seen'].date()
                if date not in activation_dates:
                    activation_dates[date] = []
                activation_dates[date].append(number)
            
            # Check for bulk activations
            bulk_activations = {
                date: numbers for date, numbers in activation_dates.items()
                if len(numbers) >= 3
            }
            
            if bulk_activations:
                results.append("\n   ⚠️ BULK ACTIVATIONS DETECTED:")
                for date, numbers in bulk_activations.items():
                    results.append(f"     {date}: {len(numbers)} SIMs activated")
        
        return "\n".join(results)
    
    def _detect_suspicious_behavior(self) -> str:
        """Comprehensive suspicious behavior detection"""
        
        results = []
        results.append("🚨 SUSPICIOUS BEHAVIOR ANALYSIS")
        results.append("=" * 80)
        
        suspicious_numbers = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' not in df.columns:
                continue
            
            for number, group in df.groupby('mobile_number'):
                suspicion_score = 0
                behaviors = []
                
                # 1. Odd hour concentration
                if 'timestamp' in group.columns:
                    hours = group['timestamp'].dt.hour
                    odd_hour_ratio = len(hours[hours.between(0, 5)]) / len(hours)
                    if odd_hour_ratio > 0.3:
                        suspicion_score += 2
                        behaviors.append(f"High odd-hour activity ({odd_hour_ratio*100:.1f}%)")
                
                # 2. Brief connections
                if 'duration' in group.columns:
                    brief_connections = group[group['duration'] < 60]
                    if len(brief_connections) > len(group) * 0.5:
                        suspicion_score += 1
                        behaviors.append("Majority brief connections (<60s)")
                
                # 3. Tower hopping
                if 'tower_id' in group.columns and 'timestamp' in group.columns:
                    sorted_group = group.sort_values('timestamp')
                    tower_changes = (sorted_group['tower_id'] != sorted_group['tower_id'].shift()).sum()
                    
                    if len(sorted_group) > 10 and tower_changes > len(sorted_group) * 0.7:
                        suspicion_score += 2
                        behaviors.append("Frequent tower switching")
                
                # 4. Silent periods followed by bursts
                if 'timestamp' in group.columns:
                    sorted_times = group['timestamp'].sort_values()
                    time_gaps = sorted_times.diff()
                    
                    long_gaps = time_gaps[time_gaps > timedelta(hours=24)]
                    if len(long_gaps) > 0:
                        suspicion_score += 1
                        behaviors.append(f"Long silent periods ({len(long_gaps)} instances)")
                
                # Store if suspicious
                if suspicion_score >= 3:
                    suspicious_numbers[number] = {
                        'score': suspicion_score,
                        'behaviors': behaviors,
                        'total_activity': len(group)
                    }
        
        # Report findings
        if suspicious_numbers:
            results.append(f"\n🎯 SUSPICIOUS NUMBERS IDENTIFIED: {len(suspicious_numbers)}")
            
            # Sort by suspicion score
            sorted_suspects = sorted(
                suspicious_numbers.items(),
                key=lambda x: x[1]['score'],
                reverse=True
            )
            
            # Categorize by risk level
            high_risk = [s for s in sorted_suspects if s[1]['score'] >= 5]
            medium_risk = [s for s in sorted_suspects if 3 <= s[1]['score'] < 5]
            
            if high_risk:
                results.append(f"\n🔴 HIGH RISK ({len(high_risk)} numbers):")
                for number, data in high_risk[:5]:
                    results.append(f"\n  📱 {number}")
                    results.append(f"     Risk Score: {data['score']}")
                    for behavior in data['behaviors']:
                        results.append(f"     • {behavior}")
            
            if medium_risk:
                results.append(f"\n🟡 MEDIUM RISK ({len(medium_risk)} numbers):")
                for number, data in medium_risk[:3]:
                    results.append(f"\n  📱 {number}")
                    results.append(f"     Risk Score: {data['score']}")
        
        return "\n".join(results)
    
    def _comprehensive_behavior_analysis(self) -> str:
        """Provide comprehensive behavioral analysis"""
        
        results = []
        results.append("📊 COMPREHENSIVE BEHAVIORAL ANALYSIS")
        results.append("=" * 80)
        
        # Run all analyses
        analyses = {
            "Frequent Visitors": self._detect_frequent_visitors(),
            "One-Time Visitors": self._detect_one_time_visitors(),
            "Reconnaissance": self._detect_reconnaissance_patterns(),
            "Group Coordination": self._detect_group_coordination(),
            "Burner Phones": self._detect_burner_phones(),
            "Suspicious Behavior": self._detect_suspicious_behavior()
        }
        
        # Summary statistics
        total_numbers = set()
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' in df.columns:
                total_numbers.update(df['mobile_number'].unique())
        
        results.append(f"\nTotal Unique Numbers Analyzed: {len(total_numbers)}")
        
        # Extract key findings from each analysis
        for analysis_type, analysis_result in analyses.items():
            # Extract first few lines as summary
            lines = analysis_result.split('\n')
            key_finding = next((line for line in lines if 'DETECTED:' in line or 'IDENTIFIED:' in line), None)
            
            if key_finding:
                results.append(f"\n{analysis_type}: {key_finding.strip()}")
        
        return "\n".join(results)