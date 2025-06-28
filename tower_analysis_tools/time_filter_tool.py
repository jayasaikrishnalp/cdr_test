"""
Time Window Filter Tool
Filters tower dump data based on time windows and temporal patterns
"""

from langchain.tools import BaseTool
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
import json

class TimeWindowFilterTool(BaseTool):
    """Tool for filtering tower dump data by time windows"""
    
    name: str = "time_window_filter"
    description: str = """Filter tower dump data by time window. Use this tool to:
    - Extract records during crime time window
    - Find activity before/after incidents
    - Analyze odd-hour patterns
    - Identify temporal anomalies
    
    Input format: "filter [time_start] to [time_end]" or "analyze odd hours" or "show activity around [time]"
    Example: "filter 2024-01-15 02:00:00 to 2024-01-15 03:00:00"
    """
    
    # Class attributes for Pydantic v2
    tower_dump_data: Dict[str, Any] = {}
    params: Dict[str, Any] = {}
    
    # Class attributes for Pydantic v2
    tower_dump_data: Dict[str, Any] = {}
    params: Dict[str, Any] = {}
    
    def __init__(self):
        super().__init__()

        
        # Time analysis parameters
        self.params = {
            'odd_hours_start': 0,      # Midnight
            'odd_hours_end': 5,        # 5 AM
            'crime_window_before': 60,  # Minutes before incident
            'crime_window_after': 60,   # Minutes after incident
            'burst_threshold': 10,      # Calls in 5 minutes
            'silence_threshold': 120    # Minutes of inactivity
        }
    
    def _run(self, query: str) -> str:
        """Execute time window filtering and analysis"""
        
        if not self.tower_dump_data:
            return "No tower dump data loaded. Please load tower dump data first."
        
        try:
            query_lower = query.lower()
            
            # Parse different query types
            if "filter" in query_lower and " to " in query_lower:
                return self._filter_time_range(query)
            elif "odd hour" in query_lower:
                return self._analyze_odd_hours()
            elif "around" in query_lower:
                return self._analyze_time_window(query)
            elif "burst" in query_lower:
                return self._detect_burst_patterns()
            elif "silence" in query_lower or "quiet" in query_lower:
                return self._detect_silence_periods()
            elif "temporal" in query_lower or "pattern" in query_lower:
                return self._analyze_temporal_patterns()
            else:
                return self._general_time_analysis()
                
        except Exception as e:
            logger.error(f"Error in time window filter: {str(e)}")
            return f"Error filtering by time: {str(e)}"
    
    async def _arun(self, query: str) -> str:
        """Async version"""
        return self._run(query)
    
    def _filter_time_range(self, query: str) -> str:
        """Filter data for specific time range"""
        
        # Extract times from query
        parts = query.split(" to ")
        if len(parts) != 2:
            return "Invalid format. Use: 'filter YYYY-MM-DD HH:MM:SS to YYYY-MM-DD HH:MM:SS'"
        
        try:
            # Extract datetime strings
            start_str = parts[0].replace("filter", "").strip()
            end_str = parts[1].strip()
            
            # Parse datetimes
            start_time = pd.to_datetime(start_str)
            end_time = pd.to_datetime(end_str)
            
            results = []
            results.append(f"🕒 TIME WINDOW ANALYSIS")
            results.append(f"Period: {start_time} to {end_time}")
            results.append(f"Duration: {end_time - start_time}")
            results.append("=" * 80)
            
            # Analyze each tower dump
            total_records = 0
            unique_devices = set()
            tower_activity = {}
            
            for dump_id, df in self.tower_dump_data.items():
                if 'timestamp' not in df.columns:
                    continue
                
                # Convert timestamp column
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                # Filter by time range
                mask = (df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)
                filtered = df[mask]
                
                if len(filtered) > 0:
                    total_records += len(filtered)
                    
                    if 'mobile_number' in filtered.columns:
                        devices = filtered['mobile_number'].unique()
                        unique_devices.update(devices)
                    
                    if 'tower_id' in filtered.columns:
                        for tower in filtered['tower_id'].unique():
                            if tower not in tower_activity:
                                tower_activity[tower] = 0
                            tower_activity[tower] += len(filtered[filtered['tower_id'] == tower])
            
            # Summary
            results.append(f"\n📊 SUMMARY:")
            results.append(f"Total Records: {total_records}")
            results.append(f"Unique Devices: {len(unique_devices)}")
            results.append(f"Active Towers: {len(tower_activity)}")
            
            # Device activity
            if unique_devices:
                results.append(f"\n📱 DEVICES PRESENT ({len(unique_devices)}):")
                device_details = self._analyze_devices_in_window(start_time, end_time)
                results.append(device_details)
            
            # Tower activity
            if tower_activity:
                results.append(f"\n📡 TOWER ACTIVITY:")
                sorted_towers = sorted(tower_activity.items(), key=lambda x: x[1], reverse=True)
                for i, (tower, count) in enumerate(sorted_towers[:5]):
                    results.append(f"{i+1}. Tower {tower}: {count} connections")
            
            # Suspicious patterns
            suspicious = self._find_suspicious_in_window(start_time, end_time)
            if suspicious:
                results.append(f"\n⚠️ SUSPICIOUS PATTERNS:")
                results.append(suspicious)
            
            return "\n".join(results)
            
        except Exception as e:
            return f"Error parsing time range: {str(e)}"
    
    def _analyze_odd_hours(self) -> str:
        """Analyze activity during odd hours (midnight to 5 AM)"""
        
        results = []
        results.append("🌙 ODD HOURS ANALYSIS (00:00 - 05:00)")
        results.append("=" * 80)
        
        odd_hour_devices = {}
        total_odd_hour_records = 0
        
        for dump_id, df in self.tower_dump_data.items():
            if 'timestamp' not in df.columns:
                continue
            
            # Convert timestamp and extract hour
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['hour'] = df['timestamp'].dt.hour
            
            # Filter odd hours
            odd_hours_mask = df['hour'].between(
                self.params['odd_hours_start'], 
                self.params['odd_hours_end'] - 1
            )
            odd_hours_data = df[odd_hours_mask]
            
            if len(odd_hours_data) > 0:
                total_odd_hour_records += len(odd_hours_data)
                
                if 'mobile_number' in odd_hours_data.columns:
                    for device in odd_hours_data['mobile_number'].unique():
                        if device not in odd_hour_devices:
                            odd_hour_devices[device] = {
                                'count': 0,
                                'hours': set(),
                                'dates': set(),
                                'towers': set()
                            }
                        
                        device_data = odd_hours_data[odd_hours_data['mobile_number'] == device]
                        odd_hour_devices[device]['count'] += len(device_data)
                        odd_hour_devices[device]['hours'].update(device_data['hour'].unique())
                        odd_hour_devices[device]['dates'].update(
                            device_data['timestamp'].dt.date.unique()
                        )
                        
                        if 'tower_id' in device_data.columns:
                            odd_hour_devices[device]['towers'].update(
                                device_data['tower_id'].unique()
                            )
        
        # Analysis results
        results.append(f"\n📊 OVERALL STATISTICS:")
        results.append(f"Total Odd Hour Records: {total_odd_hour_records}")
        results.append(f"Devices Active at Odd Hours: {len(odd_hour_devices)}")
        
        if odd_hour_devices:
            # Sort by activity count
            sorted_devices = sorted(
                odd_hour_devices.items(), 
                key=lambda x: x[1]['count'], 
                reverse=True
            )
            
            results.append(f"\n🔴 TOP ODD HOUR DEVICES:")
            
            for i, (device, info) in enumerate(sorted_devices[:10]):
                results.append(f"\n{i+1}. Device: {device}")
                results.append(f"   Activity Count: {info['count']}")
                results.append(f"   Active Hours: {sorted(info['hours'])}")
                results.append(f"   Days Active: {len(info['dates'])}")
                results.append(f"   Towers Used: {len(info['towers'])}")
                
                # Risk assessment
                risk_level = self._assess_odd_hour_risk(info)
                results.append(f"   Risk Level: {risk_level}")
        
        # Hourly distribution
        results.append(f"\n📈 HOURLY DISTRIBUTION (00:00 - 05:00):")
        hourly_dist = self._get_hourly_distribution(0, 5)
        for hour, count in hourly_dist.items():
            results.append(f"   {hour:02d}:00 - {count} records")
        
        return "\n".join(results)
    
    def _analyze_time_window(self, query: str) -> str:
        """Analyze activity around a specific time"""
        
        # Extract time from query
        if "around" in query:
            time_str = query.split("around")[1].strip()
        else:
            return "Please specify a time using 'around YYYY-MM-DD HH:MM:SS'"
        
        try:
            center_time = pd.to_datetime(time_str)
            
            # Define window
            start_time = center_time - timedelta(minutes=self.params['crime_window_before'])
            end_time = center_time + timedelta(minutes=self.params['crime_window_after'])
            
            results = []
            results.append(f"🎯 ACTIVITY AROUND {center_time}")
            results.append(f"Window: {start_time} to {end_time}")
            results.append("=" * 80)
            
            # Before, during, and after analysis
            before_devices = set()
            during_devices = set()
            after_devices = set()
            
            for dump_id, df in self.tower_dump_data.items():
                if 'timestamp' not in df.columns or 'mobile_number' not in df.columns:
                    continue
                
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                # Define periods
                before_mask = (df['timestamp'] >= start_time) & (df['timestamp'] < center_time)
                during_mask = (df['timestamp'] == center_time)
                after_mask = (df['timestamp'] > center_time) & (df['timestamp'] <= end_time)
                
                before_devices.update(df[before_mask]['mobile_number'].unique())
                during_devices.update(df[during_mask]['mobile_number'].unique())
                after_devices.update(df[after_mask]['mobile_number'].unique())
            
            # Analysis
            results.append(f"\n📊 DEVICE PRESENCE:")
            results.append(f"Before ({self.params['crime_window_before']} min): {len(before_devices)} devices")
            results.append(f"During: {len(during_devices)} devices")
            results.append(f"After ({self.params['crime_window_after']} min): {len(after_devices)} devices")
            
            # Movement patterns
            only_before = before_devices - after_devices
            only_after = after_devices - before_devices
            throughout = before_devices & after_devices
            
            results.append(f"\n🚶 MOVEMENT PATTERNS:")
            results.append(f"Left before incident: {len(only_before)} devices")
            results.append(f"Arrived after incident: {len(only_after)} devices")
            results.append(f"Present throughout: {len(throughout)} devices")
            
            # Suspicious devices
            if only_before:
                results.append(f"\n⚠️ DEVICES THAT LEFT (Suspicious):")
                for device in list(only_before)[:5]:
                    results.append(f"   - {device}")
            
            return "\n".join(results)
            
        except Exception as e:
            return f"Error analyzing time window: {str(e)}"
    
    def _detect_burst_patterns(self) -> str:
        """Detect burst activity patterns"""
        
        results = []
        results.append("💥 BURST ACTIVITY DETECTION")
        results.append("=" * 80)
        
        burst_events = []
        
        for dump_id, df in self.tower_dump_data.items():
            if 'timestamp' not in df.columns:
                continue
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Group by 5-minute windows
            df['time_window'] = df['timestamp'].dt.floor('5min')
            
            # Count activity per window
            window_counts = df.groupby('time_window').size()
            
            # Find bursts
            bursts = window_counts[window_counts >= self.params['burst_threshold']]
            
            for time_window, count in bursts.items():
                window_data = df[df['time_window'] == time_window]
                
                burst_info = {
                    'time': time_window,
                    'count': count,
                    'devices': len(window_data['mobile_number'].unique()) if 'mobile_number' in window_data.columns else 0,
                    'towers': len(window_data['tower_id'].unique()) if 'tower_id' in window_data.columns else 0,
                    'dump_id': dump_id
                }
                
                burst_events.append(burst_info)
        
        if burst_events:
            # Sort by count
            burst_events.sort(key=lambda x: x['count'], reverse=True)
            
            results.append(f"\n🔴 BURST EVENTS DETECTED: {len(burst_events)}")
            
            for i, burst in enumerate(burst_events[:10]):
                results.append(f"\n{i+1}. Time: {burst['time']}")
                results.append(f"   Activity: {burst['count']} records in 5 minutes")
                results.append(f"   Devices: {burst['devices']}")
                results.append(f"   Towers: {burst['towers']}")
                results.append(f"   Intensity: {'HIGH' if burst['count'] > 20 else 'MEDIUM'}")
        else:
            results.append("\n✅ No significant burst patterns detected")
        
        return "\n".join(results)
    
    def _detect_silence_periods(self) -> str:
        """Detect periods of unusual silence"""
        
        results = []
        results.append("🔇 SILENCE PERIOD DETECTION")
        results.append("=" * 80)
        
        # Analyze silence periods for each device
        device_silence = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if 'timestamp' not in df.columns or 'mobile_number' not in df.columns:
                continue
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            for device in df['mobile_number'].unique():
                device_data = df[df['mobile_number'] == device].sort_values('timestamp')
                
                if len(device_data) < 2:
                    continue
                
                # Calculate time gaps
                device_data['time_gap'] = device_data['timestamp'].diff()
                
                # Find silence periods
                silence_mask = device_data['time_gap'] > timedelta(minutes=self.params['silence_threshold'])
                silences = device_data[silence_mask]
                
                if len(silences) > 0:
                    if device not in device_silence:
                        device_silence[device] = []
                    
                    for _, row in silences.iterrows():
                        device_silence[device].append({
                            'start': row['timestamp'] - row['time_gap'],
                            'end': row['timestamp'],
                            'duration': row['time_gap']
                        })
        
        if device_silence:
            results.append(f"\n🔴 DEVICES WITH SUSPICIOUS SILENCE PERIODS:")
            
            # Sort by number of silence periods
            sorted_devices = sorted(device_silence.items(), key=lambda x: len(x[1]), reverse=True)
            
            for i, (device, silences) in enumerate(sorted_devices[:10]):
                results.append(f"\n{i+1}. Device: {device}")
                results.append(f"   Silence Periods: {len(silences)}")
                
                # Show longest silence
                longest = max(silences, key=lambda x: x['duration'])
                results.append(f"   Longest Silence: {longest['duration']}")
                results.append(f"   From: {longest['start']} To: {longest['end']}")
        else:
            results.append("\n✅ No significant silence periods detected")
        
        return "\n".join(results)
    
    def _analyze_temporal_patterns(self) -> str:
        """Analyze overall temporal patterns"""
        
        results = []
        results.append("📊 TEMPORAL PATTERN ANALYSIS")
        results.append("=" * 80)
        
        # Collect all temporal data
        hourly_activity = {}
        daily_activity = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if 'timestamp' not in df.columns:
                continue
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['hour'] = df['timestamp'].dt.hour
            df['date'] = df['timestamp'].dt.date
            df['day_of_week'] = df['timestamp'].dt.day_name()
            
            # Hourly patterns
            for hour in range(24):
                if hour not in hourly_activity:
                    hourly_activity[hour] = 0
                hourly_activity[hour] += len(df[df['hour'] == hour])
            
            # Daily patterns
            for day in df['day_of_week'].unique():
                if day not in daily_activity:
                    daily_activity[day] = 0
                daily_activity[day] += len(df[df['day_of_week'] == day])
        
        # Hourly analysis
        results.append("\n📈 HOURLY ACTIVITY PATTERN:")
        peak_hour = max(hourly_activity.items(), key=lambda x: x[1])
        low_hour = min(hourly_activity.items(), key=lambda x: x[1])
        
        results.append(f"Peak Hour: {peak_hour[0]:02d}:00 ({peak_hour[1]} records)")
        results.append(f"Lowest Hour: {low_hour[0]:02d}:00 ({low_hour[1]} records)")
        
        # Show hourly distribution
        results.append("\nHourly Distribution:")
        for hour in sorted(hourly_activity.keys()):
            count = hourly_activity[hour]
            bar = "█" * (count // max(hourly_activity.values()) * 20)
            results.append(f"{hour:02d}:00 {bar} {count}")
        
        # Daily patterns
        if daily_activity:
            results.append(f"\n📅 DAILY ACTIVITY PATTERN:")
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            
            for day in day_order:
                if day in daily_activity:
                    results.append(f"{day}: {daily_activity[day]} records")
        
        # Suspicious temporal patterns
        results.append(f"\n⚠️ SUSPICIOUS TEMPORAL INDICATORS:")
        
        # High odd-hour percentage
        odd_hour_total = sum(hourly_activity.get(h, 0) for h in range(0, 6))
        total_activity = sum(hourly_activity.values())
        
        if total_activity > 0:
            odd_hour_percentage = (odd_hour_total / total_activity) * 100
            results.append(f"Odd Hour Activity: {odd_hour_percentage:.1f}%")
            
            if odd_hour_percentage > 20:
                results.append("   🔴 HIGH - Suspicious nighttime activity")
            elif odd_hour_percentage > 10:
                results.append("   🟡 MODERATE - Some nighttime activity")
            else:
                results.append("   🟢 LOW - Normal activity pattern")
        
        return "\n".join(results)
    
    def _general_time_analysis(self) -> str:
        """Provide general time-based analysis"""
        
        results = []
        results.append("⏰ GENERAL TIME ANALYSIS")
        results.append("=" * 80)
        
        # Get overall time range
        min_time = None
        max_time = None
        total_records = 0
        
        for dump_id, df in self.tower_dump_data.items():
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                if min_time is None or df['timestamp'].min() < min_time:
                    min_time = df['timestamp'].min()
                
                if max_time is None or df['timestamp'].max() > max_time:
                    max_time = df['timestamp'].max()
                
                total_records += len(df)
        
        if min_time and max_time:
            results.append(f"\n📊 DATA OVERVIEW:")
            results.append(f"Time Range: {min_time} to {max_time}")
            results.append(f"Duration: {max_time - min_time}")
            results.append(f"Total Records: {total_records}")
            
            # Suggest analyses
            results.append(f"\n💡 SUGGESTED ANALYSES:")
            results.append("1. 'analyze odd hours' - Check midnight to 5 AM activity")
            results.append("2. 'detect burst patterns' - Find coordination indicators")
            results.append("3. 'detect silence periods' - Find suspicious gaps")
            results.append("4. 'filter [start] to [end]' - Analyze specific time window")
            results.append(f"5. 'around {min_time + (max_time - min_time)/2}' - Analyze midpoint activity")
        
        return "\n".join(results)
    
    def _analyze_devices_in_window(self, start_time: datetime, end_time: datetime) -> str:
        """Detailed analysis of devices in time window"""
        
        device_details = []
        
        for dump_id, df in self.tower_dump_data.items():
            if 'timestamp' not in df.columns or 'mobile_number' not in df.columns:
                continue
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            mask = (df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)
            window_data = df[mask]
            
            for device in window_data['mobile_number'].unique():
                device_data = window_data[window_data['mobile_number'] == device]
                
                first_seen = device_data['timestamp'].min()
                last_seen = device_data['timestamp'].max()
                duration = last_seen - first_seen
                
                # Classify device behavior
                if duration < timedelta(minutes=5):
                    behavior = "Brief presence"
                elif len(device_data) == 1:
                    behavior = "Single ping"
                elif duration == timedelta(0):
                    behavior = "Stationary"
                else:
                    behavior = "Active movement"
                
                device_details.append(
                    f"   {device}: {first_seen.strftime('%H:%M:%S')} - "
                    f"{last_seen.strftime('%H:%M:%S')} ({behavior})"
                )
        
        return "\n".join(device_details[:10])  # Show top 10
    
    def _find_suspicious_in_window(self, start_time: datetime, end_time: datetime) -> str:
        """Find suspicious patterns in time window"""
        
        suspicious = []
        
        # Check for devices that appear only in this window
        window_only_devices = set()
        all_devices = set()
        
        for dump_id, df in self.tower_dump_data.items():
            if 'timestamp' not in df.columns or 'mobile_number' not in df.columns:
                continue
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Devices in window
            mask = (df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)
            window_devices = set(df[mask]['mobile_number'].unique())
            
            # All devices
            all_devices.update(df['mobile_number'].unique())
            
            # Devices ONLY in this window
            outside_mask = (df['timestamp'] < start_time) | (df['timestamp'] > end_time)
            outside_devices = set(df[outside_mask]['mobile_number'].unique())
            
            window_only = window_devices - outside_devices
            window_only_devices.update(window_only)
        
        if window_only_devices:
            suspicious.append(f"Devices appearing ONLY in this window: {len(window_only_devices)}")
            for device in list(window_only_devices)[:5]:
                suspicious.append(f"   - {device} (One-time visitor)")
        
        return "\n".join(suspicious)
    
    def _assess_odd_hour_risk(self, info: Dict) -> str:
        """Assess risk level based on odd hour activity"""
        
        risk_score = 0
        
        # Frequency
        if info['count'] > 10:
            risk_score += 3
        elif info['count'] > 5:
            risk_score += 2
        else:
            risk_score += 1
        
        # Consistency (multiple days)
        if len(info['dates']) > 3:
            risk_score += 2
        elif len(info['dates']) > 1:
            risk_score += 1
        
        # Movement (multiple towers)
        if len(info['towers']) > 3:
            risk_score += 2
        elif len(info['towers']) > 1:
            risk_score += 1
        
        # Risk level
        if risk_score >= 6:
            return "🔴 HIGH RISK"
        elif risk_score >= 4:
            return "🟡 MEDIUM RISK"
        else:
            return "🟢 LOW RISK"
    
    def _get_hourly_distribution(self, start_hour: int, end_hour: int) -> Dict[int, int]:
        """Get activity count by hour"""
        
        hourly_dist = {}
        
        for hour in range(start_hour, end_hour):
            hourly_dist[hour] = 0
            
            for dump_id, df in self.tower_dump_data.items():
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df['hour'] = df['timestamp'].dt.hour
                    hourly_dist[hour] += len(df[df['hour'] == hour])
        
        return hourly_dist