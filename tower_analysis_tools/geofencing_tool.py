"""
Geofencing Tool
Analyzes activity within defined geographic boundaries and crime scene perimeters
"""

from langchain.tools import BaseTool
from typing import Dict, Any, List, Optional, Set, Tuple
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
import numpy as np
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
import json

class GeofencingTool(BaseTool):
    """Tool for geofencing analysis in tower dump data"""
    
    name: str = "geofencing_analysis"
    description: str = """Perform geofencing analysis on tower dump data. Use this tool to:
    - Define crime scene perimeters
    - Identify devices within geofenced areas
    - Track entry/exit from crime scenes
    - Analyze dwell time in critical areas
    - Multi-tower triangulation for precise location
    
    Input examples: "geofence crime scene", "who was at location X", "analyze area around tower Y", "triangulate suspect location"
    """
    
    # Class attributes for Pydantic v2
    tower_dump_data: Dict[str, Any] = {}
    tower_locations: Dict[str, Any] = {}
    geofences: Dict[str, Any] = {}
    params: Dict[str, Any] = {}
    
    def __init__(self):
        super().__init__()
        
        # Geofencing parameters
        self.params = {
            'default_radius_km': 1.0,      # Default geofence radius
            'crime_scene_radius': 0.5,     # Crime scene perimeter
            'triangulation_towers': 3,      # Min towers for triangulation
            'dwell_time_minutes': 10,       # Min time to consider dwelling
            'buffer_zone_km': 0.2          # Buffer around geofence
        }
    
    def _run(self, query: str) -> str:
        """Execute geofencing analysis"""
        
        if not self.tower_dump_data:
            return "No tower dump data loaded. Please load tower dump data first."
        
        try:
            query_lower = query.lower()
            
            if "crime scene" in query_lower or "geofence" in query_lower:
                return self._analyze_crime_scene_geofence(query)
            elif "triangulate" in query_lower:
                return self._triangulate_locations()
            elif "dwell" in query_lower or "loiter" in query_lower:
                return self._analyze_dwell_patterns()
            elif "perimeter" in query_lower:
                return self._analyze_perimeter_activity()
            elif "heat" in query_lower or "density" in query_lower:
                return self._create_density_analysis()
            else:
                return self._comprehensive_geofence_analysis()
                
        except Exception as e:
            logger.error(f"Error in geofencing analysis: {str(e)}")
            return f"Error performing geofencing: {str(e)}"
    
    async def _arun(self, query: str) -> str:
        """Async version"""
        return self._run(query)
    
    def _analyze_crime_scene_geofence(self, query: str) -> str:
        """Analyze activity within crime scene geofence"""
        
        results = []
        results.append("🎯 CRIME SCENE GEOFENCE ANALYSIS")
        results.append("=" * 80)
        
        # Extract location from query or use default
        # In production, this would parse specific coordinates or tower IDs
        
        # For demonstration, analyze high-activity towers as potential crime scenes
        tower_activity = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if 'tower_id' not in df.columns:
                continue
            
            # Count activity per tower
            tower_counts = df['tower_id'].value_counts()
            
            for tower_id, count in tower_counts.items():
                if tower_id not in tower_activity:
                    tower_activity[tower_id] = 0
                tower_activity[tower_id] += count
        
        # Identify potential crime scene towers (high activity)
        if not tower_activity:
            return "No tower activity data available for geofencing"
        
        # Take top towers as potential crime scenes
        crime_scene_towers = sorted(tower_activity.items(), key=lambda x: x[1], reverse=True)[:3]
        
        for tower_id, activity_count in crime_scene_towers:
            results.append(f"\n📍 CRIME SCENE ANALYSIS - Tower: {tower_id}")
            results.append(f"   Total Activity: {activity_count} connections")
            
            # Analyze devices in this geofence
            devices_in_geofence = self._get_devices_in_tower_geofence(tower_id)
            
            if devices_in_geofence:
                results.append(f"   Unique Devices: {len(devices_in_geofence)}")
                
                # Categorize by behavior
                categorized = self._categorize_geofence_devices(tower_id, devices_in_geofence)
                
                # Report categories
                if categorized['residents']:
                    results.append(f"\n   🏠 LIKELY RESIDENTS/WORKERS: {len(categorized['residents'])}")
                    results.append("      (Regular daily presence)")
                
                if categorized['brief_visitors']:
                    results.append(f"\n   ⚡ BRIEF VISITORS: {len(categorized['brief_visitors'])}")
                    results.append("      ⚠️ Suspicious quick visits")
                    for visitor in categorized['brief_visitors'][:3]:
                        results.append(f"      📱 {visitor['number']}: {visitor['duration_minutes']:.1f} minutes")
                
                if categorized['odd_hour_visitors']:
                    results.append(f"\n   🌙 ODD HOUR VISITORS: {len(categorized['odd_hour_visitors'])}")
                    results.append("      ⚠️ Present during suspicious hours")
                    for visitor in categorized['odd_hour_visitors'][:3]:
                        results.append(f"      📱 {visitor['number']}: {visitor['timestamp']}")
                
                if categorized['one_time_visitors']:
                    results.append(f"\n   👤 ONE-TIME VISITORS: {len(categorized['one_time_visitors'])}")
                    results.append("      ⚠️ Only appeared once in area")
        
        # Multi-tower geofence analysis
        results.append("\n🔺 MULTI-TOWER GEOFENCE ZONES:")
        
        # Find overlapping coverage areas
        overlapping_zones = self._find_overlapping_coverage_zones()
        
        if overlapping_zones:
            for zone in overlapping_zones[:3]:
                results.append(f"\n   Zone: {' + '.join(zone['towers'])}")
                results.append(f"   Overlap Area: High precision location possible")
                results.append(f"   Devices in overlap: {zone['device_count']}")
        
        return "\n".join(results)
    
    def _get_devices_in_tower_geofence(self, tower_id: str) -> List[Dict]:
        """Get all devices that connected to a specific tower"""
        
        devices = []
        
        for dump_id, df in self.tower_dump_data.items():
            if 'tower_id' not in df.columns or 'mobile_number' not in df.columns:
                continue
            
            # Filter for specific tower
            tower_data = df[df['tower_id'] == tower_id]
            
            if len(tower_data) == 0:
                continue
            
            # Analyze each device
            for number, device_data in tower_data.groupby('mobile_number'):
                device_info = {
                    'number': number,
                    'first_seen': device_data['timestamp'].min() if 'timestamp' in device_data.columns else None,
                    'last_seen': device_data['timestamp'].max() if 'timestamp' in device_data.columns else None,
                    'connection_count': len(device_data),
                    'unique_days': device_data['timestamp'].dt.date.nunique() if 'timestamp' in device_data.columns else 0
                }
                
                # Calculate duration if timestamps available
                if device_info['first_seen'] and device_info['last_seen']:
                    device_info['total_duration'] = (device_info['last_seen'] - device_info['first_seen']).total_seconds() / 60
                
                devices.append(device_info)
        
        return devices
    
    def _categorize_geofence_devices(self, tower_id: str, devices: List[Dict]) -> Dict[str, List]:
        """Categorize devices by behavior patterns"""
        
        categories = {
            'residents': [],
            'brief_visitors': [],
            'odd_hour_visitors': [],
            'one_time_visitors': [],
            'suspicious_patterns': []
        }
        
        for device in devices:
            # Residents: multiple days, regular presence
            if device.get('unique_days', 0) >= 3:
                categories['residents'].append(device)
            
            # One-time visitors
            elif device.get('connection_count', 0) == 1:
                categories['one_time_visitors'].append(device)
            
            # Brief visitors: single day, short duration
            elif device.get('total_duration', float('inf')) < self.params['dwell_time_minutes']:
                device['duration_minutes'] = device.get('total_duration', 0)
                categories['brief_visitors'].append(device)
            
            # Check for odd hour activity
            if device.get('first_seen'):
                hour = device['first_seen'].hour
                if 0 <= hour <= 5:
                    categories['odd_hour_visitors'].append(device)
        
        return categories
    
    def _triangulate_locations(self) -> str:
        """Perform multi-tower triangulation for precise location"""
        
        results = []
        results.append("📐 MULTI-TOWER TRIANGULATION ANALYSIS")
        results.append("=" * 80)
        
        triangulated_devices = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                continue
            
            # Group by device and time windows
            for number, device_data in df.groupby('mobile_number'):
                # Create 5-minute time windows
                device_data['time_window'] = pd.to_datetime(device_data['timestamp']).dt.floor('5min')
                
                for time_window, window_data in device_data.groupby('time_window'):
                    towers_in_window = window_data['tower_id'].unique()
                    
                    # Need at least 3 towers for triangulation
                    if len(towers_in_window) >= self.params['triangulation_towers']:
                        triangulated_devices[f"{number}_{time_window}"] = {
                            'number': number,
                            'timestamp': time_window,
                            'towers': list(towers_in_window),
                            'precision': 'high' if len(towers_in_window) >= 4 else 'medium'
                        }
        
        # Report findings
        if triangulated_devices:
            results.append(f"\n📍 TRIANGULATION POSSIBLE: {len(triangulated_devices)} instances")
            
            # Group by device
            device_triangulations = {}
            for key, data in triangulated_devices.items():
                number = data['number']
                if number not in device_triangulations:
                    device_triangulations[number] = []
                device_triangulations[number].append(data)
            
            # Sort by triangulation count
            sorted_devices = sorted(
                device_triangulations.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            
            results.append("\n🎯 DEVICES WITH PRECISE LOCATION DATA:")
            
            for number, triangulations in sorted_devices[:5]:
                results.append(f"\n📱 {number}")
                results.append(f"   Triangulation Points: {len(triangulations)}")
                
                # Show sample triangulation
                sample = triangulations[0]
                results.append(f"   Sample: {sample['timestamp']}")
                results.append(f"   Towers: {', '.join(sample['towers'])}")
                results.append(f"   Precision: {sample['precision']}")
        
        # Identify stationary devices with triangulation
        results.append("\n🏠 STATIONARY DEVICES (Triangulated):")
        
        stationary_triangulated = []
        for number, triangulations in device_triangulations.items():
            if len(triangulations) >= 3:
                # Check if same tower combination
                tower_sets = [set(t['towers']) for t in triangulations]
                if all(ts == tower_sets[0] for ts in tower_sets):
                    stationary_triangulated.append({
                        'number': number,
                        'location_towers': list(tower_sets[0]),
                        'duration': len(triangulations) * 5  # minutes
                    })
        
        if stationary_triangulated:
            for device in sorted(stationary_triangulated, key=lambda x: x['duration'], reverse=True)[:3]:
                results.append(f"   📱 {device['number']}: {device['duration']} minutes")
                results.append(f"      Location: {' + '.join(device['location_towers'])}")
        
        return "\n".join(results)
    
    def _analyze_dwell_patterns(self) -> str:
        """Analyze dwell/loitering patterns in areas"""
        
        results = []
        results.append("⏱️ DWELL TIME & LOITERING ANALYSIS")
        results.append("=" * 80)
        
        dwell_patterns = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                continue
            
            # Analyze dwell time per device per tower
            for (number, tower), group in df.groupby(['mobile_number', 'tower_id']):
                if len(group) < 2:
                    continue
                
                # Calculate dwell time
                first_seen = group['timestamp'].min()
                last_seen = group['timestamp'].max()
                dwell_minutes = (last_seen - first_seen).total_seconds() / 60
                
                # Check for continuous presence (not just first and last)
                time_gaps = group['timestamp'].sort_values().diff()
                max_gap = time_gaps.max().total_seconds() / 60 if len(time_gaps) > 1 else 0
                
                # Consider dwelling if continuous presence
                if dwell_minutes >= self.params['dwell_time_minutes'] and max_gap < 30:
                    if tower not in dwell_patterns:
                        dwell_patterns[tower] = []
                    
                    dwell_patterns[tower].append({
                        'number': number,
                        'dwell_time': dwell_minutes,
                        'start_time': first_seen,
                        'end_time': last_seen,
                        'connection_count': len(group),
                        'continuous': max_gap < 10
                    })
        
        # Report findings
        if dwell_patterns:
            results.append(f"\n📍 TOWERS WITH LOITERING ACTIVITY: {len(dwell_patterns)}")
            
            # Sort towers by loiterer count
            sorted_towers = sorted(
                dwell_patterns.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            
            for tower, loiterers in sorted_towers[:5]:
                results.append(f"\n🏗️ Tower: {tower}")
                results.append(f"   Loiterers Detected: {len(loiterers)}")
                
                # Categorize by dwell time
                short_dwell = [l for l in loiterers if l['dwell_time'] < 30]
                medium_dwell = [l for l in loiterers if 30 <= l['dwell_time'] < 120]
                long_dwell = [l for l in loiterers if l['dwell_time'] >= 120]
                
                if long_dwell:
                    results.append(f"\n   🔴 EXTENDED LOITERING (>2 hours): {len(long_dwell)}")
                    for loiterer in sorted(long_dwell, key=lambda x: x['dwell_time'], reverse=True)[:2]:
                        results.append(f"      📱 {loiterer['number']}: {loiterer['dwell_time']:.0f} minutes")
                        results.append(f"         Time: {loiterer['start_time']} - {loiterer['end_time']}")
                
                if medium_dwell:
                    results.append(f"\n   🟡 MEDIUM DWELL (30-120 min): {len(medium_dwell)}")
        
        # Suspicious dwell patterns
        results.append("\n🚨 SUSPICIOUS DWELL PATTERNS:")
        
        # Late night loitering
        night_loiterers = []
        for tower, loiterers in dwell_patterns.items():
            for loiterer in loiterers:
                if loiterer['start_time'].hour >= 22 or loiterer['start_time'].hour <= 5:
                    night_loiterers.append({**loiterer, 'tower': tower})
        
        if night_loiterers:
            results.append(f"\n  • Night Loitering ({len(night_loiterers)} instances):")
            results.append("    ⚠️ Extended presence during odd hours")
            
            for loiterer in sorted(night_loiterers, key=lambda x: x['dwell_time'], reverse=True)[:3]:
                results.append(f"    📱 {loiterer['number']} at {loiterer['tower']}")
                results.append(f"       {loiterer['dwell_time']:.0f} min from {loiterer['start_time'].strftime('%H:%M')}")
        
        return "\n".join(results)
    
    def _analyze_perimeter_activity(self) -> str:
        """Analyze activity around geofence perimeters"""
        
        results = []
        results.append("🔲 PERIMETER ACTIVITY ANALYSIS")
        results.append("=" * 80)
        
        # For each high-activity tower, analyze surrounding towers
        tower_activity = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if 'tower_id' not in df.columns:
                continue
            
            tower_counts = df['tower_id'].value_counts()
            for tower_id, count in tower_counts.items():
                tower_activity[tower_id] = count
        
        # Identify core zones (high activity towers)
        if tower_activity:
            sorted_towers = sorted(tower_activity.items(), key=lambda x: x[1], reverse=True)
            core_towers = [t[0] for t in sorted_towers[:5]]
            
            results.append(f"\n🎯 ANALYZING PERIMETERS OF {len(core_towers)} CORE ZONES")
            
            # Analyze movement between core and periphery
            perimeter_crossings = []
            
            for dump_id, df in self.tower_dump_data.items():
                if not all(col in df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                    continue
                
                for number, device_data in df.groupby('mobile_number'):
                    sorted_data = device_data.sort_values('timestamp')
                    
                    for i in range(1, len(sorted_data)):
                        prev_tower = sorted_data.iloc[i-1]['tower_id']
                        curr_tower = sorted_data.iloc[i]['tower_id']
                        
                        # Check for core-periphery crossing
                        if prev_tower in core_towers and curr_tower not in core_towers:
                            perimeter_crossings.append({
                                'number': number,
                                'direction': 'exit',
                                'from_tower': prev_tower,
                                'to_tower': curr_tower,
                                'timestamp': sorted_data.iloc[i]['timestamp']
                            })
                        elif prev_tower not in core_towers and curr_tower in core_towers:
                            perimeter_crossings.append({
                                'number': number,
                                'direction': 'entry',
                                'from_tower': prev_tower,
                                'to_tower': curr_tower,
                                'timestamp': sorted_data.iloc[i]['timestamp']
                            })
            
            if perimeter_crossings:
                results.append(f"\n📊 PERIMETER CROSSINGS: {len(perimeter_crossings)}")
                
                # Analyze by direction
                entries = [p for p in perimeter_crossings if p['direction'] == 'entry']
                exits = [p for p in perimeter_crossings if p['direction'] == 'exit']
                
                results.append(f"   Entries: {len(entries)}")
                results.append(f"   Exits: {len(exits)}")
                
                # Find devices with multiple crossings
                crossing_counts = {}
                for crossing in perimeter_crossings:
                    number = crossing['number']
                    crossing_counts[number] = crossing_counts.get(number, 0) + 1
                
                multiple_crossers = [(n, c) for n, c in crossing_counts.items() if c >= 3]
                
                if multiple_crossers:
                    results.append(f"\n   🔄 FREQUENT CROSSERS ({len(multiple_crossers)}):")
                    for number, count in sorted(multiple_crossers, key=lambda x: x[1], reverse=True)[:5]:
                        results.append(f"      📱 {number}: {count} crossings")
        
        return "\n".join(results)
    
    def _create_density_analysis(self) -> str:
        """Create density/heat map analysis"""
        
        results = []
        results.append("🌡️ DENSITY & HEAT MAP ANALYSIS")
        results.append("=" * 80)
        
        # Calculate density metrics for each tower
        tower_metrics = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if 'tower_id' not in df.columns:
                continue
            
            for tower_id, tower_data in df.groupby('tower_id'):
                metrics = {
                    'total_connections': len(tower_data),
                    'unique_devices': tower_data['mobile_number'].nunique() if 'mobile_number' in tower_data.columns else 0,
                    'time_coverage': 0,
                    'peak_hour': None,
                    'peak_density': 0
                }
                
                # Time-based analysis
                if 'timestamp' in tower_data.columns:
                    # Time coverage
                    time_span = (tower_data['timestamp'].max() - tower_data['timestamp'].min()).total_seconds() / 3600
                    metrics['time_coverage'] = time_span
                    
                    # Hourly density
                    tower_data['hour'] = tower_data['timestamp'].dt.hour
                    hourly_counts = tower_data['hour'].value_counts()
                    
                    if not hourly_counts.empty:
                        metrics['peak_hour'] = hourly_counts.idxmax()
                        metrics['peak_density'] = hourly_counts.max()
                
                # Calculate density score
                if metrics['time_coverage'] > 0:
                    metrics['density_score'] = metrics['total_connections'] / metrics['time_coverage']
                else:
                    metrics['density_score'] = 0
                
                tower_metrics[tower_id] = metrics
        
        # Report findings
        if tower_metrics:
            # Sort by density score
            sorted_towers = sorted(
                tower_metrics.items(),
                key=lambda x: x[1]['density_score'],
                reverse=True
            )
            
            results.append("\n🔥 HIGHEST DENSITY ZONES:")
            
            for tower_id, metrics in sorted_towers[:5]:
                results.append(f"\n📍 Tower: {tower_id}")
                results.append(f"   Density Score: {metrics['density_score']:.1f} connections/hour")
                results.append(f"   Total Connections: {metrics['total_connections']}")
                results.append(f"   Unique Devices: {metrics['unique_devices']}")
                
                if metrics['peak_hour'] is not None:
                    results.append(f"   Peak Hour: {metrics['peak_hour']:02d}:00 ({metrics['peak_density']} connections)")
            
            # Time-based heat patterns
            results.append("\n⏰ TEMPORAL HEAT PATTERNS:")
            
            # Aggregate hourly activity across all towers
            hourly_aggregate = {}
            for tower_id, metrics in tower_metrics.items():
                if 'peak_hour' in metrics and metrics['peak_hour'] is not None:
                    hour = metrics['peak_hour']
                    hourly_aggregate[hour] = hourly_aggregate.get(hour, 0) + metrics['peak_density']
            
            if hourly_aggregate:
                peak_hours = sorted(hourly_aggregate.items(), key=lambda x: x[1], reverse=True)[:3]
                
                results.append("\n   System-wide Peak Hours:")
                for hour, activity in peak_hours:
                    results.append(f"   {hour:02d}:00 - {activity} total connections")
            
            # Identify anomalous density patterns
            results.append("\n🚨 DENSITY ANOMALIES:")
            
            # Towers with unusual visitor-to-connection ratios
            anomalous_towers = []
            for tower_id, metrics in tower_metrics.items():
                if metrics['unique_devices'] > 0:
                    repeat_ratio = metrics['total_connections'] / metrics['unique_devices']
                    
                    if repeat_ratio > 10:  # High repeat connections
                        anomalous_towers.append({
                            'tower': tower_id,
                            'ratio': repeat_ratio,
                            'type': 'high_repeat'
                        })
                    elif repeat_ratio < 1.5 and metrics['unique_devices'] > 20:  # Many one-time visitors
                        anomalous_towers.append({
                            'tower': tower_id,
                            'ratio': repeat_ratio,
                            'type': 'transient'
                        })
            
            if anomalous_towers:
                for anomaly in anomalous_towers[:3]:
                    results.append(f"\n   Tower {anomaly['tower']}:")
                    if anomaly['type'] == 'high_repeat':
                        results.append(f"     ⚠️ High repeat ratio ({anomaly['ratio']:.1f}x)")
                        results.append("     May indicate: Residential area or surveillance point")
                    else:
                        results.append(f"     ⚠️ Low repeat ratio ({anomaly['ratio']:.1f}x)")
                        results.append("     May indicate: Transit point or public area")
        
        return "\n".join(results)
    
    def _find_overlapping_coverage_zones(self) -> List[Dict]:
        """Find areas covered by multiple towers (overlapping zones)"""
        
        overlapping_zones = []
        
        # Analyze which devices connect to multiple towers in short time
        multi_tower_connections = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                continue
            
            for number, device_data in df.groupby('mobile_number'):
                # Create 10-minute windows
                device_data['time_window'] = pd.to_datetime(device_data['timestamp']).dt.floor('10min')
                
                for time_window, window_data in device_data.groupby('time_window'):
                    towers = tuple(sorted(window_data['tower_id'].unique()))
                    
                    if len(towers) >= 2:  # Connected to multiple towers
                        if towers not in multi_tower_connections:
                            multi_tower_connections[towers] = set()
                        multi_tower_connections[towers].add(number)
        
        # Convert to list format
        for towers, devices in multi_tower_connections.items():
            if len(devices) >= 3:  # At least 3 devices in overlap
                overlapping_zones.append({
                    'towers': list(towers),
                    'device_count': len(devices),
                    'devices': list(devices)[:5]  # Sample devices
                })
        
        return sorted(overlapping_zones, key=lambda x: x['device_count'], reverse=True)
    
    def _comprehensive_geofence_analysis(self) -> str:
        """Provide comprehensive geofencing analysis"""
        
        results = []
        results.append("📊 COMPREHENSIVE GEOFENCING ANALYSIS")
        results.append("=" * 80)
        
        # Overall statistics
        total_towers = set()
        total_devices = set()
        
        for dump_id, df in self.tower_dump_data.items():
            if 'tower_id' in df.columns:
                total_towers.update(df['tower_id'].unique())
            if 'mobile_number' in df.columns:
                total_devices.update(df['mobile_number'].unique())
        
        results.append(f"\n📊 COVERAGE STATISTICS:")
        results.append(f"   Total Towers: {len(total_towers)}")
        results.append(f"   Total Devices: {len(total_devices)}")
        
        # Run key analyses
        key_findings = []
        
        # Check for high-precision areas
        triangulation_analysis = self._triangulate_locations()
        if "TRIANGULATION POSSIBLE" in triangulation_analysis:
            key_findings.append("📐 Multi-tower triangulation available")
        
        # Check for loitering
        dwell_analysis = self._analyze_dwell_patterns()
        if "EXTENDED LOITERING" in dwell_analysis:
            key_findings.append("⏱️ Extended loitering detected")
        
        # Check density patterns
        density_analysis = self._create_density_analysis()
        if "DENSITY ANOMALIES" in density_analysis:
            key_findings.append("🌡️ Density anomalies identified")
        
        if key_findings:
            results.append("\n🎯 KEY GEOFENCING INSIGHTS:")
            for finding in key_findings:
                results.append(f"   {finding}")
        
        return "\n".join(results)