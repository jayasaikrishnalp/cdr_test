"""
Movement Analysis Tool
Analyzes movement patterns, trajectories, and travel behaviors from tower dump data
"""

from langchain.tools import BaseTool
from typing import Dict, Any, List, Optional, Set, Tuple
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
import numpy as np
from collections import defaultdict

class MovementAnalysisTool(BaseTool):
    """Tool for analyzing movement patterns in tower dump data"""
    
    name: str = "movement_analysis"
    description: str = """Analyze movement patterns and trajectories in tower dump data. Use this tool to:
    - Track movement paths and trajectories
    - Detect fast tower switching (vehicle movement)
    - Identify impossible travel patterns
    - Analyze entry/exit patterns
    - Find movement correlations between suspects
    
    Input examples: "track movement", "detect vehicle movement", "analyze travel patterns", "find impossible travel"
    """
    
    def __init__(self):
        super().__init__()
        
        # Movement thresholds
        self.thresholds = {
            'vehicle_speed_min': 20,      # km/h minimum for vehicle
            'vehicle_speed_max': 200,     # km/h maximum reasonable speed
            'walking_speed_max': 6,       # km/h maximum walking speed
            'impossible_speed': 500,      # km/h impossible speed
            'stationary_radius': 0.5,     # km to consider stationary
            'rapid_movement_minutes': 30, # Minutes for rapid movement
            'convoy_time_window': 300,    # Seconds for convoy detection
            'border_distance': 10         # km from border to flag
        }
    
    def _run(self, query: str) -> str:
        """Execute movement analysis"""
        
        if not self.tower_dump_data:
            return "No tower dump data loaded. Please load tower dump data first."
        
        try:
            query_lower = query.lower()
            
            if "trajectory" in query_lower or "path" in query_lower:
                return self._analyze_trajectories()
            elif "vehicle" in query_lower or "fast" in query_lower:
                return self._detect_vehicle_movement()
            elif "impossible" in query_lower:
                return self._detect_impossible_travel()
            elif "entry" in query_lower or "exit" in query_lower:
                return self._analyze_entry_exit_patterns()
            elif "convoy" in query_lower or "group" in query_lower:
                return self._detect_convoy_movement()
            elif "border" in query_lower:
                return self._analyze_border_movement()
            else:
                return self._comprehensive_movement_analysis()
                
        except Exception as e:
            logger.error(f"Error in movement analysis: {str(e)}")
            return f"Error analyzing movement: {str(e)}"
    
    async def _arun(self, query: str) -> str:
        """Async version"""
        return self._run(query)
    
    def _analyze_trajectories(self) -> str:
        """Analyze movement trajectories"""
        
        results = []
        results.append("🛤️ MOVEMENT TRAJECTORY ANALYSIS")
        results.append("=" * 80)
        
        trajectories = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                continue
            
            # Add location data if available
            has_location = all(col in df.columns for col in ['tower_lat', 'tower_long'])
            
            # Analyze each number's trajectory
            for number, group in df.groupby('mobile_number'):
                # Sort by timestamp
                trajectory = group.sort_values('timestamp')
                
                # Skip if too few points
                if len(trajectory) < 3:
                    continue
                
                # Calculate trajectory metrics
                tower_sequence = list(trajectory['tower_id'])
                unique_towers = len(set(tower_sequence))
                
                # Remove consecutive duplicates to get actual movements
                movements = [tower_sequence[0]]
                for tower in tower_sequence[1:]:
                    if tower != movements[-1]:
                        movements.append(tower)
                
                movement_count = len(movements) - 1
                
                # Calculate distances and speeds if location available
                total_distance = 0
                speeds = []
                
                if has_location:
                    for i in range(1, len(trajectory)):
                        prev = trajectory.iloc[i-1]
                        curr = trajectory.iloc[i]
                        
                        if prev['tower_id'] != curr['tower_id']:
                            distance = self._calculate_distance(
                                prev['tower_lat'], prev['tower_long'],
                                curr['tower_lat'], curr['tower_long']
                            )
                            
                            time_diff = (curr['timestamp'] - prev['timestamp']).total_seconds() / 3600
                            
                            if time_diff > 0:
                                speed = distance / time_diff
                                speeds.append(speed)
                                total_distance += distance
                
                # Time span
                time_span = (trajectory['timestamp'].max() - trajectory['timestamp'].min()).total_seconds() / 3600
                
                trajectories[number] = {
                    'tower_count': unique_towers,
                    'movement_count': movement_count,
                    'total_distance': total_distance,
                    'time_span_hours': time_span,
                    'avg_speed': np.mean(speeds) if speeds else 0,
                    'max_speed': max(speeds) if speeds else 0,
                    'tower_sequence': movements[:10],  # First 10 movements
                    'start_time': trajectory['timestamp'].min(),
                    'end_time': trajectory['timestamp'].max()
                }
        
        # Categorize trajectories
        if trajectories:
            # Long distance travelers
            long_distance = {
                num: data for num, data in trajectories.items()
                if data['total_distance'] > 50  # km
            }
            
            if long_distance:
                results.append(f"\n🚗 LONG DISTANCE TRAVELERS ({len(long_distance)}):")
                
                sorted_travelers = sorted(
                    long_distance.items(),
                    key=lambda x: x[1]['total_distance'],
                    reverse=True
                )
                
                for number, data in sorted_travelers[:5]:
                    results.append(f"\n📱 {number}")
                    results.append(f"   Distance: {data['total_distance']:.1f} km")
                    results.append(f"   Duration: {data['time_span_hours']:.1f} hours")
                    results.append(f"   Avg Speed: {data['avg_speed']:.1f} km/h")
                    results.append(f"   Towers: {data['tower_count']} unique")
                    results.append(f"   Route: {' → '.join(data['tower_sequence'][:5])}...")
            
            # Circular patterns (returning to start)
            circular_patterns = []
            for number, data in trajectories.items():
                if len(data['tower_sequence']) >= 4:
                    # Check if returns to starting area
                    if data['tower_sequence'][0] in data['tower_sequence'][3:]:
                        circular_patterns.append((number, data))
            
            if circular_patterns:
                results.append(f"\n🔄 CIRCULAR MOVEMENT PATTERNS ({len(circular_patterns)}):")
                results.append("   ⚠️ May indicate reconnaissance or patrol behavior")
                
                for number, data in circular_patterns[:3]:
                    results.append(f"   📱 {number}: {data['movement_count']} movements")
        
        return "\n".join(results)
    
    def _detect_vehicle_movement(self) -> str:
        """Detect vehicle-based movement patterns"""
        
        results = []
        results.append("🚗 VEHICLE MOVEMENT DETECTION")
        results.append("=" * 80)
        
        vehicle_movements = {}
        high_speed_events = []
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'timestamp', 'tower_id', 'tower_lat', 'tower_long']):
                continue
            
            # Analyze each number
            for number, group in df.groupby('mobile_number'):
                # Sort by timestamp
                sorted_group = group.sort_values('timestamp')
                
                vehicle_indicators = []
                speeds = []
                
                for i in range(1, len(sorted_group)):
                    prev = sorted_group.iloc[i-1]
                    curr = sorted_group.iloc[i]
                    
                    # Skip if same tower
                    if prev['tower_id'] == curr['tower_id']:
                        continue
                    
                    # Calculate speed
                    distance = self._calculate_distance(
                        prev['tower_lat'], prev['tower_long'],
                        curr['tower_lat'], curr['tower_long']
                    )
                    
                    time_diff = (curr['timestamp'] - prev['timestamp']).total_seconds() / 3600
                    
                    if time_diff > 0 and time_diff < 2:  # Within 2 hours
                        speed = distance / time_diff
                        speeds.append(speed)
                        
                        # Vehicle speed detection
                        if self.thresholds['vehicle_speed_min'] <= speed <= self.thresholds['vehicle_speed_max']:
                            vehicle_indicators.append({
                                'from_tower': prev['tower_id'],
                                'to_tower': curr['tower_id'],
                                'speed': speed,
                                'distance': distance,
                                'timestamp': curr['timestamp'],
                                'duration': time_diff * 60  # minutes
                            })
                        
                        # High speed event
                        if speed > 100:  # km/h
                            high_speed_events.append({
                                'number': number,
                                'speed': speed,
                                'distance': distance,
                                'from_tower': prev['tower_id'],
                                'to_tower': curr['tower_id'],
                                'timestamp': curr['timestamp']
                            })
                
                # Store if significant vehicle movement
                if len(vehicle_indicators) >= 3:
                    vehicle_movements[number] = {
                        'vehicle_events': len(vehicle_indicators),
                        'avg_speed': np.mean([v['speed'] for v in vehicle_indicators]),
                        'max_speed': max([v['speed'] for v in vehicle_indicators]),
                        'total_vehicle_distance': sum([v['distance'] for v in vehicle_indicators]),
                        'indicators': vehicle_indicators[:5]  # Top 5
                    }
        
        # Report findings
        if vehicle_movements:
            results.append(f"\n🚗 VEHICLE USERS DETECTED: {len(vehicle_movements)}")
            
            # Sort by vehicle events
            sorted_vehicles = sorted(
                vehicle_movements.items(),
                key=lambda x: x[1]['vehicle_events'],
                reverse=True
            )
            
            for number, data in sorted_vehicles[:5]:
                results.append(f"\n📱 {number}")
                results.append(f"   Vehicle Movements: {data['vehicle_events']}")
                results.append(f"   Avg Speed: {data['avg_speed']:.1f} km/h")
                results.append(f"   Max Speed: {data['max_speed']:.1f} km/h")
                results.append(f"   Distance Covered: {data['total_vehicle_distance']:.1f} km")
                
                # Show sample movement
                if data['indicators']:
                    sample = data['indicators'][0]
                    results.append(f"   Sample: {sample['from_tower']} → {sample['to_tower']}")
                    results.append(f"           {sample['speed']:.1f} km/h, {sample['distance']:.1f} km")
        
        # High speed events
        if high_speed_events:
            results.append(f"\n⚡ HIGH SPEED EVENTS ({len(high_speed_events)}):")
            results.append("   ⚠️ Speeds over 100 km/h detected")
            
            # Sort by speed
            sorted_events = sorted(high_speed_events, key=lambda x: x['speed'], reverse=True)
            
            for event in sorted_events[:3]:
                results.append(f"\n   📱 {event['number']}")
                results.append(f"      Speed: {event['speed']:.0f} km/h")
                results.append(f"      Route: {event['from_tower']} → {event['to_tower']}")
                results.append(f"      Time: {event['timestamp']}")
        
        # Movement patterns
        results.append("\n🎯 VEHICLE MOVEMENT PATTERNS:")
        
        # Highway corridors (repeated high-speed routes)
        route_counts = defaultdict(int)
        for data in vehicle_movements.values():
            for indicator in data['indicators']:
                route = (indicator['from_tower'], indicator['to_tower'])
                if indicator['speed'] > 60:
                    route_counts[route] += 1
        
        common_routes = [(route, count) for route, count in route_counts.items() if count >= 2]
        
        if common_routes:
            results.append(f"\n  • Common Highway Routes: {len(common_routes)}")
            for route, count in sorted(common_routes, key=lambda x: x[1], reverse=True)[:3]:
                results.append(f"    {route[0]} ↔ {route[1]}: {count} trips")
        
        return "\n".join(results)
    
    def _detect_impossible_travel(self) -> str:
        """Detect impossible travel patterns"""
        
        results = []
        results.append("⚡ IMPOSSIBLE TRAVEL DETECTION")
        results.append("=" * 80)
        
        impossible_events = []
        teleportation_suspects = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'timestamp', 'tower_id', 'tower_lat', 'tower_long']):
                continue
            
            # Analyze each number
            for number, group in df.groupby('mobile_number'):
                # Sort by timestamp
                sorted_group = group.sort_values('timestamp')
                
                impossible_count = 0
                events = []
                
                for i in range(1, len(sorted_group)):
                    prev = sorted_group.iloc[i-1]
                    curr = sorted_group.iloc[i]
                    
                    # Skip if same tower
                    if prev['tower_id'] == curr['tower_id']:
                        continue
                    
                    # Calculate speed
                    distance = self._calculate_distance(
                        prev['tower_lat'], prev['tower_long'],
                        curr['tower_lat'], curr['tower_long']
                    )
                    
                    time_diff = (curr['timestamp'] - prev['timestamp']).total_seconds() / 3600
                    
                    if time_diff > 0:
                        speed = distance / time_diff
                        
                        # Impossible speed
                        if speed > self.thresholds['impossible_speed']:
                            impossible_count += 1
                            events.append({
                                'from_tower': prev['tower_id'],
                                'to_tower': curr['tower_id'],
                                'distance': distance,
                                'time_minutes': time_diff * 60,
                                'speed': speed,
                                'timestamp': curr['timestamp'],
                                'from_time': prev['timestamp'],
                                'to_time': curr['timestamp']
                            })
                            
                            # Global impossible events
                            impossible_events.append({
                                'number': number,
                                'speed': speed,
                                'distance': distance,
                                'time_minutes': time_diff * 60,
                                'from_tower': prev['tower_id'],
                                'to_tower': curr['tower_id'],
                                'timestamp': curr['timestamp']
                            })
                
                # Store if multiple impossible travels
                if impossible_count >= 2:
                    teleportation_suspects[number] = {
                        'impossible_count': impossible_count,
                        'events': events
                    }
        
        # Report findings
        if impossible_events:
            results.append(f"\n🔴 IMPOSSIBLE TRAVEL EVENTS: {len(impossible_events)}")
            results.append("   ⚠️ Speeds exceeding 500 km/h detected")
            
            # Sort by speed
            sorted_events = sorted(impossible_events, key=lambda x: x['speed'], reverse=True)
            
            for event in sorted_events[:5]:
                results.append(f"\n📱 {event['number']}")
                results.append(f"   Speed: {event['speed']:.0f} km/h")
                results.append(f"   Distance: {event['distance']:.1f} km in {event['time_minutes']:.1f} minutes")
                results.append(f"   Route: {event['from_tower']} → {event['to_tower']}")
                results.append(f"   Time: {event['timestamp']}")
        
        if teleportation_suspects:
            results.append(f"\n👥 MULTIPLE IMPOSSIBLE TRAVELS ({len(teleportation_suspects)} suspects):")
            results.append("   🚨 Strong indicator of cloned devices or data errors")
            
            for number, data in list(teleportation_suspects.items())[:3]:
                results.append(f"\n   📱 {number}")
                results.append(f"      Impossible Events: {data['impossible_count']}")
                
                # Show most extreme
                if data['events']:
                    extreme = max(data['events'], key=lambda x: x['speed'])
                    results.append(f"      Worst Case: {extreme['speed']:.0f} km/h")
        
        # Analysis
        results.append("\n📊 IMPOSSIBLE TRAVEL CAUSES:")
        results.append("  1. Device cloning (same IMEI at multiple locations)")
        results.append("  2. SIM cloning or fraud")
        results.append("  3. Data synchronization errors")
        results.append("  4. Tower location database errors")
        results.append("  5. Device time/date manipulation")
        
        return "\n".join(results)
    
    def _analyze_entry_exit_patterns(self) -> str:
        """Analyze entry and exit patterns from areas"""
        
        results = []
        results.append("🚪 ENTRY/EXIT PATTERN ANALYSIS")
        results.append("=" * 80)
        
        # Define zones (this would ideally come from configuration)
        # For now, we'll analyze entry/exit from each tower coverage area
        
        tower_entries = defaultdict(list)
        tower_exits = defaultdict(list)
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                continue
            
            # Analyze each number
            for number, group in df.groupby('mobile_number'):
                # Sort by timestamp
                sorted_group = group.sort_values('timestamp')
                
                # Track tower transitions
                prev_tower = None
                
                for _, record in sorted_group.iterrows():
                    curr_tower = record['tower_id']
                    
                    if prev_tower and prev_tower != curr_tower:
                        # Exit from prev_tower
                        tower_exits[prev_tower].append({
                            'number': number,
                            'to_tower': curr_tower,
                            'timestamp': record['timestamp']
                        })
                        
                        # Entry to curr_tower
                        tower_entries[curr_tower].append({
                            'number': number,
                            'from_tower': prev_tower,
                            'timestamp': record['timestamp']
                        })
                    
                    prev_tower = curr_tower
        
        # Analyze patterns
        high_traffic_towers = []
        
        for tower_id in set(list(tower_entries.keys()) + list(tower_exits.keys())):
            entries = tower_entries.get(tower_id, [])
            exits = tower_exits.get(tower_id, [])
            
            if len(entries) + len(exits) > 50:  # High traffic threshold
                high_traffic_towers.append({
                    'tower_id': tower_id,
                    'total_entries': len(entries),
                    'total_exits': len(exits),
                    'unique_visitors': len(set([e['number'] for e in entries])),
                    'entries': entries,
                    'exits': exits
                })
        
        # Report findings
        if high_traffic_towers:
            results.append(f"\n📍 HIGH TRAFFIC AREAS: {len(high_traffic_towers)}")
            
            # Sort by total traffic
            sorted_towers = sorted(
                high_traffic_towers,
                key=lambda x: x['total_entries'] + x['total_exits'],
                reverse=True
            )
            
            for tower_data in sorted_towers[:5]:
                results.append(f"\n🏗️ Tower: {tower_data['tower_id']}")
                results.append(f"   Entries: {tower_data['total_entries']}")
                results.append(f"   Exits: {tower_data['total_exits']}")
                results.append(f"   Unique Visitors: {tower_data['unique_visitors']}")
                
                # Peak entry times
                if tower_data['entries']:
                    entry_times = pd.DataFrame(tower_data['entries'])
                    entry_times['hour'] = pd.to_datetime(entry_times['timestamp']).dt.hour
                    peak_hours = entry_times['hour'].value_counts().head(3)
                    
                    results.append("   Peak Entry Hours:")
                    for hour, count in peak_hours.items():
                        results.append(f"     {hour:02d}:00 - {count} entries")
        
        # Suspicious entry/exit patterns
        results.append("\n🎯 SUSPICIOUS PATTERNS:")
        
        # Quick in-and-out patterns
        quick_visits = []
        for tower_id, entries in tower_entries.items():
            exits = tower_exits.get(tower_id, [])
            
            for entry in entries:
                # Find corresponding exit
                number_exits = [e for e in exits if e['number'] == entry['number']]
                
                for exit in number_exits:
                    duration = (exit['timestamp'] - entry['timestamp']).total_seconds() / 60
                    
                    if 0 < duration < 10:  # Less than 10 minutes
                        quick_visits.append({
                            'number': entry['number'],
                            'tower': tower_id,
                            'duration_minutes': duration,
                            'entry_time': entry['timestamp'],
                            'from_tower': entry['from_tower'],
                            'to_tower': exit['to_tower']
                        })
        
        if quick_visits:
            results.append(f"\n  • Quick Visits ({len(quick_visits)} detected):")
            results.append("    Brief stops may indicate:")
            results.append("    - Drop-off/pickup activities")
            results.append("    - Surveillance/reconnaissance")
            results.append("    - Dead drop locations")
            
            # Group by tower
            tower_quick_visits = defaultdict(list)
            for visit in quick_visits:
                tower_quick_visits[visit['tower']].append(visit)
            
            # Find towers with multiple quick visits
            suspicious_towers = [
                (tower, visits) for tower, visits in tower_quick_visits.items()
                if len(visits) >= 3
            ]
            
            if suspicious_towers:
                results.append("\n    ⚠️ Towers with multiple quick visits:")
                for tower, visits in sorted(suspicious_towers, key=lambda x: len(x[1]), reverse=True)[:3]:
                    results.append(f"      {tower}: {len(visits)} quick visits")
        
        return "\n".join(results)
    
    def _detect_convoy_movement(self) -> str:
        """Detect coordinated group movement (convoy patterns)"""
        
        results = []
        results.append("🚗🚗 CONVOY MOVEMENT DETECTION")
        results.append("=" * 80)
        
        convoy_patterns = []
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                continue
            
            # Group by tower and time windows
            for tower_id, tower_group in df.groupby('tower_id'):
                # Sort by timestamp
                tower_sorted = tower_group.sort_values('timestamp')
                
                # Sliding window to find groups
                for i in range(len(tower_sorted)):
                    window_start = tower_sorted.iloc[i]['timestamp']
                    window_end = window_start + timedelta(seconds=self.thresholds['convoy_time_window'])
                    
                    # Find all numbers in window
                    window_mask = (
                        (tower_sorted['timestamp'] >= window_start) &
                        (tower_sorted['timestamp'] <= window_end)
                    )
                    window_data = tower_sorted[window_mask]
                    
                    unique_numbers = window_data['mobile_number'].unique()
                    
                    if len(unique_numbers) >= 3:  # Potential convoy
                        # Check if these numbers move together
                        convoy_candidates = list(unique_numbers)
                        
                        # Track their next movements
                        next_movements = {}
                        for number in convoy_candidates:
                            number_data = df[df['mobile_number'] == number].sort_values('timestamp')
                            
                            # Find next tower after current
                            future_data = number_data[number_data['timestamp'] > window_end]
                            if not future_data.empty:
                                next_tower = future_data.iloc[0]['tower_id']
                                next_time = future_data.iloc[0]['timestamp']
                                
                                if next_tower != tower_id:
                                    next_movements[number] = {
                                        'tower': next_tower,
                                        'time': next_time
                                    }
                        
                        # Check if they move to same tower
                        if len(next_movements) >= 2:
                            tower_groups = defaultdict(list)
                            for number, movement in next_movements.items():
                                tower_groups[movement['tower']].append({
                                    'number': number,
                                    'time': movement['time']
                                })
                            
                            # Find groups moving to same tower
                            for next_tower, group_members in tower_groups.items():
                                if len(group_members) >= 2:
                                    # Check time proximity
                                    times = [m['time'] for m in group_members]
                                    time_span = (max(times) - min(times)).total_seconds()
                                    
                                    if time_span <= 600:  # Within 10 minutes
                                        convoy_patterns.append({
                                            'from_tower': tower_id,
                                            'to_tower': next_tower,
                                            'members': [m['number'] for m in group_members],
                                            'start_time': window_start,
                                            'coordination_score': len(group_members) / len(convoy_candidates)
                                        })
        
        # Analyze convoy patterns
        if convoy_patterns:
            # Remove duplicates and merge
            unique_convoys = []
            for pattern in convoy_patterns:
                is_duplicate = False
                
                for existing in unique_convoys:
                    if (existing['from_tower'] == pattern['from_tower'] and
                        existing['to_tower'] == pattern['to_tower'] and
                        len(set(existing['members']) & set(pattern['members'])) > len(pattern['members']) * 0.5):
                        # Merge members
                        existing['members'] = list(set(existing['members'] + pattern['members']))
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    unique_convoys.append(pattern)
            
            results.append(f"\n🚗 CONVOY MOVEMENTS DETECTED: {len(unique_convoys)}")
            
            # Sort by group size
            sorted_convoys = sorted(unique_convoys, key=lambda x: len(x['members']), reverse=True)
            
            for convoy in sorted_convoys[:5]:
                results.append(f"\n📍 Route: {convoy['from_tower']} → {convoy['to_tower']}")
                results.append(f"   Group Size: {len(convoy['members'])} devices")
                results.append(f"   Time: {convoy['start_time']}")
                results.append(f"   Members: {', '.join(convoy['members'][:3])}...")
                results.append(f"   Coordination: {convoy['coordination_score']*100:.0f}%")
            
            # Find recurring convoy members
            member_counts = defaultdict(int)
            for convoy in unique_convoys:
                for member in convoy['members']:
                    member_counts[member] += 1
            
            frequent_convoy_members = [
                (member, count) for member, count in member_counts.items()
                if count >= 2
            ]
            
            if frequent_convoy_members:
                results.append("\n🎯 FREQUENT CONVOY MEMBERS:")
                for member, count in sorted(frequent_convoy_members, key=lambda x: x[1], reverse=True)[:5]:
                    results.append(f"   📱 {member}: {count} convoy movements")
        
        return "\n".join(results)
    
    def _analyze_border_movement(self) -> str:
        """Analyze movement near borders"""
        
        results = []
        results.append("🛂 BORDER AREA MOVEMENT ANALYSIS")
        results.append("=" * 80)
        
        # Note: This is a simplified implementation
        # In production, you would have actual border coordinates
        
        results.append("\n⚠️ Border analysis requires border location data")
        results.append("   Analyzing patterns that may indicate border activity:")
        
        # Analyze edge towers (those with fewer neighbors)
        edge_patterns = []
        
        for dump_id, df in self.tower_dump_data.items():
            if 'tower_id' not in df.columns:
                continue
            
            # Find towers with limited connectivity
            tower_connections = defaultdict(set)
            
            for _, record in df.iterrows():
                if 'mobile_number' in record:
                    number = record['mobile_number']
                    tower = record['tower_id']
                    
                    # Track which towers each number connects to
                    number_towers = df[df['mobile_number'] == number]['tower_id'].unique()
                    
                    for other_tower in number_towers:
                        if other_tower != tower:
                            tower_connections[tower].add(other_tower)
            
            # Edge towers have fewer connections
            avg_connections = np.mean([len(conn) for conn in tower_connections.values()]) if tower_connections else 0
            
            edge_towers = [
                tower for tower, connections in tower_connections.items()
                if len(connections) < avg_connections * 0.5
            ]
            
            if edge_towers:
                results.append(f"\n📍 POTENTIAL BORDER TOWERS: {len(edge_towers)}")
                
                # Analyze activity at edge towers
                for tower in edge_towers[:5]:
                    tower_data = df[df['tower_id'] == tower]
                    unique_numbers = tower_data['mobile_number'].nunique() if 'mobile_number' in tower_data.columns else 0
                    
                    results.append(f"   🏗️ {tower}: {unique_numbers} unique devices")
                    
                    # Check for brief visits (smuggling pattern)
                    if 'timestamp' in tower_data.columns:
                        visit_durations = []
                        for number, group in tower_data.groupby('mobile_number'):
                            if len(group) > 1:
                                duration = (group['timestamp'].max() - group['timestamp'].min()).total_seconds() / 60
                                if duration < 30:  # Brief visit
                                    edge_patterns.append({
                                        'tower': tower,
                                        'number': number,
                                        'duration': duration
                                    })
        
        if edge_patterns:
            results.append(f"\n🚨 SUSPICIOUS EDGE TOWER ACTIVITY:")
            results.append(f"   Brief visits to edge towers: {len(edge_patterns)}")
            results.append("   ⚠️ May indicate:")
            results.append("      - Cross-border smuggling")
            results.append("      - Illegal border crossings")
            results.append("      - Surveillance activities")
        
        return "\n".join(results)
    
    def _comprehensive_movement_analysis(self) -> str:
        """Provide comprehensive movement analysis"""
        
        results = []
        results.append("📊 COMPREHENSIVE MOVEMENT ANALYSIS")
        results.append("=" * 80)
        
        # Gather overall statistics
        total_movements = 0
        unique_movers = set()
        
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' in df.columns and 'tower_id' in df.columns:
                # Count tower changes
                for number, group in df.groupby('mobile_number'):
                    tower_changes = (group['tower_id'] != group['tower_id'].shift()).sum() - 1
                    if tower_changes > 0:
                        total_movements += tower_changes
                        unique_movers.add(number)
        
        results.append(f"\n📊 MOVEMENT STATISTICS:")
        results.append(f"   Total Movements: {total_movements}")
        results.append(f"   Unique Movers: {len(unique_movers)}")
        
        # Run key analyses and extract summaries
        analyses_summaries = []
        
        # Vehicle movement
        vehicle_analysis = self._detect_vehicle_movement()
        if "VEHICLE USERS DETECTED" in vehicle_analysis:
            count = int(vehicle_analysis.split("VEHICLE USERS DETECTED: ")[1].split("\n")[0])
            analyses_summaries.append(f"🚗 Vehicle Users: {count}")
        
        # Impossible travel
        impossible_analysis = self._detect_impossible_travel()
        if "IMPOSSIBLE TRAVEL EVENTS:" in impossible_analysis:
            count = int(impossible_analysis.split("IMPOSSIBLE TRAVEL EVENTS: ")[1].split("\n")[0])
            analyses_summaries.append(f"⚡ Impossible Travels: {count}")
        
        # Convoy patterns
        convoy_analysis = self._detect_convoy_movement()
        if "CONVOY MOVEMENTS DETECTED:" in convoy_analysis:
            count = int(convoy_analysis.split("CONVOY MOVEMENTS DETECTED: ")[1].split("\n")[0])
            analyses_summaries.append(f"🚗🚗 Convoy Movements: {count}")
        
        if analyses_summaries:
            results.append("\n🎯 KEY FINDINGS:")
            for summary in analyses_summaries:
                results.append(f"   {summary}")
        
        return "\n".join(results)
    
    def _calculate_distance(self, lat1: float, lon1: float, 
                          lat2: float, lon2: float) -> float:
        """Calculate distance between coordinates using Haversine formula"""
        R = 6371  # Earth's radius in kilometers
        
        lat1_rad = np.radians(lat1)
        lat2_rad = np.radians(lat2)
        delta_lat = np.radians(lat2 - lat1)
        delta_lon = np.radians(lon2 - lon1)
        
        a = np.sin(delta_lat/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(delta_lon/2)**2
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
        
        return R * c