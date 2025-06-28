"""
Location Analysis Tool for CDR Intelligence
Analyzes location patterns, movement, and geographic anomalies
"""

from typing import Dict, Optional, Any, List, Type
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
import pandas as pd
from geopy.distance import geodesic
from collections import defaultdict
from loguru import logger
import numpy as np

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

class LocationAnalysisInput(BaseModel):
    """Input for location analysis tool"""
    query: str = Field(description="Location analysis query (e.g., 'analyze movement patterns', 'detect impossible travel', 'find location clusters')")
    suspect_name: Optional[str] = Field(default=None, description="Specific suspect to analyze")

class LocationAnalysisTool(BaseTool):
    """Tool for analyzing location patterns in CDR data"""
    
    name: str = "location_analysis"
    description: str = """Analyze location patterns including impossible travel speeds, 
    cell tower clustering, border area activity, movement patterns, and geographic anomalies.
    Examples: 'detect impossible travel', 'analyze location clusters', 'check border area activity'"""
    
    args_schema: Type[BaseModel] = LocationAnalysisInput
    cdr_data: Dict[str, pd.DataFrame] = {}
    
    def _run(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Run location analysis"""
        try:
            if not self.cdr_data:
                return "No CDR data loaded. Please load data first."
            
            analyze_all = "all" in query.lower() or not suspect_name
            results = []
            suspects_to_analyze = self.cdr_data.keys() if analyze_all else [suspect_name]
            
            for suspect in suspects_to_analyze:
                if suspect in self.cdr_data:
                    analysis = self._analyze_location_patterns(
                        suspect, 
                        self.cdr_data[suspect]
                    )
                    results.append(analysis)
            
            if not results:
                return "No suspects found for analysis."
            
            # Sort by risk level
            risk_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
            results.sort(key=lambda x: risk_order.get(x['risk_level'], 3))
            
            return self._format_location_analysis(results, query)
            
        except Exception as e:
            logger.error(f"Location analysis error: {str(e)}")
            return f"Error analyzing location patterns: {str(e)}"
    
    async def _arun(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Async not implemented"""
        raise NotImplementedError("Async execution not supported")
    
    def _analyze_location_patterns(self, suspect: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze location patterns for a suspect"""
        
        analysis = {
            'suspect': suspect,
            'total_records': len(df),
            'impossible_travel': [],
            'location_clusters': [],
            'border_activity': False,
            'movement_pattern': 'NORMAL',
            'risk_level': 'LOW',
            'unique_locations': 0,
            'location_variance': 0.0
        }
        
        # Check for location data
        if 'latitude' not in df.columns or 'longitude' not in df.columns:
            analysis['has_location_data'] = False
            return analysis
        
        # Filter valid locations
        df_loc = df.dropna(subset=['latitude', 'longitude']).copy()
        
        if len(df_loc) < 2:
            analysis['has_location_data'] = False
            return analysis
        
        analysis['has_location_data'] = True
        analysis['location_records'] = len(df_loc)
        
        # 1. Impossible travel detection
        impossible_travels = self._detect_impossible_travel(df_loc)
        if impossible_travels:
            analysis['impossible_travel'] = impossible_travels
            analysis['risk_level'] = 'HIGH'
            analysis['movement_pattern'] = 'SUSPICIOUS'
        
        # 2. Location clustering
        clusters = self._detect_location_clusters(df_loc)
        analysis['location_clusters'] = clusters
        
        # 3. Border area detection
        border_areas = [
            {'name': 'India-Pakistan Border', 'lat': 32.5200, 'lon': 74.5229, 'radius_km': 50},
            {'name': 'India-Bangladesh Border', 'lat': 24.3745, 'lon': 88.6042, 'radius_km': 50},
            {'name': 'India-China Border', 'lat': 34.0479, 'lon': 77.5773, 'radius_km': 50}
        ]
        
        border_activity = self._detect_border_activity(df_loc, border_areas)
        if border_activity:
            analysis['border_activity'] = True
            analysis['border_details'] = border_activity
            if analysis['risk_level'] != 'HIGH':
                analysis['risk_level'] = 'HIGH'
        
        # 4. Movement pattern analysis
        movement_stats = self._analyze_movement_patterns(df_loc)
        analysis.update(movement_stats)
        
        # 5. Location variance (how spread out are the locations)
        if len(df_loc) > 0:
            lat_variance = df_loc['latitude'].var()
            lon_variance = df_loc['longitude'].var()
            analysis['location_variance'] = round(np.sqrt(lat_variance + lon_variance), 4)
            
            # High variance might indicate wide area operations
            if analysis['location_variance'] > 0.5:
                analysis['wide_area_operations'] = True
                if analysis['risk_level'] == 'LOW':
                    analysis['risk_level'] = 'MEDIUM'
        
        return analysis
    
    def _detect_impossible_travel(self, df: pd.DataFrame) -> List[Dict]:
        """Detect physically impossible travel speeds"""
        
        impossible = []
        max_speed_kmh = 200  # Max reasonable travel speed
        
        # Ensure datetime column exists
        if 'datetime' not in df.columns:
            return impossible
        
        df_sorted = df.sort_values('datetime')
        
        for i in range(len(df_sorted) - 1):
            row1 = df_sorted.iloc[i]
            row2 = df_sorted.iloc[i + 1]
            
            # Calculate distance
            coords1 = (row1['latitude'], row1['longitude'])
            coords2 = (row2['latitude'], row2['longitude'])
            
            try:
                distance_km = geodesic(coords1, coords2).kilometers
                time_hours = (row2['datetime'] - row1['datetime']).total_seconds() / 3600
                
                if time_hours > 0 and distance_km > 0.1:  # Ignore very small movements
                    speed_kmh = distance_km / time_hours
                    
                    if speed_kmh > max_speed_kmh:
                        impossible.append({
                            'from_time': row1['datetime'].strftime('%Y-%m-%d %H:%M'),
                            'to_time': row2['datetime'].strftime('%Y-%m-%d %H:%M'),
                            'from_location': f"{round(coords1[0], 4)}, {round(coords1[1], 4)}",
                            'to_location': f"{round(coords2[0], 4)}, {round(coords2[1], 4)}",
                            'distance_km': round(distance_km, 2),
                            'time_hours': round(time_hours, 2),
                            'speed_kmh': round(speed_kmh, 2),
                            'anomaly': 'IMPOSSIBLE_TRAVEL'
                        })
            except Exception as e:
                logger.debug(f"Error calculating distance: {e}")
                continue
        
        return impossible[:5]  # Return top 5
    
    def _detect_location_clusters(self, df: pd.DataFrame) -> List[Dict]:
        """Detect location clustering patterns"""
        
        # Round coordinates to create clusters (about 100m precision)
        location_counts = defaultdict(int)
        location_details = defaultdict(list)
        
        for _, row in df.iterrows():
            # Round to 3 decimal places
            lat_round = round(row['latitude'], 3)
            lon_round = round(row['longitude'], 3)
            location_key = (lat_round, lon_round)
            
            location_counts[location_key] += 1
            if 'datetime' in row:
                location_details[location_key].append(row['datetime'])
        
        clusters = []
        for (lat, lon), count in location_counts.items():
            if count >= 5:  # At least 5 calls from same location
                cluster_info = {
                    'latitude': lat,
                    'longitude': lon,
                    'call_count': count,
                    'percentage': round((count / len(df)) * 100, 2),
                    'type': 'FREQUENT_LOCATION'
                }
                
                # Check if it's a hotspot (>20% of all calls)
                if cluster_info['percentage'] > 20:
                    cluster_info['type'] = 'HOTSPOT'
                
                # Add time pattern if available
                if location_key in location_details:
                    times = location_details[location_key]
                    if len(times) > 0 and hasattr(times[0], 'hour'):
                        hours = [t.hour for t in times if hasattr(t, 'hour')]
                        if hours:
                            # Check if calls are concentrated in specific hours
                            hour_counts = pd.Series(hours).value_counts()
                            if hour_counts.iloc[0] > len(hours) * 0.5:
                                cluster_info['pattern'] = f"Concentrated at {hour_counts.index[0]}:00 hours"
                
                clusters.append(cluster_info)
        
        # Sort by call count
        return sorted(clusters, key=lambda x: x['call_count'], reverse=True)[:10]
    
    def _detect_border_activity(self, df: pd.DataFrame, border_areas: List[Dict]) -> List[Dict]:
        """Detect activity near border areas"""
        
        border_activities = []
        
        for _, row in df.iterrows():
            call_coords = (row['latitude'], row['longitude'])
            
            for border in border_areas:
                border_coords = (border['lat'], border['lon'])
                
                try:
                    distance = geodesic(call_coords, border_coords).kilometers
                    if distance <= border['radius_km']:
                        border_activities.append({
                            'border': border['name'],
                            'distance_km': round(distance, 2),
                            'location': f"{round(row['latitude'], 4)}, {round(row['longitude'], 4)}",
                            'timestamp': row.get('datetime', 'Unknown'),
                            'severity': 'HIGH' if distance < 20 else 'MEDIUM'
                        })
                except:
                    continue
        
        # Remove duplicates and return unique border activities
        unique_borders = {}
        for activity in border_activities:
            border_name = activity['border']
            if border_name not in unique_borders or activity['distance_km'] < unique_borders[border_name]['distance_km']:
                unique_borders[border_name] = activity
        
        return list(unique_borders.values())
    
    def _analyze_movement_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze movement patterns and calculate statistics"""
        
        patterns = {
            'unique_locations': 0,
            'avg_distance_between_calls': 0.0,
            'max_distance_traveled': 0.0,
            'stationary_percentage': 0.0
        }
        
        # Count unique locations
        unique_coords = df[['latitude', 'longitude']].drop_duplicates()
        patterns['unique_locations'] = len(unique_coords)
        
        if 'datetime' not in df.columns or len(df) < 2:
            return patterns
        
        # Calculate distances between consecutive calls
        df_sorted = df.sort_values('datetime')
        distances = []
        
        for i in range(len(df_sorted) - 1):
            coords1 = (df_sorted.iloc[i]['latitude'], df_sorted.iloc[i]['longitude'])
            coords2 = (df_sorted.iloc[i+1]['latitude'], df_sorted.iloc[i+1]['longitude'])
            
            try:
                distance = geodesic(coords1, coords2).kilometers
                distances.append(distance)
            except:
                continue
        
        if distances:
            patterns['avg_distance_between_calls'] = round(np.mean(distances), 2)
            patterns['max_distance_traveled'] = round(max(distances), 2)
            
            # Calculate stationary percentage (calls from same location)
            stationary_calls = sum(1 for d in distances if d < 0.1)  # Less than 100m
            patterns['stationary_percentage'] = round((stationary_calls / len(distances)) * 100, 2)
        
        return patterns
    
    def _format_location_analysis(self, results: List[Dict], query: str) -> str:
        """Format location analysis results"""
        
        output = ["📍 LOCATION ANALYSIS RESULTS"]
        output.append("=" * 50)
        
        # Filter results with location data
        results_with_data = [r for r in results if r.get('has_location_data', True)]
        
        if not results_with_data:
            output.append("\n⚠️ No location data available for analysis")
            return "\n".join(output)
        
        # High risk suspects
        high_risk = [r for r in results_with_data if r['risk_level'] == 'HIGH']
        
        if high_risk:
            output.append("\n🚨 HIGH RISK LOCATION PATTERNS DETECTED")
            output.append("-" * 40)
            
            for result in high_risk:
                output.append(f"\n🔴 {result['suspect']}:")
                
                if result['impossible_travel']:
                    output.append("  ⚠️ IMPOSSIBLE TRAVEL DETECTED")
                    for travel in result['impossible_travel'][:3]:
                        output.append(f"    • {travel['speed_kmh']} km/h speed")
                        output.append(f"      {travel['from_time']} → {travel['to_time']}")
                        output.append(f"      Distance: {travel['distance_km']} km in {travel['time_hours']} hours")
                
                if result.get('border_activity'):
                    output.append("  ⚠️ BORDER AREA ACTIVITY DETECTED")
                    for border in result.get('border_details', []):
                        output.append(f"    • {border['border']}: {border['distance_km']} km from border")
        
        # Location clusters
        output.append("\n📍 LOCATION CLUSTERS")
        output.append("-" * 40)
        
        for result in results_with_data:
            if result['location_clusters']:
                output.append(f"\n{result['suspect']}:")
                for cluster in result['location_clusters'][:3]:
                    emoji = "🔥" if cluster['type'] == 'HOTSPOT' else "📍"
                    output.append(f"  {emoji} {cluster['call_count']} calls ({cluster['percentage']}%) from location")
                    output.append(f"     Coordinates: {cluster['latitude']}, {cluster['longitude']}")
                    if 'pattern' in cluster:
                        output.append(f"     Pattern: {cluster['pattern']}")
        
        # Movement patterns
        output.append("\n🚗 MOVEMENT PATTERNS")
        output.append("-" * 40)
        
        for result in results_with_data[:5]:
            output.append(f"\n{result['suspect']}:")
            output.append(f"  Unique locations: {result['unique_locations']}")
            output.append(f"  Avg distance between calls: {result.get('avg_distance_between_calls', 0)} km")
            output.append(f"  Stationary calls: {result.get('stationary_percentage', 0)}%")
            
            if result.get('wide_area_operations'):
                output.append("  ⚠️ WIDE AREA OPERATIONS DETECTED")
        
        # Summary
        output.append("\n📊 LOCATION RISK SUMMARY")
        output.append("-" * 40)
        
        border_suspects = [r for r in results_with_data if r.get('border_activity')]
        impossible_travel_suspects = [r for r in results_with_data if r.get('impossible_travel')]
        
        if border_suspects:
            output.append(f"🔴 Border area activity: {', '.join([s['suspect'] for s in border_suspects])}")
        if impossible_travel_suspects:
            output.append(f"🔴 Impossible travel: {', '.join([s['suspect'] for s in impossible_travel_suspects])}")
        
        # Recommendations
        if high_risk:
            output.append("\n🎯 INVESTIGATION RECOMMENDATIONS:")
            output.append("1. Verify impossible travel instances - possible device cloning")
            output.append("2. Monitor border area activities closely")
            output.append("3. Investigate location hotspots for safe houses")
            output.append("4. Cross-reference with known criminal locations")
        
        return "\n".join(output)