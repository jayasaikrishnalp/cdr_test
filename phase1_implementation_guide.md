# Phase 1 Implementation Guide - Quick Wins

## Overview
This guide provides detailed implementation instructions for Phase 1 patterns that can be completed in 1-2 weeks.

## 1. Circular Communication Loop Detection

### Implementation in `tools/communication_analysis.py`

```python
def _detect_circular_loops(self, df: pd.DataFrame, max_loop_size: int = 5) -> List[Dict]:
    """Detect circular communication patterns (A→B→C→A)"""
    loops_found = []
    
    # Build communication graph
    comm_graph = {}
    for _, row in df.iterrows():
        caller = row['a_party_clean']
        receiver = row['b_party_clean']
        
        if caller not in comm_graph:
            comm_graph[caller] = set()
        comm_graph[caller].add(receiver)
    
    # DFS to find loops
    def find_loops(start, current, path, visited):
        if len(path) > max_loop_size:
            return
        
        if current in comm_graph:
            for next_node in comm_graph[current]:
                if next_node == start and len(path) >= 3:
                    # Loop found
                    loops_found.append({
                        'loop': path + [next_node],
                        'size': len(path)
                    })
                elif next_node not in visited:
                    visited.add(next_node)
                    find_loops(start, next_node, path + [next_node], visited)
                    visited.remove(next_node)
    
    # Find all loops
    for node in comm_graph:
        find_loops(node, node, [node], set([node]))
    
    return loops_found
```

## 2. One-Ring Pattern Detection

### Add to `tools/communication_analysis.py`

```python
def _detect_one_ring_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
    """Detect one-ring patterns (missed calls as signals)"""
    
    # Filter calls with very short duration
    one_ring_threshold = 3  # seconds
    
    short_calls = df[
        (df['call_type'].isin(['CALL-IN', 'CALL-OUT'])) &
        (df['duration_seconds'] <= one_ring_threshold)
    ]
    
    analysis = {
        'total_one_rings': len(short_calls),
        'percentage': round((len(short_calls) / len(df)) * 100, 2),
        'frequent_one_ring_contacts': [],
        'one_ring_patterns': []
    }
    
    if len(short_calls) > 0:
        # Find contacts with multiple one-rings
        one_ring_counts = short_calls['b_party_clean'].value_counts()
        
        for contact, count in one_ring_counts.head(5).items():
            if count >= 3:  # At least 3 one-rings to same number
                analysis['frequent_one_ring_contacts'].append({
                    'contact': contact[-4:],
                    'count': count,
                    'pattern': 'SIGNALING'
                })
        
        # Detect sequential one-ring patterns
        short_calls_sorted = short_calls.sort_values('datetime')
        
        for i in range(len(short_calls_sorted) - 2):
            time_diff = (short_calls_sorted.iloc[i+1]['datetime'] - 
                        short_calls_sorted.iloc[i]['datetime']).total_seconds()
            
            if time_diff < 300:  # Within 5 minutes
                analysis['one_ring_patterns'].append({
                    'type': 'RAPID_SIGNALING',
                    'timestamp': short_calls_sorted.iloc[i]['datetime'],
                    'count': 2
                })
    
    return analysis
```

## 3. Silent Period Detection

### Add to `tools/temporal_analysis.py`

```python
def _detect_silent_periods(self, df: pd.DataFrame) -> List[Dict]:
    """Detect unusual communication gaps"""
    
    silent_periods = []
    
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
                'start': current_time,
                'end': next_time,
                'duration_hours': round(gap_hours, 1),
                'severity': 'HIGH' if gap_hours > 72 else 'MEDIUM'
            })
    
    return silent_periods
```

## 4. Synchronized Calling Detection

### Add to `tools/network_analysis.py`

```python
def _detect_synchronized_calling(self, cdr_data: Dict[str, pd.DataFrame]) -> List[Dict]:
    """Detect suspects active at the same time"""
    
    sync_patterns = []
    time_window = 300  # 5 minutes
    
    # Get all calls with timestamps
    all_calls = []
    for suspect, df in cdr_data.items():
        for _, row in df.iterrows():
            all_calls.append({
                'suspect': suspect,
                'timestamp': row['datetime'],
                'type': row['call_type']
            })
    
    # Sort by timestamp
    all_calls.sort(key=lambda x: x['timestamp'])
    
    # Find synchronized activities
    for i in range(len(all_calls) - 1):
        current = all_calls[i]
        
        # Check next calls within time window
        j = i + 1
        synchronized = [current['suspect']]
        
        while j < len(all_calls):
            next_call = all_calls[j]
            time_diff = (next_call['timestamp'] - current['timestamp']).total_seconds()
            
            if time_diff <= time_window:
                if next_call['suspect'] not in synchronized:
                    synchronized.append(next_call['suspect'])
            else:
                break
            j += 1
        
        if len(synchronized) >= 2:
            sync_patterns.append({
                'timestamp': current['timestamp'],
                'suspects': synchronized,
                'pattern': 'COORDINATED_ACTIVITY'
            })
    
    return sync_patterns
```

## 5. Location Analysis Tool Creation

### Create new file: `tools/location_analysis.py`

```python
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

class LocationAnalysisInput(BaseModel):
    """Input for location analysis tool"""
    query: str = Field(description="Location analysis query")
    suspect_name: Optional[str] = Field(default=None)

class LocationAnalysisTool(BaseTool):
    """Tool for analyzing location patterns in CDR data"""
    
    name: str = "location_analysis"
    description: str = """Analyze location patterns including impossible travel, 
    cell tower clustering, border area activity, and movement patterns."""
    
    args_schema: Type[BaseModel] = LocationAnalysisInput
    cdr_data: Dict[str, pd.DataFrame] = {}
    
    def _run(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Run location analysis"""
        try:
            if not self.cdr_data:
                return "No CDR data loaded."
            
            results = []
            suspects = self.cdr_data.keys() if not suspect_name else [suspect_name]
            
            for suspect in suspects:
                if suspect in self.cdr_data:
                    analysis = self._analyze_location_patterns(
                        suspect, 
                        self.cdr_data[suspect]
                    )
                    results.append(analysis)
            
            return self._format_location_analysis(results)
            
        except Exception as e:
            logger.error(f"Location analysis error: {str(e)}")
            return f"Error: {str(e)}"
    
    def _analyze_location_patterns(self, suspect: str, df: pd.DataFrame) -> Dict:
        """Analyze location patterns for a suspect"""
        
        analysis = {
            'suspect': suspect,
            'impossible_travel': [],
            'location_clusters': [],
            'border_activity': False,
            'movement_pattern': 'NORMAL',
            'risk_level': 'LOW'
        }
        
        # Check for location data
        if 'latitude' not in df.columns or 'longitude' not in df.columns:
            return analysis
        
        # Filter valid locations
        df_loc = df.dropna(subset=['latitude', 'longitude'])
        
        if len(df_loc) < 2:
            return analysis
        
        # 1. Impossible travel detection
        impossible_travels = self._detect_impossible_travel(df_loc)
        if impossible_travels:
            analysis['impossible_travel'] = impossible_travels
            analysis['risk_level'] = 'HIGH'
        
        # 2. Location clustering
        clusters = self._detect_location_clusters(df_loc)
        analysis['location_clusters'] = clusters
        
        # 3. Border area detection (example coordinates)
        border_coords = [(28.6139, 77.2090)]  # Example: Delhi
        if self._detect_border_activity(df_loc, border_coords):
            analysis['border_activity'] = True
            analysis['risk_level'] = 'HIGH'
        
        return analysis
    
    def _detect_impossible_travel(self, df: pd.DataFrame) -> List[Dict]:
        """Detect physically impossible travel speeds"""
        
        impossible = []
        max_speed_kmh = 200  # Max reasonable travel speed
        
        df_sorted = df.sort_values('datetime')
        
        for i in range(len(df_sorted) - 1):
            row1 = df_sorted.iloc[i]
            row2 = df_sorted.iloc[i + 1]
            
            # Calculate distance
            coords1 = (row1['latitude'], row1['longitude'])
            coords2 = (row2['latitude'], row2['longitude'])
            
            try:
                distance_km = geodesic(coords1, coords2).kilometers
                time_hours = (row2['datetime'] - row1['datetime']).seconds / 3600
                
                if time_hours > 0:
                    speed_kmh = distance_km / time_hours
                    
                    if speed_kmh > max_speed_kmh:
                        impossible.append({
                            'from_time': row1['datetime'],
                            'to_time': row2['datetime'],
                            'distance_km': round(distance_km, 2),
                            'speed_kmh': round(speed_kmh, 2),
                            'anomaly': 'IMPOSSIBLE_TRAVEL'
                        })
            except:
                continue
        
        return impossible
    
    def _detect_location_clusters(self, df: pd.DataFrame) -> List[Dict]:
        """Detect location clustering patterns"""
        
        # Simple clustering based on rounded coordinates
        location_counts = defaultdict(int)
        
        for _, row in df.iterrows():
            # Round to 3 decimal places (about 100m precision)
            lat_round = round(row['latitude'], 3)
            lon_round = round(row['longitude'], 3)
            location_counts[(lat_round, lon_round)] += 1
        
        clusters = []
        for (lat, lon), count in location_counts.items():
            if count >= 5:  # At least 5 calls from same location
                clusters.append({
                    'latitude': lat,
                    'longitude': lon,
                    'call_count': count,
                    'type': 'FREQUENT_LOCATION'
                })
        
        return sorted(clusters, key=lambda x: x['call_count'], reverse=True)
    
    def _detect_border_activity(self, df: pd.DataFrame, 
                               border_coords: List[tuple], 
                               threshold_km: float = 50) -> bool:
        """Detect activity near border areas"""
        
        for _, row in df.iterrows():
            call_coords = (row['latitude'], row['longitude'])
            
            for border_coord in border_coords:
                try:
                    distance = geodesic(call_coords, border_coord).kilometers
                    if distance <= threshold_km:
                        return True
                except:
                    continue
        
        return False
    
    def _format_location_analysis(self, results: List[Dict]) -> str:
        """Format location analysis results"""
        
        output = ["📍 LOCATION ANALYSIS RESULTS"]
        output.append("=" * 50)
        
        high_risk = [r for r in results if r['risk_level'] == 'HIGH']
        
        if high_risk:
            output.append("\n🚨 HIGH RISK LOCATION PATTERNS DETECTED")
            
            for result in high_risk:
                output.append(f"\n{result['suspect']}:")
                
                if result['impossible_travel']:
                    output.append("  ⚠️ IMPOSSIBLE TRAVEL DETECTED")
                    for travel in result['impossible_travel'][:3]:
                        output.append(
                            f"    - {travel['speed_kmh']} km/h between calls"
                        )
                
                if result['border_activity']:
                    output.append("  ⚠️ BORDER AREA ACTIVITY DETECTED")
        
        # Location clusters
        output.append("\n📍 LOCATION CLUSTERS:")
        for result in results:
            if result['location_clusters']:
                output.append(f"\n{result['suspect']}:")
                for cluster in result['location_clusters'][:3]:
                    output.append(
                        f"  - {cluster['call_count']} calls from same location"
                    )
        
        return "\n".join(output)
    
    async def _arun(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Async not implemented"""
        raise NotImplementedError("Async execution not supported")
```

## 6. Integration Steps

### Update `agent/cdr_agent.py`:

```python
# Add import
from tools.location_analysis import LocationAnalysisTool

# In _initialize_tools method, add:
location_tool = LocationAnalysisTool()
self.tools.append(location_tool)
```

### Update `requirements.txt`:

```txt
geopy>=2.4.0
holidays>=0.35
```

## Testing the New Features

### Test Script:

```python
# test_phase1.py
from agent.cdr_agent import CDRIntelligenceAgent

# Initialize agent
agent = CDRIntelligenceAgent()

# Load data
agent.load_cdr_data()

# Test new features
queries = [
    "Detect circular communication loops",
    "Find one-ring patterns and signaling behavior",
    "Analyze silent periods and communication gaps",
    "Check for synchronized calling between suspects",
    "Analyze location patterns and impossible travel"
]

for query in queries:
    print(f"\n{'='*60}")
    print(f"Query: {query}")
    print('='*60)
    result = agent.analyze(query)
    print(result)
```

## Expected Outputs

### 1. Circular Loop Detection:
```
🔄 CIRCULAR COMMUNICATION DETECTED
- Loop: Ali → Peer basha → Danial → Ali
- Pattern: CLOSED_NETWORK
- Risk: HIGH - Indicates organized structure
```

### 2. One-Ring Patterns:
```
📱 ONE-RING SIGNALING DETECTED
- Suspect: Ali
- One-ring calls: 15 (3.2% of total)
- Frequent signals to: ***7823 (7 times)
- Pattern: CODED_COMMUNICATION
```

### 3. Silent Periods:
```
🔇 SUSPICIOUS SILENT PERIODS
- Danial: 72-hour gap (May 15-18)
- Pattern: POST_INCIDENT_SILENCE
- Severity: HIGH
```

### 4. Synchronized Calling:
```
🔄 SYNCHRONIZED ACTIVITY DETECTED
- Time: 2024-05-20 02:15:00
- Active suspects: Ali, Peer basha, Danial
- Pattern: COORDINATED_OPERATION
```

### 5. Location Analysis:
```
📍 LOCATION ANOMALIES
- Ali: Impossible travel 250 km/h
- Peer basha: 45 calls from single location
- Danial: Border area activity detected
```

## Validation Checklist

- [ ] All new functions have error handling
- [ ] Results are properly formatted
- [ ] Risk levels are appropriately assigned
- [ ] Integration with existing tools works
- [ ] Performance is acceptable (<1s per analysis)
- [ ] No false positives in test data

This completes the Phase 1 implementation guide with concrete code examples ready for implementation.