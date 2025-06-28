"""
Movement Visualizer
Creates visualizations for device movement patterns, trajectories, and paths
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.patches import Circle, FancyArrowPatch
from matplotlib.lines import Line2D
import seaborn as sns
import folium
from folium import plugins
from datetime import datetime, timedelta
from loguru import logger
import json
from geopy.distance import geodesic

class MovementVisualizer:
    """Generate movement and trajectory visualizations"""
    
    def __init__(self):
        """Initialize movement visualizer"""
        self.output_dir = Path("visualizations/movements")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Movement speed thresholds (km/h)
        self.speed_thresholds = {
            'walking': 5,
            'bicycle': 25,
            'vehicle': 200,
            'impossible': 500
        }
        
        # Color schemes for different movement types
        self.movement_colors = {
            'walking': '#00FF00',     # Green
            'bicycle': '#FFFF00',     # Yellow
            'vehicle': '#FFA500',     # Orange
            'impossible': '#FF0000',  # Red
            'stationary': '#0000FF'   # Blue
        }
        
        logger.info("Movement Visualizer initialized")
    
    def visualize_device_trajectory(self, tower_data: pd.DataFrame, 
                                  device_id: str,
                                  output_file: str = "device_trajectory.html") -> str:
        """Visualize a single device's movement trajectory"""
        
        if 'mobile_number' not in tower_data.columns:
            logger.error("Mobile number column missing")
            return "Error: Missing mobile_number column"
        
        # Filter data for specific device
        device_data = tower_data[tower_data['mobile_number'] == device_id].copy()
        
        if len(device_data) == 0:
            return f"No data found for device {device_id}"
        
        # Required columns check
        required_cols = ['timestamp', 'tower_lat', 'tower_long', 'tower_id']
        if not all(col in device_data.columns for col in required_cols):
            return "Error: Missing required columns for trajectory visualization"
        
        # Sort by timestamp
        device_data['timestamp'] = pd.to_datetime(device_data['timestamp'])
        device_data = device_data.sort_values('timestamp')
        
        # Calculate speeds between points
        device_data = self._calculate_movement_speeds(device_data)
        
        # Create map centered on mean location
        center_lat = device_data['tower_lat'].mean()
        center_long = device_data['tower_long'].mean()
        
        m = folium.Map(location=[center_lat, center_long], zoom_start=12)
        
        # Add trajectory lines with color coding based on speed
        for i in range(len(device_data) - 1):
            start = device_data.iloc[i]
            end = device_data.iloc[i + 1]
            
            # Determine movement type
            speed = start.get('speed_kmh', 0)
            movement_type = self._classify_movement_type(speed)
            color = self.movement_colors[movement_type]
            
            # Add line
            folium.PolyLine(
                [[start['tower_lat'], start['tower_long']],
                 [end['tower_lat'], end['tower_long']]],
                color=color,
                weight=4,
                opacity=0.8,
                popup=f"Speed: {speed:.1f} km/h<br>Type: {movement_type}"
            ).add_to(m)
        
        # Add markers for each location
        for idx, row in device_data.iterrows():
            # Determine icon based on dwell time
            if idx < len(device_data) - 1:
                next_row = device_data.iloc[device_data.index.get_loc(idx) + 1]
                dwell_time = (next_row['timestamp'] - row['timestamp']).total_seconds() / 60
            else:
                dwell_time = 0
            
            if dwell_time > 30:
                icon_color = 'red'
                icon = 'pause'
            else:
                icon_color = 'green'
                icon = 'play'
            
            folium.Marker(
                [row['tower_lat'], row['tower_long']],
                popup=f"""
                Tower: {row['tower_id']}<br>
                Time: {row['timestamp']}<br>
                Dwell: {dwell_time:.0f} min<br>
                Speed to next: {row.get('speed_kmh', 0):.1f} km/h
                """,
                tooltip=f"{row['timestamp'].strftime('%H:%M:%S')}",
                icon=folium.Icon(color=icon_color, icon=icon)
            ).add_to(m)
        
        # Add start and end markers
        if len(device_data) > 0:
            # Start marker
            folium.Marker(
                [device_data.iloc[0]['tower_lat'], device_data.iloc[0]['tower_long']],
                popup="Journey Start",
                icon=folium.Icon(color='green', icon='flag', prefix='fa')
            ).add_to(m)
            
            # End marker
            folium.Marker(
                [device_data.iloc[-1]['tower_lat'], device_data.iloc[-1]['tower_long']],
                popup="Journey End",
                icon=folium.Icon(color='red', icon='flag-checkered', prefix='fa')
            ).add_to(m)
        
        # Add legend
        legend_html = '''
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: 120px; 
                    background-color: white; z-index: 1000; font-size: 14px;
                    border:2px solid grey; border-radius: 5px; padding: 10px">
            <p style="margin: 0; font-weight: bold;">Movement Types</p>
            <p style="margin: 5px 0;"><span style="color: #00FF00;">━━</span> Walking (< 5 km/h)</p>
            <p style="margin: 5px 0;"><span style="color: #FFFF00;">━━</span> Bicycle (5-25 km/h)</p>
            <p style="margin: 5px 0;"><span style="color: #FFA500;">━━</span> Vehicle (25-200 km/h)</p>
            <p style="margin: 5px 0;"><span style="color: #FF0000;">━━</span> Impossible (> 500 km/h)</p>
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))
        
        # Save map
        output_path = self.output_dir / output_file
        m.save(str(output_path))
        
        logger.info(f"Device trajectory saved to {output_path}")
        return str(output_path)
    
    def visualize_convoy_movement(self, tower_data: pd.DataFrame,
                                devices: List[str],
                                time_window: int = 300,
                                output_file: str = "convoy_movement.html") -> str:
        """Visualize coordinated movement of multiple devices"""
        
        if not devices:
            return "No devices specified for convoy analysis"
        
        # Filter data for specified devices
        convoy_data = tower_data[tower_data['mobile_number'].isin(devices)].copy()
        
        if len(convoy_data) == 0:
            return "No data found for specified devices"
        
        # Required columns check
        required_cols = ['timestamp', 'tower_lat', 'tower_long', 'tower_id', 'mobile_number']
        if not all(col in convoy_data.columns for col in required_cols):
            return "Error: Missing required columns"
        
        # Sort by timestamp
        convoy_data['timestamp'] = pd.to_datetime(convoy_data['timestamp'])
        convoy_data = convoy_data.sort_values('timestamp')
        
        # Create map
        center_lat = convoy_data['tower_lat'].mean()
        center_long = convoy_data['tower_long'].mean()
        
        m = folium.Map(location=[center_lat, center_long], zoom_start=12)
        
        # Create time windows
        convoy_data['time_window'] = convoy_data['timestamp'].dt.floor(f'{time_window}s')
        
        # Analyze coordination for each time window
        coordinated_windows = []
        
        for window, window_data in convoy_data.groupby('time_window'):
            # Check if multiple devices present
            devices_present = window_data['mobile_number'].unique()
            if len(devices_present) >= 2:
                # Check if at same tower or nearby towers
                towers = window_data['tower_id'].unique()
                if len(towers) <= 2:  # Same or adjacent towers
                    coordinated_windows.append({
                        'time': window,
                        'devices': list(devices_present),
                        'towers': list(towers),
                        'center_lat': window_data['tower_lat'].mean(),
                        'center_long': window_data['tower_long'].mean()
                    })
        
        # Color map for devices
        device_colors = {}
        colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightblue', 'darkgreen']
        for i, device in enumerate(devices[:len(colors)]):
            device_colors[device] = colors[i]
        
        # Plot individual device trajectories
        for device in devices:
            device_data = convoy_data[convoy_data['mobile_number'] == device].sort_values('timestamp')
            
            if len(device_data) > 1:
                # Create path
                path_coords = [[row['tower_lat'], row['tower_long']] for _, row in device_data.iterrows()]
                
                folium.PolyLine(
                    path_coords,
                    color=device_colors.get(device, 'gray'),
                    weight=3,
                    opacity=0.7,
                    popup=f"Device: {device}"
                ).add_to(m)
        
        # Highlight coordination points
        for coord in coordinated_windows:
            # Add circle to show coordination area
            folium.Circle(
                [coord['center_lat'], coord['center_long']],
                radius=1000,  # 1km radius
                color='red',
                fill=True,
                fillColor='red',
                fillOpacity=0.2,
                popup=f"""
                Coordination Point<br>
                Time: {coord['time']}<br>
                Devices: {len(coord['devices'])}<br>
                {', '.join(coord['devices'][:3])}...
                """
            ).add_to(m)
        
        # Add animated time slider if coordination points exist
        if coordinated_windows:
            # Create time-indexed data for animation
            time_data = []
            time_index = []
            
            for coord in coordinated_windows:
                time_index.append(str(coord['time']))
                points = []
                
                # Get positions of all devices at this time
                window_data = convoy_data[convoy_data['time_window'] == coord['time']]
                for device in coord['devices']:
                    device_pos = window_data[window_data['mobile_number'] == device]
                    if len(device_pos) > 0:
                        pos = device_pos.iloc[0]
                        points.append([pos['tower_lat'], pos['tower_long'], 1])
                
                time_data.append(points)
            
            # Add heat map with time
            plugins.HeatMapWithTime(
                time_data,
                index=time_index,
                radius=20,
                gradient={0.4: 'blue', 0.65: 'lime', 0.8: 'orange', 1.0: 'red'},
                auto_play=True,
                display_index=True
            ).add_to(m)
        
        # Add legend
        legend_items = []
        for device, color in device_colors.items():
            legend_items.append(f'<p style="margin: 5px 0;"><span style="color: {color};">━━</span> {device}</p>')
        
        legend_html = f'''
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: auto; 
                    background-color: white; z-index: 1000; font-size: 12px;
                    border:2px solid grey; border-radius: 5px; padding: 10px">
            <p style="margin: 0; font-weight: bold;">Device Tracks</p>
            {"".join(legend_items[:5])}
            <p style="margin: 10px 0 0 0; font-weight: bold;">Coordination</p>
            <p style="margin: 5px 0;"><span style="color: red;">⭕</span> Co-location point</p>
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))
        
        # Save map
        output_path = self.output_dir / output_file
        m.save(str(output_path))
        
        # Generate summary
        summary = f"\nConvoy Analysis Summary:\n"
        summary += f"Devices tracked: {len(devices)}\n"
        summary += f"Coordination points found: {len(coordinated_windows)}\n"
        
        if coordinated_windows:
            summary += f"First coordination: {coordinated_windows[0]['time']}\n"
            summary += f"Last coordination: {coordinated_windows[-1]['time']}\n"
        
        logger.info(f"Convoy movement visualization saved to {output_path}")
        return str(output_path) + summary
    
    def generate_movement_pattern_matrix(self, tower_data: pd.DataFrame,
                                       output_file: str = "movement_patterns.png") -> str:
        """Generate matrix visualization of movement patterns"""
        
        if not all(col in tower_data.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
            return "Error: Missing required columns"
        
        # Calculate movement metrics for each device
        device_metrics = {}
        
        for device, device_data in tower_data.groupby('mobile_number'):
            device_data = device_data.sort_values('timestamp')
            
            if len(device_data) > 1:
                # Calculate speeds if location data available
                if all(col in device_data.columns for col in ['tower_lat', 'tower_long']):
                    device_data = self._calculate_movement_speeds(device_data)
                    
                    metrics = {
                        'total_distance': device_data['distance_km'].sum() if 'distance_km' in device_data.columns else 0,
                        'avg_speed': device_data['speed_kmh'].mean() if 'speed_kmh' in device_data.columns else 0,
                        'max_speed': device_data['speed_kmh'].max() if 'speed_kmh' in device_data.columns else 0,
                        'unique_towers': device_data['tower_id'].nunique(),
                        'stationary_time': len(device_data[device_data.get('speed_kmh', 0) < 1]) / len(device_data) * 100,
                        'vehicle_usage': len(device_data[device_data.get('speed_kmh', 0) > 25]) / len(device_data) * 100
                    }
                else:
                    metrics = {
                        'unique_towers': device_data['tower_id'].nunique(),
                        'total_records': len(device_data),
                        'time_span_hours': (device_data['timestamp'].max() - device_data['timestamp'].min()).total_seconds() / 3600
                    }
                
                device_metrics[device] = metrics
        
        if not device_metrics:
            return "No movement data to analyze"
        
        # Convert to DataFrame
        metrics_df = pd.DataFrame.from_dict(device_metrics, orient='index')
        
        # Create visualization
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Movement Pattern Analysis', fontsize=16)
        
        # 1. Speed distribution
        if 'avg_speed' in metrics_df.columns:
            ax1 = axes[0, 0]
            metrics_df['avg_speed'].hist(bins=20, ax=ax1, color='skyblue', edgecolor='black')
            ax1.axvline(25, color='orange', linestyle='--', label='Vehicle threshold')
            ax1.axvline(200, color='red', linestyle='--', label='High speed threshold')
            ax1.set_xlabel('Average Speed (km/h)')
            ax1.set_ylabel('Number of Devices')
            ax1.set_title('Device Speed Distribution')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
        
        # 2. Movement type classification
        if 'avg_speed' in metrics_df.columns:
            ax2 = axes[0, 1]
            movement_types = []
            for _, row in metrics_df.iterrows():
                movement_types.append(self._classify_movement_type(row['avg_speed']))
            
            type_counts = pd.Series(movement_types).value_counts()
            colors = [self.movement_colors[t] for t in type_counts.index]
            
            type_counts.plot(kind='pie', ax=ax2, colors=colors, autopct='%1.1f%%')
            ax2.set_title('Movement Type Distribution')
            ax2.set_ylabel('')
        
        # 3. Tower coverage vs movement
        if 'unique_towers' in metrics_df.columns:
            ax3 = axes[1, 0]
            
            if 'total_distance' in metrics_df.columns:
                ax3.scatter(metrics_df['unique_towers'], metrics_df['total_distance'], 
                          alpha=0.6, c=metrics_df['avg_speed'], cmap='viridis', s=100)
                ax3.set_ylabel('Total Distance (km)')
                cbar = ax3.collections[0].colorbar
                cbar.set_label('Avg Speed (km/h)')
            else:
                metrics_df['unique_towers'].hist(bins=15, ax=ax3, color='lightgreen', edgecolor='black')
                ax3.set_ylabel('Number of Devices')
            
            ax3.set_xlabel('Unique Towers Visited')
            ax3.set_title('Tower Coverage Analysis')
            ax3.grid(True, alpha=0.3)
        
        # 4. Movement patterns heatmap
        if len(metrics_df.columns) > 3:
            ax4 = axes[1, 1]
            
            # Select numeric columns for correlation
            numeric_cols = metrics_df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 1:
                correlation = metrics_df[numeric_cols].corr()
                
                sns.heatmap(correlation, annot=True, fmt='.2f', cmap='coolwarm', 
                          center=0, square=True, ax=ax4)
                ax4.set_title('Movement Metrics Correlation')
        
        plt.tight_layout()
        
        # Save figure
        output_path = self.output_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Movement pattern matrix saved to {output_path}")
        return str(output_path)
    
    def visualize_entry_exit_patterns(self, tower_data: pd.DataFrame,
                                    area_towers: List[str],
                                    output_file: str = "entry_exit_patterns.png") -> str:
        """Visualize entry and exit patterns for a specific area"""
        
        if not area_towers:
            return "No area towers specified"
        
        results = []
        
        # Track entry/exit events
        entry_exit_events = []
        
        for device, device_data in tower_data.groupby('mobile_number'):
            device_data = device_data.sort_values('timestamp')
            
            # Track whether device is inside or outside area
            inside = False
            last_timestamp = None
            
            for _, row in device_data.iterrows():
                current_inside = row['tower_id'] in area_towers
                
                # Entry event
                if not inside and current_inside:
                    entry_exit_events.append({
                        'device': device,
                        'type': 'entry',
                        'timestamp': row['timestamp'],
                        'tower': row['tower_id'],
                        'hour': pd.to_datetime(row['timestamp']).hour,
                        'day_of_week': pd.to_datetime(row['timestamp']).dayofweek
                    })
                
                # Exit event
                elif inside and not current_inside and last_timestamp:
                    duration = (row['timestamp'] - last_timestamp).total_seconds() / 3600
                    entry_exit_events.append({
                        'device': device,
                        'type': 'exit',
                        'timestamp': row['timestamp'],
                        'tower': row['tower_id'],
                        'hour': pd.to_datetime(row['timestamp']).hour,
                        'day_of_week': pd.to_datetime(row['timestamp']).dayofweek,
                        'duration_hours': duration
                    })
                
                inside = current_inside
                if current_inside:
                    last_timestamp = row['timestamp']
        
        if not entry_exit_events:
            return "No entry/exit events found"
        
        # Convert to DataFrame
        events_df = pd.DataFrame(entry_exit_events)
        
        # Create visualizations
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle(f'Entry/Exit Pattern Analysis - Area Towers: {", ".join(area_towers[:3])}...', fontsize=16)
        
        # 1. Entry/Exit timeline
        ax1 = axes[0, 0]
        entry_events = events_df[events_df['type'] == 'entry']
        exit_events = events_df[events_df['type'] == 'exit']
        
        if len(entry_events) > 0:
            entry_times = pd.to_datetime(entry_events['timestamp'])
            ax1.scatter(entry_times, [1] * len(entry_times), 
                       color='green', label='Entries', s=50, alpha=0.6)
        
        if len(exit_events) > 0:
            exit_times = pd.to_datetime(exit_events['timestamp'])
            ax1.scatter(exit_times, [0] * len(exit_times), 
                       color='red', label='Exits', s=50, alpha=0.6)
        
        ax1.set_yticks([0, 1])
        ax1.set_yticklabels(['Exits', 'Entries'])
        ax1.set_xlabel('Time')
        ax1.set_title('Entry/Exit Timeline')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 2. Hourly distribution
        ax2 = axes[0, 1]
        hourly_entries = entry_events.groupby('hour').size()
        hourly_exits = exit_events.groupby('hour').size()
        
        hours = range(24)
        width = 0.35
        
        ax2.bar([h - width/2 for h in hours], 
               [hourly_entries.get(h, 0) for h in hours],
               width, label='Entries', color='green', alpha=0.7)
        
        ax2.bar([h + width/2 for h in hours],
               [hourly_exits.get(h, 0) for h in hours],
               width, label='Exits', color='red', alpha=0.7)
        
        ax2.set_xlabel('Hour of Day')
        ax2.set_ylabel('Number of Events')
        ax2.set_title('Hourly Entry/Exit Distribution')
        ax2.set_xticks(hours)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Highlight odd hours
        for h in range(0, 6):
            ax2.axvspan(h-0.5, h+0.5, alpha=0.1, color='red')
        
        # 3. Duration analysis
        ax3 = axes[1, 0]
        if 'duration_hours' in exit_events.columns and len(exit_events) > 0:
            durations = exit_events['duration_hours'].dropna()
            if len(durations) > 0:
                durations.hist(bins=20, ax=ax3, color='orange', edgecolor='black')
                ax3.axvline(durations.mean(), color='red', linestyle='--', 
                          label=f'Mean: {durations.mean():.1f}h')
                ax3.set_xlabel('Duration (hours)')
                ax3.set_ylabel('Frequency')
                ax3.set_title('Area Visit Duration Distribution')
                ax3.legend()
                ax3.grid(True, alpha=0.3)
        
        # 4. Device frequency
        ax4 = axes[1, 1]
        device_entry_counts = entry_events['device'].value_counts()
        
        if len(device_entry_counts) > 0:
            # Show top 10 frequent visitors
            top_devices = device_entry_counts.head(10)
            
            ax4.barh(range(len(top_devices)), top_devices.values, color='purple', alpha=0.7)
            ax4.set_yticks(range(len(top_devices)))
            ax4.set_yticklabels([f"Device {i+1}" for i in range(len(top_devices))])
            ax4.set_xlabel('Number of Entries')
            ax4.set_title('Top 10 Frequent Visitors')
            ax4.grid(True, alpha=0.3)
            
            # Annotate with actual counts
            for i, count in enumerate(top_devices.values):
                ax4.text(count + 0.1, i, str(count), va='center')
        
        plt.tight_layout()
        
        # Save figure
        output_path = self.output_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        # Generate summary
        summary = f"\nEntry/Exit Analysis Summary:\n"
        summary += f"Total Entries: {len(entry_events)}\n"
        summary += f"Total Exits: {len(exit_events)}\n"
        summary += f"Unique Devices: {events_df['device'].nunique()}\n"
        
        if 'duration_hours' in exit_events.columns and len(exit_events) > 0:
            durations = exit_events['duration_hours'].dropna()
            if len(durations) > 0:
                summary += f"Average Visit Duration: {durations.mean():.1f} hours\n"
                summary += f"Longest Visit: {durations.max():.1f} hours\n"
        
        logger.info(f"Entry/exit pattern visualization saved to {output_path}")
        return str(output_path) + summary
    
    def _calculate_movement_speeds(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate speed between consecutive tower connections"""
        
        df = df.copy()
        df['distance_km'] = 0.0
        df['time_diff_seconds'] = 0.0
        df['speed_kmh'] = 0.0
        
        for i in range(len(df) - 1):
            current = df.iloc[i]
            next_point = df.iloc[i + 1]
            
            # Calculate distance
            if all(col in df.columns for col in ['tower_lat', 'tower_long']):
                distance = geodesic(
                    (current['tower_lat'], current['tower_long']),
                    (next_point['tower_lat'], next_point['tower_long'])
                ).kilometers
                
                # Calculate time difference
                time_diff = (next_point['timestamp'] - current['timestamp']).total_seconds()
                
                # Calculate speed
                if time_diff > 0:
                    speed = (distance / time_diff) * 3600  # km/h
                else:
                    speed = 0
                
                df.iloc[i, df.columns.get_loc('distance_km')] = distance
                df.iloc[i, df.columns.get_loc('time_diff_seconds')] = time_diff
                df.iloc[i, df.columns.get_loc('speed_kmh')] = speed
        
        return df
    
    def _classify_movement_type(self, speed_kmh: float) -> str:
        """Classify movement based on speed"""
        
        if speed_kmh < 1:
            return 'stationary'
        elif speed_kmh < self.speed_thresholds['walking']:
            return 'walking'
        elif speed_kmh < self.speed_thresholds['bicycle']:
            return 'bicycle'
        elif speed_kmh < self.speed_thresholds['vehicle']:
            return 'vehicle'
        elif speed_kmh > self.speed_thresholds['impossible']:
            return 'impossible'
        else:
            return 'vehicle'
    
    def generate_movement_report(self, analysis_results: Dict[str, Any]) -> str:
        """Generate comprehensive movement analysis report"""
        
        report = []
        report.append("# Movement Analysis Report")
        report.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if 'device_trajectories' in analysis_results:
            report.append("\n## Device Trajectories")
            for device, trajectory in analysis_results['device_trajectories'].items():
                report.append(f"\n### Device: {device}")
                report.append(f"- Total Distance: {trajectory.get('total_distance', 0):.1f} km")
                report.append(f"- Average Speed: {trajectory.get('avg_speed', 0):.1f} km/h")
                report.append(f"- Max Speed: {trajectory.get('max_speed', 0):.1f} km/h")
                report.append(f"- Movement Type: {trajectory.get('primary_movement_type', 'Unknown')}")
        
        if 'convoy_analysis' in analysis_results:
            report.append("\n## Convoy Movement Detection")
            convoy = analysis_results['convoy_analysis']
            report.append(f"- Devices Analyzed: {convoy.get('device_count', 0)}")
            report.append(f"- Coordination Points: {convoy.get('coordination_points', 0)}")
            report.append(f"- Synchronized Movements: {convoy.get('sync_movements', 0)}")
        
        if 'impossible_travel' in analysis_results:
            report.append("\n## Impossible Travel Detection")
            report.append("⚠️ Potential device cloning detected:")
            for device, incidents in analysis_results['impossible_travel'].items():
                report.append(f"\n- Device: {device}")
                for incident in incidents:
                    report.append(f"  - {incident['time']}: {incident['speed']:.0f} km/h between {incident['towers']}")
        
        if 'entry_exit_patterns' in analysis_results:
            report.append("\n## Entry/Exit Analysis")
            patterns = analysis_results['entry_exit_patterns']
            report.append(f"- Total Entries: {patterns.get('total_entries', 0)}")
            report.append(f"- Total Exits: {patterns.get('total_exits', 0)}")
            report.append(f"- Average Duration: {patterns.get('avg_duration', 0):.1f} hours")
            report.append(f"- Peak Entry Hour: {patterns.get('peak_entry_hour', 'N/A')}")
        
        report_content = "\n".join(report)
        
        # Save report
        report_path = self.output_dir / "movement_analysis_report.md"
        report_path.write_text(report_content)
        
        logger.info(f"Movement analysis report saved to {report_path}")
        return str(report_path)