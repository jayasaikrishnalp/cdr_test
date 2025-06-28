"""
Timeline Animator
Creates animated visualizations showing temporal progression of tower dump data
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.patches import Circle, Rectangle
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime, timedelta
from loguru import logger
import json
from collections import defaultdict

class TimelineAnimator:
    """Generate animated timeline visualizations"""
    
    def __init__(self):
        """Initialize timeline animator"""
        self.output_dir = Path("visualizations/animations")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Animation settings
        self.fps = 10
        self.interval = 100  # milliseconds between frames
        
        # Visual settings
        self.colors = {
            'normal': '#1f77b4',
            'suspicious': '#ff7f0e',
            'new_device': '#2ca02c',
            'leaving': '#d62728',
            'crime_window': '#ff0000'
        }
        
        logger.info("Timeline Animator initialized")
    
    def animate_tower_activity(self, tower_data: pd.DataFrame,
                             time_interval: str = '5min',
                             output_file: str = "tower_activity_animation.mp4") -> str:
        """Animate tower activity over time"""
        
        if not all(col in tower_data.columns for col in ['timestamp', 'tower_id', 'mobile_number']):
            logger.error("Required columns missing for animation")
            return "Error: Missing required columns"
        
        # Prepare data
        tower_data['timestamp'] = pd.to_datetime(tower_data['timestamp'])
        tower_data = tower_data.sort_values('timestamp')
        
        # Create time bins
        tower_data['time_bin'] = tower_data['timestamp'].dt.floor(time_interval)
        
        # Group by time bin and tower
        activity_by_time = tower_data.groupby(['time_bin', 'tower_id']).agg({
            'mobile_number': ['count', 'nunique']
        }).reset_index()
        
        activity_by_time.columns = ['time_bin', 'tower_id', 'total_connections', 'unique_devices']
        
        # Get unique time bins and towers
        time_bins = sorted(activity_by_time['time_bin'].unique())
        towers = sorted(activity_by_time['tower_id'].unique())
        
        if len(time_bins) < 2:
            return "Insufficient time points for animation"
        
        # Create figure
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Initialize plots
        tower_indices = {tower: i for i, tower in enumerate(towers)}
        
        # Bar chart for current activity
        bars = ax1.bar(range(len(towers)), [0] * len(towers), color=self.colors['normal'])
        ax1.set_xlabel('Tower ID')
        ax1.set_ylabel('Active Devices')
        ax1.set_title('Tower Activity Over Time')
        ax1.set_xticks(range(len(towers)))
        ax1.set_xticklabels(towers, rotation=45, ha='right')
        
        # Timeline plot
        timeline_data = defaultdict(list)
        line_plots = {}
        
        # Animation function
        def animate(frame):
            current_time = time_bins[frame]
            
            # Update bar chart
            current_data = activity_by_time[activity_by_time['time_bin'] == current_time]
            
            heights = []
            colors = []
            
            for tower in towers:
                tower_data = current_data[current_data['tower_id'] == tower]
                if len(tower_data) > 0:
                    height = tower_data['unique_devices'].values[0]
                    heights.append(height)
                    
                    # Color based on activity level
                    if height > 50:
                        colors.append(self.colors['suspicious'])
                    else:
                        colors.append(self.colors['normal'])
                else:
                    heights.append(0)
                    colors.append(self.colors['normal'])
            
            # Update bars
            for bar, height, color in zip(bars, heights, colors):
                bar.set_height(height)
                bar.set_color(color)
            
            # Update title with timestamp
            ax1.set_title(f'Tower Activity at {current_time.strftime("%Y-%m-%d %H:%M")}')
            
            # Update timeline
            ax2.clear()
            
            # Plot historical data
            for i, tower in enumerate(towers[:5]):  # Show top 5 towers
                tower_history = activity_by_time[
                    (activity_by_time['tower_id'] == tower) & 
                    (activity_by_time['time_bin'] <= current_time)
                ]
                
                if len(tower_history) > 0:
                    ax2.plot(tower_history['time_bin'], 
                           tower_history['unique_devices'],
                           label=f'Tower {tower}',
                           linewidth=2,
                           alpha=0.8)
            
            # Mark current time
            ax2.axvline(current_time, color='red', linestyle='--', alpha=0.5)
            
            ax2.set_xlabel('Time')
            ax2.set_ylabel('Unique Devices')
            ax2.set_title('Activity Timeline')
            ax2.legend(loc='upper left')
            ax2.grid(True, alpha=0.3)
            
            # Format x-axis
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax2.xaxis.set_major_locator(mdates.HourLocator())
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
            
            return bars
        
        # Create animation
        anim = animation.FuncAnimation(
            fig, animate, frames=len(time_bins),
            interval=self.interval, blit=False, repeat=True
        )
        
        # Save animation
        output_path = self.output_dir / output_file
        anim.save(str(output_path), fps=self.fps, writer='ffmpeg')
        plt.close()
        
        logger.info(f"Tower activity animation saved to {output_path}")
        return str(output_path)
    
    def animate_device_appearances(self, tower_data: pd.DataFrame,
                                 focus_time: Optional[datetime] = None,
                                 window_hours: int = 2,
                                 output_file: str = "device_appearances.mp4") -> str:
        """Animate device appearances and disappearances around a specific time"""
        
        if not all(col in tower_data.columns for col in ['timestamp', 'tower_id', 'mobile_number']):
            return "Error: Missing required columns"
        
        # Prepare data
        tower_data['timestamp'] = pd.to_datetime(tower_data['timestamp'])
        
        # Set focus time if not provided
        if focus_time is None:
            focus_time = tower_data['timestamp'].min() + \
                        (tower_data['timestamp'].max() - tower_data['timestamp'].min()) / 2
        else:
            focus_time = pd.to_datetime(focus_time)
        
        # Define time window
        start_time = focus_time - timedelta(hours=window_hours)
        end_time = focus_time + timedelta(hours=window_hours)
        
        # Filter data to time window
        window_data = tower_data[
            (tower_data['timestamp'] >= start_time) & 
            (tower_data['timestamp'] <= end_time)
        ].copy()
        
        if len(window_data) == 0:
            return "No data in specified time window"
        
        # Create time bins (5-minute intervals)
        window_data['time_bin'] = window_data['timestamp'].dt.floor('5min')
        time_bins = sorted(window_data['time_bin'].unique())
        
        # Track device states
        device_states = {}  # device -> {first_seen, last_seen, towers}
        
        for _, row in window_data.iterrows():
            device = row['mobile_number']
            if device not in device_states:
                device_states[device] = {
                    'first_seen': row['timestamp'],
                    'last_seen': row['timestamp'],
                    'towers': set()
                }
            else:
                device_states[device]['last_seen'] = row['timestamp']
            
            device_states[device]['towers'].add(row['tower_id'])
        
        # Categorize devices
        new_devices = []  # First appeared near focus time
        leaving_devices = []  # Last seen near focus time
        transient_devices = []  # Appeared and left within window
        persistent_devices = []  # Present throughout
        
        for device, state in device_states.items():
            first_seen = state['first_seen']
            last_seen = state['last_seen']
            
            # Check if new around focus time
            if abs((first_seen - focus_time).total_seconds()) < 1800:  # 30 min
                new_devices.append(device)
            # Check if leaving around focus time  
            elif abs((last_seen - focus_time).total_seconds()) < 1800:
                leaving_devices.append(device)
            # Check if transient
            elif (last_seen - first_seen).total_seconds() < 3600:  # 1 hour
                transient_devices.append(device)
            else:
                persistent_devices.append(device)
        
        # Create animation
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 8))
        
        # Device count timeline
        device_counts = []
        new_counts = []
        leaving_counts = []
        
        for time_bin in time_bins:
            bin_data = window_data[window_data['time_bin'] == time_bin]
            devices_present = set(bin_data['mobile_number'].unique())
            
            device_counts.append(len(devices_present))
            new_counts.append(len(devices_present.intersection(new_devices)))
            leaving_counts.append(len(devices_present.intersection(leaving_devices)))
        
        # Animation function
        def animate(frame):
            current_time = time_bins[frame]
            
            # Clear axes
            ax1.clear()
            ax2.clear()
            
            # Plot device count timeline
            ax1.plot(time_bins[:frame+1], device_counts[:frame+1], 
                    'b-', linewidth=2, label='Total Devices')
            ax1.fill_between(time_bins[:frame+1], device_counts[:frame+1], 
                           alpha=0.3, color='blue')
            
            # Highlight new and leaving devices
            if frame > 0:
                ax1.plot(time_bins[:frame+1], new_counts[:frame+1], 
                        'g-', linewidth=2, label='New Devices', alpha=0.7)
                ax1.plot(time_bins[:frame+1], leaving_counts[:frame+1], 
                        'r-', linewidth=2, label='Leaving Devices', alpha=0.7)
            
            # Mark focus time
            ax1.axvline(focus_time, color='red', linestyle='--', 
                       label='Focus Time', alpha=0.5)
            
            # Mark current time
            ax1.axvline(current_time, color='black', linestyle='-', 
                       alpha=0.3, linewidth=2)
            
            ax1.set_xlabel('Time')
            ax1.set_ylabel('Number of Devices')
            ax1.set_title(f'Device Presence Timeline - {current_time.strftime("%H:%M")}')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            
            # Device categories pie chart
            current_bin_data = window_data[window_data['time_bin'] == current_time]
            current_devices = set(current_bin_data['mobile_number'].unique())
            
            categories = {
                'New': len(current_devices.intersection(new_devices)),
                'Leaving': len(current_devices.intersection(leaving_devices)),
                'Transient': len(current_devices.intersection(transient_devices)),
                'Persistent': len(current_devices.intersection(persistent_devices))
            }
            
            # Filter out zero values
            categories = {k: v for k, v in categories.items() if v > 0}
            
            if categories:
                colors_map = {
                    'New': self.colors['new_device'],
                    'Leaving': self.colors['leaving'],
                    'Transient': self.colors['suspicious'],
                    'Persistent': self.colors['normal']
                }
                
                wedges, texts, autotexts = ax2.pie(
                    categories.values(), 
                    labels=categories.keys(),
                    colors=[colors_map[k] for k in categories.keys()],
                    autopct='%1.0f%%',
                    startangle=90
                )
                
                ax2.set_title('Device Categories at Current Time')
            
            plt.tight_layout()
        
        # Create animation
        anim = animation.FuncAnimation(
            fig, animate, frames=len(time_bins),
            interval=self.interval, blit=False, repeat=True
        )
        
        # Save animation
        output_path = self.output_dir / output_file
        anim.save(str(output_path), fps=self.fps, writer='ffmpeg')
        plt.close()
        
        # Generate summary
        summary = f"\nDevice Appearance Analysis:\n"
        summary += f"Focus Time: {focus_time}\n"
        summary += f"New Devices (around focus): {len(new_devices)}\n"
        summary += f"Leaving Devices (around focus): {len(leaving_devices)}\n"
        summary += f"Transient Devices: {len(transient_devices)}\n"
        summary += f"Persistent Devices: {len(persistent_devices)}\n"
        
        logger.info(f"Device appearances animation saved to {output_path}")
        return str(output_path) + summary
    
    def create_crime_window_timeline(self, tower_data: pd.DataFrame,
                                   crime_start: datetime,
                                   crime_end: datetime,
                                   output_file: str = "crime_window_timeline.png") -> str:
        """Create static timeline focused on crime window"""
        
        if not all(col in tower_data.columns for col in ['timestamp', 'tower_id', 'mobile_number']):
            return "Error: Missing required columns"
        
        # Prepare data
        tower_data['timestamp'] = pd.to_datetime(tower_data['timestamp'])
        
        # Extend window for context
        window_start = crime_start - timedelta(hours=2)
        window_end = crime_end + timedelta(hours=2)
        
        # Filter data
        window_data = tower_data[
            (tower_data['timestamp'] >= window_start) & 
            (tower_data['timestamp'] <= window_end)
        ].copy()
        
        if len(window_data) == 0:
            return "No data in crime window"
        
        # Create time bins
        window_data['time_bin'] = window_data['timestamp'].dt.floor('5min')
        
        # Analyze device patterns
        device_analysis = {}
        
        for device, device_data in window_data.groupby('mobile_number'):
            first_seen = device_data['timestamp'].min()
            last_seen = device_data['timestamp'].max()
            
            # Determine relationship to crime window
            if first_seen >= crime_start and last_seen <= crime_end:
                category = 'only_during_crime'
            elif first_seen >= crime_start and first_seen <= crime_end:
                category = 'arrived_during_crime'
            elif last_seen >= crime_start and last_seen <= crime_end:
                category = 'left_during_crime'
            elif first_seen < crime_start and last_seen > crime_end:
                category = 'present_throughout'
            else:
                category = 'other'
            
            device_analysis[device] = {
                'category': category,
                'first_seen': first_seen,
                'last_seen': last_seen,
                'towers': device_data['tower_id'].nunique(),
                'connections': len(device_data)
            }
        
        # Create visualization
        fig, axes = plt.subplots(3, 1, figsize=(14, 12))
        
        # 1. Device timeline
        ax1 = axes[0]
        
        # Sort devices by category and first appearance
        sorted_devices = sorted(device_analysis.items(), 
                              key=lambda x: (x[1]['category'], x[1]['first_seen']))
        
        # Plot device timelines
        y_pos = 0
        category_colors = {
            'only_during_crime': self.colors['crime_window'],
            'arrived_during_crime': self.colors['suspicious'],
            'left_during_crime': self.colors['leaving'],
            'present_throughout': self.colors['normal'],
            'other': 'gray'
        }
        
        for device, info in sorted_devices[:50]:  # Show top 50 devices
            color = category_colors[info['category']]
            
            # Plot device presence line
            ax1.plot([info['first_seen'], info['last_seen']], 
                    [y_pos, y_pos], 
                    color=color, linewidth=3, alpha=0.7)
            
            # Add markers
            ax1.scatter(info['first_seen'], y_pos, color=color, s=20, marker='>')
            ax1.scatter(info['last_seen'], y_pos, color=color, s=20, marker='<')
            
            y_pos += 1
        
        # Highlight crime window
        ax1.axvspan(crime_start, crime_end, alpha=0.2, color='red', label='Crime Window')
        
        ax1.set_ylim(-1, y_pos)
        ax1.set_xlim(window_start, window_end)
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Devices')
        ax1.set_title('Device Presence Timeline During Crime Window')
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax1.grid(True, alpha=0.3, axis='x')
        
        # Add legend
        from matplotlib.lines import Line2D
        legend_elements = [
            Line2D([0], [0], color=category_colors['only_during_crime'], lw=3, 
                   label='Only During Crime'),
            Line2D([0], [0], color=category_colors['arrived_during_crime'], lw=3, 
                   label='Arrived During Crime'),
            Line2D([0], [0], color=category_colors['left_during_crime'], lw=3, 
                   label='Left During Crime'),
            Line2D([0], [0], color=category_colors['present_throughout'], lw=3, 
                   label='Present Throughout')
        ]
        ax1.legend(handles=legend_elements, loc='upper right')
        
        # 2. Activity intensity
        ax2 = axes[1]
        
        # Count devices per time bin
        time_bins = pd.date_range(start=window_start, end=window_end, freq='5min')
        device_counts = []
        suspicious_counts = []
        
        for time_bin in time_bins:
            bin_end = time_bin + timedelta(minutes=5)
            active_devices = window_data[
                (window_data['timestamp'] >= time_bin) & 
                (window_data['timestamp'] < bin_end)
            ]['mobile_number'].nunique()
            
            # Count suspicious devices
            suspicious = 0
            for device, info in device_analysis.items():
                if info['category'] in ['only_during_crime', 'arrived_during_crime']:
                    if info['first_seen'] <= bin_end and info['last_seen'] >= time_bin:
                        suspicious += 1
            
            device_counts.append(active_devices)
            suspicious_counts.append(suspicious)
        
        ax2.plot(time_bins, device_counts, 'b-', linewidth=2, label='Total Devices')
        ax2.fill_between(time_bins, device_counts, alpha=0.3, color='blue')
        
        ax2.plot(time_bins, suspicious_counts, 'r-', linewidth=2, 
                label='Suspicious Devices', alpha=0.7)
        
        # Highlight crime window
        ax2.axvspan(crime_start, crime_end, alpha=0.2, color='red')
        
        ax2.set_xlabel('Time')
        ax2.set_ylabel('Number of Devices')
        ax2.set_title('Device Activity Intensity')
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # 3. Category breakdown
        ax3 = axes[2]
        
        category_counts = defaultdict(int)
        for device, info in device_analysis.items():
            category_counts[info['category']] += 1
        
        # Create stacked area chart over time
        category_timeline = defaultdict(lambda: defaultdict(int))
        
        for time_bin in time_bins:
            bin_end = time_bin + timedelta(minutes=5)
            
            for device, info in device_analysis.items():
                if info['first_seen'] <= bin_end and info['last_seen'] >= time_bin:
                    category_timeline[info['category']][time_bin] += 1
        
        # Plot stacked areas
        bottom = np.zeros(len(time_bins))
        
        for category in ['only_during_crime', 'arrived_during_crime', 
                        'left_during_crime', 'present_throughout']:
            if category in category_timeline:
                values = [category_timeline[category].get(t, 0) for t in time_bins]
                ax3.fill_between(time_bins, bottom, bottom + values, 
                               color=category_colors[category],
                               alpha=0.7, label=category.replace('_', ' ').title())
                bottom += values
        
        # Highlight crime window
        ax3.axvspan(crime_start, crime_end, alpha=0.2, color='red')
        
        ax3.set_xlabel('Time')
        ax3.set_ylabel('Number of Devices')
        ax3.set_title('Device Categories Over Time')
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # Save figure
        output_path = self.output_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        # Generate summary
        summary = f"\nCrime Window Analysis:\n"
        summary += f"Crime Period: {crime_start.strftime('%H:%M')} - {crime_end.strftime('%H:%M')}\n"
        summary += f"Total Devices in Extended Window: {len(device_analysis)}\n"
        
        for category, count in category_counts.items():
            summary += f"{category.replace('_', ' ').title()}: {count}\n"
        
        # Identify key suspects
        suspects = []
        for device, info in device_analysis.items():
            if info['category'] in ['only_during_crime', 'arrived_during_crime']:
                suspects.append({
                    'device': device,
                    'category': info['category'],
                    'duration': (info['last_seen'] - info['first_seen']).total_seconds() / 60
                })
        
        if suspects:
            summary += f"\nKey Suspects ({len(suspects)} devices):\n"
            for suspect in sorted(suspects, key=lambda x: x['duration'])[:10]:
                summary += f"- {suspect['device']}: {suspect['category']}, {suspect['duration']:.0f} min\n"
        
        logger.info(f"Crime window timeline saved to {output_path}")
        return str(output_path) + summary
    
    def generate_animation_report(self, generated_files: List[str]) -> str:
        """Generate report of all animations created"""
        
        report = []
        report.append("# Timeline Animation Report")
        report.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("\n## Generated Animations:")
        
        for file_path in generated_files:
            file_name = Path(file_path).name
            report.append(f"- {file_name}: `{file_path}`")
        
        report.append("\n## Animation Descriptions:")
        report.append("- **Tower Activity Animation**: Shows device counts at each tower over time")
        report.append("- **Device Appearances**: Visualizes when devices appear and disappear")
        report.append("- **Crime Window Timeline**: Static timeline focused on crime period")
        
        report.append("\n## Usage Instructions:")
        report.append("1. MP4 files can be played in any video player")
        report.append("2. Animations loop to show patterns over time")
        report.append("3. Pay attention to color changes indicating suspicious activity")
        report.append("4. Use pause/frame-by-frame to analyze specific moments")
        
        report_content = "\n".join(report)
        
        # Save report
        report_path = self.output_dir / "animation_report.md"
        report_path.write_text(report_content)
        
        logger.info(f"Animation report saved to {report_path}")
        return str(report_path)