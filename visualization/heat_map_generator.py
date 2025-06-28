"""
Heat Map Generator
Creates heat maps for tower activity, density patterns, and temporal distributions
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import folium
from folium.plugins import HeatMap, HeatMapWithTime
from loguru import logger
import json

class HeatMapGenerator:
    """Generate various heat maps for tower dump analysis"""
    
    def __init__(self):
        """Initialize heat map generator"""
        self.output_dir = Path("visualizations/heat_maps")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Color schemes
        self.color_schemes = {
            'activity': 'YlOrRd',
            'risk': 'RdYlBu_r',
            'temporal': 'viridis',
            'density': 'hot'
        }
        
        logger.info("Heat Map Generator initialized")
    
    def generate_tower_activity_heatmap(self, tower_data: pd.DataFrame, 
                                      output_file: str = "tower_activity_heatmap.html") -> str:
        """Generate heat map of tower activity"""
        
        if not all(col in tower_data.columns for col in ['tower_id', 'tower_lat', 'tower_long']):
            logger.error("Required columns missing for tower activity heatmap")
            return "Error: Missing required columns (tower_id, tower_lat, tower_long)"
        
        # Aggregate activity by tower
        tower_activity = tower_data.groupby(['tower_id', 'tower_lat', 'tower_long']).size().reset_index(name='activity_count')
        
        # Calculate center point
        center_lat = tower_activity['tower_lat'].mean()
        center_long = tower_activity['tower_long'].mean()
        
        # Create base map
        m = folium.Map(location=[center_lat, center_long], zoom_start=12)
        
        # Prepare heat map data
        heat_data = []
        max_activity = tower_activity['activity_count'].max()
        
        for _, row in tower_activity.iterrows():
            # Normalize intensity
            intensity = row['activity_count'] / max_activity
            heat_data.append([row['tower_lat'], row['tower_long'], intensity])
        
        # Add heat map layer
        HeatMap(heat_data, radius=25, blur=15, 
                gradient={0.4: 'blue', 0.65: 'lime', 0.8: 'orange', 1.0: 'red'}).add_to(m)
        
        # Add tower markers with activity info
        for _, tower in tower_activity.iterrows():
            # Color based on activity level
            if tower['activity_count'] > max_activity * 0.8:
                color = 'red'
                icon = 'exclamation-triangle'
            elif tower['activity_count'] > max_activity * 0.5:
                color = 'orange'
                icon = 'warning'
            else:
                color = 'green'
                icon = 'signal'
            
            folium.Marker(
                [tower['tower_lat'], tower['tower_long']],
                popup=f"Tower: {tower['tower_id']}<br>Activity: {tower['activity_count']}",
                tooltip=f"Tower {tower['tower_id']}",
                icon=folium.Icon(color=color, icon=icon)
            ).add_to(m)
        
        # Save map
        output_path = self.output_dir / output_file
        m.save(str(output_path))
        
        logger.info(f"Tower activity heatmap saved to {output_path}")
        return str(output_path)
    
    def generate_temporal_heatmap(self, tower_data: pd.DataFrame,
                                output_file: str = "temporal_heatmap.png") -> str:
        """Generate temporal heat map showing activity patterns over time"""
        
        if 'timestamp' not in tower_data.columns:
            logger.error("Timestamp column missing for temporal heatmap")
            return "Error: Missing timestamp column"
        
        # Extract temporal features
        tower_data['hour'] = pd.to_datetime(tower_data['timestamp']).dt.hour
        tower_data['day_of_week'] = pd.to_datetime(tower_data['timestamp']).dt.dayofweek
        
        # Create hour vs day of week matrix
        temporal_matrix = pd.crosstab(tower_data['hour'], tower_data['day_of_week'])
        
        # Create figure
        plt.figure(figsize=(12, 8))
        
        # Create heatmap
        sns.heatmap(temporal_matrix, 
                   cmap=self.color_schemes['temporal'],
                   annot=True, 
                   fmt='d',
                   cbar_kws={'label': 'Activity Count'},
                   xticklabels=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
                   yticklabels=[f'{h:02d}:00' for h in range(24)])
        
        plt.title('Temporal Activity Heatmap - Hour vs Day of Week', fontsize=16)
        plt.xlabel('Day of Week', fontsize=12)
        plt.ylabel('Hour of Day', fontsize=12)
        
        # Add grid
        plt.grid(True, alpha=0.3, linestyle='--')
        
        # Highlight odd hours
        for h in range(0, 6):
            plt.axhspan(h-0.5, h+0.5, alpha=0.1, color='red', zorder=0)
        
        # Save figure
        output_path = self.output_dir / output_file
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Temporal heatmap saved to {output_path}")
        return str(output_path)
    
    def generate_device_density_heatmap(self, tower_data: pd.DataFrame,
                                      time_window: int = 3600,
                                      output_file: str = "device_density_heatmap.html") -> str:
        """Generate density heat map showing device concentration"""
        
        required_cols = ['timestamp', 'tower_id', 'tower_lat', 'tower_long', 'mobile_number']
        if not all(col in tower_data.columns for col in required_cols):
            logger.error("Required columns missing for density heatmap")
            return "Error: Missing required columns"
        
        # Create time windows
        tower_data['time_window'] = pd.to_datetime(tower_data['timestamp']).dt.floor(f'{time_window}s')
        
        # Calculate density for each tower and time window
        density_data = tower_data.groupby(['time_window', 'tower_id', 'tower_lat', 'tower_long'])\
                                ['mobile_number'].nunique().reset_index(name='device_count')
        
        # Create animated heatmap
        center_lat = density_data['tower_lat'].mean()
        center_long = density_data['tower_long'].mean()
        
        m = folium.Map(location=[center_lat, center_long], zoom_start=12)
        
        # Prepare time-indexed data
        time_index = []
        heat_data_time = []
        
        for time_window in sorted(density_data['time_window'].unique()):
            window_data = density_data[density_data['time_window'] == time_window]
            
            # Normalize by max devices in any window
            max_devices = density_data['device_count'].max()
            
            heat_points = []
            for _, row in window_data.iterrows():
                intensity = row['device_count'] / max_devices if max_devices > 0 else 0
                heat_points.append([row['tower_lat'], row['tower_long'], intensity])
            
            time_index.append(str(time_window))
            heat_data_time.append(heat_points)
        
        # Add animated heatmap
        if heat_data_time:
            HeatMapWithTime(
                heat_data_time,
                index=time_index,
                radius=30,
                blur=20,
                gradient={0.2: 'blue', 0.4: 'cyan', 0.6: 'yellow', 0.8: 'orange', 1.0: 'red'},
                min_opacity=0.1,
                max_opacity=0.8
            ).add_to(m)
        
        # Save map
        output_path = self.output_dir / output_file
        m.save(str(output_path))
        
        logger.info(f"Device density heatmap saved to {output_path}")
        return str(output_path)
    
    def generate_risk_heatmap(self, tower_data: pd.DataFrame,
                            risk_scores: Dict[str, float],
                            output_file: str = "risk_heatmap.png") -> str:
        """Generate risk-based heat map"""
        
        if 'mobile_number' not in tower_data.columns:
            logger.error("Mobile number column missing for risk heatmap")
            return "Error: Missing mobile_number column"
        
        # Add risk scores to data
        tower_data['risk_score'] = tower_data['mobile_number'].map(risk_scores).fillna(0)
        
        # Aggregate risk by tower
        if 'tower_id' in tower_data.columns:
            tower_risk = tower_data.groupby('tower_id').agg({
                'risk_score': ['mean', 'max', 'sum'],
                'mobile_number': 'nunique'
            })
            
            # Flatten column names
            tower_risk.columns = ['avg_risk', 'max_risk', 'total_risk', 'device_count']
            tower_risk = tower_risk.reset_index()
            
            # Create visualization
            fig, axes = plt.subplots(2, 2, figsize=(15, 12))
            fig.suptitle('Tower Risk Analysis Heatmaps', fontsize=16)
            
            # 1. Average risk per tower
            ax1 = axes[0, 0]
            tower_risk_sorted = tower_risk.sort_values('avg_risk', ascending=False).head(20)
            sns.barplot(data=tower_risk_sorted, x='avg_risk', y='tower_id', 
                       palette='RdYlBu_r', ax=ax1)
            ax1.set_title('Average Risk Score by Tower')
            ax1.set_xlabel('Average Risk Score')
            
            # 2. Maximum risk per tower
            ax2 = axes[0, 1]
            tower_risk_sorted = tower_risk.sort_values('max_risk', ascending=False).head(20)
            sns.barplot(data=tower_risk_sorted, x='max_risk', y='tower_id',
                       palette='Reds', ax=ax2)
            ax2.set_title('Maximum Risk Score by Tower')
            ax2.set_xlabel('Maximum Risk Score')
            
            # 3. Risk vs device count scatter
            ax3 = axes[1, 0]
            scatter = ax3.scatter(tower_risk['device_count'], 
                                tower_risk['avg_risk'],
                                c=tower_risk['total_risk'],
                                cmap='hot',
                                s=100,
                                alpha=0.6)
            ax3.set_xlabel('Number of Devices')
            ax3.set_ylabel('Average Risk Score')
            ax3.set_title('Risk vs Device Count')
            plt.colorbar(scatter, ax=ax3, label='Total Risk')
            
            # 4. Temporal risk pattern
            if 'timestamp' in tower_data.columns:
                ax4 = axes[1, 1]
                tower_data['hour'] = pd.to_datetime(tower_data['timestamp']).dt.hour
                hourly_risk = tower_data.groupby('hour')['risk_score'].mean()
                
                ax4.plot(hourly_risk.index, hourly_risk.values, 
                        marker='o', linewidth=2, markersize=8, color='darkred')
                ax4.fill_between(hourly_risk.index, hourly_risk.values, alpha=0.3, color='red')
                ax4.set_xlabel('Hour of Day')
                ax4.set_ylabel('Average Risk Score')
                ax4.set_title('Risk Pattern by Hour')
                ax4.grid(True, alpha=0.3)
                
                # Highlight odd hours
                for h in range(0, 6):
                    ax4.axvspan(h-0.5, h+0.5, alpha=0.1, color='red')
            
            plt.tight_layout()
            
            # Save figure
            output_path = self.output_dir / output_file
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Risk heatmap saved to {output_path}")
            return str(output_path)
        
        return "Error: tower_id column required for risk heatmap"
    
    def generate_correlation_matrix_heatmap(self, tower_data: pd.DataFrame,
                                          output_file: str = "correlation_heatmap.png") -> str:
        """Generate correlation matrix heat map for various metrics"""
        
        # Calculate various metrics per device
        device_metrics = {}
        
        if 'mobile_number' in tower_data.columns:
            for device, device_data in tower_data.groupby('mobile_number'):
                metrics = {
                    'total_activity': len(device_data),
                    'unique_towers': device_data['tower_id'].nunique() if 'tower_id' in device_data.columns else 0,
                    'unique_days': pd.to_datetime(device_data['timestamp']).dt.date.nunique() if 'timestamp' in device_data.columns else 0,
                    'odd_hour_activity': len(device_data[pd.to_datetime(device_data['timestamp']).dt.hour.between(0, 5)]) if 'timestamp' in device_data.columns else 0,
                    'unique_imeis': device_data['imei'].nunique() if 'imei' in device_data.columns else 0
                }
                
                # Calculate percentages
                if metrics['total_activity'] > 0:
                    metrics['odd_hour_percentage'] = metrics['odd_hour_activity'] / metrics['total_activity'] * 100
                    metrics['mobility_score'] = metrics['unique_towers'] / metrics['total_activity'] * 100
                else:
                    metrics['odd_hour_percentage'] = 0
                    metrics['mobility_score'] = 0
                
                device_metrics[device] = metrics
        
        # Convert to DataFrame
        metrics_df = pd.DataFrame.from_dict(device_metrics, orient='index')
        
        # Calculate correlation matrix
        correlation_matrix = metrics_df.corr()
        
        # Create heatmap
        plt.figure(figsize=(10, 8))
        
        # Create mask for upper triangle
        mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))
        
        # Create heatmap
        sns.heatmap(correlation_matrix,
                   mask=mask,
                   annot=True,
                   fmt='.2f',
                   cmap='coolwarm',
                   center=0,
                   square=True,
                   linewidths=1,
                   cbar_kws={"shrink": .8})
        
        plt.title('Device Behavior Metrics Correlation Matrix', fontsize=14)
        plt.tight_layout()
        
        # Save figure
        output_path = self.output_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Correlation matrix heatmap saved to {output_path}")
        return str(output_path)
    
    def generate_communication_heatmap(self, cdr_data: pd.DataFrame,
                                     tower_data: pd.DataFrame,
                                     output_file: str = "communication_heatmap.png") -> str:
        """Generate heat map of communication patterns"""
        
        if not all(col in cdr_data.columns for col in ['a_party', 'b_party']):
            logger.error("Required CDR columns missing")
            return "Error: Missing required CDR columns"
        
        # Create communication matrix
        comm_matrix = pd.crosstab(cdr_data['a_party'], cdr_data['b_party'])
        
        # Filter to show only top communicators
        top_callers = comm_matrix.sum(axis=1).nlargest(20).index
        top_receivers = comm_matrix.sum(axis=0).nlargest(20).index
        top_devices = list(set(top_callers) | set(top_receivers))
        
        comm_matrix_filtered = comm_matrix.loc[
            comm_matrix.index.isin(top_devices),
            comm_matrix.columns.isin(top_devices)
        ]
        
        # Create figure
        plt.figure(figsize=(12, 10))
        
        # Create heatmap
        sns.heatmap(comm_matrix_filtered,
                   cmap='YlOrRd',
                   annot=True,
                   fmt='d',
                   square=True,
                   cbar_kws={'label': 'Number of Calls'})
        
        plt.title('Communication Pattern Heatmap - Top 20 Devices', fontsize=14)
        plt.xlabel('Call Receiver (B Party)', fontsize=12)
        plt.ylabel('Call Maker (A Party)', fontsize=12)
        
        # Rotate labels
        plt.xticks(rotation=45, ha='right')
        plt.yticks(rotation=0)
        
        plt.tight_layout()
        
        # Save figure
        output_path = self.output_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Communication heatmap saved to {output_path}")
        return str(output_path)
    
    def generate_summary_report(self, generated_files: List[str]) -> str:
        """Generate summary report of all heat maps created"""
        
        report = []
        report.append("# Heat Map Generation Report")
        report.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("\n## Generated Visualizations:")
        
        for file_path in generated_files:
            file_name = Path(file_path).name
            report.append(f"- {file_name}: `{file_path}`")
        
        report.append("\n## Visualization Descriptions:")
        report.append("- **Tower Activity Heatmap**: Shows geographic distribution of tower activity")
        report.append("- **Temporal Heatmap**: Displays activity patterns by hour and day of week")
        report.append("- **Device Density Heatmap**: Animated visualization of device concentration over time")
        report.append("- **Risk Heatmap**: Multiple views of risk distribution across towers and time")
        report.append("- **Correlation Matrix**: Shows relationships between different behavioral metrics")
        report.append("- **Communication Heatmap**: Visualizes call patterns between devices")
        
        report_content = "\n".join(report)
        
        # Save report
        report_path = self.output_dir / "heatmap_report.md"
        report_path.write_text(report_content)
        
        logger.info(f"Heat map report saved to {report_path}")
        return str(report_path)