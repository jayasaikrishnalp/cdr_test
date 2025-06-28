"""
Tower Dump Data Validator
Validates and ensures data quality for tower dump analysis
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from loguru import logger
import re

class TowerDumpValidator:
    """Validates tower dump data for quality and consistency"""
    
    def __init__(self):
        """Initialize validator with validation rules"""
        
        self.validation_rules = {
            'mobile_number': {
                'pattern': r'^\d{10}$',
                'required': True,
                'description': '10-digit mobile number'
            },
            'imei': {
                'pattern': r'^\d{15}$',
                'required': False,
                'description': '15-digit IMEI'
            },
            'imsi': {
                'pattern': r'^\d{15}$',
                'required': False,
                'description': '15-digit IMSI'
            },
            'tower_id': {
                'required': True,
                'description': 'Valid tower identifier'
            },
            'timestamp': {
                'required': True,
                'type': 'datetime',
                'description': 'Valid timestamp'
            },
            'lat': {
                'range': (-90, 90),
                'required': False,
                'type': 'numeric',
                'description': 'Latitude between -90 and 90'
            },
            'long': {
                'range': (-180, 180),
                'required': False,
                'type': 'numeric',
                'description': 'Longitude between -180 and 180'
            }
        }
        
        self.anomaly_thresholds = {
            'max_towers_per_hour': 20,  # Maximum towers a device can connect to in an hour
            'max_speed_kmh': 200,  # Maximum reasonable speed (km/h)
            'min_connection_duration': 1,  # Minimum connection duration in seconds
            'max_connection_duration': 86400,  # Maximum connection duration (24 hours)
            'suspicious_imei_changes': 3,  # Number of IMEI changes to flag as suspicious
            'rapid_switch_threshold': 60  # Seconds between tower switches to consider rapid
        }
        
        logger.info("Tower Dump Validator initialized")
    
    def validate_dataframe(self, df: pd.DataFrame) -> Dict[str, any]:
        """
        Comprehensive validation of tower dump DataFrame
        
        Args:
            df: Tower dump DataFrame
            
        Returns:
            Validation report dictionary
        """
        logger.info(f"Validating tower dump with {len(df)} records")
        
        report = {
            'total_records': len(df),
            'valid_records': 0,
            'issues': [],
            'warnings': [],
            'statistics': {},
            'anomalies': []
        }
        
        # Basic structure validation
        structure_issues = self._validate_structure(df)
        report['issues'].extend(structure_issues)
        
        # Data quality validation
        quality_issues = self._validate_data_quality(df)
        report['issues'].extend(quality_issues)
        
        # Pattern validation
        pattern_issues = self._validate_patterns(df)
        report['warnings'].extend(pattern_issues)
        
        # Anomaly detection
        anomalies = self._detect_anomalies(df)
        report['anomalies'] = anomalies
        
        # Calculate statistics
        report['statistics'] = self._calculate_statistics(df)
        
        # Count valid records
        report['valid_records'] = len(df) - len([i for i in report['issues'] if i['severity'] == 'error'])
        
        logger.info(f"Validation complete: {report['valid_records']}/{report['total_records']} valid records")
        
        return report
    
    def _validate_structure(self, df: pd.DataFrame) -> List[Dict]:
        """Validate DataFrame structure and required columns"""
        issues = []
        
        # Check for required columns
        for column, rules in self.validation_rules.items():
            if rules.get('required', False) and column not in df.columns:
                issues.append({
                    'type': 'missing_column',
                    'column': column,
                    'severity': 'error',
                    'message': f"Required column '{column}' is missing"
                })
        
        # Check for empty DataFrame
        if len(df) == 0:
            issues.append({
                'type': 'empty_data',
                'severity': 'error',
                'message': 'DataFrame is empty'
            })
        
        return issues
    
    def _validate_data_quality(self, df: pd.DataFrame) -> List[Dict]:
        """Validate data quality for each column"""
        issues = []
        
        for column, rules in self.validation_rules.items():
            if column not in df.columns:
                continue
            
            # Check for null values in required columns
            if rules.get('required', False):
                null_count = df[column].isnull().sum()
                if null_count > 0:
                    issues.append({
                        'type': 'null_values',
                        'column': column,
                        'severity': 'warning',
                        'count': null_count,
                        'percentage': round(null_count / len(df) * 100, 2),
                        'message': f'{null_count} null values in required column {column}'
                    })
            
            # Pattern validation
            if 'pattern' in rules:
                pattern = rules['pattern']
                if df[column].dtype == 'object':
                    invalid_mask = ~df[column].astype(str).str.match(pattern, na=False)
                    invalid_count = invalid_mask.sum()
                    
                    if invalid_count > 0:
                        issues.append({
                            'type': 'invalid_pattern',
                            'column': column,
                            'severity': 'warning',
                            'count': invalid_count,
                            'percentage': round(invalid_count / len(df) * 100, 2),
                            'message': f'{invalid_count} values don\'t match pattern {rules["description"]}'
                        })
            
            # Range validation
            if 'range' in rules:
                min_val, max_val = rules['range']
                if pd.api.types.is_numeric_dtype(df[column]):
                    out_of_range = ((df[column] < min_val) | (df[column] > max_val)).sum()
                    
                    if out_of_range > 0:
                        issues.append({
                            'type': 'out_of_range',
                            'column': column,
                            'severity': 'warning',
                            'count': out_of_range,
                            'message': f'{out_of_range} values outside range {rules["range"]}'
                        })
        
        return issues
    
    def _validate_patterns(self, df: pd.DataFrame) -> List[Dict]:
        """Validate data patterns and consistency"""
        warnings = []
        
        # Check for duplicate records
        if 'timestamp' in df.columns and 'mobile_number' in df.columns and 'tower_id' in df.columns:
            duplicates = df.duplicated(subset=['timestamp', 'mobile_number', 'tower_id'], keep=False)
            duplicate_count = duplicates.sum()
            
            if duplicate_count > 0:
                warnings.append({
                    'type': 'duplicate_records',
                    'severity': 'warning',
                    'count': duplicate_count,
                    'message': f'{duplicate_count} duplicate records found'
                })
        
        # Check timestamp ordering
        if 'timestamp' in df.columns:
            if not df['timestamp'].is_monotonic_increasing:
                warnings.append({
                    'type': 'timestamp_order',
                    'severity': 'info',
                    'message': 'Timestamps are not in chronological order'
                })
        
        return warnings
    
    def _detect_anomalies(self, df: pd.DataFrame) -> List[Dict]:
        """Detect anomalous patterns in the data"""
        anomalies = []
        
        # Group by mobile number for device-level analysis
        if 'mobile_number' in df.columns:
            for number, group in df.groupby('mobile_number'):
                # Detect rapid tower switching
                rapid_switches = self._detect_rapid_tower_switching(group)
                if rapid_switches:
                    anomalies.extend(rapid_switches)
                
                # Detect impossible travel
                impossible_travel = self._detect_impossible_travel(group)
                if impossible_travel:
                    anomalies.extend(impossible_travel)
                
                # Detect IMEI changes
                if 'imei' in df.columns:
                    imei_changes = self._detect_imei_changes(group, number)
                    if imei_changes:
                        anomalies.extend(imei_changes)
        
        # Detect suspicious patterns
        suspicious_patterns = self._detect_suspicious_patterns(df)
        anomalies.extend(suspicious_patterns)
        
        return anomalies
    
    def _detect_rapid_tower_switching(self, group: pd.DataFrame) -> List[Dict]:
        """Detect rapid switching between towers"""
        anomalies = []
        
        if 'timestamp' not in group.columns or 'tower_id' not in group.columns:
            return anomalies
        
        # Sort by timestamp
        group = group.sort_values('timestamp')
        
        # Calculate time between consecutive tower changes
        tower_changes = group[group['tower_id'].shift() != group['tower_id']]
        
        if len(tower_changes) > 1:
            time_diffs = tower_changes['timestamp'].diff()
            rapid_switches = time_diffs[time_diffs < timedelta(seconds=self.anomaly_thresholds['rapid_switch_threshold'])]
            
            if len(rapid_switches) > 0:
                anomalies.append({
                    'type': 'rapid_tower_switching',
                    'mobile_number': group['mobile_number'].iloc[0],
                    'count': len(rapid_switches),
                    'severity': 'high',
                    'details': f'Rapid tower switches detected (< {self.anomaly_thresholds["rapid_switch_threshold"]}s)'
                })
        
        return anomalies
    
    def _detect_impossible_travel(self, group: pd.DataFrame) -> List[Dict]:
        """Detect impossible travel speeds between towers"""
        anomalies = []
        
        required_cols = ['timestamp', 'tower_id', 'tower_lat', 'tower_long']
        if not all(col in group.columns for col in required_cols):
            return anomalies
        
        # Sort by timestamp
        group = group.sort_values('timestamp')
        
        # Calculate speed between consecutive different towers
        for i in range(1, len(group)):
            prev_row = group.iloc[i-1]
            curr_row = group.iloc[i]
            
            # Skip if same tower or missing coordinates
            if (prev_row['tower_id'] == curr_row['tower_id'] or 
                pd.isna(prev_row['tower_lat']) or pd.isna(curr_row['tower_lat'])):
                continue
            
            # Calculate distance and time
            distance_km = self._calculate_distance(
                prev_row['tower_lat'], prev_row['tower_long'],
                curr_row['tower_lat'], curr_row['tower_long']
            )
            
            time_diff = (curr_row['timestamp'] - prev_row['timestamp']).total_seconds() / 3600  # hours
            
            if time_diff > 0:
                speed_kmh = distance_km / time_diff
                
                if speed_kmh > self.anomaly_thresholds['max_speed_kmh']:
                    anomalies.append({
                        'type': 'impossible_travel',
                        'mobile_number': group['mobile_number'].iloc[0],
                        'severity': 'high',
                        'speed_kmh': round(speed_kmh, 2),
                        'distance_km': round(distance_km, 2),
                        'time_hours': round(time_diff, 2),
                        'from_tower': prev_row['tower_id'],
                        'to_tower': curr_row['tower_id'],
                        'timestamp': curr_row['timestamp']
                    })
        
        return anomalies
    
    def _detect_imei_changes(self, group: pd.DataFrame, mobile_number: str) -> List[Dict]:
        """Detect suspicious IMEI changes"""
        anomalies = []
        
        unique_imeis = group['imei'].dropna().unique()
        
        if len(unique_imeis) >= self.anomaly_thresholds['suspicious_imei_changes']:
            anomalies.append({
                'type': 'multiple_imei',
                'mobile_number': mobile_number,
                'severity': 'high',
                'imei_count': len(unique_imeis),
                'imeis': list(unique_imeis),
                'details': f'SIM used in {len(unique_imeis)} different devices'
            })
        
        return anomalies
    
    def _detect_suspicious_patterns(self, df: pd.DataFrame) -> List[Dict]:
        """Detect overall suspicious patterns"""
        anomalies = []
        
        # New SIM activation detection
        if 'timestamp' in df.columns and 'mobile_number' in df.columns:
            for number, group in df.groupby('mobile_number'):
                first_seen = group['timestamp'].min()
                activity_span = (group['timestamp'].max() - first_seen).days
                
                # Flag if all activity within 7 days and first seen recently
                if activity_span <= 7 and len(group) > 10:
                    anomalies.append({
                        'type': 'new_sim_high_activity',
                        'mobile_number': number,
                        'severity': 'medium',
                        'first_seen': first_seen,
                        'activity_days': activity_span,
                        'record_count': len(group),
                        'details': 'Recently activated SIM with high activity'
                    })
        
        # One-time visitor detection
        if 'tower_id' in df.columns:
            tower_visits = df.groupby(['mobile_number', 'tower_id']).size()
            one_time_visitors = tower_visits[tower_visits == 1]
            
            for (number, tower), count in one_time_visitors.items():
                # Check if this number appears only once overall
                total_records = len(df[df['mobile_number'] == number])
                if total_records == 1:
                    anomalies.append({
                        'type': 'one_time_visitor',
                        'mobile_number': number,
                        'tower_id': tower,
                        'severity': 'medium',
                        'details': 'Number appeared only once in tower dump'
                    })
        
        return anomalies
    
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
    
    def _calculate_statistics(self, df: pd.DataFrame) -> Dict:
        """Calculate validation statistics"""
        stats = {}
        
        if 'mobile_number' in df.columns:
            stats['unique_numbers'] = df['mobile_number'].nunique()
            stats['avg_records_per_number'] = len(df) / stats['unique_numbers']
        
        if 'tower_id' in df.columns:
            stats['unique_towers'] = df['tower_id'].nunique()
        
        if 'imei' in df.columns:
            stats['unique_imeis'] = df['imei'].dropna().nunique()
            stats['numbers_with_multiple_imeis'] = df.groupby('mobile_number')['imei'].nunique().gt(1).sum()
        
        if 'timestamp' in df.columns:
            stats['time_span_hours'] = (df['timestamp'].max() - df['timestamp'].min()).total_seconds() / 3600
        
        return stats