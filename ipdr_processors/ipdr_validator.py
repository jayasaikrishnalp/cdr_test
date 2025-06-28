"""
IPDR Data Validator
Validates IPDR data format, structure, and quality
"""

import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

class IPDRValidator:
    """Validate IPDR data against expected format and quality standards"""
    
    def __init__(self):
        """Initialize validator with IPDR specifications"""
        self.ipdr_columns = settings.ipdr_columns
        self.required_columns = [
            'subscriber_id', 'start_time', 'end_time', 
            'destination_ip', 'destination_port',
            'data_volume_up', 'data_volume_down'
        ]
        self.optional_columns = [
            'imei', 'imsi', 'source_ip', 'source_port',
            'protocol', 'app_protocol', 'session_duration'
        ]
        
    def validate_dataframe(self, df: pd.DataFrame, source_name: str = "Unknown") -> Dict[str, Any]:
        """
        Validate IPDR DataFrame structure and data quality
        
        Args:
            df: IPDR DataFrame to validate
            source_name: Name of the data source (e.g., suspect name)
            
        Returns:
            Validation report dictionary
        """
        report = {
            'source': source_name,
            'timestamp': datetime.now().isoformat(),
            'is_valid': True,
            'total_records': len(df),
            'errors': [],
            'warnings': [],
            'data_quality': {},
            'column_analysis': {},
            'recommendations': []
        }
        
        # Column validation
        self._validate_columns(df, report)
        
        # Data type validation
        self._validate_data_types(df, report)
        
        # Data quality checks
        self._validate_data_quality(df, report)
        
        # Temporal validation
        self._validate_temporal_data(df, report)
        
        # Network data validation
        self._validate_network_data(df, report)
        
        # Data volume validation
        self._validate_data_volumes(df, report)
        
        # Calculate overall validity
        report['is_valid'] = len(report['errors']) == 0
        
        # Generate recommendations
        self._generate_recommendations(report)
        
        return report
    
    def _validate_columns(self, df: pd.DataFrame, report: Dict[str, Any]):
        """Validate column presence and naming"""
        
        # Check required columns
        missing_required = []
        for col in self.required_columns:
            if col not in df.columns:
                missing_required.append(col)
        
        if missing_required:
            report['errors'].append(f"Missing required columns: {missing_required}")
            report['column_analysis']['missing_required'] = missing_required
        
        # Check for extra columns
        expected_columns = set(self.required_columns + self.optional_columns)
        extra_columns = [col for col in df.columns if col not in expected_columns and 
                        col not in ['hour', 'day_of_week', 'is_odd_hour', 'detected_app', 
                                   'app_risk', 'is_encrypted', 'suspect', 'total_data_volume',
                                   'subscriber_id_clean']]
        
        if extra_columns:
            report['warnings'].append(f"Unexpected columns found: {extra_columns}")
            report['column_analysis']['extra_columns'] = extra_columns
        
        # Column completeness
        column_completeness = {}
        for col in df.columns:
            completeness = (df[col].notna().sum() / len(df)) * 100
            column_completeness[col] = round(completeness, 2)
        
        report['column_analysis']['completeness'] = column_completeness
    
    def _validate_data_types(self, df: pd.DataFrame, report: Dict[str, Any]):
        """Validate data types of columns"""
        
        type_errors = []
        
        # Timestamp columns
        timestamp_columns = ['start_time', 'end_time']
        for col in timestamp_columns:
            if col in df.columns:
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    type_errors.append(f"{col} should be datetime type")
        
        # Numeric columns
        numeric_columns = ['destination_port', 'source_port', 'data_volume_up', 
                          'data_volume_down', 'session_duration']
        for col in numeric_columns:
            if col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    type_errors.append(f"{col} should be numeric type")
        
        # String columns
        string_columns = ['subscriber_id', 'imei', 'imsi', 'destination_ip', 
                         'source_ip', 'protocol', 'app_protocol']
        for col in string_columns:
            if col in df.columns:
                if not pd.api.types.is_string_dtype(df[col]) and not pd.api.types.is_object_dtype(df[col]):
                    type_errors.append(f"{col} should be string type")
        
        if type_errors:
            report['errors'].extend(type_errors)
            report['data_quality']['type_errors'] = type_errors
    
    def _validate_data_quality(self, df: pd.DataFrame, report: Dict[str, Any]):
        """Perform data quality checks"""
        
        quality_metrics = {}
        
        # Overall completeness
        total_cells = len(df) * len(df.columns)
        non_null_cells = df.notna().sum().sum()
        quality_metrics['overall_completeness'] = round((non_null_cells / total_cells) * 100, 2)
        
        # Duplicate records
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            report['warnings'].append(f"Found {duplicate_count} duplicate records")
            quality_metrics['duplicate_percentage'] = round((duplicate_count / len(df)) * 100, 2)
        
        # Data consistency checks
        if 'start_time' in df.columns and 'end_time' in df.columns:
            invalid_duration = (df['end_time'] < df['start_time']).sum()
            if invalid_duration > 0:
                report['errors'].append(f"{invalid_duration} records have end_time before start_time")
                quality_metrics['temporal_consistency'] = round(((len(df) - invalid_duration) / len(df)) * 100, 2)
        
        report['data_quality'].update(quality_metrics)
    
    def _validate_temporal_data(self, df: pd.DataFrame, report: Dict[str, Any]):
        """Validate temporal aspects of the data"""
        
        if 'start_time' not in df.columns:
            return
        
        temporal_analysis = {}
        
        # Date range
        valid_times = df[df['start_time'].notna()]
        if len(valid_times) > 0:
            date_range = {
                'earliest': valid_times['start_time'].min(),
                'latest': valid_times['start_time'].max(),
                'span_days': (valid_times['start_time'].max() - valid_times['start_time'].min()).days
            }
            temporal_analysis['date_range'] = date_range
            
            # Check for future dates
            future_dates = (valid_times['start_time'] > datetime.now()).sum()
            if future_dates > 0:
                report['errors'].append(f"{future_dates} records have future timestamps")
            
            # Check for very old dates (>1 year)
            old_dates = (valid_times['start_time'] < pd.Timestamp.now() - pd.Timedelta(days=365)).sum()
            if old_dates > 0:
                report['warnings'].append(f"{old_dates} records are older than 1 year")
        
        report['data_quality']['temporal_analysis'] = temporal_analysis
    
    def _validate_network_data(self, df: pd.DataFrame, report: Dict[str, Any]):
        """Validate network-related data"""
        
        network_analysis = {}
        
        # Port validation
        if 'destination_port' in df.columns:
            valid_ports = df[df['destination_port'].notna()]
            if len(valid_ports) > 0:
                # Check port range (1-65535)
                invalid_ports = ((valid_ports['destination_port'] < 1) | 
                               (valid_ports['destination_port'] > 65535)).sum()
                if invalid_ports > 0:
                    report['errors'].append(f"{invalid_ports} records have invalid port numbers")
                
                # Common ports analysis
                port_distribution = valid_ports['destination_port'].value_counts().head(10)
                network_analysis['top_ports'] = port_distribution.to_dict()
        
        # IP validation (basic check)
        if 'destination_ip' in df.columns:
            valid_ips = df[df['destination_ip'].notna()]
            if len(valid_ips) > 0:
                # Simple IP format check
                ip_pattern = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'
                invalid_ips = (~valid_ips['destination_ip'].astype(str).str.match(ip_pattern)).sum()
                if invalid_ips > 0:
                    report['warnings'].append(f"{invalid_ips} records have invalid IP format")
        
        report['data_quality']['network_analysis'] = network_analysis
    
    def _validate_data_volumes(self, df: pd.DataFrame, report: Dict[str, Any]):
        """Validate data volume fields"""
        
        volume_analysis = {}
        
        for col in ['data_volume_up', 'data_volume_down']:
            if col in df.columns:
                valid_data = df[df[col].notna()]
                if len(valid_data) > 0:
                    # Check for negative values
                    negative_values = (valid_data[col] < 0).sum()
                    if negative_values > 0:
                        report['errors'].append(f"{negative_values} records have negative {col}")
                    
                    # Statistical summary
                    volume_analysis[col] = {
                        'min': valid_data[col].min(),
                        'max': valid_data[col].max(),
                        'mean': round(valid_data[col].mean(), 2),
                        'total_gb': round(valid_data[col].sum() / 1073741824, 2)  # Convert to GB
                    }
        
        report['data_quality']['volume_analysis'] = volume_analysis
    
    def _generate_recommendations(self, report: Dict[str, Any]):
        """Generate recommendations based on validation results"""
        
        recommendations = []
        
        # Based on errors
        if 'Missing required columns' in str(report['errors']):
            recommendations.append("Ensure all required IPDR fields are present in the data export")
        
        if 'datetime type' in str(report['errors']):
            recommendations.append("Convert timestamp fields to proper datetime format")
        
        # Based on data quality
        quality = report.get('data_quality', {})
        if quality.get('overall_completeness', 100) < 80:
            recommendations.append("Improve data completeness - many missing values detected")
        
        if quality.get('duplicate_percentage', 0) > 5:
            recommendations.append("Remove duplicate records to improve analysis accuracy")
        
        # Based on warnings
        if len(report['warnings']) > 3:
            recommendations.append("Review data export process to reduce quality issues")
        
        report['recommendations'] = recommendations
    
    def generate_validation_report(self, validation_results: Dict[str, Dict[str, Any]]) -> str:
        """Generate formatted validation report for multiple suspects"""
        
        lines = []
        lines.append("IPDR DATA VALIDATION REPORT")
        lines.append("=" * 60)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        # Summary
        total_suspects = len(validation_results)
        valid_suspects = sum(1 for v in validation_results.values() if v['is_valid'])
        
        lines.append(f"Total Suspects: {total_suspects}")
        lines.append(f"Valid Data: {valid_suspects}")
        lines.append(f"Data Issues: {total_suspects - valid_suspects}")
        lines.append("")
        
        # Detailed results
        for suspect, result in validation_results.items():
            lines.append(f"\n{'-' * 40}")
            lines.append(f"Suspect: {suspect}")
            lines.append(f"Records: {result['total_records']}")
            lines.append(f"Valid: {'✓' if result['is_valid'] else '✗'}")
            
            if result['errors']:
                lines.append("\nErrors:")
                for error in result['errors']:
                    lines.append(f"  • {error}")
            
            if result['warnings']:
                lines.append("\nWarnings:")
                for warning in result['warnings'][:3]:  # Limit to 3
                    lines.append(f"  • {warning}")
            
            quality = result.get('data_quality', {})
            if quality:
                lines.append(f"\nData Quality:")
                lines.append(f"  • Completeness: {quality.get('overall_completeness', 'N/A')}%")
            
            if result['recommendations']:
                lines.append("\nRecommendations:")
                for rec in result['recommendations'][:2]:  # Limit to 2
                    lines.append(f"  • {rec}")
        
        return "\n".join(lines)