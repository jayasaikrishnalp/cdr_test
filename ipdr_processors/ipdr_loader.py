"""
IPDR Data Loader
Handles loading and preprocessing of IPDR (Internet Protocol Detail Record) data
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
from loguru import logger
import numpy as np
from datetime import datetime

import sys
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

class IPDRLoader:
    """Load and preprocess IPDR data from various formats"""
    
    def __init__(self, data_path: Optional[Path] = None):
        """Initialize IPDR loader with data path"""
        self.data_path = data_path or settings.ipdr_data_path
        self.ipdr_columns = settings.ipdr_columns
        self.app_signatures = settings.app_signatures
        
    def load_ipdrs(self, file_list: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        Load IPDR files from the data directory
        
        Args:
            file_list: Optional list of specific files to load
            
        Returns:
            Dictionary mapping suspect names to their IPDR DataFrames
        """
        ipdr_data = {}
        
        # Handle file list input
        if file_list:
            # Process comma-separated list if provided as single string
            if len(file_list) == 1 and ',' in file_list[0]:
                file_list = [f.strip() for f in file_list[0].split(',')]
            
            # Normalize file names
            normalized_files = []
            for file in file_list:
                if not file.endswith(('.xlsx', '.csv')):
                    # Try both extensions
                    if (self.data_path / f"{file}.xlsx").exists():
                        normalized_files.append(f"{file}.xlsx")
                    elif (self.data_path / f"{file}.csv").exists():
                        normalized_files.append(f"{file}.csv")
                    else:
                        logger.warning(f"File not found: {file}")
                else:
                    normalized_files.append(file)
            file_list = normalized_files
        
        # Get all IPDR files if no specific list provided
        if not file_list:
            xlsx_files = list(self.data_path.glob("*.xlsx"))
            csv_files = list(self.data_path.glob("*.csv"))
            all_files = xlsx_files + csv_files
            file_list = [f.name for f in all_files]
        
        logger.info(f"Loading {len(file_list)} IPDR files from {self.data_path}")
        
        for file_name in file_list:
            file_path = self.data_path / file_name
            
            if not file_path.exists():
                logger.warning(f"IPDR file not found: {file_path}")
                continue
            
            try:
                # Load based on file extension
                if file_name.endswith('.xlsx'):
                    df = pd.read_excel(file_path)
                elif file_name.endswith('.csv'):
                    df = pd.read_csv(file_path)
                else:
                    logger.warning(f"Unsupported file format: {file_name}")
                    continue
                
                # Extract suspect name from filename
                suspect_name = file_path.stem
                
                # Preprocess the data
                df = self._preprocess_ipdr(df, suspect_name)
                
                if not df.empty:
                    ipdr_data[suspect_name] = df
                    logger.info(f"Loaded IPDR for {suspect_name}: {len(df)} records")
                else:
                    logger.warning(f"Empty IPDR data for {suspect_name}")
                    
            except Exception as e:
                logger.error(f"Error loading {file_name}: {str(e)}")
                continue
        
        logger.info(f"Successfully loaded {len(ipdr_data)} IPDR files")
        return ipdr_data
    
    def _preprocess_ipdr(self, df: pd.DataFrame, suspect_name: str) -> pd.DataFrame:
        """Preprocess IPDR data for analysis"""
        
        # Standardize column names
        df = self._standardize_columns(df)
        
        # Convert timestamps
        if 'start_time' in df.columns:
            df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
        if 'end_time' in df.columns:
            df['end_time'] = pd.to_datetime(df['end_time'], errors='coerce')
        
        # Calculate session duration if not present
        if 'session_duration' not in df.columns and 'start_time' in df.columns and 'end_time' in df.columns:
            df['session_duration'] = (df['end_time'] - df['start_time']).dt.total_seconds()
        
        # Convert data volumes to numeric
        if 'data_volume_up' in df.columns:
            df['data_volume_up'] = pd.to_numeric(df['data_volume_up'], errors='coerce')
        if 'data_volume_down' in df.columns:
            df['data_volume_down'] = pd.to_numeric(df['data_volume_down'], errors='coerce')
        
        # Add derived columns
        df['total_data_volume'] = df.get('data_volume_up', 0) + df.get('data_volume_down', 0)
        
        # Add temporal features
        if 'start_time' in df.columns:
            df['hour'] = df['start_time'].dt.hour
            df['day_of_week'] = df['start_time'].dt.day_name()
            df['is_odd_hour'] = ((df['hour'] >= settings.odd_hour_start) & 
                                (df['hour'] < settings.odd_hour_end))
        
        # App fingerprinting
        df['detected_app'] = df.apply(self._fingerprint_app, axis=1)
        df['app_risk'] = df['detected_app'].apply(self._get_app_risk)
        df['is_encrypted'] = df['detected_app'].apply(
            lambda x: x in ['whatsapp', 'telegram', 'signal', 'threema'] if x else False
        )
        
        # Clean subscriber ID
        if 'subscriber_id' in df.columns:
            df['subscriber_id_clean'] = df['subscriber_id'].astype(str).str.replace('+91', '').str.strip()
        
        # Add suspect name
        df['suspect'] = suspect_name
        
        # Remove completely null rows
        df = df.dropna(how='all')
        
        return df
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names based on configuration"""
        
        # Create mapping of actual columns to standard names
        column_mapping = {}
        df_columns_lower = {col.lower(): col for col in df.columns}
        
        for std_name, variations in self.ipdr_columns.items():
            # Check exact match first
            if variations in df.columns:
                column_mapping[variations] = std_name
            else:
                # Check case-insensitive match
                for var in [variations, variations.lower(), variations.replace('_', ' ')]:
                    if var.lower() in df_columns_lower:
                        column_mapping[df_columns_lower[var.lower()]] = std_name
                        break
        
        # Rename columns
        if column_mapping:
            df = df.rename(columns=column_mapping)
            logger.debug(f"Standardized {len(column_mapping)} columns")
        
        return df
    
    def _fingerprint_app(self, row: pd.Series) -> Optional[str]:
        """Identify application based on port and protocol signatures"""
        
        dest_port = row.get('destination_port')
        protocol = str(row.get('protocol', '')).upper()
        
        if pd.isna(dest_port):
            return None
        
        dest_port = int(dest_port)
        
        # Check against known app signatures
        for app_name, signature in self.app_signatures.items():
            if dest_port in signature['ports']:
                # Additional checks for specific apps
                if app_name in ['whatsapp', 'telegram'] and protocol != 'TCP':
                    continue
                return app_name
        
        return None
    
    def _get_app_risk(self, app_name: Optional[str]) -> str:
        """Get risk level for identified app"""
        
        if not app_name or app_name not in self.app_signatures:
            return 'LOW'
        
        return self.app_signatures[app_name].get('risk', 'LOW')
    
    def get_suspect_summary(self, ipdr_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Generate summary statistics for each suspect's IPDR data"""
        
        summary_data = []
        
        for suspect, df in ipdr_data.items():
            summary = {
                'suspect': suspect,
                'total_sessions': len(df),
                'total_data_mb': round(df['total_data_volume'].sum() / 1048576, 2) if 'total_data_volume' in df.columns else 0,
                'upload_mb': round(df['data_volume_up'].sum() / 1048576, 2) if 'data_volume_up' in df.columns else 0,
                'download_mb': round(df['data_volume_down'].sum() / 1048576, 2) if 'data_volume_down' in df.columns else 0,
                'encrypted_sessions': df['is_encrypted'].sum() if 'is_encrypted' in df.columns else 0,
                'unique_apps': df['detected_app'].nunique() if 'detected_app' in df.columns else 0,
                'odd_hour_sessions': df['is_odd_hour'].sum() if 'is_odd_hour' in df.columns else 0,
                'date_range': f"{df['start_time'].min().date()} to {df['start_time'].max().date()}" if 'start_time' in df.columns and not df['start_time'].isna().all() else 'N/A'
            }
            
            # Calculate percentages
            if summary['total_sessions'] > 0:
                summary['encrypted_percentage'] = round((summary['encrypted_sessions'] / summary['total_sessions']) * 100, 2)
                summary['odd_hour_percentage'] = round((summary['odd_hour_sessions'] / summary['total_sessions']) * 100, 2)
            else:
                summary['encrypted_percentage'] = 0
                summary['odd_hour_percentage'] = 0
            
            summary_data.append(summary)
        
        return pd.DataFrame(summary_data)
    
    def validate_ipdr_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate IPDR data structure and quality"""
        
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'data_quality': {}
        }
        
        # Check required columns
        required_columns = ['start_time', 'destination_port', 'data_volume_up', 'data_volume_down']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            validation_result['errors'].append(f"Missing required columns: {missing_columns}")
            validation_result['is_valid'] = False
        
        # Data quality checks
        if 'start_time' in df.columns:
            null_timestamps = df['start_time'].isna().sum()
            if null_timestamps > 0:
                validation_result['warnings'].append(f"{null_timestamps} records with missing timestamps")
            validation_result['data_quality']['timestamp_completeness'] = (len(df) - null_timestamps) / len(df)
        
        if 'destination_port' in df.columns:
            null_ports = df['destination_port'].isna().sum()
            validation_result['data_quality']['port_completeness'] = (len(df) - null_ports) / len(df)
        
        # Overall data quality score
        if validation_result['data_quality']:
            validation_result['data_quality']['overall_score'] = np.mean(list(validation_result['data_quality'].values()))
        
        return validation_result