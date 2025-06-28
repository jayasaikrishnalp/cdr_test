"""
Tower Dump Data Loader
Loads and processes tower dump data from various sources
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Union
from datetime import datetime
from loguru import logger
import re

class TowerDumpLoader:
    """Loads and processes tower dump data for analysis"""
    
    def __init__(self, data_path: Optional[str] = None):
        """
        Initialize tower dump loader
        
        Args:
            data_path: Default path to tower dump files
        """
        self.data_path = Path(data_path) if data_path else Path("data/tower_dumps")
        
        # Standard column mappings for different providers
        self.column_mappings = {
            'timestamp': ['timestamp', 'datetime', 'date_time', 'call_time', 'connection_time'],
            'date': ['date', 'call_date', 'connection_date'],
            'time': ['time', 'call_time', 'connection_time'],
            'mobile_number': ['mobile_number', 'msisdn', 'phone_number', 'a_party', 'subscriber', 'a party'],
            'imei': ['imei', 'device_id', 'equipment_id', 'imei a', 'imei_a'],
            'imsi': ['imsi', 'sim_id', 'subscriber_id', 'imsi a', 'imsi_a'],
            'tower_id': ['tower_id', 'cell_id', 'bts_id', 'site_id', 'tower', 'first cell id a', 'last cell id a'],
            'lat': ['lat', 'latitude', 'tower_lat', 'site_lat'],
            'long': ['long', 'longitude', 'lon', 'tower_long', 'site_long'],
            'duration': ['duration', 'connection_duration', 'session_time'],
            'signal_strength': ['signal_strength', 'rssi', 'signal_level'],
            'b_party': ['b_party', 'called_number', 'b party'],
            'call_type': ['call_type', 'call type', 'type']
        }
        
        # Data type specifications
        self.dtypes = {
            'mobile_number': str,
            'imei': str,
            'imsi': str,
            'tower_id': str,
            'lat': float,
            'long': float,
            'duration': float,
            'signal_strength': float
        }
        
        logger.info("Tower Dump Loader initialized")
    
    def load_tower_dump(self, file_path: Union[str, Path], 
                       provider: Optional[str] = None) -> pd.DataFrame:
        """
        Load tower dump data from file
        
        Args:
            file_path: Path to tower dump file
            provider: Optional provider name for specific column mappings
            
        Returns:
            DataFrame with standardized tower dump data
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Tower dump file not found: {file_path}")
        
        logger.info(f"Loading tower dump from {file_path}")
        
        # Load based on file extension
        if file_path.suffix.lower() == '.csv':
            df = pd.read_csv(file_path, low_memory=False)
        elif file_path.suffix.lower() in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
        
        logger.info(f"Loaded {len(df)} records from tower dump")
        
        # Standardize columns
        df = self._standardize_columns(df)
        
        # Clean and validate data
        df = self._clean_data(df)
        
        # Parse timestamps
        df = self._parse_timestamps(df)
        
        # Add derived columns
        df = self._add_derived_columns(df)
        
        logger.info(f"Processed {len(df)} valid tower dump records")
        
        return df
    
    def load_multiple_dumps(self, file_list: List[Union[str, Path]]) -> pd.DataFrame:
        """
        Load and combine multiple tower dump files
        
        Args:
            file_list: List of file paths
            
        Returns:
            Combined DataFrame
        """
        all_dumps = []
        
        for file_path in file_list:
            try:
                df = self.load_tower_dump(file_path)
                df['source_file'] = Path(file_path).name
                all_dumps.append(df)
                logger.info(f"Loaded {len(df)} records from {file_path}")
            except Exception as e:
                logger.error(f"Error loading {file_path}: {e}")
        
        if all_dumps:
            combined_df = pd.concat(all_dumps, ignore_index=True)
            
            # Remove duplicates based on key columns
            combined_df = combined_df.drop_duplicates(
                subset=['timestamp', 'mobile_number', 'tower_id'],
                keep='first'
            )
            
            logger.info(f"Combined {len(combined_df)} unique records from {len(file_list)} files")
            return combined_df
        else:
            return pd.DataFrame()
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names based on mappings"""
        
        # Create reverse mapping
        current_to_standard = {}
        df_columns_lower = {col.lower(): col for col in df.columns}
        
        for standard_name, variations in self.column_mappings.items():
            for variation in variations:
                if variation.lower() in df_columns_lower:
                    current_to_standard[df_columns_lower[variation.lower()]] = standard_name
                    break
        
        # Rename columns
        if current_to_standard:
            df = df.rename(columns=current_to_standard)
            logger.debug(f"Standardized {len(current_to_standard)} columns")
        
        return df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate tower dump data"""
        
        # Remove rows without essential fields
        # For timestamp, we need either 'timestamp' OR both 'date' and 'time'
        has_timestamp = 'timestamp' in df.columns
        has_date_time = 'date' in df.columns and 'time' in df.columns
        
        if not has_timestamp and not has_date_time:
            logger.warning("No timestamp information found in data")
        
        # Check other essential fields
        essential_fields = ['mobile_number', 'tower_id']
        for field in essential_fields:
            if field in df.columns:
                df = df.dropna(subset=[field])
        
        # Clean mobile numbers
        if 'mobile_number' in df.columns:
            df['mobile_number'] = df['mobile_number'].astype(str).str.strip()
            # Remove country code if present
            df['mobile_number'] = df['mobile_number'].str.replace(r'^(\+91|91)', '', regex=True)
            # Keep only valid 10-digit numbers
            df = df[df['mobile_number'].str.match(r'^\d{10}$', na=False)]
        
        # Clean IMEI
        if 'imei' in df.columns:
            df['imei'] = df['imei'].astype(str).str.strip()
            # Valid IMEI is 15 digits
            df.loc[~df['imei'].str.match(r'^\d{15}$', na=False), 'imei'] = np.nan
        
        # Clean IMSI
        if 'imsi' in df.columns:
            df['imsi'] = df['imsi'].astype(str).str.strip()
            # Valid IMSI is 15 digits
            df.loc[~df['imsi'].str.match(r'^\d{15}$', na=False), 'imsi'] = np.nan
        
        # Clean tower ID
        if 'tower_id' in df.columns:
            df['tower_id'] = df['tower_id'].astype(str).str.strip()
        
        # Validate coordinates
        if 'lat' in df.columns:
            df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
            df.loc[(df['lat'] < -90) | (df['lat'] > 90), 'lat'] = np.nan
        
        if 'long' in df.columns:
            df['long'] = pd.to_numeric(df['long'], errors='coerce')
            df.loc[(df['long'] < -180) | (df['long'] > 180), 'long'] = np.nan
        
        return df
    
    def _parse_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse and standardize timestamps"""
        
        # Check if we have separate DATE and TIME columns
        if 'date' in df.columns and 'time' in df.columns and 'timestamp' not in df.columns:
            logger.info("Found separate DATE and TIME columns, combining them")
            
            # Check if date column contains Excel serial dates (numeric)
            if pd.api.types.is_numeric_dtype(df['date']):
                logger.info("DATE column contains Excel serial dates, converting...")
                # Convert Excel serial date to datetime
                # Excel's epoch is 1899-12-30
                excel_epoch = pd.Timestamp('1899-12-30')
                df['date_converted'] = excel_epoch + pd.to_timedelta(df['date'], unit='D')
            else:
                # Try to parse as regular date
                df['date_converted'] = pd.to_datetime(df['date'], errors='coerce')
            
            # Extract date part only
            df['date_str'] = df['date_converted'].dt.strftime('%Y-%m-%d')
            
            # Ensure time is string
            df['time_str'] = df['time'].astype(str)
            
            # Combine date and time
            df['timestamp_str'] = df['date_str'] + ' ' + df['time_str']
            
            # Parse the combined timestamp
            df['timestamp'] = pd.to_datetime(df['timestamp_str'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            
            # Clean up temporary columns
            df = df.drop(['date_str', 'time_str', 'timestamp_str', 'date_converted'], axis=1)
            
            # Remove rows where timestamp parsing failed
            valid_timestamps = ~df['timestamp'].isna()
            df = df[valid_timestamps]
            
            logger.info(f"Successfully parsed {len(df)} timestamps from DATE and TIME columns")
            
        elif 'timestamp' in df.columns:
            # Try multiple datetime formats
            formats = [
                '%Y-%m-%d %H:%M:%S',
                '%d-%m-%Y %H:%M:%S',
                '%d/%m/%Y %H:%M:%S',
                '%Y/%m/%d %H:%M:%S',
                '%d-%m-%Y %I:%M:%S %p',
                '%d/%m/%Y %I:%M:%S %p'
            ]
            
            for fmt in formats:
                try:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], format=fmt)
                    break
                except:
                    continue
            
            # If all formats fail, use pandas inference
            if df['timestamp'].dtype == 'object':
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            
            # Remove invalid timestamps
            df = df.dropna(subset=['timestamp'])
        else:
            logger.warning("No timestamp, date, or time columns found in data")
            return df
        
        # Sort by timestamp
        df = df.sort_values('timestamp')
        
        return df
    
    def _add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived columns for analysis"""
        
        if 'timestamp' in df.columns:
            # Time-based features
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.day_name()
            df['date'] = df['timestamp'].dt.date
            
            # Odd hour flag (midnight to 5 AM)
            df['is_odd_hour'] = df['hour'].between(0, 5)
            
            # Weekend flag
            df['is_weekend'] = df['timestamp'].dt.dayofweek.isin([5, 6])
        
        # Connection type based on duration
        if 'duration' in df.columns:
            df['connection_type'] = pd.cut(
                df['duration'],
                bins=[0, 10, 60, 300, float('inf')],
                labels=['brief', 'short', 'normal', 'long']
            )
        
        # Add unique identifier
        df['record_id'] = df.index
        
        return df
    
    def filter_time_window(self, df: pd.DataFrame, 
                          start_time: datetime, 
                          end_time: datetime,
                          buffer_minutes: int = 0) -> pd.DataFrame:
        """
        Filter tower dump for specific time window
        
        Args:
            df: Tower dump DataFrame
            start_time: Start of time window
            end_time: End of time window
            buffer_minutes: Additional minutes before/after window
            
        Returns:
            Filtered DataFrame
        """
        if 'timestamp' not in df.columns:
            raise ValueError("DataFrame must have timestamp column")
        
        # Apply buffer
        if buffer_minutes > 0:
            start_time = start_time - pd.Timedelta(minutes=buffer_minutes)
            end_time = end_time + pd.Timedelta(minutes=buffer_minutes)
        
        mask = (df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)
        filtered_df = df[mask].copy()
        
        logger.info(f"Filtered {len(filtered_df)} records in time window")
        
        return filtered_df
    
    def filter_location(self, df: pd.DataFrame,
                       tower_ids: Optional[List[str]] = None,
                       center_lat: Optional[float] = None,
                       center_long: Optional[float] = None,
                       radius_km: float = 1.0) -> pd.DataFrame:
        """
        Filter tower dump by location
        
        Args:
            df: Tower dump DataFrame
            tower_ids: List of specific tower IDs
            center_lat: Center latitude for radius search
            center_long: Center longitude for radius search
            radius_km: Radius in kilometers
            
        Returns:
            Filtered DataFrame
        """
        if tower_ids:
            return df[df['tower_id'].isin(tower_ids)].copy()
        
        if center_lat and center_long and 'lat' in df.columns and 'long' in df.columns:
            # Calculate distance using Haversine formula
            df['distance_km'] = self._calculate_distance(
                df['lat'], df['long'], 
                center_lat, center_long
            )
            
            return df[df['distance_km'] <= radius_km].copy()
        
        return df
    
    def _calculate_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance between coordinates using Haversine formula"""
        R = 6371  # Earth's radius in kilometers
        
        lat1_rad = np.radians(lat1)
        lat2_rad = np.radians(lat2)
        delta_lat = np.radians(lat2 - lat1)
        delta_lon = np.radians(lon2 - lon1)
        
        a = np.sin(delta_lat/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(delta_lon/2)**2
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
        
        return R * c
    
    def get_summary_stats(self, df: pd.DataFrame) -> Dict:
        """Get summary statistics for tower dump"""
        
        stats = {
            'total_records': len(df),
            'unique_numbers': df['mobile_number'].nunique() if 'mobile_number' in df.columns else 0,
            'unique_towers': df['tower_id'].nunique() if 'tower_id' in df.columns else 0,
            'unique_devices': df['imei'].nunique() if 'imei' in df.columns else 0,
            'time_range': {
                'start': df['timestamp'].min() if 'timestamp' in df.columns else None,
                'end': df['timestamp'].max() if 'timestamp' in df.columns else None
            },
            'odd_hour_percentage': (df['is_odd_hour'].sum() / len(df) * 100) if 'is_odd_hour' in df.columns else 0,
            'weekend_percentage': (df['is_weekend'].sum() / len(df) * 100) if 'is_weekend' in df.columns else 0
        }
        
        return stats