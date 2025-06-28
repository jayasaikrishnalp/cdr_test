"""
CDR Data Loader Module
Handles loading and preprocessing of CDR Excel files
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import re
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

class CDRLoader:
    """Load and preprocess CDR Excel files"""
    
    def __init__(self, data_path: Optional[Path] = None):
        self.data_path = data_path or settings.cdr_data_path
        self.column_mapping = settings.cdr_columns
        self.provider_patterns = settings.provider_patterns
        
    def load_cdrs(self, file_list: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """Load CDR Excel files
        
        Args:
            file_list: Optional list of specific files to load. If None, loads all .xlsx files
        """
        cdr_data = {}
        
        if file_list:
            # Load specific files
            excel_files = []
            for filename in file_list:
                # Check if it's a full path or just filename
                if '/' in filename or '\\' in filename:
                    file_path = Path(filename)
                else:
                    file_path = self.data_path / filename
                
                # Add .xlsx extension if not present
                if not file_path.suffix:
                    file_path = file_path.with_suffix('.xlsx')
                
                if file_path.exists():
                    excel_files.append(file_path)
                else:
                    logger.warning(f"File not found: {file_path}")
        else:
            # Load all Excel files from directory
            excel_files = list(self.data_path.glob("*.xlsx"))
        
        if not excel_files:
            logger.warning(f"No Excel files found")
            return cdr_data
        
        logger.info(f"Found {len(excel_files)} CDR files to process")
        
        for file_path in excel_files:
            try:
                # Extract suspect name from filename
                # Format: 9042720423_Ali.xlsx
                filename = file_path.stem
                parts = filename.split('_', 1)
                
                if len(parts) == 2:
                    phone_number, name = parts
                    suspect_key = f"{name}_{phone_number}"
                else:
                    suspect_key = filename
                
                # Load the Excel file
                df = self._load_single_cdr(file_path)
                
                if df is not None and not df.empty:
                    cdr_data[suspect_key] = df
                    logger.info(f"Loaded {len(df)} records for {suspect_key}")
                else:
                    logger.warning(f"No data loaded from {file_path}")
                    
            except Exception as e:
                logger.error(f"Error loading {file_path}: {str(e)}")
                
        logger.info(f"Successfully loaded {len(cdr_data)} CDR files")
        return cdr_data
    
    def load_all_cdrs(self) -> Dict[str, pd.DataFrame]:
        """Load all CDR Excel files from the directory (backwards compatibility)"""
        return self.load_cdrs()
    
    def _load_single_cdr(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Load and preprocess a single CDR file"""
        try:
            # Read Excel file
            df = pd.read_excel(file_path)
            
            # Validate required columns
            if not self._validate_columns(df):
                logger.error(f"Column validation failed for {file_path}")
                return None
            
            # Preprocess the data
            df = self._preprocess_cdr(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            return None
    
    def _validate_columns(self, df: pd.DataFrame) -> bool:
        """Validate that required columns are present"""
        required_columns = [
            self.column_mapping['a_party'],
            self.column_mapping['b_party'],
            self.column_mapping['date'],
            self.column_mapping['time'],
            self.column_mapping['call_type']
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
            
        return True
    
    def _preprocess_cdr(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess CDR data"""
        # Create a copy to avoid modifying original
        df = df.copy()
        
        # Combine DATE and TIME into a single datetime column
        df['datetime'] = self._combine_date_time(df)
        
        # Clean phone numbers
        df['a_party_clean'] = df[self.column_mapping['a_party']].apply(self._clean_phone_number)
        df['b_party_clean'] = df[self.column_mapping['b_party']].apply(self._clean_phone_number)
        
        # Identify provider messages
        df['is_provider_message'] = df[self.column_mapping['b_party']].apply(self._is_provider_message)
        
        # Extract hour for temporal analysis
        df['hour'] = pd.to_datetime(df['datetime']).dt.hour
        df['day_of_week'] = pd.to_datetime(df['datetime']).dt.day_name()
        
        # Clean duration (handle missing values)
        duration_col = self.column_mapping['duration']
        if duration_col in df.columns:
            df['duration_seconds'] = pd.to_numeric(df[duration_col], errors='coerce').fillna(0)
        else:
            df['duration_seconds'] = 0
            
        # Process location data if available
        if self.column_mapping['latitude'] in df.columns and self.column_mapping['longitude'] in df.columns:
            df['has_location'] = df[[self.column_mapping['latitude'], self.column_mapping['longitude']]].notna().all(axis=1)
        else:
            df['has_location'] = False
            
        # Sort by datetime
        df = df.sort_values('datetime')
        
        return df
    
    def _combine_date_time(self, df: pd.DataFrame) -> pd.Series:
        """Combine DATE and TIME columns into datetime"""
        date_col = self.column_mapping['date']
        time_col = self.column_mapping['time']
        
        # Handle different date/time formats
        combined = []
        for _, row in df.iterrows():
            try:
                date_str = str(row[date_col])
                time_str = str(row[time_col])
                
                # Combine and parse
                datetime_str = f"{date_str} {time_str}"
                dt = pd.to_datetime(datetime_str, errors='coerce')
                combined.append(dt)
            except:
                combined.append(pd.NaT)
                
        return pd.Series(combined)
    
    def _clean_phone_number(self, phone: any) -> str:
        """Clean and standardize phone numbers"""
        if pd.isna(phone):
            return ""
            
        phone_str = str(phone).strip()
        
        # Remove any non-numeric characters for standard phones
        if not self._is_provider_message(phone_str):
            phone_str = re.sub(r'\D', '', phone_str)
            
        return phone_str
    
    def _is_provider_message(self, phone: any) -> bool:
        """Check if the phone number is a provider message"""
        if pd.isna(phone):
            return False
            
        phone_str = str(phone).upper()
        
        # Check against provider patterns
        for pattern in self.provider_patterns:
            if pattern in phone_str:
                return True
                
        # Check for other common patterns
        if re.match(r'^[A-Z]{2}-', phone_str):  # Two letters followed by dash
            return True
            
        return False
    
    def get_consolidated_data(self, cdr_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Consolidate all CDR data into a single DataFrame with suspect labels"""
        consolidated = []
        
        for suspect, df in cdr_data.items():
            df_copy = df.copy()
            df_copy['suspect'] = suspect
            consolidated.append(df_copy)
            
        if consolidated:
            return pd.concat(consolidated, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def get_suspect_summary(self, cdr_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Generate summary statistics for each suspect"""
        summaries = []
        
        for suspect, df in cdr_data.items():
            # Filter out provider messages
            df_filtered = df[~df['is_provider_message']]
            
            summary = {
                'suspect': suspect,
                'total_records': len(df),
                'filtered_records': len(df_filtered),
                'provider_messages': len(df) - len(df_filtered),
                'date_range': f"{df['datetime'].min()} to {df['datetime'].max()}",
                'unique_contacts': df_filtered['b_party_clean'].nunique(),
                'total_duration': df_filtered['duration_seconds'].sum(),
                'avg_duration': df_filtered['duration_seconds'].mean()
            }
            
            # Count by call type
            call_types = df_filtered[self.column_mapping['call_type']].value_counts()
            for call_type, count in call_types.items():
                summary[f'{call_type.lower()}_count'] = count
                
            summaries.append(summary)
            
        return pd.DataFrame(summaries)