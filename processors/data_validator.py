"""
Data Validator Module
Validates CDR data against the official CDR Data Description specification
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import re
from datetime import datetime
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

class CDRValidator:
    """Validate CDR data against specification"""
    
    def __init__(self):
        self.column_mapping = settings.cdr_columns
        self.required_columns = [
            'A PARTY', 'B PARTY', 'DATE', 'TIME', 'DURATION', 'CALL TYPE',
            'FIRST CELL ID A', 'LAST CELL ID A', 'IMEI A', 'IMSI A',
            'FIRST CELL ID A ADDRESS', 'LATITUDE', 'LONGITUDE'
        ]
        
    def validate_dataframe(self, df: pd.DataFrame, filename: str = "") -> Dict[str, any]:
        """Validate a CDR DataFrame"""
        validation_results = {
            'filename': filename,
            'is_valid': True,
            'total_rows': len(df),
            'errors': [],
            'warnings': [],
            'column_validation': {},
            'data_quality_score': 0.0
        }
        
        # Check columns
        column_check = self._validate_columns(df)
        validation_results['column_validation'] = column_check
        
        if not column_check['all_present']:
            validation_results['is_valid'] = False
            validation_results['errors'].append(f"Missing columns: {column_check['missing']}")
        
        # Validate data types and content
        content_validation = self._validate_content(df)
        validation_results.update(content_validation)
        
        # Calculate data quality score
        validation_results['data_quality_score'] = self._calculate_quality_score(validation_results)
        
        return validation_results
    
    def _validate_columns(self, df: pd.DataFrame) -> Dict[str, any]:
        """Check if all required columns are present"""
        present_columns = df.columns.tolist()
        missing_columns = [col for col in self.required_columns if col not in present_columns]
        extra_columns = [col for col in present_columns if col not in self.required_columns]
        
        return {
            'all_present': len(missing_columns) == 0,
            'missing': missing_columns,
            'extra': extra_columns,
            'match_percentage': (len(self.required_columns) - len(missing_columns)) / len(self.required_columns) * 100
        }
    
    def _validate_content(self, df: pd.DataFrame) -> Dict[str, any]:
        """Validate content of each column"""
        content_results = {
            'field_validation': {},
            'invalid_rows': []
        }
        
        # Validate A PARTY (phone numbers)
        if 'A PARTY' in df.columns:
            a_party_validation = self._validate_phone_numbers(df['A PARTY'], 'A PARTY')
            content_results['field_validation']['A PARTY'] = a_party_validation
            
        # Validate B PARTY (can be phone or service code)
        if 'B PARTY' in df.columns:
            b_party_validation = self._validate_b_party(df['B PARTY'])
            content_results['field_validation']['B PARTY'] = b_party_validation
            
        # Validate DATE
        if 'DATE' in df.columns:
            date_validation = self._validate_dates(df['DATE'])
            content_results['field_validation']['DATE'] = date_validation
            
        # Validate TIME
        if 'TIME' in df.columns:
            time_validation = self._validate_times(df['TIME'])
            content_results['field_validation']['TIME'] = time_validation
            
        # Validate DURATION
        if 'DURATION' in df.columns:
            duration_validation = self._validate_duration(df['DURATION'])
            content_results['field_validation']['DURATION'] = duration_validation
            
        # Validate CALL TYPE
        if 'CALL TYPE' in df.columns:
            call_type_validation = self._validate_call_type(df['CALL TYPE'])
            content_results['field_validation']['CALL TYPE'] = call_type_validation
            
        # Validate IMEI/IMSI
        if 'IMEI A' in df.columns:
            imei_validation = self._validate_imei(df['IMEI A'])
            content_results['field_validation']['IMEI A'] = imei_validation
            
        if 'IMSI A' in df.columns:
            imsi_validation = self._validate_imsi(df['IMSI A'])
            content_results['field_validation']['IMSI A'] = imsi_validation
            
        # Validate coordinates
        if 'LATITUDE' in df.columns and 'LONGITUDE' in df.columns:
            coord_validation = self._validate_coordinates(df['LATITUDE'], df['LONGITUDE'])
            content_results['field_validation']['COORDINATES'] = coord_validation
            
        return content_results
    
    def _validate_phone_numbers(self, series: pd.Series, field_name: str) -> Dict[str, any]:
        """Validate phone number format"""
        valid_count = 0
        invalid_samples = []
        
        for idx, value in series.items():
            if pd.notna(value):
                phone_str = str(value).strip()
                # Indian phone number: 10 digits starting with 6-9
                if re.match(r'^[6-9]\d{9}$', phone_str):
                    valid_count += 1
                else:
                    if len(invalid_samples) < 5:
                        invalid_samples.append(f"Row {idx+2}: {phone_str}")
        
        total = len(series.dropna())
        return {
            'valid_count': valid_count,
            'invalid_count': total - valid_count,
            'validity_percentage': (valid_count / total * 100) if total > 0 else 0,
            'invalid_samples': invalid_samples
        }
    
    def _validate_b_party(self, series: pd.Series) -> Dict[str, any]:
        """Validate B PARTY (can be phone or service code)"""
        phone_count = 0
        service_count = 0
        invalid_samples = []
        
        for idx, value in series.items():
            if pd.notna(value):
                value_str = str(value).strip()
                
                # Check if it's a service code
                if re.match(r'^[A-Z]{2}-', value_str) or any(pattern in value_str for pattern in settings.provider_patterns):
                    service_count += 1
                # Check if it's a valid phone number
                elif re.match(r'^[6-9]\d{9}$', value_str):
                    phone_count += 1
                else:
                    if len(invalid_samples) < 5:
                        invalid_samples.append(f"Row {idx+2}: {value_str}")
        
        total = len(series.dropna())
        valid_count = phone_count + service_count
        
        return {
            'valid_count': valid_count,
            'phone_numbers': phone_count,
            'service_codes': service_count,
            'invalid_count': total - valid_count,
            'validity_percentage': (valid_count / total * 100) if total > 0 else 0,
            'invalid_samples': invalid_samples
        }
    
    def _validate_dates(self, series: pd.Series) -> Dict[str, any]:
        """Validate date format"""
        valid_count = 0
        invalid_samples = []
        
        for idx, value in series.items():
            try:
                if pd.notna(value):
                    pd.to_datetime(value)
                    valid_count += 1
            except:
                if len(invalid_samples) < 5:
                    invalid_samples.append(f"Row {idx+2}: {value}")
        
        total = len(series.dropna())
        return {
            'valid_count': valid_count,
            'invalid_count': total - valid_count,
            'validity_percentage': (valid_count / total * 100) if total > 0 else 0,
            'invalid_samples': invalid_samples
        }
    
    def _validate_times(self, series: pd.Series) -> Dict[str, any]:
        """Validate time format"""
        valid_count = 0
        invalid_samples = []
        
        for idx, value in series.items():
            if pd.notna(value):
                time_str = str(value).strip()
                # Check for HH:MM:SS format
                if re.match(r'^\d{1,2}:\d{2}:\d{2}', time_str):
                    valid_count += 1
                else:
                    if len(invalid_samples) < 5:
                        invalid_samples.append(f"Row {idx+2}: {time_str}")
        
        total = len(series.dropna())
        return {
            'valid_count': valid_count,
            'invalid_count': total - valid_count,
            'validity_percentage': (valid_count / total * 100) if total > 0 else 0,
            'invalid_samples': invalid_samples
        }
    
    def _validate_duration(self, series: pd.Series) -> Dict[str, any]:
        """Validate duration (should be numeric)"""
        valid_count = 0
        invalid_samples = []
        
        for idx, value in series.items():
            if pd.notna(value):
                try:
                    duration = float(value)
                    if duration >= 0:
                        valid_count += 1
                    else:
                        if len(invalid_samples) < 5:
                            invalid_samples.append(f"Row {idx+2}: {value} (negative)")
                except:
                    if len(invalid_samples) < 5:
                        invalid_samples.append(f"Row {idx+2}: {value}")
        
        total = len(series.dropna())
        return {
            'valid_count': valid_count,
            'invalid_count': total - valid_count,
            'validity_percentage': (valid_count / total * 100) if total > 0 else 0,
            'invalid_samples': invalid_samples
        }
    
    def _validate_call_type(self, series: pd.Series) -> Dict[str, any]:
        """Validate call type"""
        valid_types = ['CALL-IN', 'CALL-OUT', 'SMS-IN', 'SMS-OUT']
        valid_count = 0
        invalid_samples = []
        type_distribution = {}
        
        for idx, value in series.items():
            if pd.notna(value):
                call_type = str(value).strip().upper()
                if call_type in valid_types:
                    valid_count += 1
                    type_distribution[call_type] = type_distribution.get(call_type, 0) + 1
                else:
                    if len(invalid_samples) < 5:
                        invalid_samples.append(f"Row {idx+2}: {value}")
        
        total = len(series.dropna())
        return {
            'valid_count': valid_count,
            'invalid_count': total - valid_count,
            'validity_percentage': (valid_count / total * 100) if total > 0 else 0,
            'type_distribution': type_distribution,
            'invalid_samples': invalid_samples
        }
    
    def _validate_imei(self, series: pd.Series) -> Dict[str, any]:
        """Validate IMEI format (15 digits)"""
        valid_count = 0
        invalid_samples = []
        
        for idx, value in series.items():
            if pd.notna(value):
                imei_str = str(value).strip()
                if re.match(r'^\d{15}$', imei_str):
                    valid_count += 1
                else:
                    if len(invalid_samples) < 5:
                        invalid_samples.append(f"Row {idx+2}: {imei_str}")
        
        total = len(series.dropna())
        return {
            'valid_count': valid_count,
            'invalid_count': total - valid_count,
            'validity_percentage': (valid_count / total * 100) if total > 0 else 0,
            'invalid_samples': invalid_samples
        }
    
    def _validate_imsi(self, series: pd.Series) -> Dict[str, any]:
        """Validate IMSI format (15 digits)"""
        valid_count = 0
        invalid_samples = []
        
        for idx, value in series.items():
            if pd.notna(value):
                imsi_str = str(value).strip()
                if re.match(r'^\d{15}$', imsi_str):
                    valid_count += 1
                else:
                    if len(invalid_samples) < 5:
                        invalid_samples.append(f"Row {idx+2}: {imsi_str}")
        
        total = len(series.dropna())
        return {
            'valid_count': valid_count,
            'invalid_count': total - valid_count,
            'validity_percentage': (valid_count / total * 100) if total > 0 else 0,
            'invalid_samples': invalid_samples
        }
    
    def _validate_coordinates(self, lat_series: pd.Series, lon_series: pd.Series) -> Dict[str, any]:
        """Validate latitude and longitude"""
        valid_count = 0
        invalid_samples = []
        
        for idx in lat_series.index:
            if pd.notna(lat_series[idx]) and pd.notna(lon_series[idx]):
                try:
                    lat = float(lat_series[idx])
                    lon = float(lon_series[idx])
                    
                    # Valid latitude: -90 to 90
                    # Valid longitude: -180 to 180
                    if -90 <= lat <= 90 and -180 <= lon <= 180:
                        valid_count += 1
                    else:
                        if len(invalid_samples) < 5:
                            invalid_samples.append(f"Row {idx+2}: ({lat}, {lon})")
                except:
                    if len(invalid_samples) < 5:
                        invalid_samples.append(f"Row {idx+2}: ({lat_series[idx]}, {lon_series[idx]})")
        
        total = len(lat_series.dropna())
        return {
            'valid_count': valid_count,
            'invalid_count': total - valid_count,
            'validity_percentage': (valid_count / total * 100) if total > 0 else 0,
            'invalid_samples': invalid_samples
        }
    
    def _calculate_quality_score(self, validation_results: Dict[str, any]) -> float:
        """Calculate overall data quality score"""
        scores = []
        
        # Column match score
        if 'column_validation' in validation_results:
            scores.append(validation_results['column_validation']['match_percentage'])
        
        # Field validation scores
        if 'field_validation' in validation_results:
            for field, validation in validation_results['field_validation'].items():
                if 'validity_percentage' in validation:
                    scores.append(validation['validity_percentage'])
        
        # Calculate weighted average
        if scores:
            return sum(scores) / len(scores)
        else:
            return 0.0