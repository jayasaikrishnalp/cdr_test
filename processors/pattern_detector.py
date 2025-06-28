"""
Pattern Detector Module
Implements criminal pattern detection based on YAML analysis patterns
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Set
from datetime import datetime, timedelta
from collections import defaultdict
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

class PatternDetector:
    """Detect criminal patterns in CDR data"""
    
    def __init__(self):
        self.config = settings
        
    def detect_all_patterns(self, df: pd.DataFrame, suspect_name: str) -> Dict[str, any]:
        """Run all pattern detection algorithms on a suspect's data"""
        # Filter out provider messages
        df_filtered = df[~df['is_provider_message']].copy()
        
        patterns = {
            'suspect': suspect_name,
            'device_patterns': self._detect_device_patterns(df_filtered),
            'temporal_patterns': self._detect_temporal_patterns(df_filtered),
            'communication_patterns': self._detect_communication_patterns(df_filtered),
            'frequency_patterns': self._detect_frequency_patterns(df_filtered),
            'location_patterns': self._detect_location_patterns(df_filtered),
            'behavioral_indicators': self._detect_behavioral_indicators(df_filtered)
        }
        
        # Calculate risk score
        patterns['risk_assessment'] = self._calculate_risk_score(patterns)
        
        return patterns
    
    def _detect_device_patterns(self, df: pd.DataFrame) -> Dict[str, any]:
        """Detect device switching and IMEI/IMSI patterns"""
        imei_col = self.config.cdr_columns['imei']
        imsi_col = self.config.cdr_columns['imsi']
        
        device_patterns = {
            'imei_count': 0,
            'imsi_count': 0,
            'unique_imeis': [],
            'unique_imsis': [],
            'device_switching_detected': False,
            'sim_swapping_detected': False,
            'device_changes': []
        }
        
        if imei_col in df.columns:
            unique_imeis = df[imei_col].dropna().unique()
            device_patterns['imei_count'] = len(unique_imeis)
            device_patterns['unique_imeis'] = list(unique_imeis)
            
            # Check for device switching (3+ IMEIs is high risk)
            if device_patterns['imei_count'] >= self.config.high_risk_imei_count:
                device_patterns['device_switching_detected'] = True
                
            # Track device changes over time
            device_patterns['device_changes'] = self._track_device_changes(df, imei_col)
        
        if imsi_col in df.columns:
            unique_imsis = df[imsi_col].dropna().unique()
            device_patterns['imsi_count'] = len(unique_imsis)
            device_patterns['unique_imsis'] = list(unique_imsis)
            
            # Check for SIM swapping
            if device_patterns['imsi_count'] > 1:
                device_patterns['sim_swapping_detected'] = True
        
        return device_patterns
    
    def _detect_temporal_patterns(self, df: pd.DataFrame) -> Dict[str, any]:
        """Detect time-based patterns"""
        temporal_patterns = {
            'odd_hour_percentage': 0.0,
            'odd_hour_calls': 0,
            'total_calls': len(df),
            'call_bursts': [],
            'pattern_day_activity': {},
            'hourly_distribution': {},
            'peak_hours': []
        }
        
        # Calculate odd hour activity
        odd_hour_mask = (df['hour'] >= self.config.odd_hour_start) & (df['hour'] < self.config.odd_hour_end)
        temporal_patterns['odd_hour_calls'] = odd_hour_mask.sum()
        temporal_patterns['odd_hour_percentage'] = (temporal_patterns['odd_hour_calls'] / len(df) * 100) if len(df) > 0 else 0
        
        # Detect call bursts
        temporal_patterns['call_bursts'] = self._detect_call_bursts(df)
        
        # Pattern day analysis (Tuesday/Friday for narcotics)
        for day in self.config.pattern_days:
            day_activity = df[df['day_of_week'] == day]
            temporal_patterns['pattern_day_activity'][day] = {
                'call_count': len(day_activity),
                'percentage': (len(day_activity) / len(df) * 100) if len(df) > 0 else 0
            }
        
        # Hourly distribution
        hourly_counts = df['hour'].value_counts().sort_index()
        temporal_patterns['hourly_distribution'] = hourly_counts.to_dict()
        
        # Peak hours (top 3 hours with most activity)
        if len(hourly_counts) > 0:
            temporal_patterns['peak_hours'] = hourly_counts.nlargest(3).index.tolist()
        
        return temporal_patterns
    
    def _detect_communication_patterns(self, df: pd.DataFrame) -> Dict[str, any]:
        """Detect communication behavior patterns"""
        call_type_col = self.config.cdr_columns['call_type']
        
        comm_patterns = {
            'total_communications': len(df),
            'voice_calls': 0,
            'sms_messages': 0,
            'voice_percentage': 0.0,
            'sms_percentage': 0.0,
            'voice_only_behavior': False,
            'call_type_distribution': {},
            'avg_call_duration': 0.0,
            'duration_patterns': {}
        }
        
        # Count by communication type
        if call_type_col in df.columns:
            call_types = df[call_type_col].value_counts()
            comm_patterns['call_type_distribution'] = call_types.to_dict()
            
            # Calculate voice vs SMS
            voice_types = ['CALL-IN', 'CALL-OUT']
            sms_types = ['SMS-IN', 'SMS-OUT']
            
            comm_patterns['voice_calls'] = df[df[call_type_col].isin(voice_types)].shape[0]
            comm_patterns['sms_messages'] = df[df[call_type_col].isin(sms_types)].shape[0]
            
            total = comm_patterns['voice_calls'] + comm_patterns['sms_messages']
            if total > 0:
                comm_patterns['voice_percentage'] = (comm_patterns['voice_calls'] / total) * 100
                comm_patterns['sms_percentage'] = (comm_patterns['sms_messages'] / total) * 100
                
                # Check for voice-only behavior (100% voice is suspicious)
                if comm_patterns['sms_messages'] == 0 and comm_patterns['voice_calls'] > 0:
                    comm_patterns['voice_only_behavior'] = True
        
        # Duration analysis
        if 'duration_seconds' in df.columns:
            voice_df = df[df[call_type_col].isin(['CALL-IN', 'CALL-OUT'])] if call_type_col in df.columns else df
            comm_patterns['avg_call_duration'] = voice_df['duration_seconds'].mean()
            
            # Detect duration patterns (repeated same durations)
            duration_counts = voice_df['duration_seconds'].value_counts()
            repeated_durations = duration_counts[duration_counts > 3]
            if len(repeated_durations) > 0:
                comm_patterns['duration_patterns'] = {
                    'repeated_durations': repeated_durations.head(5).to_dict(),
                    'scripted_calls_suspected': True
                }
        
        return comm_patterns
    
    def _detect_frequency_patterns(self, df: pd.DataFrame) -> Dict[str, any]:
        """Detect contact frequency patterns"""
        freq_patterns = {
            'unique_contacts': 0,
            'frequent_contacts': {},
            'contact_frequency_distribution': {},
            'high_frequency_contacts': [],
            'single_contact_numbers': []
        }
        
        # Count unique B parties
        b_party_col = 'b_party_clean'
        if b_party_col in df.columns:
            freq_patterns['unique_contacts'] = df[b_party_col].nunique()
            
            # Find frequent contacts
            contact_counts = df[b_party_col].value_counts()
            freq_patterns['contact_frequency_distribution'] = {
                'max_frequency': int(contact_counts.max()) if len(contact_counts) > 0 else 0,
                'avg_frequency': float(contact_counts.mean()) if len(contact_counts) > 0 else 0.0
            }
            
            # High frequency contacts (top 10)
            if len(contact_counts) > 0:
                top_contacts = contact_counts.head(10)
                freq_patterns['frequent_contacts'] = top_contacts.to_dict()
                
                # Identify very high frequency contacts (>20 calls)
                high_freq = contact_counts[contact_counts > 20]
                if len(high_freq) > 0:
                    freq_patterns['high_frequency_contacts'] = high_freq.index.tolist()
                
                # Single contact numbers (only called once)
                single_contacts = contact_counts[contact_counts == 1]
                freq_patterns['single_contact_numbers'] = len(single_contacts)
        
        return freq_patterns
    
    def _detect_location_patterns(self, df: pd.DataFrame) -> Dict[str, any]:
        """Detect location and movement patterns"""
        location_patterns = {
            'tower_count': 0,
            'unique_towers': [],
            'tower_frequency': {},
            'tower_hopping_detected': False,
            'rapid_movements': [],
            'has_location_data': False
        }
        
        first_cell_col = self.config.cdr_columns['first_cell']
        last_cell_col = self.config.cdr_columns['last_cell']
        
        if first_cell_col in df.columns:
            # Count unique towers
            unique_first_towers = df[first_cell_col].dropna().unique()
            location_patterns['tower_count'] = len(unique_first_towers)
            location_patterns['unique_towers'] = list(unique_first_towers)[:20]  # Limit to 20
            
            # Tower frequency
            tower_counts = df[first_cell_col].value_counts()
            location_patterns['tower_frequency'] = tower_counts.head(10).to_dict()
            
            # Detect tower hopping
            if first_cell_col in df.columns and last_cell_col in df.columns:
                # Check for calls where first and last cell are different
                tower_changes = df[df[first_cell_col] != df[last_cell_col]]
                if len(tower_changes) > 0:
                    location_patterns['tower_hopping_detected'] = True
                    
                    # Detect rapid movements
                    rapid_movements = self._detect_rapid_tower_changes(df)
                    location_patterns['rapid_movements'] = rapid_movements
        
        # Check for coordinate data
        if self.config.cdr_columns['latitude'] in df.columns:
            location_patterns['has_location_data'] = df['has_location'].any()
        
        return location_patterns
    
    def _detect_behavioral_indicators(self, df: pd.DataFrame) -> Dict[str, any]:
        """Detect behavioral anomalies and indicators"""
        behavioral = {
            'activity_changes': {},
            'daily_pattern': {},
            'weekend_vs_weekday': {},
            'suspicious_patterns': []
        }
        
        # Daily activity pattern
        df['date_only'] = pd.to_datetime(df['datetime']).dt.date
        daily_counts = df.groupby('date_only').size()
        
        if len(daily_counts) > 0:
            behavioral['daily_pattern'] = {
                'avg_daily_calls': float(daily_counts.mean()),
                'max_daily_calls': int(daily_counts.max()),
                'min_daily_calls': int(daily_counts.min()),
                'std_deviation': float(daily_counts.std())
            }
            
            # Detect sudden activity changes
            if len(daily_counts) > 7:
                recent_avg = daily_counts.tail(7).mean()
                overall_avg = daily_counts.mean()
                
                if recent_avg > overall_avg * 2:
                    behavioral['suspicious_patterns'].append("Recent activity spike detected")
                elif recent_avg < overall_avg * 0.5:
                    behavioral['suspicious_patterns'].append("Recent activity drop detected")
        
        # Weekend vs Weekday
        df['is_weekend'] = pd.to_datetime(df['datetime']).dt.dayofweek.isin([5, 6])
        weekend_calls = df[df['is_weekend']].shape[0]
        weekday_calls = df[~df['is_weekend']].shape[0]
        
        behavioral['weekend_vs_weekday'] = {
            'weekend_calls': weekend_calls,
            'weekday_calls': weekday_calls,
            'weekend_percentage': (weekend_calls / len(df) * 100) if len(df) > 0 else 0
        }
        
        return behavioral
    
    def _track_device_changes(self, df: pd.DataFrame, imei_col: str) -> List[Dict]:
        """Track IMEI changes over time"""
        changes = []
        df_sorted = df.sort_values('datetime')
        
        previous_imei = None
        for idx, row in df_sorted.iterrows():
            current_imei = row[imei_col]
            if pd.notna(current_imei) and current_imei != previous_imei and previous_imei is not None:
                changes.append({
                    'datetime': row['datetime'],
                    'from_imei': previous_imei,
                    'to_imei': current_imei
                })
            previous_imei = current_imei if pd.notna(current_imei) else previous_imei
            
        return changes[:10]  # Limit to 10 most recent changes
    
    def _detect_call_bursts(self, df: pd.DataFrame) -> List[Dict]:
        """Detect call bursts (multiple calls in short time)"""
        bursts = []
        df_sorted = df.sort_values('datetime')
        
        # Group calls within time windows
        window_minutes = self.config.call_burst_window
        threshold = self.config.call_burst_threshold
        
        for i in range(len(df_sorted)):
            window_start = df_sorted.iloc[i]['datetime']
            window_end = window_start + timedelta(minutes=window_minutes)
            
            # Count calls in window
            calls_in_window = df_sorted[
                (df_sorted['datetime'] >= window_start) & 
                (df_sorted['datetime'] <= window_end)
            ]
            
            if len(calls_in_window) >= threshold:
                bursts.append({
                    'start_time': window_start,
                    'call_count': len(calls_in_window),
                    'duration_minutes': window_minutes,
                    'numbers_involved': calls_in_window['b_party_clean'].unique().tolist()[:5]
                })
        
        # Remove duplicate/overlapping bursts
        unique_bursts = []
        for burst in bursts:
            if not any(abs((burst['start_time'] - b['start_time']).total_seconds()) < 300 for b in unique_bursts):
                unique_bursts.append(burst)
        
        return unique_bursts[:10]  # Limit to 10 most significant
    
    def _detect_rapid_tower_changes(self, df: pd.DataFrame) -> List[Dict]:
        """Detect rapid tower changes indicating vehicle movement"""
        rapid_changes = []
        df_sorted = df.sort_values('datetime')
        
        first_cell_col = self.config.cdr_columns['first_cell']
        last_cell_col = self.config.cdr_columns['last_cell']
        
        for i in range(1, len(df_sorted)):
            prev_row = df_sorted.iloc[i-1]
            curr_row = df_sorted.iloc[i]
            
            time_diff = (curr_row['datetime'] - prev_row['datetime']).total_seconds() / 60  # minutes
            
            if time_diff <= 5:  # Within 5 minutes
                prev_tower = prev_row[last_cell_col] if pd.notna(prev_row[last_cell_col]) else prev_row[first_cell_col]
                curr_tower = curr_row[first_cell_col]
                
                if prev_tower != curr_tower and pd.notna(prev_tower) and pd.notna(curr_tower):
                    rapid_changes.append({
                        'time': curr_row['datetime'],
                        'from_tower': prev_tower,
                        'to_tower': curr_tower,
                        'time_difference_minutes': time_diff
                    })
        
        return rapid_changes[:10]  # Limit to 10
    
    def _calculate_risk_score(self, patterns: Dict[str, any]) -> Dict[str, any]:
        """Calculate overall risk score based on detected patterns"""
        risk_score = 0.0
        risk_factors = []
        risk_level = "LOW"
        
        weights = self.config.risk_weights
        
        # Device switching risk
        if patterns['device_patterns']['device_switching_detected']:
            device_score = weights['device_switching']
            risk_score += device_score
            risk_factors.append(f"Multiple devices detected ({patterns['device_patterns']['imei_count']} IMEIs)")
        
        # Odd hour activity risk
        odd_hour_pct = patterns['temporal_patterns']['odd_hour_percentage']
        if odd_hour_pct > self.config.odd_hour_threshold * 100:
            odd_hour_score = weights['odd_hours'] * (odd_hour_pct / 10)  # Scale by percentage
            risk_score += odd_hour_score
            risk_factors.append(f"High odd-hour activity ({odd_hour_pct:.1f}%)")
        
        # Voice-only communication risk
        if patterns['communication_patterns']['voice_only_behavior']:
            voice_score = weights['voice_only']
            risk_score += voice_score
            risk_factors.append("100% voice communication (no SMS)")
        
        # High frequency patterns risk
        if len(patterns['temporal_patterns']['call_bursts']) > 0:
            freq_score = weights['high_frequency']
            risk_score += freq_score
            risk_factors.append(f"Call burst patterns detected ({len(patterns['temporal_patterns']['call_bursts'])} bursts)")
        
        # Normalize risk score to 0-100
        risk_score = min(risk_score * 100, 100)
        
        # Determine risk level
        if risk_score >= 70:
            risk_level = "HIGH"
        elif risk_score >= 40:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"
        
        return {
            'risk_score': round(risk_score, 2),
            'risk_level': risk_level,
            'risk_factors': risk_factors,
            'primary_concern': risk_factors[0] if risk_factors else "No significant risks detected"
        }