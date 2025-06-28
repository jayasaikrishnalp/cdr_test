"""
Device Identity Analysis Tool
Analyzes device patterns including IMEI/IMSI changes, SIM swapping, and device cloning
"""

from langchain.tools import BaseTool
from typing import Dict, Any, List, Optional, Set, Tuple
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
import numpy as np

class DeviceIdentityTool(BaseTool):
    """Tool for analyzing device identity patterns in tower dump data"""
    
    name: str = "device_identity_analysis"
    description: str = """Analyze device identity patterns in tower dump data. Use this tool to detect:
    - IMEI/IMSI associations and changes
    - SIM swapping patterns
    - Device cloning indicators
    - Stolen device usage
    - Multiple SIMs in same device
    
    Input examples: "analyze IMEI changes", "detect SIM swapping", "find device cloning", "identify stolen devices"
    """
    
    def __init__(self):
        super().__init__()
        
        # Thresholds for suspicious activity
        self.thresholds = {
            'suspicious_imei_changes': 3,  # IMEIs per SIM
            'suspicious_sim_changes': 5,   # SIMs per IMEI
            'rapid_switch_hours': 24,      # Hours for rapid switching
            'clone_time_window': 300,      # Seconds for simultaneous usage
            'stolen_device_days': 30       # Days to check for stolen devices
        }
    
    def _run(self, query: str) -> str:
        """Execute device identity analysis"""
        
        if not self.tower_dump_data:
            return "No tower dump data loaded. Please load tower dump data first."
        
        try:
            query_lower = query.lower()
            
            if "imei" in query_lower and "change" in query_lower:
                return self._analyze_imei_changes()
            elif "sim" in query_lower and ("swap" in query_lower or "switch" in query_lower):
                return self._analyze_sim_swapping()
            elif "clone" in query_lower or "cloning" in query_lower:
                return self._detect_device_cloning()
            elif "stolen" in query_lower:
                return self._detect_stolen_devices()
            elif "multiple" in query_lower:
                return self._analyze_multiple_identities()
            else:
                return self._comprehensive_device_analysis()
                
        except Exception as e:
            logger.error(f"Error in device identity analysis: {str(e)}")
            return f"Error analyzing device identity: {str(e)}"
    
    async def _arun(self, query: str) -> str:
        """Async version"""
        return self._run(query)
    
    def _analyze_imei_changes(self) -> str:
        """Analyze IMEI changes per mobile number"""
        
        results = []
        results.append("📱 IMEI CHANGE ANALYSIS")
        results.append("=" * 80)
        
        imei_changes = {}
        rapid_changes = []
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'imei', 'timestamp']):
                continue
            
            # Group by mobile number
            for number, group in df.groupby('mobile_number'):
                # Get unique IMEIs with timestamps
                imei_data = group[['imei', 'timestamp']].dropna(subset=['imei'])
                
                if len(imei_data) == 0:
                    continue
                
                unique_imeis = imei_data['imei'].unique()
                
                if len(unique_imeis) > 1:
                    # Analyze IMEI changes
                    imei_timeline = []
                    
                    for imei in unique_imeis:
                        imei_records = imei_data[imei_data['imei'] == imei]
                        imei_timeline.append({
                            'imei': imei,
                            'first_seen': imei_records['timestamp'].min(),
                            'last_seen': imei_records['timestamp'].max(),
                            'count': len(imei_records)
                        })
                    
                    # Sort by first seen
                    imei_timeline.sort(key=lambda x: x['first_seen'])
                    
                    # Check for rapid changes
                    for i in range(1, len(imei_timeline)):
                        prev_imei = imei_timeline[i-1]
                        curr_imei = imei_timeline[i]
                        
                        time_gap = (curr_imei['first_seen'] - prev_imei['last_seen']).total_seconds() / 3600
                        
                        if 0 <= time_gap <= self.thresholds['rapid_switch_hours']:
                            rapid_changes.append({
                                'number': number,
                                'from_imei': prev_imei['imei'],
                                'to_imei': curr_imei['imei'],
                                'gap_hours': time_gap,
                                'timestamp': curr_imei['first_seen']
                            })
                    
                    imei_changes[number] = {
                        'imei_count': len(unique_imeis),
                        'imeis': list(unique_imeis),
                        'timeline': imei_timeline,
                        'total_records': len(group)
                    }
        
        # Report findings
        if imei_changes:
            # Sort by IMEI count
            sorted_changes = sorted(
                imei_changes.items(),
                key=lambda x: x[1]['imei_count'],
                reverse=True
            )
            
            # High-risk cases
            high_risk = [
                (num, data) for num, data in sorted_changes
                if data['imei_count'] >= self.thresholds['suspicious_imei_changes']
            ]
            
            if high_risk:
                results.append(f"\n🔴 HIGH-RISK IMEI CHANGES ({len(high_risk)} numbers):")
                
                for number, data in high_risk[:5]:
                    results.append(f"\n📱 {number}")
                    results.append(f"   IMEIs Used: {data['imei_count']}")
                    results.append(f"   Total Records: {data['total_records']}")
                    results.append("   IMEI Timeline:")
                    
                    for item in data['timeline'][:3]:
                        results.append(f"     • {item['imei'][:8]}...")
                        results.append(f"       First: {item['first_seen']}")
                        results.append(f"       Last: {item['last_seen']}")
                        results.append(f"       Records: {item['count']}")
            
            # Rapid changes
            if rapid_changes:
                results.append(f"\n⚡ RAPID IMEI CHANGES ({len(rapid_changes)} instances):")
                results.append("   ⚠️ Device changed within 24 hours - highly suspicious")
                
                for change in rapid_changes[:5]:
                    results.append(f"\n   📱 {change['number']}")
                    results.append(f"      {change['from_imei'][:8]}... → {change['to_imei'][:8]}...")
                    results.append(f"      Gap: {change['gap_hours']:.1f} hours")
                    results.append(f"      Time: {change['timestamp']}")
        
        # Summary statistics
        total_with_changes = len(imei_changes)
        avg_imeis = np.mean([data['imei_count'] for data in imei_changes.values()]) if imei_changes else 0
        
        results.append(f"\n📊 SUMMARY:")
        results.append(f"   Numbers with IMEI changes: {total_with_changes}")
        results.append(f"   Average IMEIs per number: {avg_imeis:.1f}")
        results.append(f"   Rapid changes detected: {len(rapid_changes)}")
        
        return "\n".join(results)
    
    def _analyze_sim_swapping(self) -> str:
        """Analyze SIM swapping patterns"""
        
        results = []
        results.append("🔄 SIM SWAPPING ANALYSIS")
        results.append("=" * 80)
        
        sim_swapping = {}
        device_sharing = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'imei', 'imsi', 'timestamp']):
                continue
            
            # Analyze IMEI-IMSI associations
            imei_imsi_data = df[['imei', 'imsi', 'mobile_number', 'timestamp']].dropna(subset=['imei', 'imsi'])
            
            # Group by IMEI to find multiple SIMs
            for imei, imei_group in imei_imsi_data.groupby('imei'):
                unique_imsis = imei_group['imsi'].unique()
                unique_numbers = imei_group['mobile_number'].unique()
                
                if len(unique_imsis) > 1 or len(unique_numbers) > 1:
                    # Device used with multiple SIMs
                    sim_timeline = []
                    
                    for imsi in unique_imsis:
                        imsi_records = imei_group[imei_group['imsi'] == imsi]
                        numbers_with_imsi = imsi_records['mobile_number'].unique()
                        
                        sim_timeline.append({
                            'imsi': imsi,
                            'numbers': list(numbers_with_imsi),
                            'first_seen': imsi_records['timestamp'].min(),
                            'last_seen': imsi_records['timestamp'].max(),
                            'count': len(imsi_records)
                        })
                    
                    device_sharing[imei] = {
                        'sim_count': len(unique_imsis),
                        'number_count': len(unique_numbers),
                        'timeline': sorted(sim_timeline, key=lambda x: x['first_seen']),
                        'total_records': len(imei_group)
                    }
            
            # Group by mobile number to find SIM swapping
            for number, number_group in imei_imsi_data.groupby('mobile_number'):
                unique_imsis = number_group['imsi'].unique()
                
                if len(unique_imsis) > 1:
                    # Multiple IMSIs for same number (SIM swap)
                    imsi_timeline = []
                    
                    for imsi in unique_imsis:
                        imsi_records = number_group[number_group['imsi'] == imsi]
                        imsi_timeline.append({
                            'imsi': imsi,
                            'first_seen': imsi_records['timestamp'].min(),
                            'last_seen': imsi_records['timestamp'].max(),
                            'count': len(imsi_records),
                            'imeis': list(imsi_records['imei'].unique())
                        })
                    
                    sim_swapping[number] = {
                        'imsi_count': len(unique_imsis),
                        'timeline': sorted(imsi_timeline, key=lambda x: x['first_seen'])
                    }
        
        # Report findings
        if device_sharing:
            results.append(f"\n🔴 DEVICES WITH MULTIPLE SIMS ({len(device_sharing)}):")
            
            # Sort by SIM count
            sorted_devices = sorted(
                device_sharing.items(),
                key=lambda x: x[1]['sim_count'],
                reverse=True
            )
            
            for imei, data in sorted_devices[:5]:
                results.append(f"\n📱 IMEI: {imei[:8]}...")
                results.append(f"   SIMs Used: {data['sim_count']}")
                results.append(f"   Numbers Used: {data['number_count']}")
                results.append("   SIM Timeline:")
                
                for item in data['timeline'][:3]:
                    results.append(f"     • IMSI: {item['imsi'][:8]}...")
                    results.append(f"       Numbers: {', '.join(item['numbers'])}")
                    results.append(f"       Period: {item['first_seen'].date()} to {item['last_seen'].date()}")
        
        if sim_swapping:
            results.append(f"\n🔄 SIM REPLACEMENT DETECTED ({len(sim_swapping)} numbers):")
            
            for number, data in list(sim_swapping.items())[:5]:
                results.append(f"\n📱 {number}")
                results.append(f"   SIM Cards Used: {data['imsi_count']}")
                
                # Check for rapid swapping
                rapid_swap = False
                for i in range(1, len(data['timeline'])):
                    time_gap = (data['timeline'][i]['first_seen'] - 
                              data['timeline'][i-1]['last_seen']).days
                    if 0 <= time_gap <= 1:
                        rapid_swap = True
                        break
                
                if rapid_swap:
                    results.append("   ⚠️ RAPID SIM SWAP DETECTED")
        
        # Patterns detected
        results.append("\n🎯 PATTERNS IDENTIFIED:")
        
        if any(d['sim_count'] >= self.thresholds['suspicious_sim_changes'] for d in device_sharing.values()):
            results.append("  • Devices with excessive SIM changes (possible reseller/criminal use)")
        
        if any(d['number_count'] > d['sim_count'] for d in device_sharing.values()):
            results.append("  • SIM cards shared between multiple numbers (identity masking)")
        
        return "\n".join(results)
    
    def _detect_device_cloning(self) -> str:
        """Detect potential device cloning"""
        
        results = []
        results.append("👥 DEVICE CLONING DETECTION")
        results.append("=" * 80)
        
        cloning_suspects = []
        simultaneous_usage = []
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['imei', 'timestamp', 'tower_id']):
                continue
            
            # Group by IMEI
            for imei, imei_group in df.groupby('imei'):
                if pd.isna(imei) or len(imei_group) < 2:
                    continue
                
                # Sort by timestamp
                sorted_group = imei_group.sort_values('timestamp')
                
                # Check for simultaneous usage at different towers
                for i in range(len(sorted_group) - 1):
                    curr_record = sorted_group.iloc[i]
                    next_record = sorted_group.iloc[i + 1]
                    
                    time_diff = (next_record['timestamp'] - curr_record['timestamp']).total_seconds()
                    
                    # Same time, different towers = cloning indicator
                    if (time_diff <= self.thresholds['clone_time_window'] and 
                        curr_record['tower_id'] != next_record['tower_id']):
                        
                        # Calculate distance if location available
                        distance = None
                        if all(col in curr_record for col in ['tower_lat', 'tower_long']):
                            distance = self._calculate_distance(
                                curr_record['tower_lat'], curr_record['tower_long'],
                                next_record['tower_lat'], next_record['tower_long']
                            )
                        
                        simultaneous_usage.append({
                            'imei': imei,
                            'time1': curr_record['timestamp'],
                            'time2': next_record['timestamp'],
                            'tower1': curr_record['tower_id'],
                            'tower2': next_record['tower_id'],
                            'time_diff': time_diff,
                            'distance': distance,
                            'numbers': [curr_record.get('mobile_number'), 
                                      next_record.get('mobile_number')]
                        })
                
                # Check for impossible travel between towers
                if 'tower_lat' in sorted_group.columns:
                    for i in range(1, len(sorted_group)):
                        prev = sorted_group.iloc[i-1]
                        curr = sorted_group.iloc[i]
                        
                        if (pd.notna(prev['tower_lat']) and pd.notna(curr['tower_lat']) and
                            prev['tower_id'] != curr['tower_id']):
                            
                            distance = self._calculate_distance(
                                prev['tower_lat'], prev['tower_long'],
                                curr['tower_lat'], curr['tower_long']
                            )
                            
                            time_diff = (curr['timestamp'] - prev['timestamp']).total_seconds() / 3600
                            
                            if time_diff > 0:
                                speed = distance / time_diff
                                
                                # Impossible speed = potential cloning
                                if speed > 500:  # 500 km/h threshold
                                    cloning_suspects.append({
                                        'imei': imei,
                                        'timestamp': curr['timestamp'],
                                        'speed_kmh': speed,
                                        'distance_km': distance,
                                        'time_hours': time_diff,
                                        'towers': [prev['tower_id'], curr['tower_id']]
                                    })
        
        # Report findings
        if simultaneous_usage:
            results.append(f"\n🔴 SIMULTANEOUS USAGE DETECTED ({len(simultaneous_usage)} instances):")
            results.append("   ⚠️ Same IMEI at different towers within 5 minutes")
            
            # Group by IMEI
            imei_incidents = {}
            for incident in simultaneous_usage:
                imei = incident['imei']
                if imei not in imei_incidents:
                    imei_incidents[imei] = []
                imei_incidents[imei].append(incident)
            
            # Sort by incident count
            sorted_imeis = sorted(
                imei_incidents.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            
            for imei, incidents in sorted_imeis[:3]:
                results.append(f"\n📱 IMEI: {imei[:8]}...")
                results.append(f"   Clone Incidents: {len(incidents)}")
                
                # Show latest incident
                latest = max(incidents, key=lambda x: x['time1'])
                results.append(f"   Latest Incident:")
                results.append(f"     Time: {latest['time1']}")
                results.append(f"     Towers: {latest['tower1']} & {latest['tower2']}")
                if latest['distance']:
                    results.append(f"     Distance: {latest['distance']:.1f} km")
                results.append(f"     Gap: {latest['time_diff']:.0f} seconds")
        
        if cloning_suspects:
            results.append(f"\n⚡ IMPOSSIBLE TRAVEL DETECTED ({len(cloning_suspects)} instances):")
            
            for suspect in cloning_suspects[:5]:
                results.append(f"\n📱 IMEI: {suspect['imei'][:8]}...")
                results.append(f"   Speed: {suspect['speed_kmh']:.0f} km/h")
                results.append(f"   Distance: {suspect['distance_km']:.1f} km in {suspect['time_hours']:.1f} hours")
                results.append(f"   Towers: {suspect['towers'][0]} → {suspect['towers'][1]}")
        
        # Summary
        unique_cloned_imeis = set()
        if simultaneous_usage:
            unique_cloned_imeis.update(inc['imei'] for inc in simultaneous_usage)
        if cloning_suspects:
            unique_cloned_imeis.update(inc['imei'] for inc in cloning_suspects)
        
        results.append(f"\n📊 SUMMARY:")
        results.append(f"   Potentially Cloned IMEIs: {len(unique_cloned_imeis)}")
        results.append(f"   Simultaneous Usage Events: {len(simultaneous_usage)}")
        results.append(f"   Impossible Travel Events: {len(cloning_suspects)}")
        
        return "\n".join(results)
    
    def _detect_stolen_devices(self) -> str:
        """Detect potential stolen device usage"""
        
        results = []
        results.append("🚨 STOLEN DEVICE DETECTION")
        results.append("=" * 80)
        
        suspicious_devices = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['imei', 'mobile_number', 'timestamp']):
                continue
            
            # Group by IMEI
            for imei, imei_group in df.groupby('imei'):
                if pd.isna(imei):
                    continue
                
                # Get unique numbers using this IMEI
                number_timeline = []
                
                for number, number_group in imei_group.groupby('mobile_number'):
                    number_timeline.append({
                        'number': number,
                        'first_seen': number_group['timestamp'].min(),
                        'last_seen': number_group['timestamp'].max(),
                        'count': len(number_group)
                    })
                
                # Sort by first seen
                number_timeline.sort(key=lambda x: x['first_seen'])
                
                # Check for suspicious patterns
                if len(number_timeline) >= 2:
                    # Sudden number change after period of regular use
                    for i in range(1, len(number_timeline)):
                        prev_user = number_timeline[i-1]
                        curr_user = number_timeline[i]
                        
                        # Previous user had substantial usage
                        prev_duration = (prev_user['last_seen'] - prev_user['first_seen']).days
                        
                        if (prev_duration >= 30 and prev_user['count'] >= 100):
                            # Sudden switch to new number
                            gap_days = (curr_user['first_seen'] - prev_user['last_seen']).days
                            
                            if 0 <= gap_days <= self.thresholds['stolen_device_days']:
                                suspicious_devices[imei] = {
                                    'original_number': prev_user['number'],
                                    'original_usage_days': prev_duration,
                                    'original_records': prev_user['count'],
                                    'last_seen_original': prev_user['last_seen'],
                                    'new_number': curr_user['number'],
                                    'first_seen_new': curr_user['first_seen'],
                                    'gap_days': gap_days,
                                    'subsequent_numbers': [t['number'] for t in number_timeline[i:]]
                                }
        
        # Report findings
        if suspicious_devices:
            results.append(f"\n🔴 POTENTIALLY STOLEN DEVICES: {len(suspicious_devices)}")
            
            # Sort by gap days (shorter gap = more suspicious)
            sorted_devices = sorted(
                suspicious_devices.items(),
                key=lambda x: x[1]['gap_days']
            )
            
            for imei, data in sorted_devices[:5]:
                results.append(f"\n📱 IMEI: {imei[:8]}...")
                results.append(f"   Original User: {data['original_number']}")
                results.append(f"   Usage Period: {data['original_usage_days']} days")
                results.append(f"   Last Seen: {data['last_seen_original'].date()}")
                results.append(f"\n   🔄 Device Reappeared After {data['gap_days']} days")
                results.append(f"   New User: {data['new_number']}")
                results.append(f"   First Seen: {data['first_seen_new'].date()}")
                
                if len(data['subsequent_numbers']) > 1:
                    results.append(f"   ⚠️ Multiple subsequent users: {len(data['subsequent_numbers'])}")
        
        # Additional indicators
        results.append("\n🎯 THEFT INDICATORS:")
        results.append("  • Long-term device suddenly switches numbers")
        results.append("  • Brief gap between last use and new user")
        results.append("  • Multiple rapid user changes after switch")
        results.append("  • Geographic shift in usage patterns")
        
        return "\n".join(results)
    
    def _analyze_multiple_identities(self) -> str:
        """Analyze devices and numbers with multiple identities"""
        
        results = []
        results.append("🎭 MULTIPLE IDENTITY ANALYSIS")
        results.append("=" * 80)
        
        # Collect all identity associations
        identity_map = {
            'numbers_per_imei': {},
            'imeis_per_number': {},
            'imsis_per_imei': {},
            'imeis_per_imsi': {}
        }
        
        for dump_id, df in self.tower_dump_data.items():
            # Numbers per IMEI
            if all(col in df.columns for col in ['imei', 'mobile_number']):
                imei_number_data = df[['imei', 'mobile_number']].dropna()
                for imei, group in imei_number_data.groupby('imei'):
                    numbers = group['mobile_number'].unique()
                    if len(numbers) > 1:
                        identity_map['numbers_per_imei'][imei] = list(numbers)
            
            # IMEIs per number
            if all(col in df.columns for col in ['mobile_number', 'imei']):
                number_imei_data = df[['mobile_number', 'imei']].dropna()
                for number, group in number_imei_data.groupby('mobile_number'):
                    imeis = group['imei'].unique()
                    if len(imeis) > 1:
                        identity_map['imeis_per_number'][number] = list(imeis)
            
            # IMSIs per IMEI
            if all(col in df.columns for col in ['imei', 'imsi']):
                imei_imsi_data = df[['imei', 'imsi']].dropna()
                for imei, group in imei_imsi_data.groupby('imei'):
                    imsis = group['imsi'].unique()
                    if len(imsis) > 1:
                        identity_map['imsis_per_imei'][imei] = list(imsis)
        
        # Report findings
        results.append("\n📊 IDENTITY MULTIPLICITY OVERVIEW:")
        
        # Devices with multiple users
        if identity_map['numbers_per_imei']:
            results.append(f"\n🔄 Devices with Multiple Users: {len(identity_map['numbers_per_imei'])}")
            
            # Find most shared devices
            sorted_devices = sorted(
                identity_map['numbers_per_imei'].items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            
            for imei, numbers in sorted_devices[:3]:
                results.append(f"   📱 {imei[:8]}...: {len(numbers)} users")
                results.append(f"      Users: {', '.join(numbers[:3])}...")
        
        # Numbers with multiple devices
        if identity_map['imeis_per_number']:
            results.append(f"\n📱 Numbers with Multiple Devices: {len(identity_map['imeis_per_number'])}")
            
            # High-risk cases
            high_risk = [
                (num, imeis) for num, imeis in identity_map['imeis_per_number'].items()
                if len(imeis) >= self.thresholds['suspicious_imei_changes']
            ]
            
            if high_risk:
                results.append(f"   🔴 High Risk ({len(high_risk)} numbers with 3+ devices):")
                for number, imeis in high_risk[:3]:
                    results.append(f"      {number}: {len(imeis)} devices")
        
        # Complex identity networks
        results.append("\n🕸️ IDENTITY NETWORK COMPLEXITY:")
        
        # Find numbers that share devices
        device_sharing_network = {}
        for imei, numbers in identity_map['numbers_per_imei'].items():
            for i, num1 in enumerate(numbers):
                for num2 in numbers[i+1:]:
                    pair = tuple(sorted([num1, num2]))
                    if pair not in device_sharing_network:
                        device_sharing_network[pair] = []
                    device_sharing_network[pair].append(imei)
        
        if device_sharing_network:
            results.append(f"   Pairs sharing devices: {len(device_sharing_network)}")
            
            # Most connected pairs
            sorted_pairs = sorted(
                device_sharing_network.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            
            for pair, shared_imeis in sorted_pairs[:3]:
                results.append(f"   • {pair[0]} ↔ {pair[1]}: {len(shared_imeis)} shared devices")
        
        return "\n".join(results)
    
    def _comprehensive_device_analysis(self) -> str:
        """Provide comprehensive device identity analysis"""
        
        results = []
        results.append("📱 COMPREHENSIVE DEVICE IDENTITY ANALYSIS")
        results.append("=" * 80)
        
        # Gather statistics
        total_imeis = set()
        total_imsis = set()
        total_numbers = set()
        
        for dump_id, df in self.tower_dump_data.items():
            if 'imei' in df.columns:
                total_imeis.update(df['imei'].dropna().unique())
            if 'imsi' in df.columns:
                total_imsis.update(df['imsi'].dropna().unique())
            if 'mobile_number' in df.columns:
                total_numbers.update(df['mobile_number'].unique())
        
        results.append(f"\n📊 OVERALL STATISTICS:")
        results.append(f"   Unique IMEIs: {len(total_imeis)}")
        results.append(f"   Unique IMSIs: {len(total_imsis)}")
        results.append(f"   Unique Numbers: {len(total_numbers)}")
        
        # Run key analyses
        key_findings = []
        
        # IMEI changes
        imei_analysis = self._analyze_imei_changes()
        if "HIGH-RISK" in imei_analysis:
            key_findings.append("🔴 High-risk IMEI changes detected")
        
        # SIM swapping
        sim_analysis = self._analyze_sim_swapping()
        if "MULTIPLE SIMS" in sim_analysis:
            key_findings.append("🔄 Devices with multiple SIMs found")
        
        # Device cloning
        clone_analysis = self._detect_device_cloning()
        if "SIMULTANEOUS USAGE" in clone_analysis:
            key_findings.append("👥 Potential device cloning detected")
        
        # Stolen devices
        stolen_analysis = self._detect_stolen_devices()
        if "STOLEN DEVICES" in stolen_analysis:
            key_findings.append("🚨 Potentially stolen devices identified")
        
        if key_findings:
            results.append("\n🎯 KEY FINDINGS:")
            for finding in key_findings:
                results.append(f"   {finding}")
        
        return "\n".join(results)
    
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