"""
Cross-Reference Tool
Links tower dump data with CDR and IPDR records for comprehensive analysis
"""

from langchain.tools import BaseTool
from typing import Dict, Any, List, Optional, Set, Tuple
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
import numpy as np
from collections import defaultdict

class CrossReferenceTool(BaseTool):
    """Tool for cross-referencing tower dump with CDR/IPDR data"""
    
    name: str = "cross_reference_analysis"
    description: str = """Cross-reference tower dump data with CDR and IPDR records. Use this tool to:
    - Link tower presence with call/data activity
    - Identify silent devices (present but no CDR/IPDR)
    - Find communication patterns at crime scenes
    - Correlate movement with communication
    - Build complete suspect profiles
    
    Input examples: "cross-reference with CDR", "find silent devices", "link tower and calls", "build suspect profile"
    """
    
    # Class attributes for Pydantic v2
    tower_dump_data: Dict[str, Any] = {}
    cdr_data: Dict[str, Any] = {}
    ipdr_data: Dict[str, Any] = {}
    params: Dict[str, Any] = {}
    
    def __init__(self):
        super().__init__()
        
        # Cross-reference parameters
        self.params = {
            'time_tolerance_minutes': 5,    # Time tolerance for matching
            'location_match_radius': 2.0,   # km for location matching
            'silent_threshold_hours': 24,   # Hours to consider device silent
            'correlation_window': 3600      # Seconds for correlation window
        }
    
    def _run(self, query: str) -> str:
        """Execute cross-reference analysis"""
        
        if not self.tower_dump_data:
            return "No tower dump data loaded. Please load tower dump data first."
        
        try:
            query_lower = query.lower()
            
            if "silent" in query_lower:
                return self._find_silent_devices()
            elif "cdr" in query_lower and "link" in query_lower:
                return self._link_tower_cdr()
            elif "ipdr" in query_lower and "link" in query_lower:
                return self._link_tower_ipdr()
            elif "profile" in query_lower:
                return self._build_suspect_profiles()
            elif "communication" in query_lower and "scene" in query_lower:
                return self._analyze_crime_scene_communications()
            elif "pattern" in query_lower:
                return self._find_communication_patterns()
            else:
                return self._comprehensive_cross_reference()
                
        except Exception as e:
            logger.error(f"Error in cross-reference analysis: {str(e)}")
            return f"Error cross-referencing data: {str(e)}"
    
    async def _arun(self, query: str) -> str:
        """Async version"""
        return self._run(query)
    
    def _find_silent_devices(self) -> str:
        """Find devices present in tower dump but silent in CDR/IPDR"""
        
        results = []
        results.append("🔇 SILENT DEVICE DETECTION")
        results.append("=" * 80)
        
        # Collect all devices from tower dump
        tower_devices = set()
        tower_device_times = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' in df.columns:
                devices = df['mobile_number'].unique()
                tower_devices.update(devices)
                
                # Track time ranges for each device
                if 'timestamp' in df.columns:
                    for device in devices:
                        device_data = df[df['mobile_number'] == device]
                        if device not in tower_device_times:
                            tower_device_times[device] = []
                        
                        tower_device_times[device].append({
                            'start': device_data['timestamp'].min(),
                            'end': device_data['timestamp'].max(),
                            'records': len(device_data)
                        })
        
        # Collect devices with CDR activity
        cdr_devices = set()
        cdr_activity = {}
        
        if self.cdr_data:
            for suspect, cdr_df in self.cdr_data.items():
                if 'a_party' in cdr_df.columns:
                    numbers = cdr_df['a_party'].unique()
                    cdr_devices.update(numbers)
                    
                    # Track CDR activity times
                    if 'datetime' in cdr_df.columns:
                        for number in numbers:
                            if number not in cdr_activity:
                                cdr_activity[number] = []
                            
                            number_cdrs = cdr_df[cdr_df['a_party'] == number]
                            cdr_activity[number].append({
                                'start': number_cdrs['datetime'].min(),
                                'end': number_cdrs['datetime'].max(),
                                'call_count': len(number_cdrs)
                            })
        
        # Collect devices with IPDR activity
        ipdr_devices = set()
        ipdr_activity = {}
        
        if self.ipdr_data:
            for suspect, ipdr_df in self.ipdr_data.items():
                # Extract phone number from suspect name or IPDR data
                if 'subscriber_id' in ipdr_df.columns:
                    numbers = ipdr_df['subscriber_id'].unique()
                    ipdr_devices.update(numbers)
                    
                    # Track IPDR activity
                    if 'start_time' in ipdr_df.columns:
                        for number in numbers:
                            if number not in ipdr_activity:
                                ipdr_activity[number] = []
                            
                            number_ipdr = ipdr_df[ipdr_df['subscriber_id'] == number]
                            ipdr_activity[number].append({
                                'start': number_ipdr['start_time'].min(),
                                'end': number_ipdr['start_time'].max(),
                                'session_count': len(number_ipdr)
                            })
        
        # Find silent devices
        completely_silent = tower_devices - cdr_devices - ipdr_devices
        cdr_silent = tower_devices - cdr_devices
        ipdr_silent = tower_devices - ipdr_devices
        
        # Categorize silent devices
        silent_categories = {
            'completely_silent': [],
            'cdr_only_silent': [],
            'ipdr_only_silent': [],
            'intermittent_silent': []
        }
        
        # Analyze completely silent devices
        for device in completely_silent:
            if device in tower_device_times:
                time_info = tower_device_times[device][0]  # First occurrence
                silent_categories['completely_silent'].append({
                    'device': device,
                    'tower_records': sum(t['records'] for t in tower_device_times[device]),
                    'first_seen': time_info['start'],
                    'last_seen': time_info['end'],
                    'duration_hours': (time_info['end'] - time_info['start']).total_seconds() / 3600
                })
        
        # Report findings
        if silent_categories['completely_silent']:
            results.append(f"\n🔴 COMPLETELY SILENT DEVICES: {len(silent_categories['completely_silent'])}")
            results.append("   ⚠️ Present in area but no calls or data usage")
            
            # Sort by tower records
            sorted_silent = sorted(
                silent_categories['completely_silent'],
                key=lambda x: x['tower_records'],
                reverse=True
            )
            
            for device_info in sorted_silent[:5]:
                results.append(f"\n📱 {device_info['device']}")
                results.append(f"   Tower Records: {device_info['tower_records']}")
                results.append(f"   Duration: {device_info['duration_hours']:.1f} hours")
                results.append(f"   Period: {device_info['first_seen']} to {device_info['last_seen']}")
        
        # CDR-only silent (has IPDR but no calls)
        data_only_devices = ipdr_devices - cdr_devices
        if data_only_devices & tower_devices:
            results.append(f"\n🟡 DATA-ONLY DEVICES: {len(data_only_devices & tower_devices)}")
            results.append("   📱 Using data but making no calls")
            results.append("   ⚠️ May indicate encrypted app communication")
        
        # Voice-only devices (has CDR but no IPDR)
        voice_only_devices = cdr_devices - ipdr_devices
        if voice_only_devices & tower_devices:
            results.append(f"\n🟡 VOICE-ONLY DEVICES: {len(voice_only_devices & tower_devices)}")
            results.append("   📞 Making calls but no data usage")
            results.append("   ⚠️ May indicate operational security behavior")
        
        # Suspicious silent patterns
        results.append("\n🎯 SILENT DEVICE PATTERNS:")
        
        # Brief silent presence
        brief_silent = [
            d for d in silent_categories['completely_silent']
            if d['duration_hours'] < 1 and d['tower_records'] < 5
        ]
        
        if brief_silent:
            results.append(f"\n  • Brief Silent Presence: {len(brief_silent)} devices")
            results.append("    Appeared briefly with no communication")
            results.append("    ⚠️ Possible surveillance or reconnaissance")
        
        return "\n".join(results)
    
    def _link_tower_cdr(self) -> str:
        """Link tower dump presence with CDR activity"""
        
        results = []
        results.append("🔗 TOWER-CDR LINKAGE ANALYSIS")
        results.append("=" * 80)
        
        linked_activities = []
        
        # For each device in tower dump
        for dump_id, tower_df in self.tower_dump_data.items():
            if not all(col in tower_df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                continue
            
            # Check against CDR data
            for suspect, cdr_df in self.cdr_data.items():
                if not all(col in cdr_df.columns for col in ['a_party', 'datetime']):
                    continue
                
                # Find matching numbers
                tower_numbers = set(tower_df['mobile_number'].unique())
                cdr_numbers = set(cdr_df['a_party'].unique())
                
                common_numbers = tower_numbers & cdr_numbers
                
                for number in common_numbers:
                    # Get tower presence times
                    tower_presence = tower_df[tower_df['mobile_number'] == number]
                    
                    # Get CDR activity
                    cdr_activity = cdr_df[cdr_df['a_party'] == number]
                    
                    # Find temporal correlations
                    for _, tower_record in tower_presence.iterrows():
                        tower_time = tower_record['timestamp']
                        tower_id = tower_record['tower_id']
                        
                        # Find CDRs within time window
                        time_window_start = tower_time - timedelta(minutes=self.params['time_tolerance_minutes'])
                        time_window_end = tower_time + timedelta(minutes=self.params['time_tolerance_minutes'])
                        
                        matching_cdrs = cdr_activity[
                            (cdr_activity['datetime'] >= time_window_start) &
                            (cdr_activity['datetime'] <= time_window_end)
                        ]
                        
                        if len(matching_cdrs) > 0:
                            for _, cdr in matching_cdrs.iterrows():
                                linked_activities.append({
                                    'number': number,
                                    'tower_id': tower_id,
                                    'tower_time': tower_time,
                                    'cdr_time': cdr['datetime'],
                                    'b_party': cdr.get('b_party', 'Unknown'),
                                    'duration': cdr.get('duration', 0),
                                    'time_diff': abs((cdr['datetime'] - tower_time).total_seconds()),
                                    'call_type': cdr.get('call_type', 'voice')
                                })
        
        # Analyze linked activities
        if linked_activities:
            results.append(f"\n📊 LINKED ACTIVITIES FOUND: {len(linked_activities)}")
            
            # Group by number
            number_links = defaultdict(list)
            for link in linked_activities:
                number_links[link['number']].append(link)
            
            # Sort by link count
            sorted_numbers = sorted(
                number_links.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            
            results.append("\n🔗 DEVICES WITH TOWER-CDR CORRELATION:")
            
            for number, links in sorted_numbers[:5]:
                results.append(f"\n📱 {number}")
                results.append(f"   Correlated Activities: {len(links)}")
                
                # Analyze patterns
                towers_used = set(l['tower_id'] for l in links)
                contacts = set(l['b_party'] for l in links if l['b_party'] != 'Unknown')
                
                results.append(f"   Towers Used: {len(towers_used)}")
                results.append(f"   Unique Contacts: {len(contacts)}")
                
                # Show sample activity
                sample = links[0]
                results.append(f"   Sample Link:")
                results.append(f"     Tower: {sample['tower_id']} at {sample['tower_time']}")
                results.append(f"     Call to {sample['b_party']} at {sample['cdr_time']}")
                results.append(f"     Duration: {sample['duration']}s")
        
        # Find calls without tower presence
        results.append("\n🚨 ANOMALOUS PATTERNS:")
        
        # CDR activity without tower dump presence
        ghost_calls = []
        
        for suspect, cdr_df in self.cdr_data.items():
            if 'a_party' not in cdr_df.columns:
                continue
            
            for number in cdr_df['a_party'].unique():
                # Check if number appears in tower dump
                appears_in_tower = False
                
                for dump_id, tower_df in self.tower_dump_data.items():
                    if 'mobile_number' in tower_df.columns:
                        if number in tower_df['mobile_number'].values:
                            appears_in_tower = True
                            break
                
                if not appears_in_tower:
                    ghost_calls.append({
                        'number': number,
                        'call_count': len(cdr_df[cdr_df['a_party'] == number])
                    })
        
        if ghost_calls:
            results.append(f"\n  • Ghost Calls: {len(ghost_calls)} numbers")
            results.append("    Made calls but not in tower dump")
            results.append("    ⚠️ Possible data inconsistency or advanced evasion")
        
        return "\n".join(results)
    
    def _link_tower_ipdr(self) -> str:
        """Link tower dump presence with IPDR activity"""
        
        results = []
        results.append("🔗 TOWER-IPDR LINKAGE ANALYSIS")
        results.append("=" * 80)
        
        linked_sessions = []
        
        # For each device in tower dump
        for dump_id, tower_df in self.tower_dump_data.items():
            if not all(col in tower_df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                continue
            
            # Check against IPDR data
            for suspect, ipdr_df in self.ipdr_data.items():
                # Try to extract phone number from IPDR
                if 'subscriber_id' in ipdr_df.columns:
                    ipdr_numbers = set(ipdr_df['subscriber_id'].unique())
                else:
                    # Extract from suspect name if possible
                    import re
                    match = re.search(r'\d{10}', suspect)
                    if match:
                        ipdr_numbers = {match.group()}
                    else:
                        continue
                
                # Find matching numbers
                tower_numbers = set(tower_df['mobile_number'].unique())
                common_numbers = tower_numbers & ipdr_numbers
                
                for number in common_numbers:
                    # Get tower presence
                    tower_presence = tower_df[tower_df['mobile_number'] == number]
                    
                    # Get IPDR sessions
                    if 'subscriber_id' in ipdr_df.columns:
                        ipdr_sessions = ipdr_df[ipdr_df['subscriber_id'] == number]
                    else:
                        ipdr_sessions = ipdr_df  # All sessions for this suspect
                    
                    # Find temporal correlations
                    for _, tower_record in tower_presence.iterrows():
                        tower_time = tower_record['timestamp']
                        tower_id = tower_record['tower_id']
                        
                        # Find IPDR sessions within window
                        if 'start_time' in ipdr_sessions.columns:
                            time_window_start = tower_time - timedelta(minutes=self.params['time_tolerance_minutes'])
                            time_window_end = tower_time + timedelta(minutes=self.params['time_tolerance_minutes'])
                            
                            matching_sessions = ipdr_sessions[
                                (ipdr_sessions['start_time'] >= time_window_start) &
                                (ipdr_sessions['start_time'] <= time_window_end)
                            ]
                            
                            for _, session in matching_sessions.iterrows():
                                linked_sessions.append({
                                    'number': number,
                                    'tower_id': tower_id,
                                    'tower_time': tower_time,
                                    'session_start': session['start_time'],
                                    'app': session.get('detected_app', 'Unknown'),
                                    'encrypted': session.get('is_encrypted', False),
                                    'data_volume': session.get('total_data_volume', 0),
                                    'time_diff': abs((session['start_time'] - tower_time).total_seconds())
                                })
        
        # Analyze linked sessions
        if linked_sessions:
            results.append(f"\n📊 LINKED DATA SESSIONS: {len(linked_sessions)}")
            
            # Encrypted app usage at towers
            encrypted_sessions = [s for s in linked_sessions if s['encrypted']]
            
            if encrypted_sessions:
                results.append(f"\n🔐 ENCRYPTED APP USAGE AT TOWERS: {len(encrypted_sessions)}")
                
                # Group by app
                app_usage = defaultdict(list)
                for session in encrypted_sessions:
                    app_usage[session['app']].append(session)
                
                for app, sessions in app_usage.items():
                    results.append(f"\n   {app}: {len(sessions)} sessions")
                    
                    # Show towers where app was used
                    app_towers = set(s['tower_id'] for s in sessions)
                    results.append(f"   Towers: {', '.join(list(app_towers)[:5])}")
            
            # High data usage at specific towers
            high_data_sessions = [
                s for s in linked_sessions 
                if s['data_volume'] > 10 * 1024 * 1024  # 10MB
            ]
            
            if high_data_sessions:
                results.append(f"\n📊 HIGH DATA USAGE LOCATIONS: {len(high_data_sessions)}")
                
                # Group by tower
                tower_data = defaultdict(float)
                for session in high_data_sessions:
                    tower_data[session['tower_id']] += session['data_volume']
                
                # Sort by data volume
                sorted_towers = sorted(
                    tower_data.items(),
                    key=lambda x: x[1],
                    reverse=True
                )
                
                for tower, volume in sorted_towers[:3]:
                    results.append(f"   Tower {tower}: {volume / (1024*1024):.1f} MB total")
        
        # Silent data patterns
        results.append("\n🎯 DATA BEHAVIOR PATTERNS:")
        
        # Devices with IPDR but no tower presence during sessions
        phantom_data = []
        
        for suspect, ipdr_df in self.ipdr_data.items():
            if 'start_time' not in ipdr_df.columns:
                continue
            
            # Check each session
            for _, session in ipdr_df.iterrows():
                session_time = session['start_time']
                
                # Check if device was in tower dump around this time
                found_in_tower = False
                
                for dump_id, tower_df in self.tower_dump_data.items():
                    if 'timestamp' in tower_df.columns:
                        time_matches = tower_df[
                            (tower_df['timestamp'] >= session_time - timedelta(minutes=5)) &
                            (tower_df['timestamp'] <= session_time + timedelta(minutes=5))
                        ]
                        
                        if len(time_matches) > 0:
                            found_in_tower = True
                            break
                
                if not found_in_tower and session.get('is_encrypted', False):
                    phantom_data.append({
                        'time': session_time,
                        'app': session.get('detected_app', 'Unknown'),
                        'volume': session.get('total_data_volume', 0)
                    })
        
        if phantom_data:
            results.append(f"\n  • Phantom Data Sessions: {len(phantom_data)}")
            results.append("    Encrypted data without tower presence")
            results.append("    ⚠️ May indicate VPN usage or data inconsistency")
        
        return "\n".join(results)
    
    def _build_suspect_profiles(self) -> str:
        """Build comprehensive suspect profiles combining all data sources"""
        
        results = []
        results.append("👤 COMPREHENSIVE SUSPECT PROFILES")
        results.append("=" * 80)
        
        # Build profiles for devices appearing in multiple data sources
        suspect_profiles = {}
        
        # Collect all unique numbers from all sources
        all_numbers = set()
        
        # From tower dump
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' in df.columns:
                all_numbers.update(df['mobile_number'].unique())
        
        # From CDR
        for suspect, df in self.cdr_data.items():
            if 'a_party' in df.columns:
                all_numbers.update(df['a_party'].unique())
        
        # Build profile for each number
        for number in all_numbers:
            profile = {
                'number': number,
                'tower_dump': {},
                'cdr': {},
                'ipdr': {},
                'risk_indicators': [],
                'timeline': []
            }
            
            # Tower dump analysis
            for dump_id, tower_df in self.tower_dump_data.items():
                if 'mobile_number' in tower_df.columns:
                    device_data = tower_df[tower_df['mobile_number'] == number]
                    
                    if len(device_data) > 0:
                        profile['tower_dump'] = {
                            'presence_count': len(device_data),
                            'unique_towers': device_data['tower_id'].nunique() if 'tower_id' in device_data.columns else 0,
                            'first_seen': device_data['timestamp'].min() if 'timestamp' in device_data.columns else None,
                            'last_seen': device_data['timestamp'].max() if 'timestamp' in device_data.columns else None,
                            'unique_imeis': device_data['imei'].nunique() if 'imei' in device_data.columns else 0
                        }
                        
                        # Add to timeline
                        if 'timestamp' in device_data.columns:
                            for _, record in device_data.iterrows():
                                profile['timeline'].append({
                                    'time': record['timestamp'],
                                    'type': 'tower',
                                    'details': f"Tower {record.get('tower_id', 'Unknown')}"
                                })
            
            # CDR analysis
            for suspect, cdr_df in self.cdr_data.items():
                if 'a_party' in cdr_df.columns:
                    call_data = cdr_df[cdr_df['a_party'] == number]
                    
                    if len(call_data) > 0:
                        profile['cdr'] = {
                            'total_calls': len(call_data),
                            'unique_contacts': call_data['b_party'].nunique() if 'b_party' in call_data.columns else 0,
                            'total_duration': call_data['duration'].sum() if 'duration' in call_data.columns else 0,
                            'odd_hour_calls': len(call_data[call_data['datetime'].dt.hour.between(0, 5)]) if 'datetime' in call_data.columns else 0
                        }
                        
                        # Add to timeline
                        if 'datetime' in call_data.columns:
                            for _, call in call_data.iterrows():
                                profile['timeline'].append({
                                    'time': call['datetime'],
                                    'type': 'call',
                                    'details': f"Call to {call.get('b_party', 'Unknown')}"
                                })
            
            # Assess risk indicators
            if profile['tower_dump']:
                # Multiple IMEIs
                if profile['tower_dump'].get('unique_imeis', 0) > 1:
                    profile['risk_indicators'].append('Multiple IMEIs detected')
                
                # Brief presence
                if profile['tower_dump'].get('presence_count', 0) == 1:
                    profile['risk_indicators'].append('One-time visitor')
            
            if profile['cdr']:
                # Odd hour activity
                if profile['cdr'].get('odd_hour_calls', 0) > 0:
                    profile['risk_indicators'].append('Odd-hour calling activity')
                
                # High call volume
                if profile['cdr'].get('total_calls', 0) > 50:
                    profile['risk_indicators'].append('High call volume')
            
            # Silent device
            if profile['tower_dump'] and not profile['cdr']:
                profile['risk_indicators'].append('Silent device (no calls)')
            
            # Store profile if has any data
            if profile['tower_dump'] or profile['cdr'] or profile['ipdr']:
                suspect_profiles[number] = profile
        
        # Report top suspect profiles
        if suspect_profiles:
            # Sort by risk indicators
            sorted_profiles = sorted(
                suspect_profiles.items(),
                key=lambda x: len(x[1]['risk_indicators']),
                reverse=True
            )
            
            results.append(f"\n📊 SUSPECT PROFILES GENERATED: {len(suspect_profiles)}")
            
            # High-risk profiles
            high_risk = [(n, p) for n, p in sorted_profiles if len(p['risk_indicators']) >= 2]
            
            if high_risk:
                results.append(f"\n🔴 HIGH-RISK PROFILES ({len(high_risk)}):")
                
                for number, profile in high_risk[:5]:
                    results.append(f"\n📱 {number}")
                    
                    # Tower dump summary
                    if profile['tower_dump']:
                        td = profile['tower_dump']
                        results.append(f"   Tower Presence: {td['presence_count']} records at {td['unique_towers']} towers")
                    
                    # CDR summary
                    if profile['cdr']:
                        cdr = profile['cdr']
                        results.append(f"   Call Activity: {cdr['total_calls']} calls to {cdr['unique_contacts']} contacts")
                    
                    # Risk indicators
                    results.append(f"   Risk Indicators:")
                    for indicator in profile['risk_indicators']:
                        results.append(f"     ⚠️ {indicator}")
        
        return "\n".join(results)
    
    def _analyze_crime_scene_communications(self) -> str:
        """Analyze communications at crime scene locations"""
        
        results = []
        results.append("🔍 CRIME SCENE COMMUNICATION ANALYSIS")
        results.append("=" * 80)
        
        # Identify high-activity towers as potential crime scenes
        tower_activity = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if 'tower_id' in df.columns:
                counts = df['tower_id'].value_counts()
                for tower, count in counts.items():
                    tower_activity[tower] = tower_activity.get(tower, 0) + count
        
        # Take top towers as crime scenes
        crime_scene_towers = sorted(
            tower_activity.items(),
            key=lambda x: x[1],
            reverse=True
        )[:3]
        
        for tower_id, _ in crime_scene_towers:
            results.append(f"\n📍 CRIME SCENE: Tower {tower_id}")
            
            # Get all devices at this tower
            devices_at_scene = set()
            scene_timeline = []
            
            for dump_id, tower_df in self.tower_dump_data.items():
                if 'tower_id' in tower_df.columns:
                    scene_data = tower_df[tower_df['tower_id'] == tower_id]
                    
                    if 'mobile_number' in scene_data.columns:
                        devices_at_scene.update(scene_data['mobile_number'].unique())
                    
                    # Build timeline
                    if 'timestamp' in scene_data.columns:
                        for _, record in scene_data.iterrows():
                            scene_timeline.append({
                                'time': record['timestamp'],
                                'device': record.get('mobile_number', 'Unknown'),
                                'type': 'presence'
                            })
            
            results.append(f"   Devices Present: {len(devices_at_scene)}")
            
            # Check CDR activity for devices at scene
            calls_at_scene = []
            
            for device in devices_at_scene:
                for suspect, cdr_df in self.cdr_data.items():
                    if 'a_party' in cdr_df.columns:
                        device_calls = cdr_df[cdr_df['a_party'] == device]
                        
                        if len(device_calls) > 0:
                            calls_at_scene.append({
                                'device': device,
                                'call_count': len(device_calls),
                                'contacts': device_calls['b_party'].unique() if 'b_party' in device_calls.columns else []
                            })
            
            if calls_at_scene:
                results.append(f"\n   📞 COMMUNICATION ACTIVITY:")
                
                # Devices making calls
                active_callers = [c for c in calls_at_scene if c['call_count'] > 0]
                results.append(f"   Active Callers: {len(active_callers)}")
                
                # Check for inter-device calls
                all_devices = set(devices_at_scene)
                inter_device_calls = []
                
                for call_info in calls_at_scene:
                    common_contacts = set(call_info['contacts']) & all_devices
                    if common_contacts:
                        inter_device_calls.append({
                            'from': call_info['device'],
                            'to': list(common_contacts)
                        })
                
                if inter_device_calls:
                    results.append(f"\n   🔗 INTER-DEVICE COMMUNICATION DETECTED:")
                    for comm in inter_device_calls[:3]:
                        results.append(f"      {comm['from']} → {', '.join(comm['to'])}")
        
        return "\n".join(results)
    
    def _find_communication_patterns(self) -> str:
        """Find patterns in communication correlated with tower presence"""
        
        results = []
        results.append("📊 COMMUNICATION PATTERN ANALYSIS")
        results.append("=" * 80)
        
        # Pattern 1: Calls immediately after tower appearance
        post_arrival_calls = []
        
        for dump_id, tower_df in self.tower_dump_data.items():
            if not all(col in tower_df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                continue
            
            # Group by device
            for device, device_data in tower_df.groupby('mobile_number'):
                # Sort by time
                arrivals = device_data.sort_values('timestamp')
                
                # Check for first arrival at each tower
                for tower, tower_arrivals in arrivals.groupby('tower_id'):
                    first_arrival = tower_arrivals.iloc[0]
                    
                    # Check CDR for calls after arrival
                    for suspect, cdr_df in self.cdr_data.items():
                        if 'a_party' not in cdr_df.columns or 'datetime' not in cdr_df.columns:
                            continue
                        
                        device_calls = cdr_df[cdr_df['a_party'] == device]
                        
                        # Calls within 10 minutes of arrival
                        post_arrival = device_calls[
                            (device_calls['datetime'] >= first_arrival['timestamp']) &
                            (device_calls['datetime'] <= first_arrival['timestamp'] + timedelta(minutes=10))
                        ]
                        
                        if len(post_arrival) > 0:
                            post_arrival_calls.append({
                                'device': device,
                                'tower': tower,
                                'arrival_time': first_arrival['timestamp'],
                                'call_count': len(post_arrival),
                                'first_call_delay': (post_arrival.iloc[0]['datetime'] - first_arrival['timestamp']).total_seconds() / 60
                            })
        
        if post_arrival_calls:
            results.append(f"\n📞 POST-ARRIVAL CALLING PATTERN: {len(post_arrival_calls)} instances")
            results.append("   ⚠️ Calls immediately after arriving at location")
            
            # Show examples
            for pattern in sorted(post_arrival_calls, key=lambda x: x['first_call_delay'])[:3]:
                results.append(f"\n   📱 {pattern['device']}")
                results.append(f"      Arrived at {pattern['tower']} at {pattern['arrival_time']}")
                results.append(f"      Made {pattern['call_count']} calls within {pattern['first_call_delay']:.1f} minutes")
        
        # Pattern 2: Silent periods at specific towers
        silent_towers = []
        
        # Find towers where devices are present but don't make calls
        for tower_id in set(t[0] for t in tower_activity.items()):
            # Get devices at this tower
            devices_at_tower = set()
            
            for dump_id, tower_df in self.tower_dump_data.items():
                if 'tower_id' in tower_df.columns and 'mobile_number' in tower_df.columns:
                    tower_devices = tower_df[tower_df['tower_id'] == tower_id]['mobile_number'].unique()
                    devices_at_tower.update(tower_devices)
            
            # Check how many made calls
            calling_devices = set()
            
            for device in devices_at_tower:
                for suspect, cdr_df in self.cdr_data.items():
                    if 'a_party' in cdr_df.columns:
                        if device in cdr_df['a_party'].values:
                            calling_devices.add(device)
                            break
            
            silence_ratio = 1 - (len(calling_devices) / len(devices_at_tower)) if devices_at_tower else 0
            
            if silence_ratio > 0.7 and len(devices_at_tower) > 5:
                silent_towers.append({
                    'tower': tower_id,
                    'total_devices': len(devices_at_tower),
                    'silent_devices': len(devices_at_tower) - len(calling_devices),
                    'silence_ratio': silence_ratio
                })
        
        if silent_towers:
            results.append(f"\n🔇 SILENT ZONES DETECTED: {len(silent_towers)} towers")
            results.append("   Areas where devices congregate but don't communicate")
            
            for zone in sorted(silent_towers, key=lambda x: x['silence_ratio'], reverse=True)[:3]:
                results.append(f"\n   Tower {zone['tower']}:")
                results.append(f"      {zone['silent_devices']}/{zone['total_devices']} devices silent")
                results.append(f"      Silence ratio: {zone['silence_ratio']*100:.1f}%")
        
        return "\n".join(results)
    
    def _comprehensive_cross_reference(self) -> str:
        """Provide comprehensive cross-reference analysis"""
        
        results = []
        results.append("📊 COMPREHENSIVE CROSS-REFERENCE ANALYSIS")
        results.append("=" * 80)
        
        # Summary statistics
        tower_devices = set()
        cdr_devices = set()
        ipdr_devices = set()
        
        # Collect devices from each source
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' in df.columns:
                tower_devices.update(df['mobile_number'].unique())
        
        for suspect, df in self.cdr_data.items():
            if 'a_party' in df.columns:
                cdr_devices.update(df['a_party'].unique())
        
        # Calculate overlaps
        all_sources = tower_devices & cdr_devices  # Devices in both tower and CDR
        tower_only = tower_devices - cdr_devices
        cdr_only = cdr_devices - tower_devices
        
        results.append(f"\n📊 DATA SOURCE COVERAGE:")
        results.append(f"   Tower Dump Devices: {len(tower_devices)}")
        results.append(f"   CDR Devices: {len(cdr_devices)}")
        results.append(f"   In Both Sources: {len(all_sources)}")
        results.append(f"   Tower Only: {len(tower_only)}")
        results.append(f"   CDR Only: {len(cdr_only)}")
        
        # Key findings
        results.append("\n🎯 KEY CROSS-REFERENCE FINDINGS:")
        
        # Silent devices
        if len(tower_only) > 0:
            results.append(f"\n  🔇 Silent Devices: {len(tower_only)}")
            results.append("     Present in area but no communication records")
        
        # Ghost callers
        if len(cdr_only) > 0:
            results.append(f"\n  👻 Ghost Callers: {len(cdr_only)}")
            results.append("     Made calls but not in tower dump")
        
        # Active communicators
        if len(all_sources) > 0:
            results.append(f"\n  📞 Active Communicators: {len(all_sources)}")
            results.append("     Present in area and making calls")
        
        return "\n".join(results)