"""
Communication Analysis Tool for CDR Intelligence
Analyzes communication patterns including voice/SMS ratios and suspicious behaviors
"""

from typing import Dict, Optional, Any, List, Type
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
import pandas as pd
from collections import Counter, defaultdict
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

class CommunicationAnalysisInput(BaseModel):
    """Input for communication analysis tool"""
    query: str = Field(description="What communication patterns to analyze (e.g., 'voice vs sms', 'contact frequency')")
    suspect_name: Optional[str] = Field(default=None, description="Specific suspect to analyze")

class CommunicationAnalysisTool(BaseTool):
    """Tool for analyzing communication patterns in CDR data"""
    
    name: str = "communication_analysis"
    description: str = """Analyze communication patterns including voice/SMS ratios, contact frequencies, 
    call durations, and suspicious behaviors like 100% voice communication. Can find who made the most calls,
    frequent contacts, and communication patterns.
    Examples: 'analyze voice vs SMS patterns', 'who made the most calls', 'find frequent contacts for Ali'"""
    
    args_schema: Type[BaseModel] = CommunicationAnalysisInput
    cdr_data: Dict[str, pd.DataFrame] = {}
    
    def _run(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Run communication analysis"""
        try:
            if not self.cdr_data:
                return "No CDR data loaded. Please load data first."
            
            analyze_all = "all" in query.lower() or not suspect_name
            results = []
            suspects_to_analyze = self.cdr_data.keys() if analyze_all else [suspect_name]
            
            for suspect in suspects_to_analyze:
                if suspect in self.cdr_data:
                    analysis = self._analyze_suspect_communication(suspect, self.cdr_data[suspect])
                    results.append(analysis)
            
            if not results:
                return "No suspects found for analysis."
            
            # Sort by suspicion level (voice-only first)
            results.sort(key=lambda x: (x['voice_only'], x['voice_percentage']), reverse=True)
            
            response = self._format_communication_analysis(results, query)
            return response
            
        except Exception as e:
            logger.error(f"Error in communication analysis: {str(e)}")
            return f"Error analyzing communication patterns: {str(e)}"
    
    async def _arun(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Async not implemented"""
        raise NotImplementedError("Async execution not supported")
    
    def _analyze_suspect_communication(self, suspect: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze communication patterns for a single suspect"""
        # Filter out provider messages
        df_filtered = df[~df.get('is_provider_message', False)].copy()
        
        call_type_col = settings.cdr_columns['call_type']
        
        analysis = {
            'suspect': suspect,
            'total_communications': len(df_filtered),
            'voice_calls': 0,
            'sms_messages': 0,
            'voice_percentage': 0.0,
            'sms_percentage': 0.0,
            'voice_only': False,
            'avg_call_duration': 0.0,
            'unique_contacts': 0,
            'frequent_contacts': [],
            'communication_risk': 'LOW',
            'circular_loops': [],
            'one_ring_patterns': {}
        }
        
        if call_type_col in df_filtered.columns:
            # Count by type
            call_types = df_filtered[call_type_col].value_counts()
            
            voice_types = ['CALL-IN', 'CALL-OUT']
            sms_types = ['SMS-IN', 'SMS-OUT']
            
            analysis['voice_calls'] = df_filtered[df_filtered[call_type_col].isin(voice_types)].shape[0]
            analysis['sms_messages'] = df_filtered[df_filtered[call_type_col].isin(sms_types)].shape[0]
            
            total = analysis['voice_calls'] + analysis['sms_messages']
            if total > 0:
                analysis['voice_percentage'] = round((analysis['voice_calls'] / total) * 100, 2)
                analysis['sms_percentage'] = round((analysis['sms_messages'] / total) * 100, 2)
                
                # Check for voice-only behavior
                if analysis['sms_messages'] == 0 and analysis['voice_calls'] > 10:
                    analysis['voice_only'] = True
                    analysis['communication_risk'] = 'HIGH'
                elif analysis['voice_percentage'] > 90:
                    analysis['communication_risk'] = 'MEDIUM'
        
        # Duration analysis
        if 'duration_seconds' in df_filtered.columns:
            voice_df = df_filtered[df_filtered[call_type_col].isin(['CALL-IN', 'CALL-OUT'])] if call_type_col in df_filtered.columns else df_filtered
            if len(voice_df) > 0:
                analysis['avg_call_duration'] = round(voice_df['duration_seconds'].mean(), 2)
                
                # Check for suspicious duration patterns
                duration_counts = voice_df['duration_seconds'].value_counts()
                repeated_durations = duration_counts[duration_counts > 5]
                if len(repeated_durations) > 0:
                    analysis['repeated_durations'] = True
                    analysis['suspicious_durations'] = repeated_durations.head(3).to_dict()
        
        # Contact analysis
        if 'b_party_clean' in df_filtered.columns:
            analysis['unique_contacts'] = df_filtered['b_party_clean'].nunique()
            
            # Find most frequent contacts
            contact_counts = df_filtered['b_party_clean'].value_counts()
            if len(contact_counts) > 0:
                top_contacts = []
                for contact, count in contact_counts.head(5).items():
                    percentage = round((count / len(df_filtered)) * 100, 2)
                    top_contacts.append({
                        'number': contact[-4:] if len(contact) > 4 else contact,  # Last 4 digits
                        'calls': count,
                        'percentage': percentage
                    })
                analysis['frequent_contacts'] = top_contacts
                
                # Check for single dominant contact
                if contact_counts.iloc[0] > len(df_filtered) * 0.3:
                    analysis['dominant_contact'] = True
                    if analysis['communication_risk'] == 'LOW':
                        analysis['communication_risk'] = 'MEDIUM'
        
        # Detect circular loops
        loops = self._detect_circular_loops(df_filtered)
        if loops:
            analysis['circular_loops'] = loops
            if analysis['communication_risk'] != 'HIGH':
                analysis['communication_risk'] = 'HIGH'
        
        # Detect one-ring patterns
        one_ring_analysis = self._detect_one_ring_patterns(df_filtered)
        if one_ring_analysis:
            analysis['one_ring_patterns'] = one_ring_analysis
            if one_ring_analysis.get('signaling_detected'):
                if analysis['communication_risk'] == 'LOW':
                    analysis['communication_risk'] = 'MEDIUM'
        
        return analysis
    
    def _format_communication_analysis(self, results: list, query: str) -> str:
        """Format communication analysis results"""
        output = []
        
        output.append("📞 COMMUNICATION PATTERN ANALYSIS")
        output.append("=" * 50)
        
        # Identify suspicious patterns
        voice_only = [r for r in results if r['voice_only']]
        high_voice = [r for r in results if r['voice_percentage'] > 90 and not r['voice_only']]
        
        if voice_only:
            output.append(f"\n🚨 CRITICAL: {len(voice_only)} suspect(s) with 100% VOICE COMMUNICATION")
            output.append("   This is a strong indicator of avoiding traceable text communications")
        
        # Detailed results
        for result in results:
            if result['total_communications'] < 10:
                continue  # Skip low-activity suspects
            
            risk_emoji = {'HIGH': '🔴', 'MEDIUM': '🟡', 'LOW': '🟢'}[result['communication_risk']]
            
            output.append(f"\n{risk_emoji} {result['suspect']}")
            output.append(f"   Total: {result['total_communications']} communications")
            output.append(f"   Voice: {result['voice_percentage']}% ({result['voice_calls']} calls)")
            output.append(f"   SMS: {result['sms_percentage']}% ({result['sms_messages']} messages)")
            
            if result['voice_only']:
                output.append("   ⚠️ VOICE-ONLY BEHAVIOR DETECTED")
            
            if result['avg_call_duration'] > 0:
                output.append(f"   Avg Duration: {result['avg_call_duration']:.1f} seconds")
            
            if result.get('repeated_durations'):
                output.append("   🔍 Repeated call durations detected (possible coded communication)")
            
            output.append(f"   Unique Contacts: {result['unique_contacts']}")
            
            if result['frequent_contacts']:
                output.append("   Top Contacts:")
                for i, contact in enumerate(result['frequent_contacts'][:5], 1):
                    output.append(f"     {i}. ***{contact['number']}: {contact['calls']} calls ({contact['percentage']}%)")
            
            if result.get('dominant_contact'):
                output.append("   ⚠️ Single dominant contact detected (>30% of calls)")
        
        # Summary
        output.append("\n📊 COMMUNICATION RISK SUMMARY:")
        
        high_risk = [r for r in results if r['communication_risk'] == 'HIGH']
        medium_risk = [r for r in results if r['communication_risk'] == 'MEDIUM']
        
        if high_risk:
            output.append(f"   🔴 High Risk: {', '.join([r['suspect'] for r in high_risk[:5]])}")
        if medium_risk:
            output.append(f"   🟡 Medium Risk: {', '.join([r['suspect'] for r in medium_risk[:5]])}")
        
        # Patterns of concern
        output.append("\n⚠️ PATTERNS OF CONCERN:")
        if voice_only:
            output.append(f"   - Voice-only communication: {len(voice_only)} suspects")
        if any(r.get('repeated_durations') for r in results):
            output.append("   - Repeated call durations suggesting coded communication")
        if any(r.get('dominant_contact') for r in results):
            output.append("   - Dominant contact patterns suggesting handler relationships")
        
        # New pattern detections
        circular_suspects = [r for r in results if r.get('circular_loops')]
        if circular_suspects:
            output.append(f"   - 🔄 CIRCULAR COMMUNICATION DETECTED: {len(circular_suspects)} suspects")
            for suspect in circular_suspects[:3]:
                for loop in suspect['circular_loops'][:2]:
                    loop_str = ' → '.join(loop['loop'])
                    output.append(f"      • {loop_str}")
        
        one_ring_suspects = [r for r in results if r.get('one_ring_patterns', {}).get('signaling_detected')]
        if one_ring_suspects:
            output.append(f"   - 📱 ONE-RING SIGNALING DETECTED: {len(one_ring_suspects)} suspects")
            for suspect in one_ring_suspects[:3]:
                pattern = suspect['one_ring_patterns']
                output.append(f"      • {suspect['suspect']}: {pattern['total_one_rings']} one-ring calls ({pattern['percentage']}%)")
        
        return "\n".join(output)
    
    def _detect_circular_loops(self, df: pd.DataFrame, max_loop_size: int = 5) -> List[Dict]:
        """Detect circular communication patterns (A→B→C→A)"""
        loops_found = []
        
        # Filter out provider messages
        df_comm = df[~df.get('is_provider_message', False)].copy()
        
        # Build communication graph
        comm_graph = defaultdict(set)
        for _, row in df_comm.iterrows():
            if 'a_party_clean' in row and 'b_party_clean' in row:
                caller = row['a_party_clean']
                receiver = row['b_party_clean']
                
                if caller and receiver and caller != receiver:
                    comm_graph[caller].add(receiver)
        
        # DFS to find loops
        def find_loops(start, current, path, visited):
            if len(path) > max_loop_size:
                return
            
            if current in comm_graph:
                for next_node in comm_graph[current]:
                    if next_node == start and len(path) >= 3:
                        # Loop found - check if not already recorded
                        loop_set = set(path)
                        if not any(set(loop['loop'][:-1]) == loop_set for loop in loops_found):
                            loops_found.append({
                                'loop': path + [next_node],
                                'size': len(path),
                                'pattern': 'CIRCULAR_COMMUNICATION'
                            })
                    elif next_node not in visited:
                        visited.add(next_node)
                        find_loops(start, next_node, path + [next_node], visited)
                        visited.remove(next_node)
        
        # Find all loops
        for node in comm_graph:
            find_loops(node, node, [node], set([node]))
        
        return loops_found
    
    def _detect_one_ring_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect one-ring patterns (missed calls as signals)"""
        
        # Filter calls with very short duration
        one_ring_threshold = 3  # seconds
        
        if 'duration_seconds' not in df.columns or settings.cdr_columns['call_type'] not in df.columns:
            return {}
        
        call_type_col = settings.cdr_columns['call_type']
        
        short_calls = df[
            (df[call_type_col].isin(['CALL-IN', 'CALL-OUT'])) &
            (df['duration_seconds'] <= one_ring_threshold)
        ]
        
        analysis = {
            'total_one_rings': len(short_calls),
            'percentage': 0.0,
            'frequent_one_ring_contacts': [],
            'one_ring_sequences': [],
            'signaling_detected': False
        }
        
        if len(df) > 0:
            analysis['percentage'] = round((len(short_calls) / len(df)) * 100, 2)
        
        if len(short_calls) > 0:
            # Find contacts with multiple one-rings
            if 'b_party_clean' in short_calls.columns:
                one_ring_counts = short_calls['b_party_clean'].value_counts()
                
                for contact, count in one_ring_counts.head(5).items():
                    if count >= 3:  # At least 3 one-rings to same number
                        analysis['frequent_one_ring_contacts'].append({
                            'contact': contact[-4:] if len(contact) > 4 else contact,  # Last 4 digits
                            'count': count,
                            'pattern': 'SIGNALING'
                        })
                        analysis['signaling_detected'] = True
                
                # Detect sequential one-ring patterns
                if 'datetime' in short_calls.columns:
                    short_calls_sorted = short_calls.sort_values('datetime')
                    
                    for i in range(len(short_calls_sorted) - 1):
                        time_diff = (short_calls_sorted.iloc[i+1]['datetime'] - 
                                    short_calls_sorted.iloc[i]['datetime']).total_seconds()
                        
                        if time_diff < 300:  # Within 5 minutes
                            analysis['one_ring_sequences'].append({
                                'type': 'RAPID_SIGNALING',
                                'timestamp': str(short_calls_sorted.iloc[i]['datetime']),
                                'interval_seconds': time_diff
                            })
        
        return analysis