"""
Network Analysis Tool for CDR Intelligence
Analyzes connections between suspects and identifies criminal networks
"""

from typing import Dict, Optional, Any, List, Set, Tuple, Type
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
import pandas as pd
import networkx as nx
from collections import defaultdict
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

class NetworkAnalysisInput(BaseModel):
    """Input for network analysis tool"""
    query: str = Field(description="What network patterns to analyze (e.g., 'connections between suspects', 'common contacts')")
    suspect_name: Optional[str] = Field(default=None, description="Specific suspect to analyze connections for")

class NetworkAnalysisTool(BaseTool):
    """Tool for analyzing network connections in CDR data"""
    
    name: str = "network_analysis"
    description: str = """Analyze network connections between suspects, find common contacts, 
    identify potential handlers or intermediaries, and detect criminal network structures.
    Examples: 'find connections between suspects', 'analyze common contacts', 'identify network hierarchy'"""
    
    args_schema: Type[BaseModel] = NetworkAnalysisInput
    cdr_data: Dict[str, pd.DataFrame] = {}
    
    def _run(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Run network analysis"""
        try:
            if not self.cdr_data:
                return "No CDR data loaded. Please load data first."
            
            # Build network from all CDR data
            network_data = self._build_network()
            
            # Analyze based on query
            if "connection" in query.lower() or "between" in query.lower():
                analysis = self._analyze_inter_suspect_connections(network_data)
            elif "common" in query.lower():
                analysis = self._analyze_common_contacts(network_data)
            elif "hierarchy" in query.lower() or "handler" in query.lower():
                analysis = self._analyze_network_hierarchy(network_data)
            else:
                # Comprehensive analysis
                analysis = {
                    'connections': self._analyze_inter_suspect_connections(network_data),
                    'common_contacts': self._analyze_common_contacts(network_data),
                    'hierarchy': self._analyze_network_hierarchy(network_data),
                    'synchronized_calling': self._detect_synchronized_calling()
                }
            
            response = self._format_network_analysis(analysis, query)
            return response
            
        except Exception as e:
            logger.error(f"Error in network analysis: {str(e)}")
            return f"Error analyzing network: {str(e)}"
    
    async def _arun(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Async not implemented"""
        raise NotImplementedError("Async execution not supported")
    
    def _build_network(self) -> Dict[str, Any]:
        """Build network structure from CDR data"""
        network_data = {
            'suspect_numbers': {},  # Maps suspect names to their phone numbers
            'all_contacts': defaultdict(set),  # All contacts for each suspect
            'contact_to_suspects': defaultdict(set),  # Reverse mapping
            'call_counts': defaultdict(lambda: defaultdict(int)),  # Call frequency between numbers
            'graph': nx.Graph()  # NetworkX graph for analysis
        }
        
        # Extract data from each suspect
        for suspect_name, df in self.cdr_data.items():
            # Filter provider messages
            df_filtered = df[~df.get('is_provider_message', False)].copy()
            
            # Get suspect's phone number from filename
            parts = suspect_name.split('_')
            if len(parts) >= 2 and parts[-1].isdigit():
                suspect_number = parts[-1]
                network_data['suspect_numbers'][suspect_name] = suspect_number
            
            # Collect all contacts
            if 'b_party_clean' in df_filtered.columns:
                contacts = df_filtered['b_party_clean'].dropna().unique()
                network_data['all_contacts'][suspect_name] = set(contacts)
                
                # Build reverse mapping
                for contact in contacts:
                    network_data['contact_to_suspects'][contact].add(suspect_name)
                
                # Count calls
                contact_counts = df_filtered['b_party_clean'].value_counts()
                for contact, count in contact_counts.items():
                    network_data['call_counts'][suspect_name][contact] = count
                    # Add to graph
                    network_data['graph'].add_edge(suspect_name, contact, weight=count)
        
        return network_data
    
    def _analyze_inter_suspect_connections(self, network_data: Dict) -> Dict[str, Any]:
        """Find direct connections between suspects"""
        connections = {
            'direct_connections': [],
            'indirect_connections': [],
            'isolated_suspects': []
        }
        
        suspect_names = list(self.cdr_data.keys())
        suspect_numbers = network_data['suspect_numbers']
        
        # Check for direct connections
        for i, suspect1 in enumerate(suspect_names):
            contacts1 = network_data['all_contacts'][suspect1]
            
            # Check if any other suspect's number is in contacts
            for j, suspect2 in enumerate(suspect_names):
                if i >= j:  # Avoid duplicates
                    continue
                    
                suspect2_number = suspect_numbers.get(suspect2)
                if suspect2_number and suspect2_number in contacts1:
                    # Direct connection found
                    call_count = network_data['call_counts'][suspect1][suspect2_number]
                    connections['direct_connections'].append({
                        'from': suspect1,
                        'to': suspect2,
                        'calls': call_count
                    })
        
        # Find suspects with no connections to others
        for suspect in suspect_names:
            has_connection = False
            for conn in connections['direct_connections']:
                if suspect in [conn['from'], conn['to']]:
                    has_connection = True
                    break
            if not has_connection:
                connections['isolated_suspects'].append(suspect)
        
        return connections
    
    def _analyze_common_contacts(self, network_data: Dict) -> Dict[str, Any]:
        """Find common contacts between suspects"""
        common_contacts = {
            'shared_contacts': [],
            'potential_intermediaries': []
        }
        
        # Find contacts shared by multiple suspects
        contact_suspects_map = network_data['contact_to_suspects']
        
        for contact, suspects in contact_suspects_map.items():
            if len(suspects) > 1:
                # Calculate total calls to this contact
                total_calls = sum(
                    network_data['call_counts'][suspect].get(contact, 0) 
                    for suspect in suspects
                )
                
                common_contacts['shared_contacts'].append({
                    'contact': contact[-4:] if len(contact) > 4 else contact,  # Last 4 digits
                    'shared_by': list(suspects),
                    'suspect_count': len(suspects),
                    'total_calls': total_calls
                })
        
        # Sort by number of suspects sharing the contact
        common_contacts['shared_contacts'].sort(key=lambda x: x['suspect_count'], reverse=True)
        
        # Identify potential intermediaries (contacted by 3+ suspects)
        for contact_info in common_contacts['shared_contacts']:
            if contact_info['suspect_count'] >= 3:
                common_contacts['potential_intermediaries'].append(contact_info)
        
        return common_contacts
    
    def _analyze_network_hierarchy(self, network_data: Dict) -> Dict[str, Any]:
        """Analyze network hierarchy and identify potential handlers"""
        hierarchy = {
            'central_figures': [],
            'network_metrics': {},
            'potential_handlers': []
        }
        
        graph = network_data['graph']
        
        # Calculate centrality metrics for suspects only
        suspect_names = list(self.cdr_data.keys())
        
        # Degree centrality (number of unique contacts)
        degree_centrality = {}
        for suspect in suspect_names:
            if suspect in graph:
                degree_centrality[suspect] = len(network_data['all_contacts'][suspect])
        
        # Sort by centrality
        sorted_suspects = sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)
        
        for suspect, centrality in sorted_suspects[:5]:
            total_calls = sum(network_data['call_counts'][suspect].values())
            hierarchy['central_figures'].append({
                'suspect': suspect,
                'unique_contacts': centrality,
                'total_calls': total_calls,
                'avg_calls_per_contact': round(total_calls / centrality, 2) if centrality > 0 else 0
            })
        
        # Identify potential handlers (high unique contacts, many one-time calls)
        for suspect in suspect_names:
            contacts = network_data['all_contacts'][suspect]
            if len(contacts) > 50:  # High number of unique contacts
                # Count single-call contacts
                single_calls = sum(1 for c, count in network_data['call_counts'][suspect].items() if count == 1)
                if single_calls > len(contacts) * 0.5:  # More than 50% are single calls
                    hierarchy['potential_handlers'].append({
                        'suspect': suspect,
                        'unique_contacts': len(contacts),
                        'single_call_percentage': round(single_calls / len(contacts) * 100, 2)
                    })
        
        return hierarchy
    
    def _detect_synchronized_calling(self) -> Dict[str, Any]:
        """Detect suspects active at the same time"""
        sync_analysis = {
            'sync_patterns': [],
            'coordination_windows': [],
            'simultaneous_activity': []
        }
        
        time_window = 300  # 5 minutes in seconds
        
        # Get all calls with timestamps from all suspects
        all_calls = []
        for suspect, df in self.cdr_data.items():
            # Filter provider messages
            df_filtered = df[~df.get('is_provider_message', False)].copy()
            
            if 'datetime' in df_filtered.columns and settings.cdr_columns['call_type'] in df_filtered.columns:
                for _, row in df_filtered.iterrows():
                    all_calls.append({
                        'suspect': suspect,
                        'timestamp': row['datetime'],
                        'type': row[settings.cdr_columns['call_type']],
                        'b_party': row.get('b_party_clean', '')
                    })
        
        # Sort by timestamp
        all_calls.sort(key=lambda x: x['timestamp'])
        
        # Find synchronized activities
        processed_indices = set()
        
        for i in range(len(all_calls)):
            if i in processed_indices:
                continue
                
            current = all_calls[i]
            synchronized = [current]
            suspects_in_window = {current['suspect']}
            
            # Check calls within time window
            j = i + 1
            while j < len(all_calls):
                next_call = all_calls[j]
                time_diff = (next_call['timestamp'] - current['timestamp']).total_seconds()
                
                if time_diff <= time_window:
                    if next_call['suspect'] not in suspects_in_window:
                        synchronized.append(next_call)
                        suspects_in_window.add(next_call['suspect'])
                        processed_indices.add(j)
                else:
                    break
                j += 1
            
            # Record if multiple suspects were active
            if len(suspects_in_window) >= 2:
                sync_analysis['sync_patterns'].append({
                    'timestamp': current['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                    'suspects': list(suspects_in_window),
                    'suspect_count': len(suspects_in_window),
                    'call_count': len(synchronized),
                    'pattern': 'COORDINATED_ACTIVITY' if len(suspects_in_window) >= 3 else 'SYNCHRONIZED_CALLS'
                })
        
        # Sort by number of synchronized suspects
        sync_analysis['sync_patterns'].sort(key=lambda x: x['suspect_count'], reverse=True)
        
        # Identify coordination windows (multiple sync events close together)
        if len(sync_analysis['sync_patterns']) >= 2:
            for i in range(len(sync_analysis['sync_patterns']) - 1):
                current_sync = sync_analysis['sync_patterns'][i]
                next_sync = sync_analysis['sync_patterns'][i + 1]
                
                # Check if sync events are within 1 hour
                current_time = pd.to_datetime(current_sync['timestamp'])
                next_time = pd.to_datetime(next_sync['timestamp'])
                
                if (next_time - current_time).total_seconds() <= 3600:  # 1 hour
                    sync_analysis['coordination_windows'].append({
                        'start': current_sync['timestamp'],
                        'end': next_sync['timestamp'],
                        'events': 2,
                        'pattern': 'SUSTAINED_COORDINATION'
                    })
        
        return sync_analysis
    
    def _format_network_analysis(self, analysis: Dict, query: str) -> str:
        """Format network analysis results"""
        output = []
        
        output.append("🌐 NETWORK ANALYSIS RESULTS")
        output.append("=" * 50)
        
        # Format based on analysis type
        if isinstance(analysis, dict) and 'connections' in analysis:
            # Comprehensive analysis
            self._format_connections(output, analysis['connections'])
            self._format_common_contacts(output, analysis['common_contacts'])
            self._format_hierarchy(output, analysis['hierarchy'])
            if 'synchronized_calling' in analysis:
                self._format_synchronized_calling(output, analysis['synchronized_calling'])
        elif 'direct_connections' in analysis:
            self._format_connections(output, analysis)
        elif 'shared_contacts' in analysis:
            self._format_common_contacts(output, analysis)
        elif 'central_figures' in analysis:
            self._format_hierarchy(output, analysis)
        
        return "\n".join(output)
    
    def _format_connections(self, output: List[str], connections: Dict):
        """Format inter-suspect connections"""
        output.append("\n🔗 INTER-SUSPECT CONNECTIONS")
        output.append("-" * 30)
        
        if connections['direct_connections']:
            output.append("Direct connections found:")
            for conn in connections['direct_connections']:
                output.append(f"  • {conn['from']} ←→ {conn['to']} ({conn['calls']} calls)")
            output.append("  ⚠️ Direct communication between suspects detected!")
        else:
            output.append("  ✓ No direct connections between suspects")
            output.append("  → Suggests compartmentalized structure or intermediaries")
        
        if connections['isolated_suspects']:
            output.append(f"\nIsolated suspects: {', '.join(connections['isolated_suspects'][:5])}")
    
    def _format_common_contacts(self, output: List[str], common: Dict):
        """Format common contacts analysis"""
        output.append("\n👥 COMMON CONTACTS")
        output.append("-" * 30)
        
        if common['shared_contacts']:
            output.append(f"Found {len(common['shared_contacts'])} shared contacts")
            
            # Show top shared contacts
            for contact in common['shared_contacts'][:5]:
                suspects_str = ', '.join(contact['shared_by'][:3])
                if contact['suspect_count'] > 3:
                    suspects_str += f" +{contact['suspect_count']-3} more"
                output.append(f"  • ***{contact['contact']}: shared by {contact['suspect_count']} suspects")
                output.append(f"    ({suspects_str})")
                output.append(f"    Total calls: {contact['total_calls']}")
        
        if common['potential_intermediaries']:
            output.append("\n🚨 POTENTIAL INTERMEDIARIES/HANDLERS:")
            for interm in common['potential_intermediaries'][:3]:
                output.append(f"  • ***{interm['contact']}: connected to {interm['suspect_count']} suspects")
    
    def _format_hierarchy(self, output: List[str], hierarchy: Dict):
        """Format network hierarchy analysis"""
        output.append("\n📊 NETWORK HIERARCHY")
        output.append("-" * 30)
        
        if hierarchy['central_figures']:
            output.append("Central figures by connectivity:")
            for i, figure in enumerate(hierarchy['central_figures'][:5], 1):
                output.append(f"  {i}. {figure['suspect']}")
                output.append(f"     Contacts: {figure['unique_contacts']}, Calls: {figure['total_calls']}")
        
        if hierarchy['potential_handlers']:
            output.append("\n⚠️ POTENTIAL HANDLERS DETECTED:")
            for handler in hierarchy['potential_handlers']:
                output.append(f"  • {handler['suspect']}: {handler['unique_contacts']} contacts")
                output.append(f"    ({handler['single_call_percentage']}% single calls - distribution pattern)")
        
        return output
    
    def _format_synchronized_calling(self, output: List[str], sync_data: Dict):
        """Format synchronized calling analysis"""
        output.append("\n🔄 SYNCHRONIZED CALLING PATTERNS")
        output.append("-" * 30)
        
        if sync_data['sync_patterns']:
            output.append(f"Found {len(sync_data['sync_patterns'])} synchronized calling events")
            
            # Show top synchronized events
            for i, pattern in enumerate(sync_data['sync_patterns'][:5], 1):
                emoji = "🚨" if pattern['suspect_count'] >= 3 else "⚠️"
                output.append(f"\n{emoji} Event {i}: {pattern['timestamp']}")
                output.append(f"   Active suspects: {', '.join(pattern['suspects'])}")
                output.append(f"   Pattern: {pattern['pattern']}")
                output.append(f"   Total calls: {pattern['call_count']} within 5-minute window")
            
            # High-risk patterns
            high_risk = [p for p in sync_data['sync_patterns'] if p['suspect_count'] >= 3]
            if high_risk:
                output.append("\n🚨 COORDINATED OPERATIONS DETECTED:")
                output.append(f"   {len(high_risk)} instances of 3+ suspects active simultaneously")
                output.append("   This strongly suggests coordinated criminal activity")
        else:
            output.append("  ✓ No synchronized calling patterns detected")
        
        if sync_data['coordination_windows']:
            output.append("\n⚠️ SUSTAINED COORDINATION WINDOWS:")
            for window in sync_data['coordination_windows'][:3]:
                output.append(f"   • {window['start']} to {window['end']}")
                output.append(f"     Pattern: {window['pattern']}")
        
        return output