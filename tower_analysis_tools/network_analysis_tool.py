"""
Network Analysis Tool
Analyzes network connections and relationships in tower dump data
"""

from langchain.tools import BaseTool
from typing import Dict, Any, List, Optional, Set, Tuple
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
import numpy as np
from collections import defaultdict
import networkx as nx

class NetworkAnalysisTool(BaseTool):
    """Tool for analyzing networks and connections in tower dump data"""
    
    name: str = "network_analysis"
    description: str = """Analyze network connections and relationships in tower dump data. Use this tool to:
    - Build connection networks between devices
    - Identify network hubs and key players
    - Detect communication clusters
    - Find isolated devices
    - Analyze network evolution over time
    
    Input examples: "analyze device network", "find network hubs", "identify clusters", "network evolution"
    """
    
    # Class attributes for Pydantic v2
    tower_dump_data: Dict[str, Any] = {}
    cdr_data: Dict[str, Any] = {}
    params: Dict[str, Any] = {}
    
    def __init__(self):
        super().__init__()
        
        # Network analysis parameters
        self.params = {
            'co_location_window': 300,      # Seconds to consider co-located
            'min_co_occurrences': 3,        # Min times together to form edge
            'hub_threshold': 10,            # Min connections to be a hub
            'cluster_min_size': 3,          # Min size for a cluster
            'temporal_window': 3600         # Seconds for temporal analysis
        }
    
    def _run(self, query: str) -> str:
        """Execute network analysis"""
        
        if not self.tower_dump_data:
            return "No tower dump data loaded. Please load tower dump data first."
        
        try:
            query_lower = query.lower()
            
            if "hub" in query_lower:
                return self._identify_network_hubs()
            elif "cluster" in query_lower:
                return self._detect_clusters()
            elif "evolution" in query_lower:
                return self._analyze_network_evolution()
            elif "isolated" in query_lower:
                return self._find_isolated_devices()
            elif "hierarchy" in query_lower:
                return self._analyze_network_hierarchy()
            else:
                return self._comprehensive_network_analysis()
                
        except Exception as e:
            logger.error(f"Error in network analysis: {str(e)}")
            return f"Error analyzing network: {str(e)}"
    
    async def _arun(self, query: str) -> str:
        """Async version"""
        return self._run(query)
    
    def _build_co_location_network(self) -> nx.Graph:
        """Build network based on co-location at towers"""
        
        G = nx.Graph()
        co_locations = defaultdict(int)
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                continue
            
            # Group by tower and time windows
            df['time_window'] = pd.to_datetime(df['timestamp']).dt.floor(f'{self.params["co_location_window"]}s')
            
            for (tower, window), group in df.groupby(['tower_id', 'time_window']):
                devices = group['mobile_number'].unique()
                
                # Create edges between co-located devices
                for i, device1 in enumerate(devices):
                    for device2 in devices[i+1:]:
                        edge = tuple(sorted([device1, device2]))
                        co_locations[edge] += 1
        
        # Add edges with sufficient co-occurrences
        for (device1, device2), count in co_locations.items():
            if count >= self.params['min_co_occurrences']:
                G.add_edge(device1, device2, weight=count, type='co_location')
        
        return G
    
    def _build_communication_network(self) -> nx.DiGraph:
        """Build network based on CDR communications"""
        
        G = nx.DiGraph()
        
        for suspect, df in self.cdr_data.items():
            if not all(col in df.columns for col in ['a_party', 'b_party']):
                continue
            
            # Count calls between parties
            call_counts = df.groupby(['a_party', 'b_party']).size()
            
            for (a_party, b_party), count in call_counts.items():
                G.add_edge(a_party, b_party, weight=count, type='call')
        
        return G
    
    def _identify_network_hubs(self) -> str:
        """Identify hub devices in the network"""
        
        results = []
        results.append("🎯 NETWORK HUB ANALYSIS")
        results.append("=" * 80)
        
        # Build co-location network
        co_loc_network = self._build_co_location_network()
        
        if co_loc_network.number_of_nodes() == 0:
            return "No network connections found in the data"
        
        # Calculate centrality measures
        degree_centrality = nx.degree_centrality(co_loc_network)
        betweenness_centrality = nx.betweenness_centrality(co_loc_network)
        
        # Identify hubs by degree
        hubs = []
        for node, centrality in degree_centrality.items():
            degree = co_loc_network.degree(node)
            if degree >= self.params['hub_threshold']:
                hubs.append({
                    'device': node,
                    'connections': degree,
                    'degree_centrality': centrality,
                    'betweenness': betweenness_centrality.get(node, 0),
                    'neighbors': list(co_loc_network.neighbors(node))[:5]
                })
        
        if hubs:
            results.append(f"\n🔴 NETWORK HUBS IDENTIFIED: {len(hubs)}")
            
            # Sort by connections
            sorted_hubs = sorted(hubs, key=lambda x: x['connections'], reverse=True)
            
            for hub in sorted_hubs[:5]:
                results.append(f"\n📱 {hub['device']}")
                results.append(f"   Connections: {hub['connections']}")
                results.append(f"   Degree Centrality: {hub['degree_centrality']:.3f}")
                results.append(f"   Betweenness: {hub['betweenness']:.3f}")
                results.append(f"   Key Contacts: {', '.join(hub['neighbors'])}...")
                
                # Analyze hub behavior
                hub_patterns = self._analyze_hub_behavior(hub['device'])
                if hub_patterns:
                    results.append(f"   Behavior: {hub_patterns}")
        
        # Communication network hubs
        if self.cdr_data:
            comm_network = self._build_communication_network()
            
            if comm_network.number_of_nodes() > 0:
                results.append("\n📞 COMMUNICATION HUBS:")
                
                # In-degree (receiving calls)
                in_degrees = dict(comm_network.in_degree())
                top_receivers = sorted(in_degrees.items(), key=lambda x: x[1], reverse=True)[:3]
                
                if top_receivers:
                    results.append("\n  Call Receivers:")
                    for device, count in top_receivers:
                        results.append(f"    📱 {device}: {count} incoming connections")
                
                # Out-degree (making calls)
                out_degrees = dict(comm_network.out_degree())
                top_callers = sorted(out_degrees.items(), key=lambda x: x[1], reverse=True)[:3]
                
                if top_callers:
                    results.append("\n  Call Makers:")
                    for device, count in top_callers:
                        results.append(f"    📱 {device}: {count} outgoing connections")
        
        return "\n".join(results)
    
    def _detect_clusters(self) -> str:
        """Detect device clusters in the network"""
        
        results = []
        results.append("🔗 NETWORK CLUSTER DETECTION")
        results.append("=" * 80)
        
        # Build network
        G = self._build_co_location_network()
        
        if G.number_of_nodes() < self.params['cluster_min_size']:
            return "Insufficient data for cluster analysis"
        
        # Find connected components
        components = list(nx.connected_components(G))
        clusters = [c for c in components if len(c) >= self.params['cluster_min_size']]
        
        if clusters:
            results.append(f"\n📊 DEVICE CLUSTERS FOUND: {len(clusters)}")
            
            # Analyze each cluster
            for i, cluster in enumerate(sorted(clusters, key=len, reverse=True)[:5]):
                results.append(f"\n🔷 Cluster {i+1}: {len(cluster)} devices")
                
                # Get subgraph for cluster
                subgraph = G.subgraph(cluster)
                
                # Calculate cluster metrics
                density = nx.density(subgraph)
                avg_degree = np.mean([d for n, d in subgraph.degree()])
                
                results.append(f"   Density: {density:.2f}")
                results.append(f"   Avg Connections: {avg_degree:.1f}")
                results.append(f"   Members: {', '.join(list(cluster)[:5])}...")
                
                # Identify cluster characteristics
                cluster_patterns = self._analyze_cluster_patterns(cluster)
                if cluster_patterns:
                    results.append(f"   Characteristics: {cluster_patterns}")
        
        # Detect cliques (fully connected subgroups)
        if G.number_of_edges() > 0:
            cliques = list(nx.find_cliques(G))
            significant_cliques = [c for c in cliques if len(c) >= 3]
            
            if significant_cliques:
                results.append(f"\n🔺 TIGHT GROUPS (Cliques): {len(significant_cliques)}")
                
                for clique in sorted(significant_cliques, key=len, reverse=True)[:3]:
                    results.append(f"\n   Group of {len(clique)}: {', '.join(clique)}")
                    results.append("   ⚠️ All members connected to each other")
        
        return "\n".join(results)
    
    def _analyze_network_evolution(self) -> str:
        """Analyze how the network evolves over time"""
        
        results = []
        results.append("📈 NETWORK EVOLUTION ANALYSIS")
        results.append("=" * 80)
        
        # Create time-sliced networks
        time_networks = {}
        
        for dump_id, df in self.tower_dump_data.items():
            if not all(col in df.columns for col in ['mobile_number', 'timestamp', 'tower_id']):
                continue
            
            # Group by day
            df['date'] = pd.to_datetime(df['timestamp']).dt.date
            
            for date, day_data in df.groupby('date'):
                if date not in time_networks:
                    time_networks[date] = nx.Graph()
                
                # Build network for this day
                day_data['time_window'] = pd.to_datetime(day_data['timestamp']).dt.floor('5min')
                
                for (tower, window), group in day_data.groupby(['tower_id', 'time_window']):
                    devices = group['mobile_number'].unique()
                    
                    for i, device1 in enumerate(devices):
                        time_networks[date].add_node(device1)
                        for device2 in devices[i+1:]:
                            time_networks[date].add_edge(device1, device2)
        
        if time_networks:
            # Sort by date
            sorted_dates = sorted(time_networks.keys())
            
            results.append(f"\n📅 NETWORK EVOLUTION OVER {len(sorted_dates)} DAYS")
            
            # Track metrics over time
            evolution_metrics = []
            
            for date in sorted_dates:
                G = time_networks[date]
                metrics = {
                    'date': date,
                    'nodes': G.number_of_nodes(),
                    'edges': G.number_of_edges(),
                    'density': nx.density(G) if G.number_of_nodes() > 1 else 0,
                    'components': nx.number_connected_components(G)
                }
                evolution_metrics.append(metrics)
            
            # Analyze trends
            if len(evolution_metrics) > 1:
                # Growth analysis
                first = evolution_metrics[0]
                last = evolution_metrics[-1]
                
                node_growth = ((last['nodes'] - first['nodes']) / first['nodes'] * 100) if first['nodes'] > 0 else 0
                edge_growth = ((last['edges'] - first['edges']) / first['edges'] * 100) if first['edges'] > 0 else 0
                
                results.append(f"\n📊 GROWTH METRICS:")
                results.append(f"   Node Growth: {node_growth:+.1f}%")
                results.append(f"   Edge Growth: {edge_growth:+.1f}%")
                
                # Show daily metrics
                results.append("\n📈 DAILY NETWORK SIZE:")
                for metric in evolution_metrics[-5:]:  # Last 5 days
                    results.append(f"   {metric['date']}: {metric['nodes']} devices, {metric['edges']} connections")
                
                # Identify sudden changes
                sudden_changes = []
                for i in range(1, len(evolution_metrics)):
                    prev = evolution_metrics[i-1]
                    curr = evolution_metrics[i]
                    
                    node_change = abs(curr['nodes'] - prev['nodes']) / prev['nodes'] if prev['nodes'] > 0 else 0
                    
                    if node_change > 0.5:  # 50% change
                        sudden_changes.append({
                            'date': curr['date'],
                            'change': node_change,
                            'type': 'increase' if curr['nodes'] > prev['nodes'] else 'decrease'
                        })
                
                if sudden_changes:
                    results.append("\n⚠️ SUDDEN NETWORK CHANGES:")
                    for change in sudden_changes:
                        results.append(f"   {change['date']}: {change['change']*100:.0f}% {change['type']}")
        
        return "\n".join(results)
    
    def _find_isolated_devices(self) -> str:
        """Find isolated or loosely connected devices"""
        
        results = []
        results.append("🏝️ ISOLATED DEVICE ANALYSIS")
        results.append("=" * 80)
        
        # Build network
        G = self._build_co_location_network()
        
        # Find all devices in tower dump
        all_devices = set()
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' in df.columns:
                all_devices.update(df['mobile_number'].unique())
        
        # Completely isolated (no connections)
        isolated = all_devices - set(G.nodes())
        
        if isolated:
            results.append(f"\n🔴 COMPLETELY ISOLATED DEVICES: {len(isolated)}")
            results.append("   ⚠️ Never co-located with other devices")
            
            # Analyze isolated device behavior
            for device in list(isolated)[:5]:
                device_info = self._analyze_isolated_device(device)
                if device_info:
                    results.append(f"\n   📱 {device}")
                    results.append(f"      Activity: {device_info['activity_count']} records")
                    results.append(f"      Towers: {device_info['tower_count']}")
                    results.append(f"      Pattern: {device_info['pattern']}")
        
        # Weakly connected (few connections)
        if G.number_of_nodes() > 0:
            weakly_connected = []
            
            for node in G.nodes():
                degree = G.degree(node)
                if 1 <= degree <= 2:
                    weakly_connected.append({
                        'device': node,
                        'connections': degree,
                        'neighbors': list(G.neighbors(node))
                    })
            
            if weakly_connected:
                results.append(f"\n🟡 WEAKLY CONNECTED DEVICES: {len(weakly_connected)}")
                results.append("   Only 1-2 connections to network")
                
                for device_info in weakly_connected[:5]:
                    results.append(f"\n   📱 {device_info['device']}")
                    results.append(f"      Connections: {device_info['connections']}")
                    results.append(f"      Connected to: {', '.join(device_info['neighbors'])}")
        
        # Peripheral nodes (connected but on network edge)
        if G.number_of_nodes() > 10:
            centrality = nx.eigenvector_centrality(G, max_iter=1000)
            avg_centrality = np.mean(list(centrality.values()))
            
            peripheral = [
                node for node, cent in centrality.items()
                if cent < avg_centrality * 0.5
            ]
            
            if peripheral:
                results.append(f"\n🟢 PERIPHERAL DEVICES: {len(peripheral)}")
                results.append("   Connected but on network periphery")
        
        return "\n".join(results)
    
    def _analyze_network_hierarchy(self) -> str:
        """Analyze hierarchical structure in the network"""
        
        results = []
        results.append("🏛️ NETWORK HIERARCHY ANALYSIS")
        results.append("=" * 80)
        
        # Build communication network for hierarchy
        if not self.cdr_data:
            return "CDR data required for hierarchy analysis"
        
        G = self._build_communication_network()
        
        if G.number_of_nodes() < 5:
            return "Insufficient data for hierarchy analysis"
        
        # Calculate various centrality measures
        in_degree = dict(G.in_degree())
        out_degree = dict(G.out_degree())
        
        # Identify hierarchy levels
        hierarchy_levels = {
            'commanders': [],    # High out-degree, low in-degree
            'coordinators': [],  # High both in and out
            'soldiers': [],      # High in-degree, low out-degree
            'isolates': []       # Low both
        }
        
        avg_in = np.mean(list(in_degree.values()))
        avg_out = np.mean(list(out_degree.values()))
        
        for node in G.nodes():
            in_deg = in_degree[node]
            out_deg = out_degree[node]
            
            if out_deg > avg_out * 1.5 and in_deg < avg_in:
                hierarchy_levels['commanders'].append({
                    'device': node,
                    'out_calls': out_deg,
                    'in_calls': in_deg,
                    'ratio': out_deg / (in_deg + 1)
                })
            elif in_deg > avg_in * 1.5 and out_deg > avg_out * 1.5:
                hierarchy_levels['coordinators'].append({
                    'device': node,
                    'out_calls': out_deg,
                    'in_calls': in_deg,
                    'total': in_deg + out_deg
                })
            elif in_deg > avg_in * 1.5 and out_deg < avg_out:
                hierarchy_levels['soldiers'].append({
                    'device': node,
                    'out_calls': out_deg,
                    'in_calls': in_deg,
                    'ratio': in_deg / (out_deg + 1)
                })
        
        # Report findings
        if hierarchy_levels['commanders']:
            results.append(f"\n👑 COMMAND LEVEL ({len(hierarchy_levels['commanders'])} devices):")
            results.append("   High outgoing, low incoming calls")
            
            for cmd in sorted(hierarchy_levels['commanders'], key=lambda x: x['ratio'], reverse=True)[:3]:
                results.append(f"\n   📱 {cmd['device']}")
                results.append(f"      Outgoing: {cmd['out_calls']} calls")
                results.append(f"      Incoming: {cmd['in_calls']} calls")
                results.append(f"      Command Ratio: {cmd['ratio']:.1f}")
        
        if hierarchy_levels['coordinators']:
            results.append(f"\n🔄 COORDINATION LEVEL ({len(hierarchy_levels['coordinators'])} devices):")
            results.append("   High bidirectional communication")
            
            for coord in sorted(hierarchy_levels['coordinators'], key=lambda x: x['total'], reverse=True)[:3]:
                results.append(f"\n   📱 {coord['device']}")
                results.append(f"      Total Activity: {coord['total']} calls")
                results.append(f"      Incoming: {coord['in_calls']}, Outgoing: {coord['out_calls']}")
        
        if hierarchy_levels['soldiers']:
            results.append(f"\n🎖️ OPERATIONAL LEVEL ({len(hierarchy_levels['soldiers'])} devices):")
            results.append("   High incoming, low outgoing calls")
            
            for soldier in sorted(hierarchy_levels['soldiers'], key=lambda x: x['ratio'], reverse=True)[:3]:
                results.append(f"\n   📱 {soldier['device']}")
                results.append(f"      Incoming: {soldier['in_calls']} calls")
                results.append(f"      Outgoing: {soldier['out_calls']} calls")
        
        # Chain of command analysis
        results.append("\n🔗 COMMAND CHAINS:")
        
        # Find paths from commanders to soldiers
        chains = []
        for cmd in hierarchy_levels['commanders'][:2]:
            for soldier in hierarchy_levels['soldiers'][:2]:
                try:
                    path = nx.shortest_path(G, cmd['device'], soldier['device'])
                    if len(path) > 2:  # Indirect connection
                        chains.append({
                            'commander': cmd['device'],
                            'soldier': soldier['device'],
                            'path': path,
                            'length': len(path) - 1
                        })
                except nx.NetworkXNoPath:
                    pass
        
        if chains:
            for chain in sorted(chains, key=lambda x: x['length'])[:3]:
                results.append(f"\n   {' → '.join(chain['path'])}")
                results.append(f"   Command depth: {chain['length']} levels")
        
        return "\n".join(results)
    
    def _analyze_hub_behavior(self, device: str) -> str:
        """Analyze behavior patterns of a hub device"""
        
        patterns = []
        
        # Check tower dump behavior
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' in df.columns:
                device_data = df[df['mobile_number'] == device]
                
                if len(device_data) > 0:
                    # Movement pattern
                    if 'tower_id' in device_data.columns:
                        tower_count = device_data['tower_id'].nunique()
                        if tower_count > 5:
                            patterns.append("High mobility")
                    
                    # Time pattern
                    if 'timestamp' in device_data.columns:
                        hours = pd.to_datetime(device_data['timestamp']).dt.hour
                        if any(hours.between(0, 5)):
                            patterns.append("Odd-hour activity")
        
        return ", ".join(patterns) if patterns else "Normal patterns"
    
    def _analyze_cluster_patterns(self, cluster: Set[str]) -> str:
        """Analyze patterns within a cluster"""
        
        patterns = []
        
        # Check temporal concentration
        cluster_times = []
        
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' in df.columns and 'timestamp' in df.columns:
                cluster_data = df[df['mobile_number'].isin(cluster)]
                if len(cluster_data) > 0:
                    cluster_times.extend(pd.to_datetime(cluster_data['timestamp']).dt.hour.tolist())
        
        if cluster_times:
            hour_concentration = pd.Series(cluster_times).value_counts()
            if hour_concentration.iloc[0] > len(cluster_times) * 0.3:
                patterns.append(f"Peak hour: {hour_concentration.index[0]:02d}:00")
        
        # Check location concentration
        cluster_towers = []
        
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' in df.columns and 'tower_id' in df.columns:
                cluster_data = df[df['mobile_number'].isin(cluster)]
                if len(cluster_data) > 0:
                    cluster_towers.extend(cluster_data['tower_id'].tolist())
        
        if cluster_towers:
            tower_concentration = pd.Series(cluster_towers).value_counts()
            if tower_concentration.iloc[0] > len(cluster_towers) * 0.4:
                patterns.append(f"Primary tower: {tower_concentration.index[0]}")
        
        return ", ".join(patterns) if patterns else "Distributed activity"
    
    def _analyze_isolated_device(self, device: str) -> Dict[str, Any]:
        """Analyze an isolated device's behavior"""
        
        info = {
            'activity_count': 0,
            'tower_count': 0,
            'pattern': 'Unknown'
        }
        
        for dump_id, df in self.tower_dump_data.items():
            if 'mobile_number' in df.columns:
                device_data = df[df['mobile_number'] == device]
                
                if len(device_data) > 0:
                    info['activity_count'] += len(device_data)
                    
                    if 'tower_id' in device_data.columns:
                        info['tower_count'] = device_data['tower_id'].nunique()
                    
                    # Determine pattern
                    if info['tower_count'] == 1:
                        info['pattern'] = 'Stationary'
                    elif info['activity_count'] < 5:
                        info['pattern'] = 'Minimal activity'
                    else:
                        info['pattern'] = 'Mobile but isolated'
        
        return info
    
    def _comprehensive_network_analysis(self) -> str:
        """Provide comprehensive network analysis"""
        
        results = []
        results.append("📊 COMPREHENSIVE NETWORK ANALYSIS")
        results.append("=" * 80)
        
        # Build networks
        co_loc_network = self._build_co_location_network()
        
        # Basic statistics
        results.append(f"\n📊 NETWORK STATISTICS:")
        results.append(f"   Total Devices: {co_loc_network.number_of_nodes()}")
        results.append(f"   Total Connections: {co_loc_network.number_of_edges()}")
        
        if co_loc_network.number_of_nodes() > 0:
            results.append(f"   Network Density: {nx.density(co_loc_network):.3f}")
            results.append(f"   Connected Components: {nx.number_connected_components(co_loc_network)}")
            
            # Average metrics
            avg_degree = np.mean([d for n, d in co_loc_network.degree()])
            results.append(f"   Average Connections: {avg_degree:.1f}")
        
        # Key findings
        results.append("\n🎯 KEY NETWORK INSIGHTS:")
        
        # Check for hubs
        hub_analysis = self._identify_network_hubs()
        if "NETWORK HUBS IDENTIFIED" in hub_analysis:
            results.append("   • Network hubs detected (key connectors)")
        
        # Check for clusters
        cluster_analysis = self._detect_clusters()
        if "DEVICE CLUSTERS FOUND" in cluster_analysis:
            results.append("   • Multiple device clusters identified")
        
        # Check for isolated devices
        isolated_analysis = self._find_isolated_devices()
        if "COMPLETELY ISOLATED DEVICES" in isolated_analysis:
            results.append("   • Isolated devices found (potential surveillance)")
        
        return "\n".join(results)