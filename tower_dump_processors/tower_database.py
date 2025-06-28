"""
Tower Database
Manages tower location data and coverage information
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from loguru import logger
import json

class TowerDatabase:
    """Database for managing cell tower information"""
    
    def __init__(self, tower_data_path: Optional[str] = None):
        """
        Initialize tower database
        
        Args:
            tower_data_path: Path to tower location data
        """
        self.tower_data_path = Path(tower_data_path) if tower_data_path else Path("data/tower_locations")
        self.towers = {}
        self.tower_df = pd.DataFrame()
        
        # Load tower data if available
        if self.tower_data_path.exists():
            self.load_tower_data()
        
        logger.info("Tower Database initialized")
    
    def load_tower_data(self, file_path: Optional[str] = None):
        """
        Load tower location data from file
        
        Args:
            file_path: Path to tower data file
        """
        if file_path:
            path = Path(file_path)
        else:
            # Look for tower data files
            for ext in ['.csv', '.xlsx', '.json']:
                files = list(self.tower_data_path.glob(f'*{ext}'))
                if files:
                    path = files[0]
                    break
            else:
                logger.warning("No tower data files found")
                return
        
        logger.info(f"Loading tower data from {path}")
        
        if path.suffix == '.csv':
            self.tower_df = pd.read_csv(path)
        elif path.suffix in ['.xlsx', '.xls']:
            self.tower_df = pd.read_excel(path)
        elif path.suffix == '.json':
            with open(path, 'r') as f:
                data = json.load(f)
                self.tower_df = pd.DataFrame(data)
        
        # Standardize columns
        self._standardize_tower_data()
        
        # Build tower dictionary
        self._build_tower_dict()
        
        logger.info(f"Loaded {len(self.towers)} tower locations")
    
    def _standardize_tower_data(self):
        """Standardize tower data columns"""
        
        column_mappings = {
            'tower_id': ['tower_id', 'cell_id', 'site_id', 'bts_id'],
            'lat': ['lat', 'latitude', 'tower_lat'],
            'long': ['long', 'longitude', 'lon', 'tower_long'],
            'address': ['address', 'location', 'site_address'],
            'area': ['area', 'locality', 'region'],
            'city': ['city', 'district'],
            'state': ['state', 'province'],
            'coverage_radius': ['coverage_radius', 'radius', 'range']
        }
        
        # Rename columns
        for standard_name, variations in column_mappings.items():
            for col in self.tower_df.columns:
                if col.lower() in [v.lower() for v in variations]:
                    self.tower_df.rename(columns={col: standard_name}, inplace=True)
                    break
        
        # Ensure required columns
        required_cols = ['tower_id', 'lat', 'long']
        for col in required_cols:
            if col not in self.tower_df.columns:
                logger.warning(f"Missing required column: {col}")
        
        # Set default coverage radius (in km)
        if 'coverage_radius' not in self.tower_df.columns:
            self.tower_df['coverage_radius'] = 1.0  # Default 1km radius
    
    def _build_tower_dict(self):
        """Build dictionary for fast tower lookup"""
        
        for _, row in self.tower_df.iterrows():
            if pd.notna(row.get('tower_id')):
                self.towers[str(row['tower_id'])] = {
                    'lat': row.get('lat'),
                    'long': row.get('long'),
                    'address': row.get('address', ''),
                    'area': row.get('area', ''),
                    'city': row.get('city', ''),
                    'state': row.get('state', ''),
                    'coverage_radius': row.get('coverage_radius', 1.0)
                }
    
    def get_tower_location(self, tower_id: str) -> Optional[Dict]:
        """
        Get location information for a tower
        
        Args:
            tower_id: Tower identifier
            
        Returns:
            Dictionary with tower location info
        """
        return self.towers.get(str(tower_id))
    
    def get_towers_in_area(self, lat: float, long: float, 
                          radius_km: float = 5.0) -> List[Dict]:
        """
        Get all towers within a radius of given coordinates
        
        Args:
            lat: Latitude
            long: Longitude
            radius_km: Search radius in kilometers
            
        Returns:
            List of towers in the area
        """
        towers_in_area = []
        
        for tower_id, info in self.towers.items():
            if pd.notna(info.get('lat')) and pd.notna(info.get('long')):
                distance = self._calculate_distance(
                    lat, long, info['lat'], info['long']
                )
                
                if distance <= radius_km:
                    towers_in_area.append({
                        'tower_id': tower_id,
                        'distance_km': round(distance, 2),
                        **info
                    })
        
        # Sort by distance
        towers_in_area.sort(key=lambda x: x['distance_km'])
        
        return towers_in_area
    
    def get_neighboring_towers(self, tower_id: str, 
                              max_distance_km: float = 5.0) -> List[Dict]:
        """
        Get neighboring towers for handover analysis
        
        Args:
            tower_id: Source tower ID
            max_distance_km: Maximum distance for neighbors
            
        Returns:
            List of neighboring towers
        """
        tower_info = self.get_tower_location(tower_id)
        if not tower_info or pd.isna(tower_info.get('lat')):
            return []
        
        neighbors = self.get_towers_in_area(
            tower_info['lat'], 
            tower_info['long'], 
            max_distance_km
        )
        
        # Remove the source tower
        neighbors = [t for t in neighbors if t['tower_id'] != tower_id]
        
        return neighbors
    
    def enrich_tower_dump(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich tower dump data with location information
        
        Args:
            df: Tower dump DataFrame with tower_id column
            
        Returns:
            Enriched DataFrame
        """
        if 'tower_id' not in df.columns:
            logger.warning("No tower_id column found for enrichment")
            return df
        
        # Add location columns
        location_cols = ['lat', 'long', 'address', 'area', 'city', 'state']
        
        for col in location_cols:
            if col not in df.columns:
                df[f'tower_{col}'] = None
        
        # Enrich each unique tower
        unique_towers = df['tower_id'].unique()
        tower_mapping = {}
        
        for tower_id in unique_towers:
            tower_info = self.get_tower_location(str(tower_id))
            if tower_info:
                tower_mapping[tower_id] = tower_info
        
        # Apply enrichment
        for tower_id, info in tower_mapping.items():
            mask = df['tower_id'] == tower_id
            for col in location_cols:
                if info.get(col) is not None:
                    df.loc[mask, f'tower_{col}'] = info[col]
        
        logger.info(f"Enriched {len(tower_mapping)} towers with location data")
        
        return df
    
    def calculate_coverage_overlap(self, tower1_id: str, 
                                 tower2_id: str) -> float:
        """
        Calculate coverage overlap between two towers
        
        Args:
            tower1_id: First tower ID
            tower2_id: Second tower ID
            
        Returns:
            Overlap percentage (0-100)
        """
        tower1 = self.get_tower_location(tower1_id)
        tower2 = self.get_tower_location(tower2_id)
        
        if not tower1 or not tower2:
            return 0.0
        
        # Calculate distance between towers
        distance = self._calculate_distance(
            tower1['lat'], tower1['long'],
            tower2['lat'], tower2['long']
        )
        
        # Get coverage radii
        r1 = tower1.get('coverage_radius', 1.0)
        r2 = tower2.get('coverage_radius', 1.0)
        
        # If towers are too far apart, no overlap
        if distance > r1 + r2:
            return 0.0
        
        # If one tower is inside the other
        if distance <= abs(r1 - r2):
            return 100.0
        
        # Calculate overlap area using lens formula
        # This is an approximation
        overlap_ratio = 1 - (distance / (r1 + r2))
        overlap_percentage = overlap_ratio * 100
        
        return min(100.0, max(0.0, overlap_percentage))
    
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
    
    def identify_border_towers(self, state_border: Optional[str] = None,
                             international: bool = False) -> List[str]:
        """
        Identify towers near borders
        
        Args:
            state_border: Specific state border to check
            international: Check for international borders
            
        Returns:
            List of border tower IDs
        """
        border_towers = []
        
        # This is a simplified implementation
        # In production, you would have actual border coordinates
        
        if 'state' in self.tower_df.columns:
            # Group by state to find border areas
            state_groups = self.tower_df.groupby('state')
            
            # Logic to identify border towers would go here
            # For now, return empty list
        
        return border_towers
    
    def export_tower_map(self, output_path: str, 
                        tower_ids: Optional[List[str]] = None):
        """
        Export tower locations for mapping
        
        Args:
            output_path: Output file path
            tower_ids: Specific towers to export (None for all)
        """
        if tower_ids:
            export_data = {tid: self.towers[tid] 
                          for tid in tower_ids 
                          if tid in self.towers}
        else:
            export_data = self.towers
        
        output_path = Path(output_path)
        
        if output_path.suffix == '.json':
            with open(output_path, 'w') as f:
                json.dump(export_data, f, indent=2)
        elif output_path.suffix == '.csv':
            df = pd.DataFrame.from_dict(export_data, orient='index')
            df.index.name = 'tower_id'
            df.to_csv(output_path)
        
        logger.info(f"Exported {len(export_data)} towers to {output_path}")