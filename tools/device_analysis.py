"""
Device Analysis Tool for CDR Intelligence
Tracks IMEI/IMSI changes and device switching patterns
"""

from typing import Dict, Optional, Any, Type
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
import pandas as pd
import json
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

class DeviceAnalysisInput(BaseModel):
    """Input for device analysis tool"""
    query: str = Field(description="What to analyze about devices (e.g., 'device switching', 'all suspects')")
    suspect_name: Optional[str] = Field(default=None, description="Specific suspect to analyze")

class DeviceAnalysisTool(BaseTool):
    """Tool for analyzing device patterns in CDR data"""
    
    name: str = "device_analysis"
    description: str = """Analyze device patterns including IMEI/IMSI changes, device switching, and SIM swapping. 
    Use this tool to detect suspicious device behavior like multiple IMEIs (burner phones) or frequent device changes.
    Examples: 'analyze device switching for all suspects', 'check IMEI count for Peer basha'"""
    
    args_schema: Type[BaseModel] = DeviceAnalysisInput
    cdr_data: Dict[str, pd.DataFrame] = {}
    
    def _run(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Run device analysis"""
        try:
            if not self.cdr_data:
                return "No CDR data loaded. Please load data first."
            
            # Determine what to analyze
            analyze_all = "all" in query.lower() or not suspect_name
            
            results = []
            suspects_to_analyze = self.cdr_data.keys() if analyze_all else [suspect_name]
            
            for suspect in suspects_to_analyze:
                if suspect in self.cdr_data:
                    analysis = self._analyze_suspect_devices(suspect, self.cdr_data[suspect])
                    results.append(analysis)
            
            # Format results
            if not results:
                return "No suspects found for analysis."
            
            # Sort by risk (IMEI count)
            results.sort(key=lambda x: x['imei_count'], reverse=True)
            
            # Build response
            response = self._format_device_analysis(results, query)
            return response
            
        except Exception as e:
            logger.error(f"Error in device analysis: {str(e)}")
            return f"Error analyzing devices: {str(e)}"
    
    async def _arun(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Async not implemented"""
        raise NotImplementedError("Async execution not supported")
    
    def _analyze_suspect_devices(self, suspect: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze device patterns for a single suspect"""
        # Filter out provider messages
        df_filtered = df[~df.get('is_provider_message', False)].copy()
        
        imei_col = settings.cdr_columns['imei']
        imsi_col = settings.cdr_columns['imsi']
        
        analysis = {
            'suspect': suspect,
            'imei_count': 0,
            'imsi_count': 0,
            'unique_imeis': [],
            'unique_imsis': [],
            'device_switching_risk': 'LOW',
            'device_changes': []
        }
        
        # Analyze IMEIs
        if imei_col in df_filtered.columns:
            unique_imeis = df_filtered[imei_col].dropna().unique()
            analysis['imei_count'] = len(unique_imeis)
            analysis['unique_imeis'] = list(unique_imeis)[:5]  # Limit to 5
            
            # Determine risk level
            if analysis['imei_count'] >= settings.high_risk_imei_count:
                analysis['device_switching_risk'] = 'HIGH'
            elif analysis['imei_count'] == 2:
                analysis['device_switching_risk'] = 'MEDIUM'
            
            # Track device changes
            analysis['device_changes'] = self._track_device_timeline(df_filtered, imei_col)
        
        # Analyze IMSIs
        if imsi_col in df_filtered.columns:
            unique_imsis = df_filtered[imsi_col].dropna().unique()
            analysis['imsi_count'] = len(unique_imsis)
            analysis['unique_imsis'] = list(unique_imsis)[:5]
        
        return analysis
    
    def _track_device_timeline(self, df: pd.DataFrame, imei_col: str) -> list:
        """Track IMEI changes over time"""
        changes = []
        df_sorted = df.sort_values('datetime')
        
        previous_imei = None
        for _, row in df_sorted.iterrows():
            current_imei = row[imei_col]
            if pd.notna(current_imei) and current_imei != previous_imei and previous_imei is not None:
                changes.append({
                    'date': str(row['datetime'].date()),
                    'from': str(previous_imei)[-4:],  # Last 4 digits
                    'to': str(current_imei)[-4:]
                })
            if pd.notna(current_imei):
                previous_imei = current_imei
        
        return changes[:5]  # Limit to 5 most recent
    
    def _format_device_analysis(self, results: list, query: str) -> str:
        """Format device analysis results"""
        output = []
        
        # Summary header
        high_risk = [r for r in results if r['device_switching_risk'] == 'HIGH']
        medium_risk = [r for r in results if r['device_switching_risk'] == 'MEDIUM']
        
        output.append("🔍 DEVICE ANALYSIS RESULTS")
        output.append("=" * 50)
        
        if high_risk:
            output.append(f"\n🚨 HIGH RISK DEVICE SWITCHING DETECTED: {len(high_risk)} suspect(s)")
        
        # Detailed results
        for result in results:
            risk_emoji = {
                'HIGH': '🔴',
                'MEDIUM': '🟡', 
                'LOW': '🟢'
            }[result['device_switching_risk']]
            
            output.append(f"\n{risk_emoji} {result['suspect']}")
            output.append(f"   IMEIs: {result['imei_count']} device(s)")
            
            if result['imei_count'] > 1:
                output.append(f"   Devices: {', '.join([imei[-4:] for imei in result['unique_imeis'][:3]])}...")
                
            if result['imsi_count'] > 1:
                output.append(f"   SIMs: {result['imsi_count']} SIM card(s)")
            
            if result['device_changes']:
                output.append("   Recent changes:")
                for change in result['device_changes'][:2]:
                    output.append(f"   - {change['date']}: {change['from']} → {change['to']}")
        
        # Risk summary
        output.append(f"\n📊 RISK SUMMARY:")
        output.append(f"   High Risk (3+ devices): {len(high_risk)} suspects")
        output.append(f"   Medium Risk (2 devices): {len(medium_risk)} suspects")
        
        if high_risk:
            output.append(f"\n⚠️ IMMEDIATE INVESTIGATION NEEDED:")
            for r in high_risk:
                output.append(f"   - {r['suspect']}: {r['imei_count']} devices")
        
        return "\n".join(output)