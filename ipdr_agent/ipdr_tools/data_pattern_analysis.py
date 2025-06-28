"""
Data Pattern Analysis Tool for IPDR Intelligence
Analyzes upload/download patterns, large file transfers, and data usage anomalies
"""

from typing import Dict, Optional, Any, List, Type
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from config import settings

class DataPatternAnalysisInput(BaseModel):
    """Input for data pattern analysis tool"""
    query: str = Field(description="What data patterns to analyze (e.g., 'large uploads', 'download patterns', 'data spikes')")
    suspect_name: Optional[str] = Field(default=None, description="Specific suspect to analyze")

class DataPatternAnalysisTool(BaseTool):
    """Tool for analyzing data usage patterns in IPDR data"""
    
    name: str = "ipdr_data_pattern_analysis"
    description: str = """Analyze data usage patterns including upload/download volumes, large file transfers, 
    pattern day activities, and suspicious data behaviors. Detects anomalies like massive uploads (possible 
    evidence sharing), download spikes, and coordinated data transfers. 
    Examples: 'find large uploads', 'analyze Tuesday/Friday data patterns', 'check for data spikes'"""
    
    args_schema: Type[BaseModel] = DataPatternAnalysisInput
    ipdr_data: Dict[str, pd.DataFrame] = {}
    
    def _run(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Run data pattern analysis on IPDR data"""
        try:
            if not self.ipdr_data:
                return "No IPDR data loaded. Please load IPDR data first."
            
            analyze_all = "all" in query.lower() or not suspect_name
            results = []
            suspects_to_analyze = self.ipdr_data.keys() if analyze_all else [suspect_name]
            
            for suspect in suspects_to_analyze:
                if suspect in self.ipdr_data:
                    analysis = self._analyze_suspect_data_patterns(suspect, self.ipdr_data[suspect])
                    results.append(analysis)
            
            if not results:
                return "No suspects found for data pattern analysis."
            
            # Sort by data anomaly score
            results.sort(key=lambda x: x['data_anomaly_score'], reverse=True)
            
            response = self._format_data_pattern_analysis(results, query)
            return response
            
        except Exception as e:
            logger.error(f"Error in data pattern analysis: {str(e)}")
            return f"Error analyzing data patterns: {str(e)}"
    
    async def _arun(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Async not implemented"""
        raise NotImplementedError("Async execution not supported")
    
    def _analyze_suspect_data_patterns(self, suspect: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data usage patterns for a single suspect"""
        
        analysis = {
            'suspect': suspect,
            'total_sessions': len(df),
            'total_upload_mb': 0,
            'total_download_mb': 0,
            'total_data_mb': 0,
            'large_uploads': [],
            'large_downloads': [],
            'data_spikes': [],
            'pattern_day_activity': {},
            'upload_download_ratio': 0.0,
            'data_risk': 'LOW',
            'data_anomaly_score': 0,
            'suspicious_data_patterns': []
        }
        
        # Calculate data volumes
        if 'data_volume_up' in df.columns:
            analysis['total_upload_mb'] = round(df['data_volume_up'].sum() / 1048576, 2)
        
        if 'data_volume_down' in df.columns:
            analysis['total_download_mb'] = round(df['data_volume_down'].sum() / 1048576, 2)
        
        analysis['total_data_mb'] = analysis['total_upload_mb'] + analysis['total_download_mb']
        
        # Upload/Download ratio
        if analysis['total_download_mb'] > 0:
            analysis['upload_download_ratio'] = round(
                analysis['total_upload_mb'] / analysis['total_download_mb'], 2
            )
        
        # Analyze large transfers
        if 'data_volume_up' in df.columns:
            large_upload_threshold = settings.large_upload_threshold  # 10MB default
            large_uploads = df[df['data_volume_up'] > large_upload_threshold].copy()
            
            if len(large_uploads) > 0:
                for _, row in large_uploads.iterrows():
                    upload_info = {
                        'timestamp': row.get('start_time', 'Unknown'),
                        'size_mb': round(row['data_volume_up'] / 1048576, 2),
                        'app': row.get('detected_app', 'Unknown'),
                        'duration': row.get('session_duration', 0)
                    }
                    analysis['large_uploads'].append(upload_info)
                
                # Check for evidence sharing pattern
                if len(large_uploads) > 5:
                    analysis['suspicious_data_patterns'].append({
                        'pattern': 'FREQUENT_LARGE_UPLOADS',
                        'value': f"{len(large_uploads)} large uploads detected",
                        'severity': 'HIGH',
                        'description': 'Possible evidence/material sharing'
                    })
        
        # Analyze temporal patterns
        if 'start_time' in df.columns and pd.api.types.is_datetime64_any_dtype(df['start_time']):
            df['date'] = df['start_time'].dt.date
            df['day_name'] = df['start_time'].dt.day_name()
            
            # Pattern day analysis (Tuesday/Friday)
            for day in ['Tuesday', 'Friday']:
                day_data = df[df['day_name'] == day]
                if len(day_data) > 0:
                    day_upload_mb = day_data['data_volume_up'].sum() / 1048576 if 'data_volume_up' in day_data.columns else 0
                    analysis['pattern_day_activity'][day] = {
                        'sessions': len(day_data),
                        'upload_mb': round(day_upload_mb, 2),
                        'percentage_of_total': round((day_upload_mb / analysis['total_upload_mb']) * 100, 2) if analysis['total_upload_mb'] > 0 else 0
                    }
            
            # Check for pattern day concentration
            tuesday_pct = analysis['pattern_day_activity'].get('Tuesday', {}).get('percentage_of_total', 0)
            friday_pct = analysis['pattern_day_activity'].get('Friday', {}).get('percentage_of_total', 0)
            
            if tuesday_pct + friday_pct > 40:
                analysis['suspicious_data_patterns'].append({
                    'pattern': 'PATTERN_DAY_CONCENTRATION',
                    'value': f"{tuesday_pct + friday_pct:.1f}% uploads on Tuesday/Friday",
                    'severity': 'HIGH',
                    'description': 'Matches narcotics transport days'
                })
            
            # Daily spike detection
            daily_uploads = df.groupby('date')['data_volume_up'].sum() if 'data_volume_up' in df.columns else pd.Series()
            
            if len(daily_uploads) > 5:
                mean_daily = daily_uploads.mean()
                std_daily = daily_uploads.std()
                
                for date, volume in daily_uploads.items():
                    if volume > mean_daily + (2 * std_daily):
                        analysis['data_spikes'].append({
                            'date': str(date),
                            'upload_mb': round(volume / 1048576, 2),
                            'severity': 'HIGH' if volume > mean_daily + (3 * std_daily) else 'MEDIUM'
                        })
        
        # Check for suspicious ratios
        if analysis['upload_download_ratio'] > 2.0 and analysis['total_upload_mb'] > 100:
            analysis['suspicious_data_patterns'].append({
                'pattern': 'HIGH_UPLOAD_RATIO',
                'value': f"Upload/Download ratio: {analysis['upload_download_ratio']}",
                'severity': 'MEDIUM',
                'description': 'Unusual upload-heavy behavior'
            })
        
        # Video/Image sharing detection (large uploads with messaging apps)
        if analysis['large_uploads']:
            messaging_uploads = [u for u in analysis['large_uploads'] 
                               if u['app'] in ['whatsapp', 'telegram', 'signal']]
            if len(messaging_uploads) > 3:
                total_size = sum(u['size_mb'] for u in messaging_uploads)
                analysis['suspicious_data_patterns'].append({
                    'pattern': 'MEDIA_SHARING_VIA_ENCRYPTED_APPS',
                    'value': f"{len(messaging_uploads)} large files ({total_size:.1f} MB) via encrypted apps",
                    'severity': 'HIGH',
                    'description': 'Possible evidence/contraband sharing'
                })
        
        # Calculate anomaly score
        analysis['data_anomaly_score'] = self._calculate_data_anomaly_score(analysis)
        
        # Determine risk level
        if analysis['data_anomaly_score'] >= 70:
            analysis['data_risk'] = 'HIGH'
        elif analysis['data_anomaly_score'] >= 40:
            analysis['data_risk'] = 'MEDIUM'
        else:
            analysis['data_risk'] = 'LOW'
        
        return analysis
    
    def _calculate_data_anomaly_score(self, analysis: Dict[str, Any]) -> int:
        """Calculate data anomaly score (0-100)"""
        
        score = 0
        
        # Large upload frequency
        large_upload_count = len(analysis['large_uploads'])
        if large_upload_count >= 10:
            score += 25
        elif large_upload_count >= 5:
            score += 15
        elif large_upload_count >= 3:
            score += 10
        
        # Total upload volume
        if analysis['total_upload_mb'] >= 1000:  # 1GB+
            score += 20
        elif analysis['total_upload_mb'] >= 500:
            score += 10
        
        # Pattern day concentration
        pattern_concentration = sum(d.get('percentage_of_total', 0) 
                                  for d in analysis['pattern_day_activity'].values())
        if pattern_concentration > 40:
            score += 20
        elif pattern_concentration > 25:
            score += 10
        
        # Data spikes
        high_severity_spikes = sum(1 for spike in analysis['data_spikes'] 
                                  if spike['severity'] == 'HIGH')
        score += min(high_severity_spikes * 10, 20)
        
        # Suspicious patterns
        high_severity_patterns = sum(1 for p in analysis['suspicious_data_patterns'] 
                                   if p['severity'] == 'HIGH')
        score += min(high_severity_patterns * 15, 30)
        
        return min(score, 100)
    
    def _format_data_pattern_analysis(self, results: List[Dict], query: str) -> str:
        """Format data pattern analysis results"""
        
        output = []
        output.append("📊 IPDR DATA PATTERN ANALYSIS")
        output.append("=" * 50)
        
        # High-risk suspects
        high_risk = [r for r in results if r['data_risk'] == 'HIGH']
        
        if high_risk:
            output.append("\n🚨 HIGH DATA ANOMALY SUSPECTS")
            output.append("-" * 40)
            
            for result in high_risk:
                output.append(f"\n🔴 {result['suspect']}")
                output.append(f"   Data Anomaly Score: {result['data_anomaly_score']}/100")
                output.append(f"   Total Upload: {result['total_upload_mb']} MB")
                output.append(f"   Total Download: {result['total_download_mb']} MB")
                output.append(f"   Upload/Download Ratio: {result['upload_download_ratio']}")
                
                if result['large_uploads']:
                    output.append(f"   Large Uploads: {len(result['large_uploads'])} detected")
                    for upload in result['large_uploads'][:3]:
                        output.append(f"     • {upload['size_mb']} MB via {upload['app']} at {upload['timestamp']}")
                
                if result['suspicious_data_patterns']:
                    output.append("   ⚠️ Suspicious Patterns:")
                    for pattern in result['suspicious_data_patterns']:
                        output.append(f"     • {pattern['value']} - {pattern['description']}")
        
        # Pattern day analysis
        pattern_day_suspects = [r for r in results 
                              if any(p['pattern'] == 'PATTERN_DAY_CONCENTRATION' 
                                    for p in r['suspicious_data_patterns'])]
        
        if pattern_day_suspects:
            output.append("\n📅 PATTERN DAY ACTIVITY (Tuesday/Friday)")
            output.append("-" * 40)
            for suspect in pattern_day_suspects:
                output.append(f"\n{suspect['suspect']}:")
                for day, data in suspect['pattern_day_activity'].items():
                    if data['percentage_of_total'] > 15:
                        output.append(f"   • {day}: {data['upload_mb']} MB ({data['percentage_of_total']}% of total)")
        
        # Data spike summary
        all_spikes = []
        for result in results:
            for spike in result['data_spikes']:
                all_spikes.append({
                    'suspect': result['suspect'],
                    'date': spike['date'],
                    'upload_mb': spike['upload_mb'],
                    'severity': spike['severity']
                })
        
        if all_spikes:
            output.append("\n📈 DATA USAGE SPIKES")
            output.append("-" * 40)
            # Sort by date
            all_spikes.sort(key=lambda x: x['date'])
            for spike in all_spikes[:5]:  # Show top 5
                emoji = "🔴" if spike['severity'] == 'HIGH' else "🟡"
                output.append(f"   {emoji} {spike['date']}: {spike['suspect']} uploaded {spike['upload_mb']} MB")
        
        # Overall statistics
        output.append("\n📊 OVERALL DATA STATISTICS")
        output.append("-" * 40)
        total_upload = sum(r['total_upload_mb'] for r in results)
        total_download = sum(r['total_download_mb'] for r in results)
        output.append(f"Total Upload Volume: {total_upload:.1f} MB")
        output.append(f"Total Download Volume: {total_download:.1f} MB")
        output.append(f"Overall Upload/Download Ratio: {(total_upload/total_download):.2f}" if total_download > 0 else "N/A")
        
        # Media sharing detection
        media_sharers = [r for r in results 
                        if any(p['pattern'] == 'MEDIA_SHARING_VIA_ENCRYPTED_APPS' 
                              for p in r['suspicious_data_patterns'])]
        
        if media_sharers:
            output.append("\n🖼️ ENCRYPTED MEDIA SHARING DETECTED")
            output.append("-" * 40)
            for sharer in media_sharers:
                pattern = next(p for p in sharer['suspicious_data_patterns'] 
                             if p['pattern'] == 'MEDIA_SHARING_VIA_ENCRYPTED_APPS')
                output.append(f"   • {sharer['suspect']}: {pattern['value']}")
        
        # Recommendations
        output.append("\n💡 INVESTIGATION RECOMMENDATIONS")
        output.append("-" * 40)
        
        if high_risk:
            output.append("1. Examine large uploads for evidence/contraband sharing")
            output.append("2. Correlate data spikes with criminal activity dates")
            output.append("3. Focus on Tuesday/Friday uploads (drug transport days)")
            output.append("4. Check encrypted app uploads for media evidence")
        else:
            output.append("1. Monitor for sudden increases in upload activity")
            output.append("2. Watch for pattern day concentration development")
        
        return "\n".join(output)