"""
Tower Dump Intelligence Agent
Natural language interface for tower dump analysis
"""

import os
from typing import Dict, List, Optional, Any
from pathlib import Path
import pandas as pd
from loguru import logger

from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor
from langchain.prompts import PromptTemplate

import sys
sys.path.append(str(Path(__file__).parent.parent))

from config import settings
from tower_dump_processors import TowerDumpLoader, TowerDatabase, TowerDumpValidator
from tower_analysis_tools import (
    TimeWindowFilterTool,
    BehaviorPatternTool,
    DeviceIdentityTool,
    MovementAnalysisTool,
    GeofencingTool,
    CrossReferenceTool,
    NetworkAnalysisTool
)

class TowerDumpAgent:
    """
    Tower Dump Intelligence Agent for analyzing mobile tower connection data
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize Tower Dump Agent"""
        
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("OpenRouter API key not provided")
        
        # Initialize data processors
        self.loader = TowerDumpLoader()
        self.tower_db = TowerDatabase()
        self.validator = TowerDumpValidator()
        
        # Data storage
        self.tower_dump_data = {}
        self.validation_reports = {}
        
        # Initialize LLM
        self.llm = ChatOpenAI(
            model_name=settings.openrouter_model,
            openai_api_key=self.api_key,
            openai_api_base=settings.openrouter_base_url,
            temperature=0.3,
            max_tokens=4000
        )
        
        # Initialize tools
        self.tools = self._initialize_tools()
        
        # Create agent
        self.agent_executor = self._create_agent()
        
        logger.info("Tower Dump Agent initialized successfully")
    
    def _initialize_tools(self) -> List:
        """Initialize all tower dump analysis tools"""
        
        tools = [
            TimeWindowFilterTool(),
            BehaviorPatternTool(),
            DeviceIdentityTool(),
            MovementAnalysisTool(),
            GeofencingTool(),
            CrossReferenceTool(),
            NetworkAnalysisTool()
        ]
        
        # Share data with tools
        for tool in tools:
            if hasattr(tool, 'tower_dump_data'):
                tool.tower_dump_data = self.tower_dump_data
        
        logger.info(f"Initialized {len(tools)} tower dump analysis tools")
        return tools
    
    def _create_agent(self) -> AgentExecutor:
        """Create the tower dump analysis agent"""
        
        prompt = PromptTemplate.from_template("""
You are a Tower Dump Intelligence Analyst specializing in analyzing mobile tower connection data for criminal investigations. You help law enforcement identify suspects present at crime scenes and analyze their behavioral patterns.

Your capabilities include:
1. Time Window Analysis: Filter data by crime windows, detect odd-hour activity
2. Behavior Detection: Identify one-time visitors, frequent visitors, reconnaissance patterns
3. Device Identity: Track IMEI/IMSI changes, detect SIM swapping and device cloning
4. Movement Analysis: Track trajectories, detect vehicle movement, identify impossible travel
5. Geofencing: Define crime scene perimeters, analyze dwell times
6. Cross-Reference: Link tower data with CDR/IPDR records
7. Network Analysis: Identify communication hubs and device clusters

Current tower dump data loaded:
- Tower Dumps: {tower_dumps}
- Total Records: {total_records}
- Time Range: {time_range}

Available tools:
{tools}

When analyzing tower dump data:
- Focus on patterns relevant to criminal investigations
- Identify suspicious behaviors and anomalies
- Provide clear, actionable intelligence
- Consider legitimate reasons for presence when appropriate

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Begin!

Question: {input}
Thought: {agent_scratchpad}""")
        
        agent = create_react_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=prompt
        )
        
        agent_executor = AgentExecutor(
            agent=agent,
            tools=self.tools,
            verbose=True,
            max_iterations=10,
            handle_parsing_errors=True,
            return_intermediate_steps=True
        )
        
        return agent_executor
    
    def load_tower_dump(self, file_path: str, provider: Optional[str] = None,
                       dump_id: Optional[str] = None) -> Dict[str, Any]:
        """Load a tower dump file"""
        
        try:
            # Generate dump ID if not provided
            if not dump_id:
                dump_id = Path(file_path).stem
            
            logger.info(f"Loading tower dump: {dump_id} from {file_path}")
            
            # Load data
            tower_data = self.loader.load_tower_dump(file_path, provider)
            
            # Validate data
            validation_report = self.validator.validate_tower_dump(tower_data)
            self.validation_reports[dump_id] = validation_report
            
            if not validation_report['is_valid']:
                logger.warning(f"Validation issues: {validation_report['errors']}")
            
            # Store data
            self.tower_dump_data[dump_id] = tower_data
            
            # Update tools with new data
            for tool in self.tools:
                if hasattr(tool, 'tower_dump_data'):
                    tool.tower_dump_data = self.tower_dump_data
            
            # Get summary
            summary = {
                'dump_id': dump_id,
                'status': 'success',
                'records': len(tower_data),
                'unique_devices': tower_data['mobile_number'].nunique() if 'mobile_number' in tower_data.columns else 0,
                'unique_towers': tower_data['tower_id'].nunique() if 'tower_id' in tower_data.columns else 0,
                'time_range': {
                    'start': str(tower_data['timestamp'].min()) if 'timestamp' in tower_data.columns else None,
                    'end': str(tower_data['timestamp'].max()) if 'timestamp' in tower_data.columns else None
                },
                'validation': validation_report
            }
            
            logger.info(f"Tower dump loaded: {summary['records']} records, {summary['unique_devices']} devices")
            
            return summary
            
        except Exception as e:
            logger.error(f"Error loading tower dump: {str(e)}")
            return {
                'dump_id': dump_id,
                'status': 'error',
                'message': str(e)
            }
    
    def load_multiple_dumps(self, file_paths: List[str]) -> Dict[str, Any]:
        """Load multiple tower dump files"""
        
        results = {
            'loaded': [],
            'failed': [],
            'total_records': 0,
            'total_devices': set()
        }
        
        for file_path in file_paths:
            result = self.load_tower_dump(file_path)
            
            if result['status'] == 'success':
                results['loaded'].append(result['dump_id'])
                results['total_records'] += result['records']
                
                # Add unique devices
                dump_data = self.tower_dump_data[result['dump_id']]
                if 'mobile_number' in dump_data.columns:
                    results['total_devices'].update(dump_data['mobile_number'].unique())
            else:
                results['failed'].append({
                    'file': file_path,
                    'error': result.get('message', 'Unknown error')
                })
        
        results['total_devices'] = len(results['total_devices'])
        results['summary'] = f"Loaded {len(results['loaded'])} dumps with {results['total_records']} records and {results['total_devices']} unique devices"
        
        return results
    
    def set_cdr_data(self, cdr_data: Dict[str, pd.DataFrame]):
        """Set CDR data for cross-reference analysis"""
        
        # Share with cross-reference tool
        for tool in self.tools:
            if hasattr(tool, 'cdr_data'):
                tool.cdr_data = cdr_data
        
        logger.info(f"CDR data set for {len(cdr_data)} suspects")
    
    def set_ipdr_data(self, ipdr_data: Dict[str, pd.DataFrame]):
        """Set IPDR data for cross-reference analysis"""
        
        # Share with cross-reference tool
        for tool in self.tools:
            if hasattr(tool, 'ipdr_data'):
                tool.ipdr_data = ipdr_data
        
        logger.info(f"IPDR data set for {len(ipdr_data)} suspects")
    
    def analyze(self, query: str) -> str:
        """Analyze tower dump data using natural language query"""
        
        # Prepare context
        tower_dumps = ", ".join(self.tower_dump_data.keys()) if self.tower_dump_data else "None"
        
        total_records = sum(len(df) for df in self.tower_dump_data.values()) if self.tower_dump_data else 0
        
        time_range = "Not available"
        if self.tower_dump_data:
            all_data = pd.concat(self.tower_dump_data.values(), ignore_index=True)
            if 'timestamp' in all_data.columns:
                time_range = f"{all_data['timestamp'].min()} to {all_data['timestamp'].max()}"
        
        try:
            result = self.agent_executor.invoke({
                "input": query,
                "tower_dumps": tower_dumps,
                "total_records": total_records,
                "time_range": time_range
            })
            
            return result['output']
            
        except Exception as e:
            logger.error(f"Error during tower dump analysis: {str(e)}")
            return f"Error analyzing tower dump: {str(e)}"
    
    def analyze_crime_scene(self, crime_time: str, duration_hours: int = 2,
                          tower_ids: Optional[List[str]] = None) -> str:
        """Analyze activity around a crime scene"""
        
        query = f"""Analyze tower dump data for a crime that occurred at {crime_time}.
        Focus on a {duration_hours} hour window around the crime time.
        """
        
        if tower_ids:
            query += f"Specifically analyze towers: {', '.join(tower_ids)}. "
        
        query += """Identify:
        1. One-time visitors during the crime window
        2. Devices that left immediately after
        3. Any reconnaissance patterns before the crime
        4. Suspicious device behaviors (IMEI changes, rapid movement)
        5. Provide a prioritized list of suspects
        """
        
        return self.analyze(query)
    
    def find_device_patterns(self, device_id: str) -> str:
        """Analyze patterns for a specific device"""
        
        return self.analyze(
            f"Analyze all patterns for device {device_id} including: "
            f"1) Movement trajectory and speed analysis "
            f"2) Tower connection patterns "
            f"3) Time-based activity patterns "
            f"4) IMEI/IMSI changes "
            f"5) Co-location with other devices"
        )
    
    def detect_group_activity(self) -> str:
        """Detect coordinated group activities"""
        
        return self.analyze(
            "Detect group activities and coordination patterns including: "
            "1) Devices moving together (convoy detection) "
            "2) Devices appearing/disappearing at same times "
            "3) Network clusters and communication hubs "
            "4) Synchronized tower connections"
        )
    
    def analyze_movement_patterns(self) -> str:
        """Analyze movement patterns in the data"""
        
        return self.analyze(
            "Analyze movement patterns including: "
            "1) Vehicle-based movements (20-200 km/h) "
            "2) Impossible travel (>500 km/h) indicating cloning "
            "3) Entry and exit patterns from areas "
            "4) Suspicious movement trajectories"
        )
    
    def cross_reference_analysis(self) -> str:
        """Cross-reference tower dump with CDR/IPDR if available"""
        
        return self.analyze(
            "Perform cross-reference analysis: "
            "1) Find devices present in tower dump but silent (no CDR) "
            "2) Correlate tower presence with call/data activity "
            "3) Identify devices with mismatched location and activity "
            "4) Build comprehensive suspect profiles"
        )
    
    def generate_investigation_report(self, output_file: Optional[Path] = None) -> str:
        """Generate comprehensive tower dump investigation report"""
        
        report_sections = []
        
        # Header
        report_sections.append("# TOWER DUMP ANALYSIS REPORT")
        report_sections.append("=" * 80)
        
        # Data Summary
        report_sections.append("\n## DATA SUMMARY")
        if self.tower_dump_data:
            summary_text = f"Tower Dumps Loaded: {len(self.tower_dump_data)}\n"
            
            for dump_id, data in self.tower_dump_data.items():
                summary_text += f"\n{dump_id}:"
                summary_text += f"\n  - Records: {len(data)}"
                if 'mobile_number' in data.columns:
                    summary_text += f"\n  - Unique Devices: {data['mobile_number'].nunique()}"
                if 'tower_id' in data.columns:
                    summary_text += f"\n  - Towers: {data['tower_id'].nunique()}"
                if 'timestamp' in data.columns:
                    summary_text += f"\n  - Time Range: {data['timestamp'].min()} to {data['timestamp'].max()}"
            
            report_sections.append(summary_text)
        else:
            report_sections.append("No tower dump data loaded")
        
        # Key Findings
        report_sections.append("\n## KEY FINDINGS")
        findings = self.analyze(
            "Summarize the most important findings from tower dump analysis including: "
            "suspicious devices, behavioral patterns, and investigative leads"
        )
        report_sections.append(findings)
        
        # One-Time Visitors
        report_sections.append("\n## ONE-TIME VISITORS ANALYSIS")
        one_time = self.analyze("Identify and analyze one-time visitors and their significance")
        report_sections.append(one_time)
        
        # Movement Patterns
        report_sections.append("\n## MOVEMENT ANALYSIS")
        movement = self.analyze_movement_patterns()
        report_sections.append(movement)
        
        # Device Identity Analysis
        report_sections.append("\n## DEVICE IDENTITY ANALYSIS")
        identity = self.analyze("Analyze device identity patterns including IMEI/IMSI changes and anomalies")
        report_sections.append(identity)
        
        # Network Analysis
        report_sections.append("\n## NETWORK ANALYSIS")
        network = self.analyze("Provide network analysis including hubs, clusters, and relationships")
        report_sections.append(network)
        
        # Investigation Recommendations
        report_sections.append("\n## INVESTIGATION RECOMMENDATIONS")
        recommendations = self.analyze(
            "Based on all findings, provide specific investigation recommendations "
            "including priority suspects and next steps"
        )
        report_sections.append(recommendations)
        
        report_content = "\n".join(report_sections)
        
        # Save to file if specified
        if output_file:
            output_file.write_text(report_content)
            logger.info(f"Tower dump report saved to {output_file}")
        
        return report_content
    
    def get_suspect_list(self, risk_threshold: float = 0.7) -> str:
        """Get prioritized list of suspects based on risk"""
        
        return self.analyze(
            f"Generate a prioritized list of suspects with risk scores above {risk_threshold}. "
            f"Include: device ID, risk factors, key behaviors, and investigation priority"
        )