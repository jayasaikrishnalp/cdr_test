"""
Enhanced Integrated CDR-IPDR-Tower Intelligence Agent
Combines CDR, IPDR, and Tower Dump analysis for comprehensive criminal intelligence
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
from agent.cdr_agent import CDRIntelligenceAgent
from ipdr_agent.ipdr_agent import IPDRAgent
from tower_dump_agent.tower_dump_agent import TowerDumpAgent
from integrated_agent.correlation_tools.cdr_ipdr_correlator import CDRIPDRCorrelator

class EnhancedIntegratedAgent:
    """
    Enhanced integrated agent that combines CDR, IPDR, and Tower Dump analysis
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize enhanced integrated agent with CDR, IPDR, and Tower Dump capabilities"""
        
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("OpenRouter API key not provided")
        
        # Initialize sub-agents
        logger.info("Initializing CDR Agent...")
        self.cdr_agent = CDRIntelligenceAgent()
        
        logger.info("Initializing IPDR Agent...")
        self.ipdr_agent = IPDRAgent(api_key=self.api_key)
        
        logger.info("Initializing Tower Dump Agent...")
        self.tower_dump_agent = TowerDumpAgent(api_key=self.api_key)
        
        # Initialize correlator
        self.correlator = CDRIPDRCorrelator()
        
        # Data storage
        self.cdr_data = {}
        self.ipdr_data = {}
        self.tower_dump_data = {}
        self.correlation_results = {}
        
        # Initialize integrated LLM
        self.llm = ChatOpenAI(
            model_name=settings.openrouter_model,
            openai_api_key=self.api_key,
            openai_api_base=settings.openrouter_base_url,
            temperature=0.3,
            max_tokens=4000
        )
        
        # Create enhanced integrated agent
        self.agent_executor = self._create_integrated_agent()
        
        logger.info("Enhanced Integrated Intelligence Agent initialized successfully")
    
    def _create_integrated_agent(self) -> AgentExecutor:
        """Create the enhanced integrated analysis agent"""
        
        prompt = PromptTemplate.from_template("""
You are an Enhanced Criminal Intelligence Analyst with expertise in CDR (Call Detail Records), IPDR (Internet Protocol Detail Records), and Tower Dump analysis. You correlate voice communications, digital footprints, and physical presence data to build comprehensive criminal profiles and identify suspects.

Your capabilities include:
1. CDR Analysis: Call patterns, device switching, network analysis, temporal anomalies
2. IPDR Analysis: Encrypted apps, data transfers, session behaviors, app fingerprinting
3. Tower Dump Analysis: Crime scene presence, movement patterns, device identity tracking, geofencing
4. Correlation: Voice-to-data patterns, location-to-activity matching, comprehensive suspect profiling

Current data loaded:
- CDR Suspects: {cdr_suspects}
- IPDR Suspects: {ipdr_suspects}
- Tower Dumps: {tower_dumps}
- Correlation Status: {correlation_status}

You have access to the following tools:

{tools}

When analyzing:
- Cross-reference findings across all three data sources
- Identify patterns that span voice, data, and location
- Build comprehensive suspect profiles
- Provide actionable intelligence for investigations

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
        
        # Combine tools from all agents
        all_tools = (self.cdr_agent.tools + 
                    self.ipdr_agent.tools + 
                    self.tower_dump_agent.tools)
        
        agent = create_react_agent(
            llm=self.llm,
            tools=all_tools,
            prompt=prompt
        )
        
        agent_executor = AgentExecutor(
            agent=agent,
            tools=all_tools,
            verbose=True,
            max_iterations=20,
            handle_parsing_errors=True,
            return_intermediate_steps=True
        )
        
        return agent_executor
    
    def load_all_data(self, cdr_files: Optional[List[str]] = None, 
                      ipdr_files: Optional[List[str]] = None,
                      tower_dump_files: Optional[List[str]] = None) -> Dict[str, Any]:
        """Load CDR, IPDR, and Tower Dump data"""
        
        results = {
            'cdr_load': None,
            'ipdr_load': None,
            'tower_dump_load': None,
            'status': 'success',
            'message': ''
        }
        
        # Load CDR data
        if cdr_files:
            try:
                logger.info("Loading CDR data...")
                cdr_result = self.cdr_agent.load_cdr_data(cdr_files)
                self.cdr_data = self.cdr_agent.cdr_data
                results['cdr_load'] = cdr_result
                
                # Share CDR data with tools
                for tool in self.cdr_agent.tools:
                    if hasattr(tool, 'cdr_data'):
                        tool.cdr_data = self.cdr_data
                
                # Share with tower dump agent for cross-reference
                self.tower_dump_agent.set_cdr_data(self.cdr_data)
                
            except Exception as e:
                logger.error(f"Error loading CDR data: {e}")
                results['cdr_load'] = {'status': 'error', 'message': str(e)}
                results['status'] = 'partial'
        
        # Load IPDR data
        if ipdr_files:
            try:
                logger.info("Loading IPDR data...")
                ipdr_result = self.ipdr_agent.load_ipdr_data(ipdr_files)
                self.ipdr_data = self.ipdr_agent.ipdr_data
                results['ipdr_load'] = ipdr_result
                
                # Share IPDR data with tools
                for tool in self.ipdr_agent.tools:
                    if hasattr(tool, 'ipdr_data'):
                        tool.ipdr_data = self.ipdr_data
                
                # Share with tower dump agent for cross-reference
                self.tower_dump_agent.set_ipdr_data(self.ipdr_data)
                
            except Exception as e:
                logger.error(f"Error loading IPDR data: {e}")
                results['ipdr_load'] = {'status': 'error', 'message': str(e)}
                results['status'] = 'partial'
        
        # Load Tower Dump data
        if tower_dump_files:
            try:
                logger.info("Loading Tower Dump data...")
                if len(tower_dump_files) == 1:
                    tower_result = self.tower_dump_agent.load_tower_dump(tower_dump_files[0])
                else:
                    tower_result = self.tower_dump_agent.load_multiple_dumps(tower_dump_files)
                
                self.tower_dump_data = self.tower_dump_agent.tower_dump_data
                results['tower_dump_load'] = tower_result
                
            except Exception as e:
                logger.error(f"Error loading Tower Dump data: {e}")
                results['tower_dump_load'] = {'status': 'error', 'message': str(e)}
                results['status'] = 'partial'
        
        # Summary
        cdr_count = len(self.cdr_data)
        ipdr_count = len(self.ipdr_data)
        tower_dump_count = len(self.tower_dump_data)
        
        results['summary'] = {
            'cdr_suspects': cdr_count,
            'ipdr_suspects': ipdr_count,
            'tower_dumps': tower_dump_count,
            'common_cdr_ipdr': len(set(self.cdr_data.keys()) & set(self.ipdr_data.keys()))
        }
        
        logger.info(f"Data loading complete: {cdr_count} CDR, {ipdr_count} IPDR, {tower_dump_count} Tower Dumps")
        
        return results
    
    def correlate_all_data(self) -> Dict[str, Any]:
        """Correlate CDR, IPDR, and Tower Dump data"""
        
        results = {
            'cdr_ipdr_correlation': None,
            'tower_correlation': None,
            'status': 'success'
        }
        
        # CDR-IPDR correlation
        if self.cdr_data and self.ipdr_data:
            try:
                logger.info("Correlating CDR and IPDR data...")
                self.correlation_results = self.correlator.correlate_suspects(
                    self.cdr_data, 
                    self.ipdr_data
                )
                results['cdr_ipdr_correlation'] = {
                    'correlated_suspects': len(self.correlation_results['suspect_correlations']),
                    'cross_patterns': len(self.correlation_results['cross_suspect_patterns']),
                    'evidence_chains': len(self.correlation_results['evidence_chains'])
                }
            except Exception as e:
                logger.error(f"Error in CDR-IPDR correlation: {e}")
                results['cdr_ipdr_correlation'] = {'error': str(e)}
                results['status'] = 'partial'
        
        # Tower dump correlation
        if self.tower_dump_data and (self.cdr_data or self.ipdr_data):
            try:
                logger.info("Correlating Tower Dump with CDR/IPDR...")
                # This will be handled by the cross-reference tool during analysis
                results['tower_correlation'] = {
                    'status': 'ready',
                    'message': 'Tower dump correlation available through cross-reference analysis'
                }
            except Exception as e:
                logger.error(f"Error in tower correlation: {e}")
                results['tower_correlation'] = {'error': str(e)}
                results['status'] = 'partial'
        
        return results
    
    def analyze(self, query: str) -> str:
        """Analyze using integrated CDR-IPDR-Tower intelligence"""
        
        # Prepare context
        cdr_suspects = ", ".join(list(self.cdr_data.keys())[:5]) if self.cdr_data else "None"
        if len(self.cdr_data) > 5:
            cdr_suspects += f" (and {len(self.cdr_data) - 5} more)"
        
        ipdr_suspects = ", ".join(list(self.ipdr_data.keys())[:5]) if self.ipdr_data else "None"
        if len(self.ipdr_data) > 5:
            ipdr_suspects += f" (and {len(self.ipdr_data) - 5} more)"
        
        tower_dumps = ", ".join(list(self.tower_dump_data.keys())[:3]) if self.tower_dump_data else "None"
        if len(self.tower_dump_data) > 3:
            tower_dumps += f" (and {len(self.tower_dump_data) - 3} more)"
        
        correlation_status = "Available" if self.correlation_results else "Not performed"
        
        try:
            result = self.agent_executor.invoke({
                "input": query,
                "cdr_suspects": cdr_suspects,
                "ipdr_suspects": ipdr_suspects,
                "tower_dumps": tower_dumps,
                "correlation_status": correlation_status
            })
            
            return result['output']
            
        except Exception as e:
            logger.error(f"Error during integrated analysis: {str(e)}")
            return f"Error analyzing data: {str(e)}"
    
    def analyze_crime_scene(self, crime_location: str, crime_time: str, 
                          radius_km: float = 1.0) -> str:
        """Comprehensive crime scene analysis using all data sources"""
        
        return self.analyze(
            f"Perform comprehensive crime scene analysis for incident at {crime_location} "
            f"at {crime_time} with {radius_km}km radius. Include: "
            f"1) Tower dump: devices present at scene "
            f"2) CDR: calls made before/during/after "
            f"3) IPDR: data activity and encrypted communications "
            f"4) Cross-reference: silent devices, coordinated activity "
            f"5) Prioritized suspect list with evidence"
        )
    
    def track_suspect_journey(self, suspect: str, date: str) -> str:
        """Track complete journey of a suspect using all data sources"""
        
        return self.analyze(
            f"Track complete journey of {suspect} on {date} using: "
            f"1) Tower dump: movement trajectory and speed "
            f"2) CDR: calls made at each location "
            f"3) IPDR: data usage patterns during movement "
            f"4) Identify: stops, meetings, suspicious behaviors"
        )
    
    def find_network_connections(self, suspect: str) -> str:
        """Find all network connections for a suspect across data sources"""
        
        return self.analyze(
            f"Map complete network for {suspect} including: "
            f"1) CDR: voice call contacts "
            f"2) IPDR: digital connections and apps used "
            f"3) Tower dump: co-located devices "
            f"4) Identify: communication hierarchy, key associates"
        )
    
    def detect_surveillance_evasion(self) -> str:
        """Detect surveillance evasion techniques across all data"""
        
        return self.analyze(
            "Detect surveillance evasion techniques including: "
            "1) Device switching (IMEI/IMSI changes) "
            "2) Encrypted communication usage "
            "3) Silent periods with movement "
            "4) Burner phone patterns "
            "5) Location spoofing attempts"
        )
    
    def generate_comprehensive_report(self, output_file: Optional[Path] = None) -> str:
        """Generate comprehensive intelligence report using all data sources"""
        
        report_sections = []
        
        # Header
        report_sections.append("# COMPREHENSIVE INTELLIGENCE REPORT")
        report_sections.append("CDR + IPDR + Tower Dump Analysis")
        report_sections.append("=" * 80)
        
        # Executive Summary
        report_sections.append("\n## EXECUTIVE SUMMARY")
        summary = self.analyze(
            "Provide executive summary of key findings from CDR, IPDR, and Tower Dump analysis. "
            "Highlight critical patterns, highest risk suspects, and immediate action items."
        )
        report_sections.append(summary)
        
        # Data Overview
        report_sections.append("\n## DATA OVERVIEW")
        overview = f"""
CDR Data: {len(self.cdr_data)} suspects loaded
IPDR Data: {len(self.ipdr_data)} suspects loaded  
Tower Dumps: {len(self.tower_dump_data)} dumps loaded
Common Suspects (CDR+IPDR): {len(set(self.cdr_data.keys()) & set(self.ipdr_data.keys()))}
"""
        report_sections.append(overview)
        
        # Tower Dump Analysis
        report_sections.append("\n## TOWER DUMP ANALYSIS")
        if self.tower_dump_data:
            tower_analysis = self.tower_dump_agent.analyze(
                "Summarize key findings from tower dump analysis including "
                "crime scene presence, movement patterns, and device anomalies"
            )
            report_sections.append(tower_analysis)
        else:
            report_sections.append("No tower dump data loaded")
        
        # CDR Analysis
        report_sections.append("\n## CDR ANALYSIS")
        if self.cdr_data:
            cdr_analysis = self.cdr_agent.analyze(
                "Summarize key CDR findings including communication patterns, "
                "device switching, and network relationships"
            )
            report_sections.append(cdr_analysis)
        else:
            report_sections.append("No CDR data loaded")
        
        # IPDR Analysis
        report_sections.append("\n## IPDR ANALYSIS")
        if self.ipdr_data:
            ipdr_analysis = self.ipdr_agent.analyze(
                "Summarize key IPDR findings including encryption usage, "
                "data patterns, and digital behaviors"
            )
            report_sections.append(ipdr_analysis)
        else:
            report_sections.append("No IPDR data loaded")
        
        # Cross-Reference Analysis
        report_sections.append("\n## CROSS-REFERENCE ANALYSIS")
        if self.tower_dump_data and (self.cdr_data or self.ipdr_data):
            cross_ref = self.analyze(
                "Perform cross-reference analysis between tower dump and CDR/IPDR. "
                "Find silent devices, location-activity correlations, and evidence chains."
            )
            report_sections.append(cross_ref)
        else:
            report_sections.append("Insufficient data for cross-reference analysis")
        
        # Integrated Risk Assessment
        report_sections.append("\n## INTEGRATED RISK ASSESSMENT")
        risk_assessment = self.analyze(
            "Provide integrated risk assessment combining all data sources. "
            "Rank suspects by overall risk and evidence strength."
        )
        report_sections.append(risk_assessment)
        
        # Evidence Chains
        report_sections.append("\n## EVIDENCE CHAINS")
        evidence = self.analyze(
            "Identify evidence chains linking tower presence, voice calls, and data activity. "
            "Focus on proving suspect presence and criminal activity."
        )
        report_sections.append(evidence)
        
        # Investigation Roadmap
        report_sections.append("\n## INVESTIGATION ROADMAP")
        roadmap = self.analyze(
            "Provide detailed investigation roadmap with: "
            "1) Priority suspects for immediate action "
            "2) Additional data needs "
            "3) Surveillance recommendations "
            "4) Evidence collection guidance"
        )
        report_sections.append(roadmap)
        
        report_content = "\n".join(report_sections)
        
        # Save to file if specified
        if output_file:
            output_file.write_text(report_content)
            logger.info(f"Comprehensive report saved to {output_file}")
        
        return report_content
    
    def real_time_alert(self, device_id: str) -> str:
        """Generate real-time alert for a specific device"""
        
        return self.analyze(
            f"Generate real-time intelligence alert for device {device_id}: "
            f"1) Current tower location and movement "
            f"2) Recent call activity "
            f"3) Data usage patterns "
            f"4) Risk indicators and recommendations"
        )