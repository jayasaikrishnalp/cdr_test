"""
Integrated CDR-IPDR Intelligence Agent
Combines CDR and IPDR analysis for comprehensive criminal intelligence
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
from integrated_agent.correlation_tools.cdr_ipdr_correlator import CDRIPDRCorrelator
from integrated_agent.unicode_filter import clean_unicode_text

class IntegratedIntelligenceAgent:
    """
    Integrated agent that combines CDR and IPDR analysis with correlation capabilities
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize integrated agent with both CDR and IPDR capabilities"""
        
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("OpenRouter API key not provided")
        
        # Initialize sub-agents
        logger.info("Initializing CDR Agent...")
        self.cdr_agent = CDRIntelligenceAgent()
        
        logger.info("Initializing IPDR Agent...")
        self.ipdr_agent = IPDRAgent(api_key=self.api_key)
        
        # Initialize correlator
        self.correlator = CDRIPDRCorrelator()
        
        # Data storage
        self.cdr_data = {}
        self.ipdr_data = {}
        self.correlation_results = {}
        
        # Initialize integrated LLM
        self.llm = ChatOpenAI(
            model_name=settings.openrouter_model,
            openai_api_key=self.api_key,
            openai_api_base=settings.openrouter_base_url,
            temperature=0.3,
            max_tokens=4000
        )
        
        # Create integrated agent
        self.agent_executor = self._create_integrated_agent()
        
        logger.info("Integrated Intelligence Agent initialized successfully")
    
    def _create_integrated_agent(self) -> AgentExecutor:
        """Create the integrated analysis agent"""
        
        prompt = PromptTemplate.from_template("""
You are an Integrated Criminal Intelligence Analyst with expertise in both CDR (Call Detail Records) and IPDR (Internet Protocol Detail Records) analysis. You correlate voice communication patterns with digital footprints to build comprehensive criminal profiles.

Your capabilities include:
1. CDR Analysis: Call patterns, device switching, network analysis, temporal anomalies
2. IPDR Analysis: Encrypted apps, data transfers, session behaviors, app fingerprinting
3. Correlation: Voice-to-data patterns, silence analysis, evidence chains, cross-suspect coordination

Current data loaded:
- CDR Suspects: {cdr_suspects}
- IPDR Suspects: {ipdr_suspects}
- Correlation Available: {correlation_status}

You have access to the following tools:

{tools}

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
        
        # Combine tools from both agents
        all_tools = self.cdr_agent.tools + self.ipdr_agent.tools
        
        agent = create_react_agent(
            llm=self.llm,
            tools=all_tools,
            prompt=prompt
        )
        
        agent_executor = AgentExecutor(
            agent=agent,
            tools=all_tools,
            verbose=True,
            max_iterations=15,
            handle_parsing_errors=True,
            return_intermediate_steps=True
        )
        
        return agent_executor
    
    def load_all_data(self, cdr_files: Optional[List[str]] = None, 
                      ipdr_files: Optional[List[str]] = None) -> Dict[str, Any]:
        """Load both CDR and IPDR data"""
        
        results = {
            'cdr_load': None,
            'ipdr_load': None,
            'status': 'success',
            'message': ''
        }
        
        # Load CDR data
        try:
            logger.info("Loading CDR data...")
            cdr_result = self.cdr_agent.load_cdr_data(cdr_files)
            self.cdr_data = self.cdr_agent.cdr_data
            results['cdr_load'] = cdr_result
            
            # Share CDR data with tools
            for tool in self.cdr_agent.tools:
                if hasattr(tool, 'cdr_data'):
                    tool.cdr_data = self.cdr_data
        except Exception as e:
            logger.error(f"Error loading CDR data: {e}")
            results['cdr_load'] = {'status': 'error', 'message': str(e)}
            results['status'] = 'partial'
        
        # Load IPDR data
        try:
            logger.info("Loading IPDR data...")
            ipdr_result = self.ipdr_agent.load_ipdr_data(ipdr_files)
            self.ipdr_data = self.ipdr_agent.ipdr_data
            results['ipdr_load'] = ipdr_result
            
            # Share IPDR data with tools
            for tool in self.ipdr_agent.tools:
                if hasattr(tool, 'ipdr_data'):
                    tool.ipdr_data = self.ipdr_data
        except Exception as e:
            logger.error(f"Error loading IPDR data: {e}")
            results['ipdr_load'] = {'status': 'error', 'message': str(e)}
            results['status'] = 'partial'
        
        # Summary
        cdr_count = len(self.cdr_data)
        ipdr_count = len(self.ipdr_data)
        
        results['summary'] = {
            'cdr_suspects': cdr_count,
            'ipdr_suspects': ipdr_count,
            'common_suspects': len(set(self.cdr_data.keys()) & set(self.ipdr_data.keys()))
        }
        
        logger.info(f"Data loading complete: {cdr_count} CDR, {ipdr_count} IPDR suspects")
        
        return results
    
    def correlate_data(self) -> Dict[str, Any]:
        """Correlate CDR and IPDR data"""
        
        if not self.cdr_data or not self.ipdr_data:
            return {
                'status': 'error',
                'message': 'Both CDR and IPDR data must be loaded before correlation'
            }
        
        try:
            logger.info("Starting CDR-IPDR correlation...")
            self.correlation_results = self.correlator.correlate_suspects(
                self.cdr_data, 
                self.ipdr_data
            )
            
            # Generate correlation report
            report = self.correlator.generate_correlation_report(self.correlation_results)
            
            return {
                'status': 'success',
                'correlated_suspects': len(self.correlation_results['suspect_correlations']),
                'cross_patterns': len(self.correlation_results['cross_suspect_patterns']),
                'evidence_chains': len(self.correlation_results['evidence_chains']),
                'report': report
            }
            
        except Exception as e:
            logger.error(f"Error during correlation: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def analyze(self, query: str) -> str:
        """Analyze using integrated CDR-IPDR intelligence"""
        
        # Prepare context
        cdr_suspects = ", ".join(self.cdr_data.keys()) if self.cdr_data else "None"
        ipdr_suspects = ", ".join(self.ipdr_data.keys()) if self.ipdr_data else "None"
        correlation_status = "Available" if self.correlation_results else "Not performed"
        
        try:
            result = self.agent_executor.invoke({
                "input": query,
                "cdr_suspects": cdr_suspects,
                "ipdr_suspects": ipdr_suspects,
                "correlation_status": correlation_status
            })
            
            # Ensure output is properly encoded
            output = result.get('output', '')
            if isinstance(output, str):
                # Clean Unicode characters
                output = clean_unicode_text(output)
            return output
            
        except Exception as e:
            logger.error(f"Error during integrated analysis: {str(e)}")
            # Handle encoding errors
            error_msg = str(e)
            if 'codec' in error_msg and 'encode' in error_msg:
                return "Error: Unable to encode response. Please check for special characters in the data."
            return f"Error analyzing data: {error_msg}"
    
    def get_integrated_risk_assessment(self) -> str:
        """Get comprehensive risk assessment combining CDR and IPDR"""
        
        if not self.cdr_data and not self.ipdr_data:
            return "No data loaded. Please load CDR and/or IPDR data first."
        
        return self.analyze(
            "Provide a comprehensive risk assessment combining CDR and IPDR analysis. "
            "Include correlation findings and identify the highest priority suspects."
        )
    
    def analyze_suspect(self, suspect: str) -> str:
        """Comprehensive analysis of a specific suspect"""
        
        return self.analyze(
            f"Provide detailed analysis of {suspect} including: "
            f"1) CDR patterns and anomalies "
            f"2) IPDR digital footprint "
            f"3) Correlation between voice and data "
            f"4) Risk assessment and recommendations"
        )
    
    def find_evidence_chains(self) -> str:
        """Find evidence chains linking CDR and IPDR activities"""
        
        return self.analyze(
            "Identify evidence chains that link voice calls to data activities. "
            "Focus on: calls followed by encryption, data during silence periods, "
            "and coordinated activities between suspects."
        )
    
    def analyze_coordination_patterns(self) -> str:
        """Analyze coordination patterns between suspects"""
        
        return self.analyze(
            "Analyze coordination patterns between suspects using both CDR and IPDR data. "
            "Look for: synchronized calls, coordinated encryption, pattern day activities, "
            "and communication channel shifts."
        )
    
    def generate_integrated_report(self, output_file: Optional[Path] = None) -> str:
        """Generate comprehensive integrated intelligence report"""
        
        report_sections = []
        
        # Header
        report_sections.append("# INTEGRATED CDR-IPDR INTELLIGENCE REPORT")
        report_sections.append("=" * 80)
        
        # Executive Summary
        report_sections.append("\n## EXECUTIVE SUMMARY")
        summary = self.analyze(
            "Provide an executive summary of key findings from integrated CDR-IPDR analysis. "
            "Highlight the most critical patterns and highest risk suspects."
        )
        report_sections.append(summary)
        
        # CDR Analysis
        report_sections.append("\n## CDR ANALYSIS")
        if self.cdr_data:
            cdr_analysis = self.cdr_agent.analyze(
                "Summarize key CDR findings including device switching, odd hours, and network patterns"
            )
            report_sections.append(cdr_analysis)
        else:
            report_sections.append("No CDR data loaded")
        
        # IPDR Analysis
        report_sections.append("\n## IPDR ANALYSIS")
        if self.ipdr_data:
            ipdr_analysis = self.ipdr_agent.analyze(
                "Summarize key IPDR findings including encryption usage, data patterns, and app risks"
            )
            report_sections.append(ipdr_analysis)
        else:
            report_sections.append("No IPDR data loaded")
        
        # Correlation Analysis
        report_sections.append("\n## CDR-IPDR CORRELATION")
        if self.correlation_results:
            correlation_report = self.correlator.generate_correlation_report(
                self.correlation_results
            )
            report_sections.append(correlation_report)
        else:
            report_sections.append("Correlation not performed")
        
        # Integrated Risk Assessment
        report_sections.append("\n## INTEGRATED RISK ASSESSMENT")
        risk_assessment = self.get_integrated_risk_assessment()
        report_sections.append(risk_assessment)
        
        # Evidence Chains
        report_sections.append("\n## EVIDENCE CHAINS")
        evidence_chains = self.find_evidence_chains()
        report_sections.append(evidence_chains)
        
        # Coordination Patterns
        report_sections.append("\n## COORDINATION PATTERNS")
        coordination = self.analyze_coordination_patterns()
        report_sections.append(coordination)
        
        # Investigation Recommendations
        report_sections.append("\n## INVESTIGATION RECOMMENDATIONS")
        recommendations = self.analyze(
            "Based on all findings, provide prioritized investigation recommendations. "
            "Include specific actions for high-risk suspects and evidence collection guidance."
        )
        report_sections.append(recommendations)
        
        report_content = "\n".join(report_sections)
        
        # Save to file if specified
        if output_file:
            output_file.write_text(report_content)
            logger.info(f"Integrated report saved to {output_file}")
        
        return report_content
    
    def get_suspect_network(self, suspect: str) -> str:
        """Get comprehensive network analysis for a suspect"""
        
        return self.analyze(
            f"Analyze {suspect}'s complete network including: "
            f"1) Voice call contacts from CDR "
            f"2) Digital connections from IPDR "
            f"3) Encrypted communication partners "
            f"4) Coordination with other suspects"
        )
    
    def analyze_timeline(self, date: str) -> str:
        """Analyze all activities on a specific date"""
        
        return self.analyze(
            f"Provide a timeline analysis for {date} showing: "
            f"1) All voice calls from CDR "
            f"2) All data sessions from IPDR "
            f"3) Correlations between voice and data "
            f"4) Suspicious patterns or coordination"
        )