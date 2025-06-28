"""
IPDR Intelligence Agent
Main agent class for IPDR analysis using LangChain and natural language processing
"""

import os
from typing import Dict, List, Optional, Any
from pathlib import Path
import pandas as pd
from loguru import logger

from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor
from langchain.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory
from langchain.schema import Document

import sys
sys.path.append(str(Path(__file__).parent.parent))

from config import settings
from ipdr_processors.ipdr_loader import IPDRLoader
from ipdr_processors.ipdr_validator import IPDRValidator
from ipdr_agent.ipdr_tools import (
    EncryptionAnalysisTool,
    DataPatternAnalysisTool,
    SessionAnalysisTool,
    AppFingerprintingTool,
    IPDRRiskScorerTool
)

class IPDRAgent:
    """
    IPDR Intelligence Agent for analyzing Internet Protocol Detail Records
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize IPDR Agent with tools and LLM"""
        
        # Initialize OpenRouter API
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("OpenRouter API key not provided")
        
        # Initialize components
        self.ipdr_loader = IPDRLoader()
        self.ipdr_validator = IPDRValidator()
        self.ipdr_data: Dict[str, pd.DataFrame] = {}
        
        # Initialize LLM
        self.llm = ChatOpenAI(
            model_name=settings.default_model,
            openai_api_key=self.api_key,
            openai_api_base=settings.openrouter_api_base,
            temperature=settings.temperature,
            max_tokens=settings.max_tokens
        )
        
        # Initialize tools
        self.tools = self._initialize_tools()
        
        # Initialize agent
        self.agent_executor = self._create_agent()
        
        logger.info("IPDR Intelligence Agent initialized successfully")
    
    def _initialize_tools(self) -> List:
        """Initialize IPDR analysis tools"""
        
        tools = [
            EncryptionAnalysisTool(),
            DataPatternAnalysisTool(),
            SessionAnalysisTool(),
            AppFingerprintingTool(),
            IPDRRiskScorerTool()
        ]
        
        return tools
    
    def _create_agent(self) -> AgentExecutor:
        """Create the IPDR analysis agent"""
        
        # Create prompt template
        prompt = PromptTemplate.from_template("""
You are an IPDR (Internet Protocol Detail Record) Intelligence Analyst specializing in digital forensics and criminal pattern detection. You analyze internet usage data to identify suspicious patterns, encrypted communications, and criminal behaviors.

Your expertise includes:
- Encrypted application usage analysis (WhatsApp, Telegram, Signal)
- Data transfer pattern detection (large uploads, pattern day activity)
- Session behavior analysis (timing, duration, concurrency)
- Application fingerprinting and behavioral profiling
- Comprehensive risk assessment

Current IPDR data loaded for suspects: {suspects}

Tools available:
{tools}

Use these tools to analyze IPDR data and provide actionable intelligence. When analyzing:
1. Look for encrypted communication patterns that might indicate criminal coordination
2. Identify large data transfers that could be evidence sharing
3. Detect unusual session patterns (marathon sessions, rapid switching)
4. Profile application usage for operational security awareness
5. Calculate comprehensive risk scores to prioritize investigations

To answer questions, use this format:

Question: the input question you must answer
Thought: think about what analysis is needed
Action: the action to take, must be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (repeat Thought/Action/Action Input/Observation as needed)
Thought: I now have enough information to provide a comprehensive answer
Final Answer: the final answer with risk assessment and recommendations

Question: {input}

{agent_scratchpad}
""")
        
        # Create agent
        agent = create_react_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=prompt
        )
        
        # Create agent executor
        agent_executor = AgentExecutor(
            agent=agent,
            tools=self.tools,
            verbose=True,
            max_iterations=10,
            handle_parsing_errors=True,
            return_intermediate_steps=True
        )
        
        return agent_executor
    
    def load_ipdr_data(self, file_list: Optional[List[str]] = None) -> Dict[str, Any]:
        """Load IPDR data files"""
        
        try:
            # Load IPDR data
            self.ipdr_data = self.ipdr_loader.load_ipdrs(file_list)
            
            if not self.ipdr_data:
                return {
                    'status': 'error',
                    'message': 'No IPDR data files found',
                    'loaded_count': 0
                }
            
            # Validate loaded data
            validation_results = {}
            for suspect, df in self.ipdr_data.items():
                validation_results[suspect] = self.ipdr_validator.validate_dataframe(df, suspect)
            
            # Share data with tools
            for tool in self.tools:
                if hasattr(tool, 'ipdr_data'):
                    tool.ipdr_data = self.ipdr_data
            
            # Generate summary
            summary = self.ipdr_loader.get_suspect_summary(self.ipdr_data)
            
            logger.info(f"Loaded IPDR data for {len(self.ipdr_data)} suspects")
            
            return {
                'status': 'success',
                'loaded_count': len(self.ipdr_data),
                'suspects': list(self.ipdr_data.keys()),
                'summary': summary.to_dict('records'),
                'validation': validation_results
            }
            
        except Exception as e:
            logger.error(f"Error loading IPDR data: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'loaded_count': 0
            }
    
    def analyze(self, query: str) -> str:
        """Analyze IPDR data using natural language query"""
        
        if not self.ipdr_data:
            return "No IPDR data loaded. Please load IPDR data first using 'load_ipdr_data()'"
        
        try:
            # Add context about loaded suspects
            suspects_list = ", ".join(self.ipdr_data.keys())
            
            # Run analysis
            result = self.agent_executor.invoke({
                "input": query,
                "suspects": suspects_list
            })
            
            return result['output']
            
        except Exception as e:
            logger.error(f"Error during IPDR analysis: {str(e)}")
            return f"Error analyzing IPDR data: {str(e)}"
    
    def get_risk_summary(self) -> str:
        """Get comprehensive risk summary for all suspects"""
        
        if not self.ipdr_data:
            return "No IPDR data loaded."
        
        return self.analyze("Generate a comprehensive risk assessment for all suspects")
    
    def analyze_encryption_patterns(self, suspect: Optional[str] = None) -> str:
        """Analyze encryption patterns"""
        
        if suspect:
            return self.analyze(f"Analyze encryption patterns for {suspect}")
        return self.analyze("Analyze encryption patterns for all suspects")
    
    def analyze_data_patterns(self, suspect: Optional[str] = None) -> str:
        """Analyze data transfer patterns"""
        
        if suspect:
            return self.analyze(f"Analyze data transfer patterns for {suspect}")
        return self.analyze("Find large uploads and suspicious data patterns")
    
    def analyze_sessions(self, suspect: Optional[str] = None) -> str:
        """Analyze session patterns"""
        
        if suspect:
            return self.analyze(f"Analyze session timing and patterns for {suspect}")
        return self.analyze("Find session anomalies and concurrent sessions")
    
    def analyze_apps(self, suspect: Optional[str] = None) -> str:
        """Analyze application usage"""
        
        if suspect:
            return self.analyze(f"Perform app fingerprinting analysis for {suspect}")
        return self.analyze("Identify high-risk apps and unknown services")
    
    def generate_report(self, output_file: Optional[Path] = None) -> str:
        """Generate comprehensive IPDR analysis report"""
        
        if not self.ipdr_data:
            return "No IPDR data loaded."
        
        report_sections = []
        
        # Header
        report_sections.append("# IPDR INTELLIGENCE ANALYSIS REPORT")
        report_sections.append("=" * 80)
        
        # Executive Summary
        report_sections.append("\n## EXECUTIVE SUMMARY")
        summary = self.analyze("Provide an executive summary of key findings and priority targets")
        report_sections.append(summary)
        
        # Risk Assessment
        report_sections.append("\n## RISK ASSESSMENT")
        risk_assessment = self.get_risk_summary()
        report_sections.append(risk_assessment)
        
        # Encryption Analysis
        report_sections.append("\n## ENCRYPTION ANALYSIS")
        encryption_analysis = self.analyze_encryption_patterns()
        report_sections.append(encryption_analysis)
        
        # Data Pattern Analysis
        report_sections.append("\n## DATA PATTERN ANALYSIS")
        data_analysis = self.analyze_data_patterns()
        report_sections.append(data_analysis)
        
        # Session Analysis
        report_sections.append("\n## SESSION BEHAVIOR ANALYSIS")
        session_analysis = self.analyze_sessions()
        report_sections.append(session_analysis)
        
        # App Analysis
        report_sections.append("\n## APPLICATION FINGERPRINTING")
        app_analysis = self.analyze_apps()
        report_sections.append(app_analysis)
        
        # Recommendations
        report_sections.append("\n## INVESTIGATION RECOMMENDATIONS")
        recommendations = self.analyze("Provide detailed investigation recommendations based on all findings")
        report_sections.append(recommendations)
        
        report_content = "\n".join(report_sections)
        
        # Save to file if specified
        if output_file:
            output_file.write_text(report_content)
            logger.info(f"IPDR report saved to {output_file}")
        
        return report_content