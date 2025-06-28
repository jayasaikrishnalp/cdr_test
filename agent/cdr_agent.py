"""
CDR Intelligence Agent
Main LangChain agent for analyzing CDR data and detecting criminal patterns
"""

from typing import Dict, List, Optional, Any
from langchain.agents import AgentExecutor, create_react_agent
from langchain.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory
from langchain_core.messages import BaseMessage
from langchain.tools import Tool
from langchain_core.language_models import BaseChatModel
import pandas as pd
from loguru import logger

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from agent.openrouter_llm import create_openrouter_llm
from tools.device_analysis import DeviceAnalysisTool
from tools.temporal_analysis import TemporalAnalysisTool
from tools.communication_analysis import CommunicationAnalysisTool
from tools.network_analysis import NetworkAnalysisTool
from tools.risk_scorer import RiskScoringTool
from tools.location_analysis import LocationAnalysisTool
from processors.cdr_loader import CDRLoader
from processors.data_validator import CDRValidator
from config import settings

class CDRIntelligenceAgent:
    """Main agent for CDR criminal intelligence analysis"""
    
    def __init__(self, llm: Optional[BaseChatModel] = None):
        """Initialize the CDR Intelligence Agent"""
        self.llm = llm or create_openrouter_llm()
        self.cdr_data: Dict[str, pd.DataFrame] = {}
        self.tools = []
        self.agent_executor = None
        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=False,
            output_key="output"
        )
        
        # Initialize components
        self.cdr_loader = CDRLoader()
        self.validator = CDRValidator()
        
        # Initialize tools
        self._initialize_tools()
        
        # Create agent
        self._create_agent()
        
    def _initialize_tools(self):
        """Initialize all analysis tools"""
        # Create tool instances
        device_tool = DeviceAnalysisTool()
        temporal_tool = TemporalAnalysisTool()
        comm_tool = CommunicationAnalysisTool()
        network_tool = NetworkAnalysisTool()
        risk_tool = RiskScoringTool()
        location_tool = LocationAnalysisTool()
        
        # Store tools
        self.tools = [
            device_tool,
            temporal_tool,
            comm_tool,
            network_tool,
            risk_tool,
            location_tool
        ]
        
        logger.info(f"Initialized {len(self.tools)} analysis tools")
    
    def _create_agent(self):
        """Create the ReAct agent"""
        # Create the prompt template
        prompt = PromptTemplate(
            input_variables=["input", "chat_history", "agent_scratchpad", "tools", "tool_names"],
            template="""You are an expert Criminal Intelligence Analyst specializing in Call Detail Record (CDR) analysis.
Your mission is to analyze CDR data to detect criminal patterns, identify suspects, and provide actionable intelligence for law enforcement.

You have access to the following tools:
{tools}

When analyzing CDR data, focus on:
1. Device switching patterns (multiple IMEIs indicate burner phones)
2. Odd hour activity (midnight-5AM calls suggest covert operations)
3. Communication patterns (100% voice calls indicate avoiding text traces)
4. Network connections between suspects
5. Risk assessment and prioritization
6. Location patterns (impossible travel, border areas, clustering)
7. Advanced patterns (circular loops, one-ring signals, synchronized calling)

Provider messages (like AA-AIRTEL, JZ-REGINF) should be filtered out as they are service notifications.

Current conversation:
{chat_history}

To answer questions, use this format:

Question: the input question you must answer
Thought: think about what analysis is needed
Action: the action to take, must be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (repeat Thought/Action/Action Input/Observation as needed)
Thought: I now have enough information to provide a comprehensive answer
Final Answer: the final answer with risk assessment and recommendations

Remember to:
- Be thorough in your analysis
- Identify HIGH RISK suspects (3+ IMEIs, high odd-hour activity, voice-only behavior)
- Provide specific evidence for each finding
- Suggest follow-up investigations when needed
- Format output with clear sections and risk indicators (🔴 HIGH, 🟡 MEDIUM, 🟢 LOW)

Question: {input}
{agent_scratchpad}"""
        )
        
        # Create the agent
        agent = create_react_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=prompt
        )
        
        # Create agent executor
        self.agent_executor = AgentExecutor(
            agent=agent,
            tools=self.tools,
            memory=self.memory,
            verbose=settings.langchain_verbose,
            max_iterations=settings.max_iterations,
            handle_parsing_errors=True,
            return_intermediate_steps=False
        )
        
        logger.info("Created ReAct agent with memory")
    
    def load_cdr_data(self, data_path: Optional[str] = None, file_list: Optional[List[str]] = None) -> Dict[str, Any]:
        """Load CDR data from Excel files
        
        Args:
            data_path: Path to CDR files directory
            file_list: Optional list of specific files to load
        """
        try:
            # Load CDR files
            self.cdr_data = self.cdr_loader.load_cdrs(file_list=file_list)
            
            if not self.cdr_data:
                return {
                    'status': 'error',
                    'message': 'No CDR files found',
                    'files_loaded': 0
                }
            
            # Validate data
            validation_results = {}
            for suspect, df in self.cdr_data.items():
                validation = self.validator.validate_dataframe(df, suspect)
                validation_results[suspect] = validation
            
            # Set data for all tools
            for tool in self.tools:
                if hasattr(tool, 'cdr_data'):
                    tool.cdr_data = self.cdr_data
            
            # Generate summary
            summary = self.cdr_loader.get_suspect_summary(self.cdr_data)
            
            return {
                'status': 'success',
                'files_loaded': len(self.cdr_data),
                'suspects': list(self.cdr_data.keys()),
                'validation_results': validation_results,
                'summary': summary.to_dict('records') if not summary.empty else []
            }
            
        except Exception as e:
            logger.error(f"Error loading CDR data: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'files_loaded': 0
            }
    
    def analyze(self, query: str) -> str:
        """
        Analyze CDR data based on the query
        
        Args:
            query: Natural language query about CDR analysis
            
        Returns:
            Analysis results as formatted string
        """
        if not self.cdr_data:
            return "No CDR data loaded. Please load data first using load_cdr_data()."
        
        try:
            # Run the agent with new invoke method
            result = self.agent_executor.invoke({"input": query})
            
            # Extract the output
            if isinstance(result, dict):
                return result.get("output", str(result))
            else:
                return str(result)
            
        except Exception as e:
            logger.error(f"Error during analysis: {str(e)}")
            return f"Error analyzing CDR data: {str(e)}"
    
    def generate_comprehensive_report(self) -> str:
        """Generate a comprehensive criminal intelligence report"""
        query = """Generate a comprehensive criminal intelligence report analyzing all suspects.
        Include:
        1. Critical anomalies detected (device switching, odd hours, suspicious patterns)
        2. Risk ranking of all suspects with color-coded levels
        3. Network observations and connections
        4. Investigation priorities and recommendations
        5. Executive summary with actionable intelligence
        
        Format the report with clear sections, emojis for visual indicators, and prioritized recommendations."""
        
        return self.analyze(query)
    
    def reset_memory(self):
        """Clear the agent's conversation memory"""
        self.memory.clear()
        logger.info("Agent memory cleared")
    
    def get_suspect_list(self) -> List[str]:
        """Get list of loaded suspects"""
        return list(self.cdr_data.keys())
    
    def get_data_summary(self) -> pd.DataFrame:
        """Get summary of loaded CDR data"""
        if self.cdr_data:
            return self.cdr_loader.get_suspect_summary(self.cdr_data)
        return pd.DataFrame()