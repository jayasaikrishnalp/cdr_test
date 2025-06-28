"""
Base Tool for LangChain CDR Analysis Tools
"""

from langchain.tools import BaseTool
from typing import Optional, Type, Any, Dict
from pydantic import BaseModel, Field
from abc import abstractmethod
import pandas as pd
from loguru import logger

class CDRAnalysisInput(BaseModel):
    """Base input schema for CDR analysis tools"""
    query: str = Field(description="Analysis query or parameters")
    suspect_name: Optional[str] = Field(default=None, description="Specific suspect to analyze")

class BaseCDRTool(BaseTool):
    """Base class for CDR analysis tools"""
    
    cdr_data: Dict[str, pd.DataFrame] = {}
    
    @abstractmethod
    def _run(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Run the tool"""
        pass
    
    async def _arun(self, query: str, suspect_name: Optional[str] = None) -> str:
        """Async run - not implemented for these tools"""
        raise NotImplementedError("Async execution not supported")
    
    def set_cdr_data(self, cdr_data: Dict[str, pd.DataFrame]):
        """Set the CDR data for analysis"""
        self.cdr_data = cdr_data