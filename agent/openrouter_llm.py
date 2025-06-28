"""
OpenRouter LLM Integration for CDR Intelligence Agent
Provides OpenRouter-compatible LLM for use with LangChain
"""

from typing import Optional, Dict, Any, List, Tuple
from langchain_openai import ChatOpenAI
from langchain_core.language_models.chat_models import BaseChatModel
from pydantic import Field, SecretStr
from loguru import logger
import tiktoken

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import settings

class ChatOpenRouter(ChatOpenAI):
    """
    ChatOpenRouter extends ChatOpenAI to work with OpenRouter API.
    OpenRouter is compatible with OpenAI API format.
    """
    
    openai_api_key: Optional[SecretStr] = Field(
        alias="api_key",
        default=None
    )
    
    openai_api_base: str = Field(
        default="https://openrouter.ai/api/v1"
    )
    
    model_name: str = Field(
        alias="model",
        default="anthropic/claude-3-opus-20240229"
    )
    
    default_headers: Optional[Dict[str, str]] = Field(
        default=None
    )
    
    def __init__(self, **kwargs):
        """Initialize ChatOpenRouter with OpenRouter-specific settings"""
        # Set defaults from config if not provided
        if 'api_key' not in kwargs and 'openai_api_key' not in kwargs:
            kwargs['api_key'] = settings.openrouter_api_key_str
        
        if 'model' not in kwargs and 'model_name' not in kwargs:
            kwargs['model'] = settings.openrouter_model
        
        if 'openai_api_base' not in kwargs:
            kwargs['openai_api_base'] = settings.openrouter_base_url
        
        # Set OpenRouter headers
        if 'default_headers' not in kwargs:
            kwargs['default_headers'] = settings.get_openrouter_headers()
        
        # Set other defaults
        if 'temperature' not in kwargs:
            kwargs['temperature'] = 0.7
        
        if 'max_tokens' not in kwargs:
            kwargs['max_tokens'] = 4000
        
        super().__init__(**kwargs)
        
        logger.info(f"Initialized ChatOpenRouter with model: {self.model_name}")
    
    @property
    def _llm_type(self) -> str:
        """Return identifier of LLM type"""
        return "openrouter"
    
    def _get_encoding_model(self) -> Tuple[str, tiktoken.Encoding]:
        """Override to handle OpenRouter models"""
        # For OpenRouter, we'll use cl100k_base encoding as a default
        # This is the encoding used by GPT-3.5 and GPT-4
        return "cl100k_base", tiktoken.get_encoding("cl100k_base")

def create_openrouter_llm(
    model: Optional[str] = None,
    temperature: float = 0.7,
    max_tokens: int = 4000,
    **kwargs
) -> BaseChatModel:
    """
    Factory function to create OpenRouter LLM instance
    
    Args:
        model: Model name (defaults to config)
        temperature: Sampling temperature
        max_tokens: Maximum tokens in response
        **kwargs: Additional arguments for ChatOpenRouter
        
    Returns:
        ChatOpenRouter instance
    """
    return ChatOpenRouter(
        model=model or settings.openrouter_model,
        temperature=temperature,
        max_tokens=max_tokens,
        **kwargs
    )

def create_fallback_llm_chain() -> List[BaseChatModel]:
    """
    Create a chain of LLMs with fallback options
    
    Returns:
        List of LLMs to try in order
    """
    llms = []
    
    # Primary model
    try:
        primary = create_openrouter_llm()
        llms.append(primary)
    except Exception as e:
        logger.error(f"Failed to create primary LLM: {e}")
    
    # Fallback models
    for fallback_model in settings.fallback_models:
        try:
            fallback = create_openrouter_llm(model=fallback_model, temperature=0.5)
            llms.append(fallback)
        except Exception as e:
            logger.error(f"Failed to create fallback LLM {fallback_model}: {e}")
    
    if not llms:
        raise ValueError("No LLMs could be initialized. Check your API key and configuration.")
    
    return llms