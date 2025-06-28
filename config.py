"""
Configuration module for CDR Intelligence Agent
Handles all configuration settings including OpenRouter API setup
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr
from typing import Optional, Dict, List, Any
import os
from pathlib import Path

class Settings(BaseSettings):
    """Application settings using Pydantic 2 BaseSettings"""
    
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='ignore'
    )
    
    # OpenRouter API Configuration
    openrouter_api_key: SecretStr = Field(
        default=None,
        description="OpenRouter API key for accessing LLMs",
        alias='OPENROUTER_API_KEY'
    )
    openrouter_base_url: str = Field(
        default="https://openrouter.ai/api/v1",
        description="OpenRouter API base URL"
    )
    openrouter_model: str = Field(
        default="anthropic/claude-sonnet-4",
        description="Default model to use with OpenRouter"
    )
    
    # Alternative models for fallback
    fallback_models: List[str] = Field(
        default=[
            "anthropic/claude-3.7-sonnet",
            "openai/gpt-4.1",
            "google/gemini-2.5-pro",
            "deepseek/deepseek-chat-v3-0324"
        ],
        description="Fallback models if primary fails"
    )
    
    # File paths
    cdr_data_path: Path = Field(
        default=Path("../CDRs"),
        description="Path to CDR Excel files"
    )
    output_path: Path = Field(
        default=Path("reports"),
        description="Path for generated reports"
    )
    ipdr_data_path: Path = Field(
        default=Path("../../IPDRs/IPDR"),
        description="Path to IPDR data files"
    )
    
    # Analysis parameters
    odd_hour_start: int = Field(default=0, description="Start of odd hours (midnight)")
    odd_hour_end: int = Field(default=5, description="End of odd hours (5 AM)")
    
    # Risk scoring weights
    risk_weights: Dict[str, float] = Field(
        default={
            "device_switching": 0.40,  # Multiple IMEIs
            "odd_hours": 0.25,         # Late night activity
            "voice_only": 0.20,        # 100% voice communication
            "high_frequency": 0.15     # Call burst patterns
        },
        description="Weights for risk scoring algorithm"
    )
    
    # Thresholds
    high_risk_imei_count: int = Field(default=3, description="IMEIs for high risk")
    odd_hour_threshold: float = Field(default=0.03, description="3% odd hour calls")
    call_burst_threshold: int = Field(default=5, description="Calls in 15 minutes")
    call_burst_window: int = Field(default=15, description="Time window in minutes")
    
    # Provider patterns (service codes)
    provider_patterns: List[str] = Field(
        default=[
            "AA-AIRTEL", "JZ-REGINF", "VM-", "AD-", "AX-", "BW-", "TM-",
            "VK-", "VF-", "IM-", "ID-", "BP-", "BZ-", "AW-"
        ],
        description="Service provider message patterns"
    )
    
    # Pattern day detection (for narcotics)
    pattern_days: List[str] = Field(
        default=["Tuesday", "Friday"],
        description="Days to check for pattern activity"
    )
    
    # Report formatting
    report_emojis: Dict[str, str] = Field(
        default={
            "alert": "🚨",
            "chart": "📊",
            "network": "🌐",
            "target": "🎯",
            "warning": "⚠️",
            "high_risk": "🔴",
            "medium_risk": "🟡",
            "low_risk": "🟢",
            "check": "✓"
        },
        description="Emojis for report formatting"
    )
    
    # LangChain settings
    langchain_verbose: bool = Field(default=True, description="Verbose LangChain output")
    max_iterations: int = Field(default=10, description="Max agent iterations")
    
    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    log_file: Optional[Path] = Field(default=Path("cdr_analysis.log"))
    
    # CDR column mappings
    cdr_columns: Dict[str, str] = Field(
        default={
            "a_party": "A PARTY",
            "b_party": "B PARTY",
            "date": "DATE",
            "time": "TIME",
            "duration": "DURATION",
            "call_type": "CALL TYPE",
            "first_cell": "FIRST CELL ID A",
            "last_cell": "LAST CELL ID A",
            "imei": "IMEI A",
            "imsi": "IMSI A",
            "address": "FIRST CELL ID A ADDRESS",
            "latitude": "LATITUDE",
            "longitude": "LONGITUDE"
        },
        description="CDR column name mappings"
    )
    
    # Analysis features
    enable_network_analysis: bool = Field(default=True)
    enable_geographic_analysis: bool = Field(default=True)
    enable_temporal_patterns: bool = Field(default=True)
    enable_device_tracking: bool = Field(default=True)
    
    # IPDR Analysis Configuration
    ipdr_columns: Dict[str, str] = Field(
        default={
            "subscriber_id": "SUBSCRIBER_ID",
            "imei": "IMEI",
            "imsi": "IMSI",
            "start_time": "START_TIME",
            "end_time": "END_TIME",
            "source_ip": "SOURCE_IP",
            "destination_ip": "DESTINATION_IP",
            "source_port": "SOURCE_PORT",
            "destination_port": "DESTINATION_PORT",
            "protocol": "PROTOCOL",
            "data_volume_up": "DATA_VOLUME_UP",
            "data_volume_down": "DATA_VOLUME_DOWN",
            "app_protocol": "APP_PROTOCOL",
            "session_duration": "SESSION_DURATION"
        },
        description="IPDR column name mappings"
    )
    
    # IPDR Risk Weights
    ipdr_risk_weights: Dict[str, float] = Field(
        default={
            "encryption": 0.30,           # Encrypted app usage
            "data_patterns": 0.25,        # Large uploads/downloads
            "session_anomalies": 0.25,    # Session behavior
            "app_behavior": 0.20          # App fingerprinting
        },
        description="IPDR risk scoring weights"
    )
    
    # IPDR Thresholds
    large_upload_threshold: int = Field(default=10485760, description="10MB upload threshold")
    encryption_session_threshold: int = Field(default=20, description="Suspicious encryption count")
    ipdr_odd_hour_threshold: float = Field(default=0.05, description="5% odd hour data usage")
    
    # IPDR Risk Thresholds
    ipdr_risk_thresholds: Dict[str, int] = Field(
        default={
            "high": 70,
            "medium": 40,
            "low": 0
        },
        description="IPDR risk level thresholds"
    )
    
    # App Port Signatures
    app_signatures: Dict[str, Dict[str, Any]] = Field(
        default={
            "whatsapp": {"ports": [443, 5222], "risk": "HIGH", "type": "encrypted_messaging"},
            "telegram": {"ports": [443], "risk": "HIGH", "type": "encrypted_messaging"},
            "signal": {"ports": [443], "risk": "HIGH", "type": "encrypted_messaging"},
            "threema": {"ports": [5222], "risk": "HIGH", "type": "encrypted_messaging"},
            "banking": {"ports": [443, 8443], "risk": "MEDIUM", "type": "financial"},
            "video_call": {"ports": [3478, 19302], "risk": "MEDIUM", "type": "communication"},
            "vpn": {"ports": [1194, 1723, 500], "risk": "HIGH", "type": "anonymization"},
            "tor": {"ports": [9001, 9050], "risk": "CRITICAL", "type": "anonymization"}
        },
        description="Application port signatures for fingerprinting"
    )
    
    # Additional properties for compatibility
    @property
    def openrouter_api_base(self) -> str:
        """Get OpenRouter API base URL"""
        return self.openrouter_base_url
    
    @property
    def default_model(self) -> str:
        """Get default model"""
        return self.openrouter_model
    
    @property
    def temperature(self) -> float:
        """Get temperature setting"""
        return 0.3
    
    @property
    def max_tokens(self) -> int:
        """Get max tokens setting"""
        return 4000
    
    def get_openrouter_headers(self) -> Dict[str, str]:
        """Get headers for OpenRouter API calls"""
        return {
            "HTTP-Referer": "https://localhost:3000/",
            "X-Title": "CDR Intelligence Agent"
        }
    
    @property
    def openrouter_api_key_str(self) -> str:
        """Get OpenRouter API key as string"""
        if self.openrouter_api_key:
            return self.openrouter_api_key.get_secret_value()
        raise ValueError("OpenRouter API key not configured")

# Create global settings instance
settings = Settings()

# Ensure output directory exists
settings.output_path.mkdir(exist_ok=True)
