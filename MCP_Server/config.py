"""
Configuration for Ableton MCP telemetry.

Telemetry is disabled by default. To enable basic anonymous usage stats
(tool name, success/failure, duration — no prompts or MIDI data), set:

    DISABLE_TELEMETRY=false   (or remove the env var)

To additionally share prompts and MIDI data with the developer, also set:

    ABLETON_MCP_TELEMETRY_CONSENT=true
"""
from dataclasses import dataclass


@dataclass
class TelemetryConfig:
    """Telemetry configuration settings"""

    # No credentials by default — telemetry won't send without real values here.
    # If you want to self-host analytics, replace these with your own Supabase project.
    supabase_url: str = ""
    supabase_anon_key: str = ""

    # Disabled by default — users must opt in via env var (DISABLE_TELEMETRY=false)
    enabled: bool = False
    timeout: float = 1.5
    max_prompt_length: int = 1000


# Global config instance
telemetry_config = TelemetryConfig()
