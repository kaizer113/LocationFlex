#!/usr/bin/env python3
"""
Configuration management for LocationFlex Redis project
"""

import os
import json
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
from pathlib import Path
from datetime import datetime


def generate_version_from_date(date_obj: Optional[datetime] = None) -> str:
    """
    Generate a version string from a date (format: vDD where DD is day of month)

    Args:
        date_obj: Date to use, or None for current date

    Returns:
        Version string like 'v22' for September 22nd
    """
    if date_obj is None:
        date_obj = datetime.now()
    return f"v{date_obj.day}"


@dataclass
class RedisConfig:
    """Redis connection configuration"""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    retry_on_timeout: bool = True
    decode_responses: bool = True
    max_connections: int = 50


@dataclass
class WriterConfig:
    """Writer thread configuration"""
    num_writers: int = 1
    batch_size: int = 1000
    stream_name: str = "ip_work_queue"
    consumer_group: str = "ip_writers"
    consumer_timeout_ms: int = 5000
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    pipeline_timeout: float = 30.0
    key_ttl_seconds: int = 259200  # 3 days = 3 * 24 * 60 * 60


@dataclass
class ReaderConfig:
    """Reader thread configuration"""
    num_readers: int = 1
    batch_size: int = 100
    read_timeout_ms: int = 1000


@dataclass
class NetworkConfig:
    """Network block configuration"""
    default_blocks: List[str] = None
    block_size: int = 24  # /24 networks (256 IPs each)
    exclude_private: bool = False
    exclude_multicast: bool = True
    exclude_reserved: bool = True
    
    def __post_init__(self):
        if self.default_blocks is None:
            # Default to some common public IP ranges for testing
            self.default_blocks = [
                "8.8.8.0/24",      # Google DNS range
                "1.1.1.0/24",      # Cloudflare DNS range
                "208.67.222.0/24", # OpenDNS range
            ]


@dataclass
class LocationFlexConfig:
    """Main application configuration"""
    redis: RedisConfig = None
    writer: WriterConfig = None
    reader: ReaderConfig = None
    network: NetworkConfig = None
    version: str = "v22"  # Default to current date (Sept 22)
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    def __post_init__(self):
        if self.redis is None:
            self.redis = RedisConfig()
        if self.writer is None:
            self.writer = WriterConfig()
        if self.reader is None:
            self.reader = ReaderConfig()
        if self.network is None:
            self.network = NetworkConfig()
        # Set default version if not specified
        if self.version == "v22":  # Default placeholder
            self.version = generate_version_from_date()


class ConfigManager:
    """Configuration manager with file loading and environment variable support"""
    
    DEFAULT_CONFIG_FILE = "config.json"
    ENV_PREFIX = "LOCATIONFLEX_"
    
    @classmethod
    def load_config(cls, config_file: Optional[str] = None) -> LocationFlexConfig:
        """
        Load configuration from file and environment variables
        
        Args:
            config_file: Path to JSON config file (optional)
            
        Returns:
            LocationFlexConfig instance
        """
        config = LocationFlexConfig()
        
        # Load from file if it exists
        if config_file is None:
            config_file = cls.DEFAULT_CONFIG_FILE
            
        config_path = Path(config_file)
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    file_config = json.load(f)
                config = cls._merge_config(config, file_config)
                print(f"✅ Loaded configuration from {config_file}")
            except Exception as e:
                print(f"⚠️  Failed to load config file {config_file}: {e}")
        
        # Override with environment variables
        config = cls._load_env_overrides(config)
        
        return config
    
    @classmethod
    def save_config(cls, config: LocationFlexConfig, config_file: Optional[str] = None) -> bool:
        """
        Save configuration to JSON file
        
        Args:
            config: Configuration to save
            config_file: Path to save to (optional)
            
        Returns:
            True if successful, False otherwise
        """
        if config_file is None:
            config_file = cls.DEFAULT_CONFIG_FILE
            
        try:
            config_dict = asdict(config)
            with open(config_file, 'w') as f:
                json.dump(config_dict, f, indent=2)
            print(f"✅ Saved configuration to {config_file}")
            return True
        except Exception as e:
            print(f"❌ Failed to save config to {config_file}: {e}")
            return False
    
    @classmethod
    def _merge_config(cls, base_config: LocationFlexConfig, file_config: Dict[str, Any]) -> LocationFlexConfig:
        """Merge file configuration into base configuration"""
        config_dict = asdict(base_config)
        
        # Deep merge the dictionaries
        def deep_merge(base: Dict, override: Dict) -> Dict:
            result = base.copy()
            for key, value in override.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = deep_merge(result[key], value)
                else:
                    result[key] = value
            return result
        
        merged = deep_merge(config_dict, file_config)
        
        # Reconstruct the config object
        return LocationFlexConfig(
            redis=RedisConfig(**merged.get('redis', {})),
            writer=WriterConfig(**merged.get('writer', {})),
            reader=ReaderConfig(**merged.get('reader', {})),
            network=NetworkConfig(**merged.get('network', {})),
            log_level=merged.get('log_level', 'INFO'),
            log_format=merged.get('log_format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
    
    @classmethod
    def _load_env_overrides(cls, config: LocationFlexConfig) -> LocationFlexConfig:
        """Load configuration overrides from environment variables"""
        env_mappings = {
            f"{cls.ENV_PREFIX}REDIS_HOST": ("redis", "host"),
            f"{cls.ENV_PREFIX}REDIS_PORT": ("redis", "port", int),
            f"{cls.ENV_PREFIX}REDIS_DB": ("redis", "db", int),
            f"{cls.ENV_PREFIX}REDIS_PASSWORD": ("redis", "password"),
            f"{cls.ENV_PREFIX}WRITER_COUNT": ("writer", "num_writers", int),
            f"{cls.ENV_PREFIX}WRITER_BATCH_SIZE": ("writer", "batch_size", int),
            f"{cls.ENV_PREFIX}WRITER_TTL": ("writer", "key_ttl_seconds", int),
            f"{cls.ENV_PREFIX}READER_COUNT": ("reader", "num_readers", int),
            f"{cls.ENV_PREFIX}VERSION": ("version",),
            f"{cls.ENV_PREFIX}LOG_LEVEL": ("log_level",),
        }
        
        for env_var, mapping in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                try:
                    # Apply type conversion if specified
                    if len(mapping) > 2:
                        converter = mapping[2]
                        value = converter(value)
                    
                    # Set the value in the config
                    if len(mapping) == 2:
                        section, attr = mapping
                        setattr(getattr(config, section), attr, value)
                    else:
                        attr = mapping[0]
                        setattr(config, attr, value)
                        
                except (ValueError, AttributeError) as e:
                    print(f"⚠️  Invalid environment variable {env_var}={value}: {e}")
        
        return config


def create_default_config() -> LocationFlexConfig:
    """Create a default configuration"""
    return LocationFlexConfig()


if __name__ == "__main__":
    # Example usage
    config = ConfigManager.load_config()
    print("Current configuration:")
    print(f"Redis: {config.redis.host}:{config.redis.port}")
    print(f"Version: {config.version}")
    print(f"Writers: {config.writer.num_writers}")
    print(f"Batch size: {config.writer.batch_size}")
    print(f"Network blocks: {config.network.default_blocks}")
