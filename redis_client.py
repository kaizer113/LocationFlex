#!/usr/bin/env python3
"""
Redis Client Connection with Centralized Configuration
Uses config.py and config.json for Redis connection settings
"""

import redis
import json
import sys
from typing import Optional, Any, Dict, List
from config import ConfigManager, RedisConfig


class RedisClient:
    """Redis client wrapper with centralized configuration support"""

    def __init__(self, config: Optional[RedisConfig] = None):
        """
        Initialize Redis client using centralized configuration

        Args:
            config: Optional RedisConfig object. If None, loads from config.json
        """
        if config is None:
            # Load from centralized configuration
            app_config = ConfigManager.load_config()
            config = app_config.redis

        self.config = config
        self.host = config.host
        self.port = config.port
        self.db = config.db
        self.password = config.password

        try:
            # Create Redis connection (remove deprecated retry_on_timeout)
            redis_kwargs = {
                'host': config.host,
                'port': config.port,
                'db': config.db,
                'password': config.password,
                'decode_responses': config.decode_responses,
                'socket_connect_timeout': config.socket_connect_timeout,
                'socket_timeout': config.socket_timeout,
                'max_connections': config.max_connections
            }

            # Only add retry_on_timeout for older redis-py versions
            try:
                if hasattr(redis, '__version__') and redis.__version__ < '6.0.0':
                    redis_kwargs['retry_on_timeout'] = config.retry_on_timeout
            except:
                pass

            self.client = redis.Redis(**redis_kwargs)
            # Test connection
            self.client.ping()
            print(f"✅ Connected to Redis at {config.host}:{config.port}")
        except redis.ConnectionError as e:
            print(f"❌ Failed to connect to Redis: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            sys.exit(1)
    
    def ping(self) -> bool:
        """Test Redis connection"""
        try:
            return self.client.ping()
        except Exception as e:
            print(f"Ping failed: {e}")
            return False
    
    def set_value(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """
        Set a key-value pair

        Args:
            key: Redis key
            value: Value to store (will be JSON serialized if not string)
            ex: Expiration time in seconds
        """
        try:
            if not isinstance(value, str):
                value = json.dumps(value)
            return self.client.set(key, value, ex=ex)
        except Exception as e:
            print(f"Error setting {key}: {e}")
            return False

    def set_value_with_ttl(self, key: str, value: Any, ttl_seconds: int) -> bool:
        """
        Set a key-value pair with TTL

        Args:
            key: Redis key
            value: Value to store (will be JSON serialized if not string)
            ttl_seconds: Time to live in seconds
        """
        try:
            if not isinstance(value, str):
                value = json.dumps(value)
            return self.client.setex(key, ttl_seconds, value)
        except Exception as e:
            print(f"Error setting {key} with TTL: {e}")
            return False
    
    def get_value(self, key: str) -> Optional[Any]:
        """
        Get value by key
        
        Args:
            key: Redis key
            
        Returns:
            Value or None if key doesn't exist
        """
        try:
            value = self.client.get(key)
            if value is None:
                return None
            
            # Try to parse as JSON, fallback to string
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        except Exception as e:
            print(f"Error getting {key}: {e}")
            return None
    
    def delete_key(self, key: str) -> bool:
        """Delete a key"""
        try:
            return bool(self.client.delete(key))
        except Exception as e:
            print(f"Error deleting {key}: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """Check if key exists"""
        try:
            return bool(self.client.exists(key))
        except Exception as e:
            print(f"Error checking existence of {key}: {e}")
            return False
    
    def get_keys(self, pattern: str = "*") -> List[str]:
        """Get all keys matching pattern"""
        try:
            return self.client.keys(pattern)
        except Exception as e:
            print(f"Error getting keys with pattern {pattern}: {e}")
            return []
    
    def flush_db(self) -> bool:
        """Clear current database"""
        try:
            return self.client.flushdb()
        except Exception as e:
            print(f"Error flushing database: {e}")
            return False
    
    def get_ttl(self, key: str) -> int:
        """
        Get TTL (time to live) for a key

        Args:
            key: Redis key

        Returns:
            TTL in seconds, -1 if no expiry, -2 if key doesn't exist
        """
        try:
            return self.client.ttl(key)
        except Exception as e:
            print(f"Error getting TTL for {key}: {e}")
            return -2

    def info(self) -> Dict[str, Any]:
        """Get Redis server info"""
        try:
            return self.client.info()
        except Exception as e:
            print(f"Error getting server info: {e}")
            return {}


    @classmethod
    def create_with_host(cls, host: str = 'localhost', port: int = 6379, db: int = 0,
                        password: Optional[str] = None, decode_responses: bool = True):
        """
        Create RedisClient with explicit connection parameters (backward compatibility)

        Args:
            host: Redis server host
            port: Redis server port
            db: Redis database number
            password: Redis password if required
            decode_responses: Whether to decode responses to strings
        """
        config = RedisConfig(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=decode_responses
        )
        return cls(config)


def main():
    """Example usage of RedisClient with centralized configuration"""
    print("🚀 Redis Client Example (Using Centralized Config)")
    print("-" * 50)

    # Initialize client using config.json
    print("📋 Loading configuration from config.json...")
    redis_client = RedisClient()

    # Show configuration being used
    print(f"📡 Using Redis server: {redis_client.host}:{redis_client.port}")
    print(f"📊 Database: {redis_client.db}")
    print(f"🔐 Password: {'***' if redis_client.password else 'None'}")

    # Test basic operations
    print("\n📝 Testing basic operations:")

    # Set some values
    redis_client.set_value("test:string", "Hello Redis!")
    redis_client.set_value("test:number", 42)
    redis_client.set_value("test:json", {"name": "John", "age": 30})
    redis_client.set_value("test:expiring", "This will expire", ex=10)

    # Get values
    print(f"String value: {redis_client.get_value('test:string')}")
    print(f"Number value: {redis_client.get_value('test:number')}")
    print(f"JSON value: {redis_client.get_value('test:json')}")
    print(f"Expiring value: {redis_client.get_value('test:expiring')}")

    # Check existence
    print(f"Key 'test:string' exists: {redis_client.exists('test:string')}")
    print(f"Key 'nonexistent' exists: {redis_client.exists('nonexistent')}")

    # List keys
    keys = redis_client.get_keys("test:*")
    print(f"Keys matching 'test:*': {keys}")

    # Server info
    info = redis_client.info()
    if info:
        print(f"\n📊 Redis Server Info:")
        print(f"Version: {info.get('redis_version', 'Unknown')}")
        print(f"Used memory: {info.get('used_memory_human', 'Unknown')}")
        print(f"Connected clients: {info.get('connected_clients', 'Unknown')}")

    print("\n✅ Example completed successfully!")
    print("\n💡 Configuration loaded from config.json")
    print("   To use different Redis server, update config.json or set environment variables:")
    print("   LOCATIONFLEX_REDIS_HOST=your-server")
    print("   LOCATIONFLEX_REDIS_PORT=6379")
    print("   LOCATIONFLEX_REDIS_PASSWORD=your-password")


if __name__ == "__main__":
    main()
