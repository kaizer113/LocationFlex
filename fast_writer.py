#!/usr/bin/env python3
"""
Fast Writer - Uses pre-generated sample keys for maximum speed
"""

import time
import signal
import json
from typing import Optional, Dict

from config import ConfigManager
from redis_client import RedisClient
from network_utils import create_redis_key_simple


class FastWriter:
    """Fast writer using pre-generated sample keys for maximum speed"""

    def __init__(self, max_daily_keys: int = 200000, skip_probability: float = 0.05):
        self.config = ConfigManager.load_config()
        self.redis_client = RedisClient()
        self.max_daily_keys = max_daily_keys
        self.skip_probability = skip_probability
        self.version = self.config.version

        # Performance tracking
        self.keys_written = 0
        self.keys_skipped = 0
        self.start_time = time.time()
        self.running = True

        # Sequential key generation
        self.current_key_id = 0

        # Load pre-generated sample keys
        self.sample_keys = self._load_sample_keys()
        
        print(f"🚀 Fast Writer initialized:")
        print(f"   Version: {self.version}")
        print(f"   Max daily keys: {max_daily_keys:,}")
        print(f"   Skip probability: {skip_probability*100:.1f}%")
        print(f"   Sample keys loaded: {len(self.sample_keys)}")
        print(f"   Average key size: {self._calculate_avg_key_size():.0f} bytes")
        print(f"   Write mode: Non-transactional pipeline (Redis Cluster compatible)")
    
    def _load_sample_keys(self) -> Dict[str, str]:
        """Load pre-generated sample keys from JSON file"""
        try:
            with open("sampleKeys.json", "r") as f:
                sample_data = json.load(f)
            
            # Convert to JSON strings for Redis storage
            sample_keys = {}
            for key_id, data in sample_data.items():
                sample_keys[key_id] = json.dumps(data)
            
            print(f"✅ Loaded {len(sample_keys)} sample keys from sampleKeys.json")
            return sample_keys
            
        except FileNotFoundError:
            print("❌ sampleKeys.json not found. Run generate_sample_keys.py first.")
            raise
        except Exception as e:
            print(f"❌ Error loading sample keys: {e}")
            raise
    
    def _calculate_avg_key_size(self) -> float:
        """Calculate average size of sample keys"""
        if not self.sample_keys:
            return 0
        
        total_size = sum(len(value) for value in self.sample_keys.values())
        return total_size / len(self.sample_keys)
    
    def get_key_data(self, key_id: int) -> tuple[str, str]:
        """Get key and value data for a given key ID (fast lookup)"""
        # Use modulo to cycle through the 100 sample keys
        sample_index = str(key_id % 100)
        
        # Get pre-generated JSON value
        redis_value = self.sample_keys[sample_index]
        
        # Create Redis key
        redis_key = create_redis_key_simple(key_id, self.version)
        
        return redis_key, redis_value
    
    def write_pipeline_batches(self, count: int, batch_size: int = 50) -> tuple[int, int]:
        """Write keys using non-transactional pipeline (Redis Cluster compatible)"""
        written = 0
        skipped = 0

        # Process in batches
        remaining = count
        while remaining > 0 and self.current_key_id < self.max_daily_keys:
            current_batch_size = min(batch_size, remaining)
            batch_data = {}

            # Prepare batch
            for _ in range(current_batch_size):
                if self.current_key_id >= self.max_daily_keys:
                    break

                key_id = self.current_key_id
                self.current_key_id += 1

                # Check if we should skip this write (cache miss simulation)
                #if should_skip_write(self.skip_probability):
                #    skipped += 1
                #    continue

                redis_key, redis_value = self.get_key_data(key_id)
                batch_data[redis_key] = redis_value

            # Write batch with non-transactional pipeline
            if batch_data:
                try:
                    # Create NON-transactional pipeline (Redis Cluster compatible)
                    pipe = self.redis_client.client.pipeline(transaction=False)
                    for key, value in batch_data.items():
                        pipe.setex(key, self.config.writer.key_ttl_seconds, value)

                    results = pipe.execute()
                    written += len([r for r in results if r])  # Count successful operations

                except Exception as e:
                    print(f"❌ Error writing batch of {len(batch_data)} keys: {e}")
                    # Fallback to individual writes for this batch
                    for key, value in batch_data.items():
                        try:
                            success = self.redis_client.set_value_with_ttl(key, value, self.config.writer.key_ttl_seconds)
                            if success:
                                written += 1
                        except Exception as e2:
                            print(f"❌ Failed individual write for {key}: {e2}")

            remaining -= current_batch_size

        return written, skipped
    
    def run_continuous(self, target_keys: Optional[int] = None, duration_seconds: Optional[int] = None):
        """Run continuous writing until target or duration reached"""
        print(f"\n🚀 Starting fast continuous writing...")
        
        if target_keys:
            print(f"   Target: {target_keys:,} keys")
        if duration_seconds:
            print(f"   Duration: {duration_seconds} seconds")
        
        write_count = 100  # Write 100 keys at a time (no batching)
        last_report = time.time()

        # Setup signal handler for graceful shutdown (only in main thread)
        try:
            signal.signal(signal.SIGINT, lambda _s, _f: setattr(self, 'running', False))
        except ValueError:
            # Signal handlers only work in main thread - ignore in worker threads
            pass

        try:
            while self.running:
                # Check stopping conditions
                if target_keys and self.keys_written >= target_keys:
                    break
                if duration_seconds and (time.time() - self.start_time) >= duration_seconds:
                    break
                if self.current_key_id >= self.max_daily_keys:
                    print(f"✅ Reached end of key space ({self.max_daily_keys:,} keys)")
                    break

                # Write keys using non-transactional pipeline (Redis Cluster compatible)
                written, skipped = self.write_pipeline_batches(write_count, batch_size=50)
                self.keys_written += written
                self.keys_skipped += skipped
                
                # Progress reporting every 5 seconds
                now = time.time()
                if now - last_report >= 5.0:
                    elapsed = now - self.start_time
                    rate = self.keys_written / elapsed if elapsed > 0 else 0
                    
                    print(f"📊 Progress: {self.keys_written:,} written, {self.keys_skipped:,} skipped | "
                          f"Rate: {rate:.0f} keys/sec | "
                          f"Skip rate: {(self.keys_skipped/(self.keys_written + self.keys_skipped))*100:.1f}%")
                    
                    last_report = now
                
                # Minimal delay for maximum speed
                time.sleep(0.001)  # 1ms delay
        
        except KeyboardInterrupt:
            print(f"\n🛑 Writing interrupted by user")
            self.running = False
        
        # Final statistics
        self.print_final_stats()
    
    def print_final_stats(self):
        """Print final writing statistics"""
        elapsed = time.time() - self.start_time
        total_attempts = self.keys_written + self.keys_skipped
        avg_key_size = self._calculate_avg_key_size()
        
        print(f"\n📊 Fast Writing Complete!")
        print("=" * 30)
        print(f"⏱️  Duration: {elapsed:.1f} seconds")
        print(f"✅ Keys written: {self.keys_written:,}")
        print(f"⏭️  Keys skipped: {self.keys_skipped:,}")
        print(f"🎯 Total attempts: {total_attempts:,}")
        print(f"📈 Write rate: {self.keys_written/elapsed:.0f} keys/sec")
        print(f"🎲 Skip rate: {(self.keys_skipped/total_attempts)*100:.1f}%")
        print(f"💾 Data written: ~{(self.keys_written * avg_key_size)/1024/1024:.1f} MB")
        print(f"📏 Average key size: {avg_key_size:.0f} bytes ({avg_key_size/1024:.2f} KB)")
        
        # Key range coverage
        keys_attempted = self.current_key_id
        coverage = min(keys_attempted / self.max_daily_keys, 1.0)
        print(f"🔢 Key range attempted: {keys_attempted:,} of {self.max_daily_keys:,} ({coverage*100:.1f}%)")
        print(f"🔢 Keys actually written: {self.keys_written:,} ({(self.keys_written/keys_attempted)*100:.1f}% success rate)")
        
        # Performance comparison
        if elapsed > 0:
            throughput_mbps = (self.keys_written * avg_key_size / elapsed) / (1024 * 1024)
            print(f"🚀 Data throughput: {throughput_mbps:.1f} MB/sec")


def main():
    """Main function"""
    print("🚀 Fast Writer - Pre-generated Sample Keys")
    print("=" * 45)
    
    print("Choose writing mode:")
    print("1. Write specific number of keys")
    print("2. Write for specific duration")
    print("3. Continuous writing (Ctrl+C to stop)")
    print("4. Speed test (10K keys)")
    print("5. Mega speed test (100K keys)")
    
    choice = input("Enter choice (1-5): ").strip()
    
    # Get configuration
    max_keys = int(input("Max daily keys (default 200000): ") or "200000")
    skip_prob = float(input("Skip probability 0-1 (default 0.05): ") or "0.05")
    
    # Create writer
    writer = FastWriter(max_daily_keys=max_keys, skip_probability=skip_prob)
    
    if choice == "1":
        target = int(input("Number of keys to write: "))
        writer.run_continuous(target_keys=target)
    elif choice == "2":
        duration = int(input("Duration in seconds: "))
        writer.run_continuous(duration_seconds=duration)
    elif choice == "3":
        print("Starting continuous writing. Press Ctrl+C to stop.")
        writer.run_continuous()
    elif choice == "4":
        print("Running speed test with 10K keys...")
        writer.run_continuous(target_keys=10000)
    elif choice == "5":
        print("Running mega speed test with 100K keys...")
        writer.run_continuous(target_keys=100000)
    else:
        print("Invalid choice. Running speed test...")
        writer.run_continuous(target_keys=10000)


if __name__ == "__main__":
    main()
