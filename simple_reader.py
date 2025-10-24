#!/usr/bin/env python3
"""
Simple Reader - Reads random integer keys with fallback support
"""


import time
import threading
import signal
import json
from typing import Optional, List
from dataclasses import dataclass

from config import ConfigManager
from redis_client import RedisClient
from network_utils import create_redis_key_simple, generate_random_key_id


@dataclass
class ReadResult:
    """Result of a read operation"""
    key_id: int
    found_version: Optional[str]
    value_size: int
    read_time: float
    success: bool


@dataclass
class ReadStats:
    """Reading statistics"""
    total_reads: int = 0
    successful_reads: int = 0
    cache_misses: int = 0
    primary_hits: int = 0
    fallback_hits: int = 0
    total_time: float = 0.0
    total_data_bytes: int = 0


class SimpleReader:
    """Simple reader using random integer keys with version fallback"""
    
    def __init__(self, max_daily_keys: int = 200000, primary_version: str = "v23", fallback_version: str = "v22"):
        self.config = ConfigManager.load_config()
        self.redis_client = RedisClient()
        self.max_daily_keys = max_daily_keys
        self.primary_version = primary_version
        self.fallback_version = fallback_version
        
        # Performance tracking
        self.results: List[ReadResult] = []
        self.running = True
        self.lock = threading.Lock()
        
        print(f"📖 Simple Reader initialized:")
        print(f"   Max daily keys: {max_daily_keys:,}")
        print(f"   Primary version: {primary_version}")
        print(f"   Fallback version: {fallback_version}")
    
    def read_key_with_fallback(self, key_id: int) -> ReadResult:
        """Read a key with version fallback"""
        start_time = time.time()
        
        # Try primary version first
        primary_key = create_redis_key_simple(key_id, self.primary_version)
        
        try:
            value = self.redis_client.get_value(primary_key)
            if value:
                read_time = time.time() - start_time
                value_str = json.dumps(value) if isinstance(value, dict) else str(value)
                return ReadResult(
                    key_id=key_id,
                    found_version=self.primary_version,
                    value_size=len(value_str.encode('utf-8')),
                    read_time=read_time,
                    success=True
                )
        except Exception:
            pass
        
        # Try fallback version
        fallback_key = create_redis_key_simple(key_id, self.fallback_version)
        
        try:
            value = self.redis_client.get_value(fallback_key)
            if value:
                read_time = time.time() - start_time
                value_str = json.dumps(value) if isinstance(value, dict) else str(value)
                return ReadResult(
                    key_id=key_id,
                    found_version=self.fallback_version,
                    value_size=len(value_str.encode('utf-8')),
                    read_time=read_time,
                    success=True
                )
        except Exception:
            pass
        
        # Key not found in either version
        read_time = time.time() - start_time
        return ReadResult(
            key_id=key_id,
            found_version=None,
            value_size=0,
            read_time=read_time,
            success=False
        )

    def read_keys_pipeline_batch(self, key_ids: List[int], batch_size: int = 100) -> List[ReadResult]:
        """Read multiple keys using non-transactional pipeline for better performance"""
        results = []

        # Process in batches
        for i in range(0, len(key_ids), batch_size):
            batch_key_ids = key_ids[i:i + batch_size]
            batch_results = self._read_batch_with_pipeline(batch_key_ids)
            results.extend(batch_results)

        return results

    def _read_batch_with_pipeline(self, key_ids: List[int]) -> List[ReadResult]:
        """Read a batch of keys using non-transactional pipeline"""
        start_time = time.time()

        # Create non-transactional pipeline
        pipe = self.redis_client.client.pipeline(transaction=False)

        # Build key mappings
        primary_keys = []
        fallback_keys = []

        for key_id in key_ids:
            primary_key = create_redis_key_simple(key_id, self.primary_version)
            fallback_key = create_redis_key_simple(key_id, self.fallback_version)
            primary_keys.append((key_id, primary_key))
            fallback_keys.append((key_id, fallback_key))

            # Add both primary and fallback to pipeline
            pipe.get(primary_key)
            pipe.get(fallback_key)

        try:
            # Execute pipeline - get all results at once
            pipeline_results = pipe.execute()

            # Process results (pairs of primary/fallback for each key)
            results = []
            for i, key_id in enumerate(key_ids):
                primary_result = pipeline_results[i * 2]      # Primary result
                fallback_result = pipeline_results[i * 2 + 1] # Fallback result

                read_time = time.time() - start_time

                # Check primary first
                if primary_result:
                    value_str = json.dumps(primary_result) if isinstance(primary_result, dict) else str(primary_result)
                    results.append(ReadResult(
                        key_id=key_id,
                        found_version=self.primary_version,
                        value_size=len(value_str.encode('utf-8')),
                        read_time=read_time,
                        success=True
                    ))
                # Check fallback
                elif fallback_result:
                    value_str = json.dumps(fallback_result) if isinstance(fallback_result, dict) else str(fallback_result)
                    results.append(ReadResult(
                        key_id=key_id,
                        found_version=self.fallback_version,
                        value_size=len(value_str.encode('utf-8')),
                        read_time=read_time,
                        success=True
                    ))
                # Cache miss
                else:
                    results.append(ReadResult(
                        key_id=key_id,
                        found_version=None,
                        value_size=0,
                        read_time=read_time,
                        success=False
                    ))

            return results

        except Exception as e:
            print(f"❌ Pipeline read error: {e}")
            # Fallback to individual reads
            return [self.read_key_with_fallback(key_id) for key_id in key_ids]
    
    def worker_thread(self, thread_id: int, num_reads: int, results_list: List[ReadResult]):
        """Worker thread for parallel reading"""
        thread_results = []
        
        for i in range(num_reads):
            if not self.running:
                break
            
            # Generate random key ID
            key_id = generate_random_key_id(self.max_daily_keys)
            
            # Perform read with fallback
            result = self.read_key_with_fallback(key_id)
            thread_results.append(result)
            
            # Progress update every 1000 reads
            if (i + 1) % 1000 == 0:
                print(f"   Thread {thread_id}: {i + 1:,}/{num_reads:,} reads completed")
        
        # Add results to shared list
        with self.lock:
            results_list.extend(thread_results)
        
        print(f"✅ Thread {thread_id} completed: {len(thread_results):,} reads")
    
    def run_benchmark(self, total_reads: int = 10000, num_threads: int = 4) -> ReadStats:
        """Run reading benchmark"""
        print(f"\n📖 Starting reading benchmark...")
        print(f"   Total reads: {total_reads:,}")
        print(f"   Threads: {num_threads}")
        
        # Setup signal handler
        signal.signal(signal.SIGINT, lambda s, f: setattr(self, 'running', False))
        
        # Distribute reads among threads
        reads_per_thread = total_reads // num_threads
        remaining_reads = total_reads % num_threads
        
        start_time = time.time()
        threads = []
        results_list = []
        
        # Start worker threads
        for i in range(num_threads):
            thread_reads = reads_per_thread + (1 if i < remaining_reads else 0)
            thread = threading.Thread(
                target=self.worker_thread,
                args=(i + 1, thread_reads, results_list),
                daemon=True
            )
            threads.append(thread)
            thread.start()
        
        # Monitor progress
        last_report = time.time()
        
        try:
            while any(t.is_alive() for t in threads) and self.running:
                time.sleep(2)
                
                now = time.time()
                if now - last_report >= 5.0:
                    with self.lock:
                        current_reads = len(results_list)
                    
                    if current_reads > 0:
                        elapsed = now - start_time
                        rate = current_reads / elapsed if elapsed > 0 else 0
                        progress = (current_reads / total_reads) * 100
                        
                        print(f"📊 Progress: {current_reads:,}/{total_reads:,} ({progress:.1f}%) | "
                              f"Rate: {rate:.0f} reads/sec")
                        
                        last_report = now
        
        except KeyboardInterrupt:
            print(f"\n🛑 Reading interrupted by user")
            self.running = False
        
        # Wait for threads to complete
        for thread in threads:
            thread.join(timeout=5)
        
        # Calculate statistics
        end_time = time.time()
        total_time = end_time - start_time
        
        with self.lock:
            self.results = results_list.copy()
        
        return self._calculate_stats(total_time)
    
    def _calculate_stats(self, total_time: float) -> ReadStats:
        """Calculate reading statistics"""
        stats = ReadStats()
        
        stats.total_reads = len(self.results)
        stats.total_time = total_time
        
        if not self.results:
            return stats
        
        # Analyze results
        successful_results = [r for r in self.results if r.success]
        stats.successful_reads = len(successful_results)
        stats.cache_misses = stats.total_reads - stats.successful_reads
        
        stats.primary_hits = len([r for r in successful_results if r.found_version == self.primary_version])
        stats.fallback_hits = len([r for r in successful_results if r.found_version == self.fallback_version])
        
        if successful_results:
            stats.total_data_bytes = sum(r.value_size for r in successful_results)
        
        return stats

    def run_pipeline_benchmark(self, total_reads: int = 10000, batch_size: int = 100) -> ReadStats:
        """Run reading benchmark using pipeline batching for better performance"""
        print(f"\n📖 Starting PIPELINE reading benchmark...")
        print(f"   Total reads: {total_reads:,}")
        print(f"   Batch size: {batch_size}")
        print(f"   Expected: 10-20x speedup vs individual reads")

        self.running = True
        start_time = time.time()

        # Generate all key IDs upfront
        key_ids = [generate_random_key_id(self.max_daily_keys) for _ in range(total_reads)]

        # Process in pipeline batches
        all_results = self.read_keys_pipeline_batch(key_ids, batch_size=batch_size)

        end_time = time.time()
        total_time = end_time - start_time

        # Calculate statistics
        stats = ReadStats()
        stats.total_reads = len(all_results)
        stats.total_time = total_time

        for result in all_results:
            if result.success:
                stats.successful_reads += 1
                stats.total_data_bytes += result.value_size

                if result.found_version == self.primary_version:
                    stats.primary_hits += 1
                else:
                    stats.fallback_hits += 1
            else:
                stats.cache_misses += 1

        # Print results
        hit_rate = (stats.successful_reads / stats.total_reads) * 100 if stats.total_reads > 0 else 0
        read_rate = stats.total_reads / total_time if total_time > 0 else 0

        print(f"\n📊 PIPELINE Benchmark Results:")
        print(f"   Total reads: {stats.total_reads:,}")
        print(f"   Successful reads: {stats.successful_reads:,}")
        print(f"   Cache misses: {stats.cache_misses:,}")
        print(f"   Primary hits ({self.primary_version}): {stats.primary_hits:,}")
        print(f"   Fallback hits ({self.fallback_version}): {stats.fallback_hits:,}")
        print(f"   Hit rate: {hit_rate:.1f}%")
        print(f"   Duration: {total_time:.1f} seconds")
        print(f"   Read rate: {read_rate:.0f} reads/sec")
        print(f"   Data read: {stats.total_data_bytes / (1024*1024):.1f} MB")
        print(f"   Throughput: {(stats.total_data_bytes / (1024*1024)) / total_time:.1f} MB/sec")

        if read_rate > 1000:
            print(f"🚀 INCREDIBLE: >1,000 reads/sec with pipeline!")
        elif read_rate > 500:
            print(f"🚀 EXCELLENT: >500 reads/sec with pipeline!")
        elif read_rate > 100:
            print(f"✅ GOOD: >100 reads/sec with pipeline!")
        else:
            print(f"⚠️  Pipeline rate: {read_rate:.0f} reads/sec")

        return stats

    def pipeline_worker_thread(self, thread_id: int, key_ids: List[int], batch_size: int, results_list: List[ReadResult]):
        """Worker thread for multi-threaded pipeline reading"""
        thread_results = []

        # Process key_ids in pipeline batches
        for i in range(0, len(key_ids), batch_size):
            if not self.running:
                break

            batch_key_ids = key_ids[i:i + batch_size]
            batch_results = self._read_batch_with_pipeline(batch_key_ids)
            thread_results.extend(batch_results)

            # Progress update every 1000 reads
            if len(thread_results) % 1000 == 0:
                print(f"   Thread {thread_id}: {len(thread_results):,} reads completed")

        # Add results to shared list
        with self.lock:
            results_list.extend(thread_results)

        print(f"✅ Thread {thread_id} completed: {len(thread_results):,} reads")

    def run_multithreaded_pipeline_benchmark(self, total_reads: int = 10000, num_threads: int = 4, batch_size: int = 10) -> ReadStats:
        """Run multi-threaded pipeline reading benchmark for maximum performance"""
        print(f"\n📖 Starting MULTI-THREADED PIPELINE reading benchmark...")
        print(f"   Total reads: {total_reads:,}")
        print(f"   Threads: {num_threads}")
        print(f"   Batch size: {batch_size}")
        print(f"   Expected: Massive speedup with threading + pipeline!")

        # Setup signal handler
        signal.signal(signal.SIGINT, lambda s, f: setattr(self, 'running', False))

        self.running = True
        start_time = time.time()

        # Generate all key IDs upfront
        all_key_ids = [generate_random_key_id(self.max_daily_keys) for _ in range(total_reads)]

        # Distribute key IDs among threads
        reads_per_thread = total_reads // num_threads
        remaining_reads = total_reads % num_threads

        threads = []
        results_list = []

        # Start worker threads
        key_offset = 0
        for i in range(num_threads):
            thread_reads = reads_per_thread + (1 if i < remaining_reads else 0)
            thread_key_ids = all_key_ids[key_offset:key_offset + thread_reads]
            key_offset += thread_reads

            thread = threading.Thread(
                target=self.pipeline_worker_thread,
                args=(i + 1, thread_key_ids, batch_size, results_list),
                daemon=True
            )
            threads.append(thread)
            thread.start()

        # Monitor progress
        last_report = time.time()

        try:
            while any(t.is_alive() for t in threads) and self.running:
                time.sleep(2)

                now = time.time()
                if now - last_report >= 5.0:
                    with self.lock:
                        current_reads = len(results_list)

                    if current_reads > 0:
                        elapsed = now - start_time
                        rate = current_reads / elapsed if elapsed > 0 else 0
                        progress = (current_reads / total_reads) * 100

                        print(f"📊 Progress: {current_reads:,}/{total_reads:,} ({progress:.1f}%) | "
                              f"Rate: {rate:.0f} reads/sec")

                        last_report = now

        except KeyboardInterrupt:
            print(f"\n🛑 Reading interrupted by user")
            self.running = False

        # Wait for threads to complete
        for thread in threads:
            thread.join(timeout=5)

        end_time = time.time()
        total_time = end_time - start_time

        # Calculate statistics
        stats = ReadStats()
        with self.lock:
            all_results = results_list.copy()

        stats.total_reads = len(all_results)
        stats.total_time = total_time

        for result in all_results:
            if result.success:
                stats.successful_reads += 1
                stats.total_data_bytes += result.value_size

                if result.found_version == self.primary_version:
                    stats.primary_hits += 1
                else:
                    stats.fallback_hits += 1
            else:
                stats.cache_misses += 1

        # Print results
        hit_rate = (stats.successful_reads / stats.total_reads) * 100 if stats.total_reads > 0 else 0
        read_rate = stats.total_reads / total_time if total_time > 0 else 0

        print(f"\n📊 MULTI-THREADED PIPELINE Results:")
        print(f"   Total reads: {stats.total_reads:,}")
        print(f"   Successful reads: {stats.successful_reads:,}")
        print(f"   Cache misses: {stats.cache_misses:,}")
        print(f"   Primary hits ({self.primary_version}): {stats.primary_hits:,}")
        print(f"   Fallback hits ({self.fallback_version}): {stats.fallback_hits:,}")
        print(f"   Hit rate: {hit_rate:.1f}%")
        print(f"   Duration: {total_time:.1f} seconds")
        print(f"   Read rate: {read_rate:.0f} reads/sec")
        print(f"   Data read: {stats.total_data_bytes / (1024*1024):.1f} MB")
        print(f"   Throughput: {(stats.total_data_bytes / (1024*1024)) / total_time:.1f} MB/sec")

        if read_rate > 5000:
            print(f"🚀 INCREDIBLE: >5,000 reads/sec with multi-threaded pipeline!")
        elif read_rate > 2000:
            print(f"🚀 EXCELLENT: >2,000 reads/sec with multi-threaded pipeline!")
        elif read_rate > 1000:
            print(f"✅ VERY GOOD: >1,000 reads/sec with multi-threaded pipeline!")
        else:
            print(f"✅ Rate: {read_rate:.0f} reads/sec")

        return stats

    def print_results(self, stats: ReadStats):
        """Print detailed results"""
        print(f"\n📊 Reading Results")
        print("=" * 20)
        
        print(f"📋 Read Statistics:")
        print(f"   Total reads: {stats.total_reads:,}")
        print(f"   Successful reads: {stats.successful_reads:,}")
        print(f"   Cache misses: {stats.cache_misses:,}")
        
        if stats.total_reads > 0:
            success_rate = (stats.successful_reads / stats.total_reads) * 100
            miss_rate = (stats.cache_misses / stats.total_reads) * 100
            print(f"   Success rate: {success_rate:.1f}%")
            print(f"   Cache miss rate: {miss_rate:.1f}%")
        
        print(f"\n🔄 Version Distribution:")
        if stats.successful_reads > 0:
            primary_pct = (stats.primary_hits / stats.successful_reads) * 100
            fallback_pct = (stats.fallback_hits / stats.successful_reads) * 100
            print(f"   {self.primary_version} hits: {stats.primary_hits:,} ({primary_pct:.1f}%)")
            print(f"   {self.fallback_version} hits: {stats.fallback_hits:,} ({fallback_pct:.1f}%)")
        else:
            print(f"   {self.primary_version} hits: 0 (0.0%)")
            print(f"   {self.fallback_version} hits: 0 (0.0%)")
        
        print(f"\n⚡ Performance Metrics:")
        print(f"   Total time: {stats.total_time:.2f} seconds")
        
        if stats.total_time > 0:
            reads_per_sec = stats.total_reads / stats.total_time
            print(f"   Reads per second: {reads_per_sec:.0f}")
            
            if stats.total_data_bytes > 0:
                throughput_mbps = (stats.total_data_bytes / stats.total_time) / (1024 * 1024)
                print(f"   Data throughput: {throughput_mbps:.2f} MB/s")
                print(f"   Total data read: {stats.total_data_bytes / 1024 / 1024:.1f} MB")
        
        # Response time analysis
        if self.results:
            successful_times = [r.read_time * 1000 for r in self.results if r.success]
            if successful_times:
                successful_times.sort()
                p50 = successful_times[len(successful_times)//2]
                p95 = successful_times[int(len(successful_times)*0.95)]
                
                print(f"\n📈 Response Times:")
                print(f"   P50: {p50:.2f} ms")
                print(f"   P95: {p95:.2f} ms")
                print(f"   Min: {min(successful_times):.2f} ms")
                print(f"   Max: {max(successful_times):.2f} ms")


def main():
    """Main function"""
    print("📖 Simple Reader - Random Integer Keys")
    print("=" * 40)
    
    print("Choose reading mode:")
    print("1. Quick test (1K reads)")
    print("2. Medium test (10K reads)")
    print("3. Large test (100K reads)")
    print("4. Custom configuration")
    
    choice = input("Enter choice (1-4): ").strip()
    
    # Get configuration
    max_keys = int(input("Max daily keys (default 200000): ") or "200000")
    primary_ver = input("Primary version (default v22): ") or "v22"
    fallback_ver = input("Fallback version (default v23): ") or "v23"
    
    if choice == "1":
        total_reads = 1000
        threads = 2
    elif choice == "2":
        total_reads = 10000
        threads = 4
    elif choice == "3":
        total_reads = 100000
        threads = 8
    elif choice == "4":
        total_reads = int(input("Number of reads: "))
        threads = int(input("Number of threads: "))
    else:
        total_reads = 1000
        threads = 2
    
    # Create and run reader
    reader = SimpleReader(
        max_daily_keys=max_keys,
        primary_version=primary_ver,
        fallback_version=fallback_ver
    )
    
    stats = reader.run_benchmark(total_reads, threads)
    reader.print_results(stats)
    
    print(f"\n🎉 Reading benchmark completed!")


if __name__ == "__main__":
    main()
