#!/usr/bin/env python3
"""
Benchmark Reader - Test random key reads with version fallback
"""

import random
import time
import threading
import signal
from typing import List, Optional
from dataclasses import dataclass

from config import ConfigManager
from redis_client import RedisClient
from ip_dataset_manager import IPDatasetManager


@dataclass
class ReadResult:
    """Result of a key read operation"""
    key: str
    found_version: Optional[str]
    value_size: int
    read_time: float
    attempts: int
    success: bool


@dataclass
class BenchmarkStats:
    """Benchmark statistics"""
    total_reads: int = 0
    successful_reads: int = 0
    v22_hits: int = 0
    v23_hits: int = 0
    cache_misses: int = 0
    total_time: float = 0.0
    total_data_bytes: int = 0
    avg_read_time: float = 0.0
    reads_per_second: float = 0.0
    data_throughput_mbps: float = 0.0


class BenchmarkReader:
    """High-performance benchmark reader with version fallback"""
    
    def __init__(self, primary_version: str = "v22", fallback_version: str = "v23"):
        self.config = ConfigManager.load_config()
        self.redis_client = RedisClient()
        self.primary_version = primary_version
        self.fallback_version = fallback_version
        
        # Performance tracking
        self.stats = BenchmarkStats()
        self.results: List[ReadResult] = []
        self.running = True
        self.lock = threading.Lock()
        
        # Load available IP addresses for random selection
        self.available_ips = self._load_available_ips()
        
    def _load_available_ips(self) -> List[str]:
        """Load available IP addresses from the 500M dataset"""
        print("📂 Loading available IP addresses from 500M dataset...")
        
        try:
            dataset_manager = IPDatasetManager()
            networks = dataset_manager.load_network_list("network_blocks_500m.json")
            
            # Generate sample IPs from each network (for performance, we'll sample)
            sample_ips = []
            max_samples_per_network = 100  # Limit samples per network for memory efficiency
            
            for i, network_str in enumerate(networks[:1000]):  # Use first 1000 networks for demo
                try:
                    import ipaddress
                    network = ipaddress.IPv4Network(network_str)
                    
                    # Sample random IPs from this network
                    hosts = list(network.hosts())
                    if hosts:
                        sample_count = min(max_samples_per_network, len(hosts))
                        sampled_hosts = random.sample(hosts, sample_count)
                        sample_ips.extend([str(ip) for ip in sampled_hosts])
                    
                    if (i + 1) % 100 == 0:
                        print(f"   Processed {i + 1:,} networks, {len(sample_ips):,} IPs sampled")
                        
                except Exception as e:
                    continue
            
            print(f"✅ Loaded {len(sample_ips):,} sample IP addresses for benchmarking")
            return sample_ips
            
        except Exception as e:
            print(f"❌ Error loading IP addresses: {e}")
            # Fallback to generating some test IPs
            return [f"192.168.1.{i}" for i in range(1, 255)]
    
    def read_key_with_fallback(self, ip: str) -> ReadResult:
        """Read a key with version fallback logic"""
        start_time = time.time()
        attempts = 0
        
        # Try primary version first
        primary_key = f"ip:{self.primary_version}:{ip}"
        attempts += 1
        
        try:
            value = self.redis_client.get_value(primary_key)
            if value:
                read_time = time.time() - start_time
                return ReadResult(
                    key=primary_key,
                    found_version=self.primary_version,
                    value_size=len(str(value).encode('utf-8')),
                    read_time=read_time,
                    attempts=attempts,
                    success=True
                )
        except Exception:
            pass
        
        # Try fallback version
        fallback_key = f"ip:{self.fallback_version}:{ip}"
        attempts += 1
        
        try:
            value = self.redis_client.get_value(fallback_key)
            if value:
                read_time = time.time() - start_time
                return ReadResult(
                    key=fallback_key,
                    found_version=self.fallback_version,
                    value_size=len(str(value).encode('utf-8')),
                    read_time=read_time,
                    attempts=attempts,
                    success=True
                )
        except Exception:
            pass
        
        # Key not found in either version
        read_time = time.time() - start_time
        return ReadResult(
            key=f"ip:*:{ip}",
            found_version=None,
            value_size=0,
            read_time=read_time,
            attempts=attempts,
            success=False
        )
    
    def worker_thread(self, thread_id: int, num_reads: int, results_queue: List[ReadResult]):
        """Worker thread for parallel reading"""
        thread_results = []
        
        for i in range(num_reads):
            if not self.running:
                break
                
            # Select random IP
            ip = random.choice(self.available_ips)
            
            # Perform read with fallback
            result = self.read_key_with_fallback(ip)
            thread_results.append(result)
            
            # Progress update every 1000 reads
            if (i + 1) % 1000 == 0:
                print(f"   Thread {thread_id}: {i + 1:,}/{num_reads:,} reads completed")
        
        # Add results to shared queue
        with self.lock:
            results_queue.extend(thread_results)
        
        print(f"✅ Thread {thread_id} completed: {len(thread_results):,} reads")
    
    def run_benchmark(self, total_reads: int = 100000, num_threads: int = 4) -> BenchmarkStats:
        """Run the benchmark with specified parameters"""
        print("🎯 LocationFlex Benchmark Reader")
        print("=" * 40)
        
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, lambda s, f: setattr(self, 'running', False))
        
        print(f"📋 Benchmark Configuration:")
        print(f"   Total reads: {total_reads:,}")
        print(f"   Threads: {num_threads}")
        print(f"   Primary version: {self.primary_version}")
        print(f"   Fallback version: {self.fallback_version}")
        print(f"   Available IPs: {len(self.available_ips):,}")
        
        # Distribute reads among threads
        reads_per_thread = total_reads // num_threads
        remaining_reads = total_reads % num_threads
        
        print(f"\n🚀 Starting {num_threads} reader threads...")
        start_time = time.time()
        
        # Start worker threads
        threads = []
        results_queue = []
        
        for i in range(num_threads):
            thread_reads = reads_per_thread + (1 if i < remaining_reads else 0)
            thread = threading.Thread(
                target=self.worker_thread,
                args=(i + 1, thread_reads, results_queue),
                daemon=True
            )
            threads.append(thread)
            thread.start()
        
        # Monitor progress
        try:
            while any(t.is_alive() for t in threads) and self.running:
                time.sleep(5)
                
                with self.lock:
                    current_reads = len(results_queue)
                
                if current_reads > 0:
                    elapsed = time.time() - start_time
                    rate = current_reads / elapsed if elapsed > 0 else 0
                    progress = (current_reads / total_reads) * 100
                    
                    print(f"📊 Progress: {current_reads:,}/{total_reads:,} ({progress:.1f}%) | "
                          f"Rate: {rate:.0f} reads/sec")
        
        except KeyboardInterrupt:
            print(f"\n🛑 Benchmark interrupted by user")
            self.running = False
        
        # Wait for threads to complete
        for thread in threads:
            thread.join(timeout=10)
        
        # Calculate final statistics
        end_time = time.time()
        total_time = end_time - start_time
        
        with self.lock:
            self.results = results_queue.copy()
        
        return self._calculate_stats(total_time)
    
    def _calculate_stats(self, total_time: float) -> BenchmarkStats:
        """Calculate benchmark statistics"""
        stats = BenchmarkStats()
        
        stats.total_reads = len(self.results)
        stats.total_time = total_time
        
        if not self.results:
            return stats
        
        # Analyze results
        successful_results = [r for r in self.results if r.success]
        stats.successful_reads = len(successful_results)
        
        stats.v22_hits = len([r for r in successful_results if r.found_version == self.primary_version])
        stats.v23_hits = len([r for r in successful_results if r.found_version == self.fallback_version])
        stats.cache_misses = stats.total_reads - stats.successful_reads
        
        # Calculate performance metrics
        if successful_results:
            stats.total_data_bytes = sum(r.value_size for r in successful_results)
            stats.avg_read_time = sum(r.read_time for r in successful_results) / len(successful_results)
        
        if total_time > 0:
            stats.reads_per_second = stats.total_reads / total_time
            stats.data_throughput_mbps = (stats.total_data_bytes / total_time) / (1024 * 1024)
        
        return stats
    
    def print_results(self, stats: BenchmarkStats):
        """Print detailed benchmark results"""
        print(f"\n🎯 Benchmark Results")
        print("=" * 25)
        
        print(f"📊 Read Statistics:")
        print(f"   Total reads: {stats.total_reads:,}")
        print(f"   Successful reads: {stats.successful_reads:,}")
        print(f"   Cache misses: {stats.cache_misses:,}")
        print(f"   Success rate: {(stats.successful_reads/stats.total_reads)*100:.1f}%")
        
        print(f"\n📋 Version Distribution:")
        if stats.successful_reads > 0:
            print(f"   {self.primary_version} hits: {stats.v22_hits:,} ({(stats.v22_hits/stats.successful_reads)*100:.1f}%)")
            print(f"   {self.fallback_version} hits: {stats.v23_hits:,} ({(stats.v23_hits/stats.successful_reads)*100:.1f}%)")
        else:
            print(f"   {self.primary_version} hits: {stats.v22_hits:,} (0.0%)")
            print(f"   {self.fallback_version} hits: {stats.v23_hits:,} (0.0%)")
        
        print(f"\n⚡ Performance Metrics:")
        print(f"   Total time: {stats.total_time:.2f} seconds")
        print(f"   Reads per second: {stats.reads_per_second:.0f}")
        print(f"   Average read time: {stats.avg_read_time*1000:.2f} ms")
        print(f"   Data throughput: {stats.data_throughput_mbps:.2f} MB/s")
        print(f"   Total data read: {stats.total_data_bytes/1024/1024:.1f} MB")
        
        # Response time distribution
        if self.results:
            successful_times = [r.read_time * 1000 for r in self.results if r.success]
            if successful_times:
                successful_times.sort()
                p50 = successful_times[len(successful_times)//2]
                p95 = successful_times[int(len(successful_times)*0.95)]
                p99 = successful_times[int(len(successful_times)*0.99)]
                
                print(f"\n📈 Response Time Distribution:")
                print(f"   P50: {p50:.2f} ms")
                print(f"   P95: {p95:.2f} ms")
                print(f"   P99: {p99:.2f} ms")
                print(f"   Min: {min(successful_times):.2f} ms")
                print(f"   Max: {max(successful_times):.2f} ms")


def main():
    """Main benchmark function"""
    print("🎯 LocationFlex Benchmark Reader")
    print("=" * 40)
    print("Choose benchmark scenario:")
    print("1. Quick test (1K reads)")
    print("2. Medium test (10K reads)")
    print("3. Large test (100K reads)")
    print("4. Stress test (1M reads)")
    print("5. Version migration test (mixed v22/v23)")
    print("6. Cache miss test (non-existent keys)")
    print("7. Custom configuration")

    choice = input("Enter choice (1-7): ").strip()

    if choice == "1":
        total_reads = 1000
        threads = 2
        primary_version = "v22"
        fallback_version = "v23"
    elif choice == "2":
        total_reads = 10000
        threads = 4
        primary_version = "v22"
        fallback_version = "v23"
    elif choice == "3":
        total_reads = 100000
        threads = 8
        primary_version = "v22"
        fallback_version = "v23"
    elif choice == "4":
        total_reads = 1000000
        threads = 16
        primary_version = "v22"
        fallback_version = "v23"
    elif choice == "5":
        print("\n🔄 Version Migration Test:")
        print("This test simulates reading from an older version (v21) with fallback to v22")
        total_reads = int(input("Enter number of reads (default 10000): ") or "10000")
        threads = int(input("Enter number of threads (default 4): ") or "4")
        primary_version = "v21"  # Non-existent version
        fallback_version = "v22"
    elif choice == "6":
        print("\n❌ Cache Miss Test:")
        print("This test uses random IPs that likely don't exist in the dataset")
        total_reads = int(input("Enter number of reads (default 5000): ") or "5000")
        threads = int(input("Enter number of threads (default 4): ") or "4")
        primary_version = "v22"
        fallback_version = "v23"
        # We'll modify the reader to use random IPs for this test
    elif choice == "7":
        total_reads = int(input("Enter number of reads: "))
        threads = int(input("Enter number of threads: "))
        primary_version = input("Enter primary version (default v22): ") or "v22"
        fallback_version = input("Enter fallback version (default v23): ") or "v23"
    else:
        total_reads = 1000
        threads = 2
        primary_version = "v22"
        fallback_version = "v23"

    # Create and run benchmark
    reader = BenchmarkReader(primary_version=primary_version, fallback_version=fallback_version)

    # Special handling for cache miss test
    if choice == "6":
        print("🎲 Generating random IPs for cache miss testing...")
        reader.available_ips = [f"{random.randint(200,250)}.{random.randint(200,250)}.{random.randint(200,250)}.{random.randint(1,254)}"
                               for _ in range(10000)]
        print(f"✅ Generated {len(reader.available_ips):,} random IPs")

    stats = reader.run_benchmark(total_reads, threads)
    reader.print_results(stats)

    # Additional analysis for specific test types
    if choice == "5":
        print(f"\n🔄 Migration Analysis:")
        print(f"   Primary version ({primary_version}) availability: {(stats.v22_hits/stats.total_reads)*100:.1f}%")
        print(f"   Fallback usage rate: {(stats.v23_hits/stats.total_reads)*100:.1f}%")
        print(f"   Migration efficiency: {'High' if stats.v23_hits > stats.total_reads * 0.8 else 'Medium' if stats.v23_hits > stats.total_reads * 0.5 else 'Low'}")

    elif choice == "6":
        print(f"\n❌ Cache Miss Analysis:")
        print(f"   Cache hit rate: {(stats.successful_reads/stats.total_reads)*100:.1f}%")
        print(f"   Cache miss rate: {(stats.cache_misses/stats.total_reads)*100:.1f}%")
        print(f"   Average attempts per read: {sum(r.attempts for r in reader.results)/len(reader.results):.1f}")

    print(f"\n🎉 Benchmark completed successfully!")


if __name__ == "__main__":
    main()
