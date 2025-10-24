#!/usr/bin/env python3
"""
Parallel Fast Writer - Launch Multiple FastWriter Instances
For high-speed parallel writing to remote Redis servers
"""

import time
import signal
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from fast_writer import FastWriter
from config import ConfigManager


@dataclass
class WriterStats:
    """Statistics for a single writer thread"""
    writer_id: int
    keys_written: int = 0
    keys_skipped: int = 0
    duration: float = 0.0
    start_key: int = 0
    end_key: int = 0
    version: str = "v25"


class ParallelWriter:
    """Parallel writer that launches multiple FastWriter instances"""
    
    def __init__(self, num_writers: int = 4, max_daily_keys: int = 1000000, 
                 skip_probability: float = 0.05):
        self.num_writers = num_writers
        self.max_daily_keys = max_daily_keys
        self.skip_probability = skip_probability
        self.config = ConfigManager.load_config()
        self.running = True
        self.writer_stats: List[WriterStats] = []
        
        print(f"🚀 Parallel Writer System initialized:")
        print(f"   Number of writers: {num_writers}")
        print(f"   Max daily keys: {max_daily_keys:,}")
        print(f"   Skip probability: {skip_probability*100:.1f}%")
        print(f"   Key space per writer: {max_daily_keys // num_writers:,}")
    
    def _create_writer_for_range(self, writer_id: int, start_key: int, 
                                end_key: int, version: str) -> WriterStats:
        """Create and run a FastWriter for a specific key range"""
        try:
            # Create writer with custom key range
            writer = FastWriter(
                max_daily_keys=end_key,  # Set max to end_key
                skip_probability=self.skip_probability
            )
            writer.version = version
            writer.current_key_id = start_key  # Start from specific key
            
            target_keys = end_key - start_key
            print(f"🔄 Writer {writer_id} starting: keys {start_key:,}-{end_key-1:,} ({target_keys:,} keys)")
            
            start_time = time.time()
            writer.run_continuous(target_keys=target_keys)
            duration = time.time() - start_time
            
            stats = WriterStats(
                writer_id=writer_id,
                keys_written=writer.keys_written,
                keys_skipped=writer.keys_skipped,
                duration=duration,
                start_key=start_key,
                end_key=end_key,
                version=version
            )
            
            print(f"✅ Writer {writer_id} completed: {writer.keys_written:,} keys in {duration:.1f}s ({writer.keys_written/duration:.0f} keys/sec)")
            return stats
            
        except Exception as e:
            print(f"❌ Writer {writer_id} failed: {e}")
            return WriterStats(writer_id=writer_id, start_key=start_key, end_key=end_key, version=version)
    
    def run_parallel_import(self, version: str = "v25", target_keys: Optional[int] = None) -> Dict:
        """Run parallel import for a single version"""
        if target_keys is None:
            target_keys = self.max_daily_keys
            
        print(f"\n🎯 Starting parallel import for {version}")
        print(f"   Target keys: {target_keys:,}")
        print(f"   Writers: {self.num_writers}")
        
        # Calculate key ranges for each writer
        keys_per_writer = target_keys // self.num_writers
        remainder = target_keys % self.num_writers
        
        # Setup signal handler
        signal.signal(signal.SIGINT, lambda _s, _f: setattr(self, 'running', False))
        
        start_time = time.time()
        
        # Create thread pool and submit tasks
        with ThreadPoolExecutor(max_workers=self.num_writers) as executor:
            futures = []
            
            for i in range(self.num_writers):
                start_key = i * keys_per_writer
                end_key = start_key + keys_per_writer
                
                # Add remainder to last writer
                if i == self.num_writers - 1:
                    end_key += remainder
                
                future = executor.submit(
                    self._create_writer_for_range,
                    i + 1, start_key, end_key, version
                )
                futures.append(future)
            
            # Collect results
            completed_stats = []
            for future in as_completed(futures):
                if not self.running:
                    print("🛑 Parallel import interrupted")
                    break
                    
                try:
                    stats = future.result()
                    completed_stats.append(stats)
                except Exception as e:
                    print(f"❌ Writer thread failed: {e}")
        
        total_duration = time.time() - start_time
        
        # Calculate totals
        total_written = sum(s.keys_written for s in completed_stats)
        total_skipped = sum(s.keys_skipped for s in completed_stats)
        total_rate = total_written / total_duration if total_duration > 0 else 0
        
        results = {
            'version': version,
            'total_keys_written': total_written,
            'total_keys_skipped': total_skipped,
            'total_duration': total_duration,
            'total_rate': total_rate,
            'num_writers': len(completed_stats),
            'writer_stats': completed_stats
        }
        
        print(f"\n📊 Parallel Import Results for {version}:")
        print(f"   Keys written: {total_written:,}")
        print(f"   Keys skipped: {total_skipped:,}")
        print(f"   Duration: {total_duration:.1f} seconds")
        print(f"   Combined rate: {total_rate:.0f} keys/sec")
        print(f"   Speedup: {total_rate/11:.1f}x vs single writer")
        
        return results
    
    def run_full_parallel_import(self, target_keys: Optional[int] = None) -> Dict:
        """Run parallel import for both v22 and v23"""
        if target_keys is None:
            target_keys = self.max_daily_keys
            
        print(f"🚀 Starting FULL parallel import")
        print(f"   Target: {target_keys:,} keys each for v22 and v23")
        print(f"   Total target: {target_keys * 2:,} keys")
        print(f"   Parallel writers: {self.num_writers}")
        
        overall_start = time.time()
        
        # Import v22
        print(f"\n" + "="*50)
        v22_results = self.run_parallel_import("v22", target_keys)
        
        # Brief pause between versions
        print(f"\n⏸️  Brief pause between versions...")
        time.sleep(2)
        
        # Import v23
        print(f"\n" + "="*50)
        v23_results = self.run_parallel_import("v23", target_keys)
        
        overall_duration = time.time() - overall_start
        
        # Combined results
        total_written = v22_results['total_keys_written'] + v23_results['total_keys_written']
        total_skipped = v22_results['total_keys_skipped'] + v23_results['total_keys_skipped']
        combined_rate = total_written / overall_duration
        
        final_results = {
            'v22_results': v22_results,
            'v23_results': v23_results,
            'total_keys_written': total_written,
            'total_keys_skipped': total_skipped,
            'overall_duration': overall_duration,
            'combined_rate': combined_rate,
            'num_writers': self.num_writers
        }
        
        print(f"\n🎉 FULL PARALLEL IMPORT COMPLETE!")
        print(f"=" * 50)
        print(f"📊 Final Results:")
        print(f"   v22 keys: {v22_results['total_keys_written']:,}")
        print(f"   v23 keys: {v23_results['total_keys_written']:,}")
        print(f"   Total written: {total_written:,}")
        print(f"   Total skipped: {total_skipped:,}")
        print(f"   Overall duration: {overall_duration/60:.1f} minutes")
        print(f"   Combined rate: {combined_rate:.0f} keys/sec")
        print(f"   Parallel speedup: {combined_rate/11:.1f}x")
        
        # Estimate for 66M
        if target_keys < 66000000:
            estimated_66m_time = (66000000 * 2 * 0.95) / combined_rate / 3600
            print(f"\n💡 Estimated 66M import time:")
            print(f"   At {combined_rate:.0f} keys/sec: ~{estimated_66m_time:.1f} hours")
        
        return final_results


def main():
    """Interactive parallel writer"""
    print("🚀 Parallel Fast Writer System")
    print("=" * 40)
    
    # Get configuration
    try:
        num_writers = int(input("Number of parallel writers (default 4): ") or "4")
        max_keys = int(input("Max daily keys (default 1000000): ") or "1000000")
        skip_prob = float(input("Skip probability 0-1 (default 0.05): ") or "0.05")
        
        print(f"\n🎯 Configuration:")
        print(f"   Writers: {num_writers}")
        print(f"   Max keys: {max_keys:,}")
        print(f"   Skip probability: {skip_prob*100:.1f}%")
        
        confirm = input(f"\nStart parallel import? (y/N): ")
        if confirm.lower() != 'y':
            print("Import cancelled.")
            return
            
    except (ValueError, KeyboardInterrupt):
        print("Invalid input or cancelled.")
        return
    
    # Create and run parallel writer
    parallel_writer = ParallelWriter(
        num_writers=num_writers,
        max_daily_keys=max_keys,
        skip_probability=skip_prob
    )
    
    try:
        results = parallel_writer.run_full_parallel_import()
        
        # Save results
        import json
        with open('parallel_import_stats.json', 'w') as f:
            # Convert stats objects to dicts for JSON serialization
            json_results = results.copy()
            for version_key in ['v22_results', 'v23_results']:
                if version_key in json_results:
                    stats_list = json_results[version_key]['writer_stats']
                    json_results[version_key]['writer_stats'] = [
                        {
                            'writer_id': s.writer_id,
                            'keys_written': s.keys_written,
                            'keys_skipped': s.keys_skipped,
                            'duration': s.duration,
                            'start_key': s.start_key,
                            'end_key': s.end_key,
                            'version': s.version
                        } for s in stats_list
                    ]
            
            json.dump(json_results, f, indent=2)
        
        print(f"\n💾 Results saved to parallel_import_stats.json")
        
    except KeyboardInterrupt:
        print(f"\n🛑 Import interrupted by user")
    except Exception as e:
        print(f"❌ Import failed: {e}")


if __name__ == "__main__":
    main()
