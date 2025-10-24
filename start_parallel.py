#!/usr/bin/env python3
"""
Quick Start - Parallel Writer System
Simple launcher for common parallel import scenarios
"""

import sys
import time
from parallel_writer import ParallelWriter


def quick_test():
    """Quick 400-key test with 4 writers"""
    print("🧪 Quick Test - 400 keys with 4 writers")
    print("=" * 45)

    version = input("Version to write (e.g. v22, v23): ").strip() or "v22"

    parallel_writer = ParallelWriter(
        num_writers=4,
        max_daily_keys=400,
        skip_probability=0.05
    )

    start_time = time.time()
    results = parallel_writer.run_parallel_import(version, target_keys=400)
    duration = time.time() - start_time

    print(f"\n✅ Quick test complete!")
    print(f"   Version: {version}")
    print(f"   Rate: {results['total_rate']:.0f} keys/sec")
    print(f"   Duration: {duration:.1f} seconds")
    return results


def high_performance_test():
    """High performance test with 8 writers"""
    print("🚀 High Performance Test - 800 keys with 8 writers")
    print("=" * 55)

    version = input("Version to write (e.g. v22, v23): ").strip() or "v22"

    parallel_writer = ParallelWriter(
        num_writers=8,
        max_daily_keys=800,
        skip_probability=0.05
    )

    start_time = time.time()
    results = parallel_writer.run_parallel_import(version, target_keys=800)
    duration = time.time() - start_time

    print(f"\n🚀 High performance test complete!")
    print(f"   Version: {version}")
    print(f"   Rate: {results['total_rate']:.0f} keys/sec")
    print(f"   Duration: {duration:.1f} seconds")
    return results


def production():

    version =  "v22"
    num_writers=30
    max_daily_keys=60000000
    
    print(f"\n🎯 Starting {max_daily_keys} import for {version} with {num_writers} writers")


    parallel_writer = ParallelWriter(
        num_writers=num_writers,
        max_daily_keys=max_daily_keys,
        skip_probability=0.01
    )

    try:
        start_time = time.time()
        results = parallel_writer.run_parallel_import(version, target_keys=60000000)
        duration = time.time() - start_time

        print(f"\n🎉 66M Import SUCCESS!")
        print(f"   Version: {version}")
        print(f"   Keys written: {results['total_keys_written']:,}")
        print(f"   Duration: {duration/3600:.1f} hours")
        print(f"   Rate: {results['total_rate']:.0f} keys/sec")

        return results

    except KeyboardInterrupt:
        print(f"\n🛑 Import stopped by user")
        return None


def custom_import():
    """Custom configuration import"""
    print("⚙️  Custom Parallel Import")
    print("=" * 30)
    
    try:
        writers = int(input("Number of writers (1-32): "))
        keys = int(input("Keys per version (e.g. 1000000): "))
        
        if writers < 1 or writers > 32:
            print("❌ Writers must be 1-32")
            return
            
        if keys < 100:
            print("❌ Keys must be at least 100")
            return
            
        print(f"\n🎯 Custom import: {keys:,} keys with {writers} writers")
        confirm = input("Start import? (y/N): ")
        
        if confirm.lower() != 'y':
            print("Import cancelled.")
            return
            
        parallel_writer = ParallelWriter(
            num_writers=writers,
            max_daily_keys=keys,
            skip_probability=0.05
        )
        
        # Get version to write
        version = input("Version to write (e.g. v22, v23): ").strip() or "v22"
        results = parallel_writer.run_parallel_import(version, target_keys=keys)
            
        print(f"\n✅ Custom import complete!")
        return results
        
    except (ValueError, KeyboardInterrupt):
        print("❌ Invalid input or cancelled")
        return None


def main():
    """Main menu"""
    print("🚀 Parallel Writer - Quick Start")
    print("=" * 35)
    #print("1. Quick Test (400 keys, 4 writers)")
    #print("2. High Performance (800 keys, 8 writers)")
    #print("3. Production 66M (single version, 20 writers)")
    #print("4. Custom Configuration")
    #print("5. Exit")
    
    try:
        #choice = input("\nSelect option (1-5): ").strip()
        choice = '3'

        if choice == '1':
            quick_test()
        elif choice == '2':
            high_performance_test()
        elif choice == '3':
            production()
        elif choice == '4':
            custom_import()
        elif choice == '5':
            print("👋 Goodbye!")
            sys.exit(0)
        else:
            print("❌ Invalid choice")
            
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
        sys.exit(0)


if __name__ == "__main__":
    main()
