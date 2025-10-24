# LocationFlex Redis Benchmark - Setup & Reproduction Guide

## Prerequisites

- Python 3.8+
- Redis Cloud account with 400GB+ database with Eviction enabled
- 2-3 c7i.2xl instances (or similar) in the same region as Redis Cloud database
- VPC peering configured between instances and Redis Cloud subscription

---

## Step 1: Setup

### 1.1 Install Dependencies

```bash
python -m venv .
source ./bin/activate
pip install -r requirements.txt
```

### 1.2 Configure Redis Connection

Edit `config.json` and add your Redis Cloud credentials:

```json
{
  "redis": {
    "host": "your-redis-cloud-hostname.cloud.rlrcp.com",
    "port": 11893,
    "password": "your-redis-password"
  }
}
```

---

## Step 2: Initialize Dataset (60M keys per version)

### 2.1 Edit start_parallel.py

Edit lines 62-64 to configure the import:

```python
version = "v22"           # Version label (can be any string)
num_writers = 30          # Number of parallel writers
max_daily_keys = 60000000 # 60M keys per version
```

### 2.2 Run Imports

**Import v21 (first):**
```bash
python start_parallel.py
# Expected: ~3,000 keys/sec with 30 writers
# Duration: ~20 minutes for 60M keys
```

**Import v22 (second, in separate terminal):**
```bash
# Edit start_parallel.py line 62: version = "v22"
python start_parallel.py
# Expected: ~3,000 keys/sec with 30 writers
# Duration: ~20 minutes for 60M keys
```

**Import v23 :**
```bash
# Edit start_parallel.py line 62: version = "v23"
python start_parallel.py
# Run this AFTER v22 completes
# This fills the 400GB DB and triggers evictions
# Running separately ensures v23 keys stay in RAM
```

**Expected Results:**
- 180M keys, > 200 GB data
- DB fills to 400GB with evictions

---

## Step 3: Run Benchmark (Read + Write)

### 3.1 Terminal 1: Start Writing v25

```bash
# Edit start_parallel.py line 62: version = "v25"
python start_parallel.py
```

### 3.2 Terminal 2: Start Reading v23 (with v22 fallback)

```bash
python -c "
from simple_reader import SimpleReader
reader = SimpleReader(max_daily_keys=60000000, primary_version='v23', fallback_version='v22')
stats = reader.run_multithreaded_pipeline_benchmark(total_reads=2000000, num_threads=16, batch_size=10)
print(f'2M reads: {stats.total_reads/stats.total_time:.0f} reads/sec')
print(f'Duration: {stats.total_time/60:.1f} minutes')
"
```

---

## Tuning Parameters

### Reader Configuration

```python
# Adjust these parameters based on your setup:
total_reads=2000000    # Total reads (increase for longer tests)
num_threads=16         # Parallel threads (match CPU cores)
batch_size=10          # Keys per pipeline batch
```

---

## Monitoring

Monitor in Redis Cloud console:
- **Throughput:** Ops/sec
- **Latency:** 
- **Memory:** Current usage vs. max

---

## Troubleshooting

### Connection Issues
```
Error: Failed to connect to Redis
→ Check config.json credentials
→ Verify VPC peering is configured
→ Check security groups allow port 11893
```

---
