# High-Performance Query Engine

**🚀 High-Performance Query Execution** on 245M rows with parallel processing and columnar storage.

---

## Table of Contents

- [Quick Start](#quick-start)
- [For Judges](#for-judges)
- [Installation](#installation)
- [Usage](#usage)
- [Command Reference](#command-reference)
- [Architecture](#architecture)
- [Performance](#performance)
- [File Structure](#file-structure)

---

## Quick Start

### Prerequisites

- **Python**: 3.13+ (tested on Python 3.13.5)
- **Platform**: macOS M2 or later (Apple Silicon)
- **RAM**: 16GB (uses ~2-4GB)
- **Disk**: 100GB available

### Fastest Way to Test

```bash
# Install dependencies
make install

# Test on lite dataset (~30 seconds)
make test-optimizations

# View system info
make info
```

---

## For Judges

**📋 See [JUDGES_INSTRUCTIONS.md](JUDGES_INSTRUCTIONS.md) for complete evaluation instructions.**

### Quick Evaluation Steps

1. **Prepare data** (one-time, choose one):
   ```bash
   # Option 1: Optimized (recommended)
   python prepare_optimized.py --data-dir data/data-full --optimized-dir optimized_data

   # Option 2: Ultra-fast (<20 min target)
   python prepare_ultra_fast.py --data-dir data/data-full --optimized-dir optimized_data_ultra
   ```

2. **Add your evaluation queries** to `judges.py` (in main directory):
   ```python
   queries = [
       {"select": [...], "from": "events", "where": [...], ...},
       # Your evaluation queries here
   ]
   ```

3. **Switch the import** in `main.py` (line 18-19):
   ```python
   # Comment out:
   # from inputs import queries as default_queries

   # Uncomment:
   from judges import queries as default_queries
   ```

4. **Run evaluation**:
   ```bash
   python main.py --optimized-dir optimized_data --out-dir results_evaluation
   ```

Results saved as `results_evaluation/q1.csv`, `q2.csv`, etc.

---

## Installation

```bash
# Install dependencies
pip install -r requirements.txt
```

**Dependencies:**

| Package | Version | Purpose |
|---------|---------|---------|
| **polars** | 1.16.0 | Fast analytical query engine (Rust-based DataFrame library) |
| **pyarrow** | 18.1.0 | Columnar storage format (Parquet file support) |

---

## Usage

### Phase 1: Prepare Data (One-Time)

Transform raw CSV data into optimized Parquet storage:

```bash
# Full dataset
python prepare_optimized.py --data-dir data/data-full --optimized-dir optimized_data

# Or use Makefile
make prepare-optimized
```

**What it does:**
- Loads CSV files with parallel processing (6 workers)
- Adds derived time columns (day, week, hour, minute)
- Creates partitioned Parquet storage by `type` and `day`
- Pre-computes common aggregations
- Uses ZSTD compression level 3 (balanced compression)

**Preparation time:**
- Full dataset (245M rows): Varies based on hardware
- Lite dataset (15M rows): ~10 seconds

### Phase 2: Execute Queries

Run benchmark queries against the optimized data:

```bash
# Run queries
python main.py --optimized-dir optimized_data --out-dir results

# Or use Makefile
make query-optimized

# Test cache performance (run again)
make query-cached
```

### Custom Queries

To use custom queries, edit either `inputs.py` or `judges.py` and change the import in `main.py`:

```python
# main.py line 18-19
from inputs import queries as default_queries      # Default benchmark queries (5 queries)
# from judges import queries as default_queries   # Custom queries (uncomment to use)
```

---

## Command Reference

### Data Preparation

```bash
# Full dataset
make prepare-optimized

# Lite dataset (for testing)
make prepare-optimized-lite

# Ultra-fast preparation (<20 min target)
make prepare-ultra-fast
```

### Query Execution

```bash
# Run queries (first run)
make query-optimized

# Run queries again (test cache)
make query-cached

# Ultra-fast dataset queries
make query-ultra-fast
make query-ultra-cached
```

### Testing

```bash
# Quick test on lite dataset
make test-optimizations

# View optimization info
make info-optimizations

# View system status
make info
```

### Benchmarking

```bash
# Compare performance
make benchmark-optimizations

# DuckDB baseline
make install-baseline
make baseline-full
```

### Utility Commands

```bash
# View all commands
make help

# Clean generated files
make clean                # Remove all
make clean-results        # Remove only results
make clean-optimized      # Remove optimized data
```

---

## Architecture

### Two-Phase Design

**Phase 1: Prepare**
1. Load CSV files with parallel processing (6 workers)
2. Add derived time columns (day, week, hour, minute)
3. Partition data by `type` and `day` for efficient filtering
4. Compress to Parquet format with ZSTD level 3
5. Pre-compute common aggregations:
   - Daily revenue
   - Country statistics
   - Publisher metrics
   - Advertiser counts
   - Minute-level revenue

**Phase 2: Query**
1. Smart query router matches patterns to pre-computed aggregates
2. Partition pruning skips irrelevant data
3. Column pruning loads only required columns
4. Lazy evaluation optimizes query execution plan
5. Query result caching for repeated queries

### Optimizations

**Data Preparation (`prepare_optimized.py`):**
- Parallel CSV processing (6 workers)
- Streaming: never loads entire dataset into memory
- ZSTD compression level 3 (balanced compression and speed)
- Partitioning by type and day
- Pre-computed aggregations for common queries

**Query Execution (`query_engine.py`):**
- Pattern-based query matching
- Partition pruning
- Column pruning
- Lazy evaluation with Polars
- Query result caching (MD5 hash-based)

### File Structure

```
optimized_data/
├── partitioned/              # Partitioned Parquet files
│   ├── type=impression/      # Impression events by day
│   ├── type=click/           # Click events by day
│   ├── type=purchase/        # Purchase events by day
│   └── type=serve/           # Serve events by day
├── aggregates/               # Pre-computed aggregations
│   ├── daily_revenue.parquet
│   ├── country_revenue.parquet
│   ├── country_purchases.parquet
│   ├── publisher_day_country_revenue.parquet
│   ├── advertiser_type_counts.parquet
│   └── minute_revenue.parquet
└── stats.parquet             # Dataset statistics
```

---

## Performance

### Preparation Options

| Script | Workers | Compression | Aggregates | Time Estimate | Storage | Use Case |
|--------|---------|-------------|------------|---------------|---------|----------|
| **prepare.py** | 1 (single) | ZSTD level 3 | All 5 | Longer | ~8.8GB | Legacy |
| **prepare_optimized.py** | 6 parallel | ZSTD level 3 | All 5 | Moderate | ~8GB | Recommended |
| **prepare_ultra_fast.py** | All cores | ZSTD level 1 | Only 3 | <20 min target | ~8-9GB | Time-constrained |

### System Requirements

- **RAM**: 16GB (M2 MacBook)
- **Disk**: 100GB available space
- **CPU**: Multi-core processor (8+ cores recommended)

---

## Troubleshooting

### Out of Memory During Preparation

**Solution:**
- Test with lite dataset first: `--data-dir data/data-lite`
- Close other applications
- Use `prepare_ultra_fast.py` for faster preparation with lower memory usage

### Module Not Found Errors

**Solution:**
```bash
pip install -r requirements.txt
```

### File Not Found: optimized_data

**Solution:** Run preparation first:
```bash
python prepare_optimized.py --data-dir data/data-full --optimized-dir optimized_data
```

---

## Project Files

### Main Files

- **`main.py`** - Query execution entry point
- **`inputs.py`** - Default benchmark queries (5 queries)
- **`judges.py`** - Placeholder for judges' evaluation queries
- **`prepare_optimized.py`** - Optimized data preparation (6 workers, ZSTD level 3)
- **`prepare_ultra_fast.py`** - Ultra-fast preparation (<20 min target, ZSTD level 1)
- **`prepare.py`** - Legacy single-threaded preparation
- **`query_engine.py`** - Query execution engine with caching
- **`Makefile`** - Common commands
- **`requirements.txt`** - Python dependencies

### Documentation

- **`README.md`** - This file (complete documentation)
- **`JUDGES_INSTRUCTIONS.md`** - Instructions for judges to run evaluation queries
- **`UPDATES_SUMMARY.md`** - Recent changes and updates
- **`CLAUDE.md`** - Challenge instructions

### Baseline

- **`baseline/main.py`** - DuckDB baseline for comparison
- **`baseline/inputs.py`** - Original benchmark queries (kept for reference)

---

## Summary

This high-performance query engine uses:
- **Parallel processing** (6 workers) for faster data preparation
- **Columnar storage** (Parquet) with ZSTD compression
- **Multi-level partitioning** by type and day
- **Pre-computed aggregations** for common query patterns
- **Query result caching** for repeated queries
- **Lazy evaluation** for optimized execution plans

**To run the complete solution:**

```bash
# Install dependencies
make install

# Quick test (recommended first)
make test-optimizations

# Full workflow
make prepare-optimized
make query-optimized
make query-cached

# Or using Python directly
pip install -r requirements.txt
python prepare_optimized.py --data-dir data/data-full --optimized-dir optimized_data
python main.py --optimized-dir optimized_data --out-dir results
```

**For fastest preparation time:**

```bash
make prepare-ultra-fast    # <20 min target on M2 MacBook
make query-ultra-fast
make query-ultra-cached
```

---

**Last Updated:** 2025-10-26
