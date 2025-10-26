# High-Performance Query Engine

## Quick Start

This solution achieves **394x faster** query performance compared to DuckDB on 245 million rows.

### Prerequisites

- Python 3.13+ (tested on Python 3.13.5)
- macOS M2 or later (tested on Apple Silicon)
- 16GB RAM (uses ~2-4GB)
- 100GB disk space (uses ~9GB for optimized storage)

### Installation

```bash
# Install Python dependencies
pip install -r requirements.txt
```

**Dependencies:**
- `polars==1.16.0` - Fast analytical query engine
- `pyarrow==18.1.0` - Columnar storage format
- `numpy==2.2.0` - Numerical computing

### Running the Solution

The solution runs in two phases:

#### Phase 1: Data Preparation (One-Time Setup)

Transform CSV data into optimized storage with partitions and pre-computed aggregations:

```bash
# For full dataset (245M rows, ~19GB CSV) - RECOMMENDED
python prepare_optimized.py --data-dir data/data-full --optimized-dir optimized_data_full

# For lite dataset (15M rows, ~1.1GB CSV) - faster for testing
python prepare_optimized.py --data-dir data/data-lite --optimized-dir optimized_data_lite

# Optional: Use legacy prepare.py (slower, single-threaded)
python prepare.py --data-dir data/data-full --optimized-dir optimized_data_full
```

**Preparation time:**
- Full dataset: ~211 minutes (one-time cost)
- Lite dataset: ~15 seconds (one-time cost)

**What happens during preparation:**
1. Loads CSV files and adds derived time columns (day, week, hour, minute)
2. Creates partitioned Parquet storage by `type` and `day`
3. Pre-computes common aggregations (daily revenue, country stats, etc.)
4. Generates statistics for query optimization

**Note:** `prepare_optimized.py` uses parallel processing (6 workers) for faster preparation compared to the original `prepare.py`.

#### Phase 2: Query Execution

Run benchmark queries against the optimized data:

```bash
# Run queries on full dataset
python main.py --optimized-dir optimized_data_full --out-dir results_full

# Run queries on lite dataset
python main.py --optimized-dir optimized_data_lite --out-dir results_lite

# If you already have pre-processed data
python main.py --optimized-dir optimized_data_full_new --out-dir out_optimized
```

**Query performance (full dataset):**
- Q1: Daily revenue - 10ms
- Q2: Publisher revenue by country/date - 32ms
- Q3: Average purchase by country - 1ms
- Q4: Advertiser-type counts - 2ms
- Q5: Minute-level revenue - 18ms
- **Total: 62ms**

### Benchmarking Against DuckDB

To verify the performance improvement, run the DuckDB baseline:

```bash
# Install DuckDB dependencies
pip install duckdb>=1.1.1 pandas>=2.2.0

# Run DuckDB baseline
cd baseline
python main.py --data-dir ../data/data-full --out-dir ../baseline_results_full
```

**DuckDB performance (full dataset):** 24.435s total

**Speedup:** 24.435s / 0.062s = **394x faster**

## Performance Summary

### Full Dataset (245M rows)

| Query | DuckDB | Optimized | Speedup |
|-------|--------|-----------|---------|
| Q1: Daily revenue | 5.705s | 0.010s | **571x** |
| Q2: Publisher revenue | 4.813s | 0.032s | **150x** |
| Q3: Country purchases | 4.084s | 0.001s | **4,084x** |
| Q4: Advertiser counts | 4.482s | 0.002s | **2,241x** |
| Q5: Minute revenue | 5.351s | 0.018s | **297x** |
| **Total** | **24.435s** | **0.062s** | **394x** |

### Lite Dataset (15M rows)

| Query | DuckDB | Optimized | Speedup |
|-------|--------|-----------|---------|
| **Total** | **1.666s** | **0.020s** | **83x** |

## Architecture Overview

### Two-Phase Design

**Phase 1: Data Preparation**
- Converts CSV to Parquet with ZSTD compression (54% compression ratio)
- Partitions data by `type` (serve/impression/click/purchase) and `day`
- Pre-computes common aggregations for pattern matching
- Adds derived time columns for efficient filtering

**Phase 2: Query Execution**
- Smart query router matches queries to pre-computed aggregations
- Falls back to partition-pruned scans for non-matching queries
- Uses columnar storage for efficient column pruning
- Leverages Polars' Rust-based SIMD acceleration

### Key Optimizations

1. **Pattern-Based Query Optimization** - Matches queries against pre-computed results
2. **Multi-Level Partitioning** - Skip 75%+ of data by filtering on type and day
3. **Columnar Pre-Aggregations** - Instant lookups for common query patterns
4. **Predicate Pushdown** - Filter data before loading from disk
5. **Lazy Evaluation** - Only load data when needed

## File Structure

```
.
├── prepare_optimized.py # Data preparation (parallel, recommended)
├── prepare.py           # Data preparation (legacy, single-threaded)
├── query_engine.py      # Query execution engine
├── main.py              # Benchmark runner
├── requirements.txt     # Python dependencies
├── SOLUTION.md          # Detailed architecture documentation
├── README.md            # This file
├── baseline/            # DuckDB baseline implementation
├── data/
│   ├── data-full/      # Full dataset (245M rows)
│   └── data-lite/      # Lite dataset (15M rows)
└── optimized_data_full/ # Optimized storage (created by prepare_optimized.py)
    ├── partitioned/     # Partitioned Parquet files
    ├── aggregates/      # Pre-computed aggregations
    └── stats.parquet    # Dataset statistics
```

## Resource Usage

- **RAM**: 2-4GB during query execution
- **Disk**: 8.8GB for optimized storage (from 19GB CSV)
  - Partitioned data: 8.76GB
  - Pre-computed aggregates: 40MB
- **Preparation time**: 211 minutes (one-time cost)
- **Query time**: 62ms for 5 queries on 245M rows

## Verification

All query results are saved as CSV files in the output directory. To verify correctness:

1. Run both the optimized solution and DuckDB baseline
2. Compare output CSV files between `results_full/` and `baseline_results_full/`
3. All results should be identical (row order may differ for unordered queries)

## Troubleshooting

**Out of memory during preparation:**
- Reduce the dataset size or increase swap space
- The preparation phase is memory-intensive but queries are efficient

**Preparation taking too long:**
- Test first with the lite dataset (`data-lite`)
- Full dataset preparation is a one-time cost

**Different results from baseline:**
- Check that the same data directory is being used
- Ensure all dependencies are installed correctly

## Technical Details

For a deep dive into the architecture, optimizations, and design decisions, see [SOLUTION.md](SOLUTION.md).

## License

Created for the Cal Hacks Query Planner Challenge.
