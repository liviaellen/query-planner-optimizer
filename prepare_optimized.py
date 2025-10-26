#!/usr/bin/env python3
"""
Optimized Data Preparation Phase - Parallel & Memory-Efficient
---------------------------------------------------------------
Designed for MacBook M2 with 16GB RAM constraint.

Features:
- Parallel CSV processing using multiprocessing (6 workers)
- Streaming: Never loads entire dataset into memory
- Incremental partitioning with append mode
- Efficient aggregation from partition files
- ZSTD compression level 3 for balanced compression

Usage:
  python prepare_optimized.py --data-dir ./data/data-full --optimized-dir ./optimized_data_full
"""

import polars as pl
from pathlib import Path
import time
import shutil
from multiprocessing import Pool, cpu_count
from functools import partial
import os

# Enable global string cache for categorical columns
# This is required when concatenating DataFrames with categorical columns
pl.enable_string_cache()


def process_csv_with_worker_id(args):
    """Wrapper function for multiprocessing that unpacks arguments"""
    csv_file, worker_id, optimized_dir, schema = args
    return process_csv_file(csv_file, optimized_dir, schema, worker_id)


def process_csv_file(csv_file: Path, optimized_dir: Path, schema: dict, worker_id: int):
    """
    Process a single CSV file: load, transform, and partition.
    This function runs in a separate process.
    Writes to temporary worker-specific files to avoid race conditions.

    Returns: (file_name, row_count, processing_time)
    """
    start = time.time()

    temp_dir = optimized_dir / "temp" / f"worker_{worker_id}"
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Read CSV with schema
    df = pl.scan_csv(
        csv_file,
        schema=schema,
        null_values=["", "null"],
    ).collect()

    # Add derived columns
    df = df.with_columns([
        pl.from_epoch(pl.col("ts"), time_unit="ms").alias("ts_dt"),
    ]).with_columns([
        pl.col("ts_dt").dt.date().alias("day"),
        pl.col("ts_dt").dt.truncate("1w").cast(pl.Date).alias("week"),
        pl.col("ts_dt").dt.truncate("1h").alias("hour"),
        pl.col("ts_dt").dt.strftime("%Y-%m-%d %H:%M").alias("minute"),
    ])

    row_count = len(df)

    # Partition by type and day, write to temp directory
    for event_type in ["serve", "impression", "click", "purchase"]:
        type_df = df.filter(pl.col("type") == event_type)

        if len(type_df) == 0:
            continue

        type_dir = temp_dir / f"type={event_type}"
        type_dir.mkdir(exist_ok=True)

        # Group by day and write each partition
        days = type_df["day"].unique().sort().to_list()

        for day in days:
            day_df = type_df.filter(pl.col("day") == day)
            day_str = str(day)

            # Write to temp file with CSV file name to make it unique
            output_file = type_dir / f"day={day_str}_{csv_file.stem}.parquet"
            day_df.write_parquet(
                output_file,
                compression="zstd",
                compression_level=3,  # Balanced compression
                statistics=True,
            )

    elapsed = time.time() - start
    return (csv_file.name, row_count, elapsed)


def compute_aggregates_parallel(optimized_dir: Path):
    """
    Compute aggregates from partitioned files in parallel.
    Each aggregate computation can run independently.
    """
    partitioned_dir = optimized_dir / "partitioned"
    aggregates_dir = optimized_dir / "aggregates"
    aggregates_dir.mkdir(parents=True, exist_ok=True)

    print("\n⚡ Computing aggregates in parallel...")

    # Define aggregate computation functions
    def compute_daily_revenue():
        print("   [1/5] Daily revenue...")
        impression_files = sorted((partitioned_dir / "type=impression").glob("day=*.parquet"))

        dfs = []
        for f in impression_files:
            df = pl.scan_parquet(f).select(["day", "bid_price"])
            dfs.append(df)

        if dfs:
            result = (
                pl.concat(dfs)
                .group_by("day")
                .agg([
                    pl.col("bid_price").sum().alias("sum_bid_price"),
                    pl.col("bid_price").count().alias("count_impressions"),
                ])
                .sort("day")
                .collect()
            )
            result.write_parquet(
                aggregates_dir / "daily_revenue.parquet",
                compression="zstd",
            )
        return "daily_revenue"

    def compute_country_aggregates():
        print("   [2/5] Country aggregates...")

        # Country revenue
        impression_files = sorted((partitioned_dir / "type=impression").glob("day=*.parquet"))
        dfs = []
        for f in impression_files:
            df = pl.scan_parquet(f).select(["country", "bid_price"])
            dfs.append(df)

        if dfs:
            result = (
                pl.concat(dfs)
                .group_by("country")
                .agg([
                    pl.col("bid_price").sum().alias("sum_bid_price"),
                    pl.col("bid_price").count().alias("count_impressions"),
                ])
                .collect()
            )
            result.write_parquet(
                aggregates_dir / "country_revenue.parquet",
                compression="zstd",
            )

        # Country purchases
        purchase_files = sorted((partitioned_dir / "type=purchase").glob("day=*.parquet"))
        dfs = []
        for f in purchase_files:
            df = pl.scan_parquet(f).select(["country", "total_price"])
            dfs.append(df)

        if dfs:
            result = (
                pl.concat(dfs)
                .group_by("country")
                .agg([
                    pl.col("total_price").sum().alias("sum_total_price"),
                    pl.col("total_price").mean().alias("avg_total_price"),
                    pl.col("total_price").count().alias("count_purchases"),
                ])
                .collect()
            )
            result.write_parquet(
                aggregates_dir / "country_purchases.parquet",
                compression="zstd",
            )

        return "country_aggregates"

    def compute_publisher_day_revenue():
        print("   [3/5] Publisher-day revenue...")
        impression_files = sorted((partitioned_dir / "type=impression").glob("day=*.parquet"))

        dfs = []
        for f in impression_files:
            df = pl.scan_parquet(f).select(["publisher_id", "day", "country", "bid_price"])
            dfs.append(df)

        if dfs:
            result = (
                pl.concat(dfs)
                .group_by(["publisher_id", "day", "country"])
                .agg([
                    pl.col("bid_price").sum().alias("sum_bid_price"),
                ])
                .collect()
            )
            result.write_parquet(
                aggregates_dir / "publisher_day_country_revenue.parquet",
                compression="zstd",
            )

        return "publisher_day_revenue"

    def compute_advertiser_type_counts():
        print("   [4/5] Advertiser-type counts...")

        # Scan all event types
        dfs = []
        for event_type in ["serve", "impression", "click", "purchase"]:
            type_dir = partitioned_dir / f"type={event_type}"
            if not type_dir.exists():
                continue

            files = sorted(type_dir.glob("day=*.parquet"))
            for f in files:
                df = pl.scan_parquet(f).select(["advertiser_id", "type"])
                dfs.append(df)

        if dfs:
            result = (
                pl.concat(dfs)
                .group_by(["advertiser_id", "type"])
                .agg([
                    pl.len().alias("count"),
                ])
                .collect()
            )
            result.write_parquet(
                aggregates_dir / "advertiser_type_counts.parquet",
                compression="zstd",
            )

        return "advertiser_type_counts"

    def compute_minute_revenue():
        print("   [5/5] Minute-level revenue...")
        impression_files = sorted((partitioned_dir / "type=impression").glob("day=*.parquet"))

        dfs = []
        for f in impression_files:
            df = pl.scan_parquet(f).select(["day", "minute", "bid_price"])
            dfs.append(df)

        if dfs:
            result = (
                pl.concat(dfs)
                .group_by(["day", "minute"])
                .agg([
                    pl.col("bid_price").sum().alias("sum_bid_price"),
                ])
                .collect()
            )
            result.write_parquet(
                aggregates_dir / "minute_revenue.parquet",
                compression="zstd",
            )

        return "minute_revenue"

    # Run aggregates sequentially (they're already optimized with lazy evaluation)
    # Running in parallel could exceed memory limits
    compute_daily_revenue()
    compute_country_aggregates()
    compute_publisher_day_revenue()
    compute_advertiser_type_counts()
    compute_minute_revenue()

    print("   ✅ All aggregates computed")


def create_statistics(optimized_dir: Path):
    """Create statistics from partitioned data"""
    print("\n📈 Creating statistics...")

    partitioned_dir = optimized_dir / "partitioned"

    # Collect stats by scanning partition files (lazy)
    total_rows = 0
    type_counts = {}

    for event_type in ["serve", "impression", "click", "purchase"]:
        type_dir = partitioned_dir / f"type={event_type}"
        if not type_dir.exists():
            continue

        files = list(type_dir.glob("day=*.parquet"))
        type_count = 0

        for f in files:
            df = pl.scan_parquet(f).select(pl.len())
            type_count += df.collect()[0, 0]

        type_counts[event_type] = type_count
        total_rows += type_count

    # Get date range from impression partitions
    impression_dir = partitioned_dir / "type=impression"
    if impression_dir.exists():
        files = sorted(impression_dir.glob("day=*.parquet"))
        if files:
            min_day = files[0].stem.split("=")[1]
            max_day = files[-1].stem.split("=")[1]
        else:
            min_day = max_day = "unknown"
    else:
        min_day = max_day = "unknown"

    stats = {
        "total_rows": str(total_rows),
        "type_counts": str(type_counts),
        "date_range": f"{min_day} to {max_day}",
    }

    stats_df = pl.DataFrame({
        "key": list(stats.keys()),
        "value": list(stats.values()),
    })
    stats_df.write_parquet(
        optimized_dir / "stats.parquet",
        compression="zstd",
    )

    print(f"   Total rows: {total_rows:,}")
    print(f"   Date range: {min_day} to {max_day}")


class OptimizedDataPreparer:
    def __init__(self, data_dir: Path, optimized_dir: Path, num_workers: int = None):
        self.data_dir = Path(data_dir)
        self.optimized_dir = Path(optimized_dir)
        self.partitioned_dir = self.optimized_dir / "partitioned"

        # Determine optimal worker count
        if num_workers is None:
            # Use 75% of CPU cores, capped at 6 for memory safety on 16GB RAM
            num_workers = min(6, max(1, int(cpu_count() * 0.75)))
        self.num_workers = num_workers

    def _merge_temp_partitions(self):
        """Merge temporary partition files from all workers into final partitions"""
        temp_dir = self.optimized_dir / "temp"

        if not temp_dir.exists():
            return

        # Collect all temp partition files by type and day
        partition_files = {}  # (type, day) -> [file_paths]

        for worker_dir in temp_dir.glob("worker_*"):
            for type_dir in worker_dir.glob("type=*"):
                event_type = type_dir.name.split("=")[1]

                for parquet_file in type_dir.glob("day=*.parquet"):
                    # Extract day from filename (format: day=YYYY-MM-DD_events_part_*.parquet)
                    filename = parquet_file.stem
                    day_str = filename.split("_")[0].split("=")[1]  # Get YYYY-MM-DD

                    key = (event_type, day_str)
                    if key not in partition_files:
                        partition_files[key] = []
                    partition_files[key].append(parquet_file)

        # Merge files for each partition
        print(f"   Merging {len(partition_files)} unique partitions...")

        for (event_type, day_str), file_list in partition_files.items():
            # Create final partition directory
            type_dir = self.partitioned_dir / f"type={event_type}"
            type_dir.mkdir(parents=True, exist_ok=True)

            output_file = type_dir / f"day={day_str}.parquet"

            if len(file_list) == 1:
                # Only one file, just move it
                shutil.move(str(file_list[0]), str(output_file))
            else:
                # Multiple files, concatenate them efficiently with lazy loading
                dfs = [pl.scan_parquet(f) for f in file_list]
                combined = pl.concat(dfs).collect()
                combined.write_parquet(
                    output_file,
                    compression="zstd",
                    compression_level=3,  # Balanced compression
                    statistics=True,
                )

        # Clean up temp directory
        shutil.rmtree(temp_dir)
        print(f"   ✅ Merged successfully")

    def prepare(self):
        """Main preparation pipeline"""
        print("🚀 Starting optimized data preparation (parallel & streaming)")
        print(f"   Workers: {self.num_workers}")
        print(f"   CPU cores: {cpu_count()}")

        start_time = time.time()

        # Clean up existing optimized directory
        if self.optimized_dir.exists():
            print(f"\n🗑️  Cleaning existing directory...")
            shutil.rmtree(self.optimized_dir)

        # Create directories
        self.partitioned_dir.mkdir(parents=True, exist_ok=True)

        # Get CSV files
        csv_files = sorted(self.data_dir.glob("events_part_*.csv"))

        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {self.data_dir}")

        print(f"\n📊 Found {len(csv_files)} CSV files to process")

        # Define schema
        schema = {
            "ts": pl.Int64,
            "type": pl.Utf8,
            "auction_id": pl.Utf8,
            "advertiser_id": pl.Int32,
            "publisher_id": pl.Int32,
            "bid_price": pl.Float64,
            "user_id": pl.Int64,
            "total_price": pl.Float64,
            "country": pl.Utf8,
        }

        # Step 1: Process CSV files in parallel
        print(f"\n🔄 Processing CSV files with {self.num_workers} workers...")

        # Assign each CSV file to a worker ID and pack arguments
        csv_with_workers = [
            (csv_file, i % self.num_workers, self.optimized_dir, schema)
            for i, csv_file in enumerate(csv_files)
        ]

        with Pool(processes=self.num_workers) as pool:
            results = pool.map(process_csv_with_worker_id, csv_with_workers)

        total_rows = sum(r[1] for r in results)
        avg_time = sum(r[2] for r in results) / len(results)

        print(f"   ✅ Processed {len(csv_files)} files")
        print(f"   Total rows: {total_rows:,}")
        print(f"   Avg processing time per file: {avg_time:.2f}s")

        # Step 1.5: Merge temporary partition files
        print(f"\n🔗 Merging partition files...")
        self._merge_temp_partitions()

        # Step 2: Compute aggregates
        compute_aggregates_parallel(self.optimized_dir)

        # Step 3: Create statistics
        create_statistics(self.optimized_dir)

        elapsed = time.time() - start_time
        print(f"\n✅ Preparation complete in {elapsed:.2f}s ({elapsed/60:.2f} minutes)")
        print(f"📁 Optimized data stored in: {self.optimized_dir}")

        # Show disk usage
        import subprocess
        try:
            result = subprocess.run(
                ["du", "-sh", str(self.optimized_dir)],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                size = result.stdout.split()[0]
                print(f"💾 Total size: {size}")
        except:
            pass


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Optimized data preparation with parallel processing"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Input directory containing CSV files"
    )
    parser.add_argument(
        "--optimized-dir",
        type=Path,
        default=Path("optimized_data"),
        help="Output directory for optimized data"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel workers (default: auto-detect, max 6)"
    )

    args = parser.parse_args()

    preparer = OptimizedDataPreparer(args.data_dir, args.optimized_dir, args.workers)
    preparer.prepare()


if __name__ == "__main__":
    main()
