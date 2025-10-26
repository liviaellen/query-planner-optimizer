#!/usr/bin/env python3
"""
Optimized Data Preparation Phase
---------------------------------
Converts CSV data to optimized columnar format with:
- Parquet storage for compression and column pruning
- Partitioning by type and day for efficient filtering
- Pre-computed aggregations for common query patterns
- Statistics and indexes for query optimization
"""

import polars as pl
from pathlib import Path
import time
from datetime import datetime
import shutil

# Enable global string cache for categorical columns
# This is required when concatenating DataFrames with categorical columns
pl.enable_string_cache()


class DataPreparer:
    def __init__(self, data_dir: Path, optimized_dir: Path):
        self.data_dir = Path(data_dir)
        self.optimized_dir = Path(optimized_dir)
        self.partitioned_dir = self.optimized_dir / "partitioned"
        self.aggregates_dir = self.optimized_dir / "aggregates"

    def prepare(self):
        """Main preparation pipeline"""
        print("🚀 Starting optimized data preparation...")
        start_time = time.time()

        # Clean up existing optimized directory
        if self.optimized_dir.exists():
            shutil.rmtree(self.optimized_dir)

        # Create directories
        self.partitioned_dir.mkdir(parents=True, exist_ok=True)
        self.aggregates_dir.mkdir(parents=True, exist_ok=True)

        # Step 1: Load and transform data
        print("\n📊 Step 1: Loading and transforming data...")
        df = self._load_and_transform()

        # Step 2: Create partitioned storage
        print("\n🗂️  Step 2: Creating partitioned storage...")
        self._create_partitions(df)

        # Step 3: Pre-compute common aggregations
        print("\n⚡ Step 3: Pre-computing aggregations...")
        self._create_aggregations(df)

        # Step 4: Create statistics
        print("\n📈 Step 4: Creating statistics...")
        self._create_statistics(df)

        elapsed = time.time() - start_time
        print(f"\n✅ Preparation complete in {elapsed:.2f}s")
        print(f"📁 Optimized data stored in: {self.optimized_dir}")

    def _load_and_transform(self):
        """Load CSV files and add derived columns"""
        csv_files = sorted(self.data_dir.glob("events_part_*.csv"))

        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {self.data_dir}")

        print(f"   Loading {len(csv_files)} CSV files...")

        # Define schema for proper type conversion
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

        # Load all CSV files with lazy evaluation
        dfs = []
        for csv_file in csv_files:
            df = pl.scan_csv(
                csv_file,
                schema=schema,
                null_values=["", "null"],
            )
            dfs.append(df)

        # Concatenate all dataframes
        df = pl.concat(dfs)

        # Add derived columns using lazy transformations
        df = df.with_columns([
            # Convert timestamp from milliseconds to datetime
            # Use from_epoch to convert milliseconds since epoch to datetime
            pl.from_epoch(pl.col("ts"), time_unit="ms").alias("ts_dt"),
        ]).with_columns([
            # Extract date components
            pl.col("ts_dt").dt.date().alias("day"),
            pl.col("ts_dt").dt.truncate("1w").cast(pl.Date).alias("week"),
            pl.col("ts_dt").dt.truncate("1h").alias("hour"),
            pl.col("ts_dt").dt.strftime("%Y-%m-%d %H:%M").alias("minute"),
        ])

        # Collect to memory (execute lazy operations)
        print("   Executing transformations...")
        df = df.collect()

        print(f"   Loaded {len(df):,} rows")
        return df

    def _create_partitions(self, df: pl.DataFrame):
        """Create partitioned Parquet files by type and day"""
        print("   Partitioning by type and day...")

        # Group by type and day, then save each partition
        types = df["type"].unique().to_list()

        for event_type in types:
            type_df = df.filter(pl.col("type") == event_type)
            type_dir = self.partitioned_dir / f"type={event_type}"
            type_dir.mkdir(exist_ok=True)

            # Further partition by day
            days = type_df["day"].unique().sort().to_list()

            for day in days:
                day_df = type_df.filter(pl.col("day") == day)
                day_str = str(day)

                # Save as Parquet with optimal compression
                output_file = type_dir / f"day={day_str}.parquet"
                day_df.write_parquet(
                    output_file,
                    compression="zstd",  # Excellent compression and speed
                    compression_level=3,  # Balanced compression
                    statistics=True,      # Enable statistics for query optimization
                )

            print(f"      {event_type}: {len(days)} days partitioned")

    def _create_aggregations(self, df: pl.DataFrame):
        """Pre-compute common aggregations"""

        # Aggregation 1: Daily revenue (most common query pattern)
        print("   Creating daily revenue aggregates...")
        daily_revenue = (
            df
            .filter(pl.col("type") == "impression")
            .group_by("day")
            .agg([
                pl.col("bid_price").sum().alias("sum_bid_price"),
                pl.col("bid_price").count().alias("count_impressions"),
            ])
            .sort("day")
        )
        daily_revenue.write_parquet(
            self.aggregates_dir / "daily_revenue.parquet",
            compression="zstd",
        )

        # Aggregation 2: Country aggregates
        print("   Creating country aggregates...")

        # Revenue by country
        country_revenue = (
            df
            .filter(pl.col("type") == "impression")
            .group_by("country")
            .agg([
                pl.col("bid_price").sum().alias("sum_bid_price"),
                pl.col("bid_price").count().alias("count_impressions"),
            ])
        )
        country_revenue.write_parquet(
            self.aggregates_dir / "country_revenue.parquet",
            compression="zstd",
        )

        # Purchases by country
        country_purchases = (
            df
            .filter(pl.col("type") == "purchase")
            .group_by("country")
            .agg([
                pl.col("total_price").sum().alias("sum_total_price"),
                pl.col("total_price").mean().alias("avg_total_price"),
                pl.col("total_price").count().alias("count_purchases"),
            ])
        )
        country_purchases.write_parquet(
            self.aggregates_dir / "country_purchases.parquet",
            compression="zstd",
        )

        # Aggregation 3: Publisher revenue by day
        print("   Creating publisher-day aggregates...")
        publisher_day_revenue = (
            df
            .filter(pl.col("type") == "impression")
            .group_by(["publisher_id", "day", "country"])
            .agg([
                pl.col("bid_price").sum().alias("sum_bid_price"),
            ])
        )
        publisher_day_revenue.write_parquet(
            self.aggregates_dir / "publisher_day_country_revenue.parquet",
            compression="zstd",
        )

        # Aggregation 4: Advertiser-type counts
        print("   Creating advertiser-type aggregates...")
        advertiser_type_counts = (
            df
            .group_by(["advertiser_id", "type"])
            .agg([
                pl.len().alias("count"),
            ])
        )
        advertiser_type_counts.write_parquet(
            self.aggregates_dir / "advertiser_type_counts.parquet",
            compression="zstd",
        )

        # Aggregation 5: Minute-level aggregates by day (for minute queries)
        print("   Creating minute-level aggregates...")
        minute_revenue = (
            df
            .filter(pl.col("type") == "impression")
            .group_by(["day", "minute"])
            .agg([
                pl.col("bid_price").sum().alias("sum_bid_price"),
            ])
        )
        minute_revenue.write_parquet(
            self.aggregates_dir / "minute_revenue.parquet",
            compression="zstd",
        )

    def _create_statistics(self, df: pl.DataFrame):
        """Create statistics file for query optimization"""
        stats = {
            "total_rows": len(df),
            "total_size_mb": df.estimated_size() / (1024 * 1024),
            "types": df["type"].value_counts().sort("type").to_dicts(),
            "date_range": {
                "min": str(df["day"].min()),
                "max": str(df["day"].max()),
            },
            "countries": df["country"].n_unique(),
            "advertisers": df["advertiser_id"].n_unique(),
            "publishers": df["publisher_id"].n_unique(),
        }

        # Save statistics as Parquet for fast loading
        stats_df = pl.DataFrame({
            "key": list(stats.keys()),
            "value": [str(v) for v in stats.values()],
        })
        stats_df.write_parquet(
            self.optimized_dir / "stats.parquet",
            compression="zstd",
        )

        print(f"   Total rows: {stats['total_rows']:,}")
        print(f"   Date range: {stats['date_range']['min']} to {stats['date_range']['max']}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Prepare and optimize data for high-performance queries"
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

    args = parser.parse_args()

    preparer = DataPreparer(args.data_dir, args.optimized_dir)
    preparer.prepare()


if __name__ == "__main__":
    main()
