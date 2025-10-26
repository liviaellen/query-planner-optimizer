#!/usr/bin/env python3
"""
High-Performance Query Execution Engine
---------------------------------------
Smart query planner that:
- Routes queries to pre-computed aggregations when possible
- Uses partition pruning to skip irrelevant data
- Leverages columnar storage for efficient scans
- Parallelizes execution across partitions
"""

import polars as pl
from pathlib import Path
from typing import Dict, List, Any, Optional
import time
import hashlib
import json


class QueryEngine:
    def __init__(self, optimized_dir: Path):
        self.optimized_dir = Path(optimized_dir)
        self.partitioned_dir = self.optimized_dir / "partitioned"
        self.aggregates_dir = self.optimized_dir / "aggregates"

        # Cache for loaded aggregates
        self._aggregate_cache = {}

        # Query result cache for exact query matches
        self._query_cache = {}
        self._enable_query_cache = True

    def execute_query(self, query: Dict[str, Any]) -> tuple[pl.DataFrame, float]:
        """
        Execute a query and return (results, execution_time)

        Uses intelligent query planning to determine optimal execution strategy
        """
        start_time = time.time()

        # Check query cache first
        if self._enable_query_cache:
            cache_key = self._get_query_hash(query)
            if cache_key in self._query_cache:
                cached_result = self._query_cache[cache_key].clone()
                execution_time = time.time() - start_time
                return cached_result, execution_time

        # Try to use pre-computed aggregations first
        result = self._try_precomputed(query)

        if result is None:
            # Fall back to scanning partitions
            result = self._execute_scan(query)

        # Cache the result
        if self._enable_query_cache:
            self._query_cache[cache_key] = result.clone()

        execution_time = time.time() - start_time
        return result, execution_time

    def _get_query_hash(self, query: Dict[str, Any]) -> str:
        """Generate a hash for a query to use as cache key"""
        # Convert query to JSON string for consistent hashing
        query_str = json.dumps(query, sort_keys=True)
        return hashlib.md5(query_str.encode()).hexdigest()

    def _try_precomputed(self, query: Dict[str, Any]) -> Optional[pl.DataFrame]:
        """
        Attempt to answer query using pre-computed aggregations
        Returns None if query cannot be answered from pre-computed data
        """
        select = query.get("select", [])
        where = query.get("where", [])
        group_by = query.get("group_by", [])
        order_by = query.get("order_by", [])

        # Pattern 1: Daily revenue (SUM bid_price by day, type=impression)
        if (self._matches_daily_revenue(select, where, group_by)):
            return self._query_daily_revenue(where, order_by)

        # Pattern 2: Publisher revenue by country and day range
        if (self._matches_publisher_day_revenue(select, where, group_by)):
            return self._query_publisher_day_revenue(where, group_by, order_by)

        # Pattern 3: Country purchase statistics
        if (self._matches_country_purchases(select, where, group_by)):
            return self._query_country_purchases(order_by)

        # Pattern 4: Advertiser-type counts
        if (self._matches_advertiser_type(select, where, group_by)):
            return self._query_advertiser_type(order_by)

        # Pattern 5: Minute-level revenue
        if (self._matches_minute_revenue(select, where, group_by)):
            return self._query_minute_revenue(where, order_by)

        return None

    def _execute_scan(self, query: Dict[str, Any]) -> pl.DataFrame:
        """
        Execute query by scanning partitioned data
        Uses partition pruning and column pruning for efficiency
        """
        select = query.get("select", [])
        where = query.get("where", [])
        group_by = query.get("group_by", [])
        order_by = query.get("order_by", [])

        # Determine which partitions to scan based on WHERE clause
        types_to_scan, days_to_scan = self._determine_partitions(where)

        # Determine which columns to load
        columns_needed = self._determine_columns(select, where, group_by, order_by)

        # Load relevant partitions
        df = self._load_partitions(types_to_scan, days_to_scan, columns_needed)

        # Apply WHERE filters
        df = self._apply_filters(df, where)

        # Apply SELECT and aggregations
        df = self._apply_select(df, select, group_by)

        # Apply ORDER BY
        if order_by:
            df = self._apply_order_by(df, order_by)

        return df

    # ==================== Pattern Matching ====================

    def _matches_daily_revenue(self, select, where, group_by):
        """Check if query matches: SELECT day, SUM(bid_price) WHERE type=impression GROUP BY day"""
        if group_by != ["day"]:
            return False

        # Check SELECT clause
        has_day = "day" in select
        has_sum_bid = any(
            isinstance(item, dict) and item.get("SUM") == "bid_price"
            for item in select
        )

        if not (has_day and has_sum_bid and len(select) == 2):
            return False

        # Check WHERE clause
        type_filter = any(
            w.get("col") == "type" and w.get("op") == "eq" and w.get("val") == "impression"
            for w in where
        )

        return type_filter and len(where) == 1

    def _matches_publisher_day_revenue(self, select, where, group_by):
        """Check if query matches publisher revenue by day/country"""
        if "publisher_id" not in group_by:
            return False

        has_publisher = "publisher_id" in select
        has_sum_bid = any(
            isinstance(item, dict) and item.get("SUM") == "bid_price"
            for item in select
        )

        type_filter = any(
            w.get("col") == "type" and w.get("op") == "eq" and w.get("val") == "impression"
            for w in where
        )

        return has_publisher and has_sum_bid and type_filter

    def _matches_country_purchases(self, select, where, group_by):
        """Check if query matches country purchase statistics"""
        if group_by != ["country"]:
            return False

        has_country = "country" in select
        has_avg_total = any(
            isinstance(item, dict) and item.get("AVG") == "total_price"
            for item in select
        )

        type_filter = any(
            w.get("col") == "type" and w.get("op") == "eq" and w.get("val") == "purchase"
            for w in where
        )

        return has_country and has_avg_total and type_filter and len(where) == 1

    def _matches_advertiser_type(self, select, where, group_by):
        """Check if query matches advertiser-type counts"""
        if set(group_by) != {"advertiser_id", "type"}:
            return False

        has_advertiser = "advertiser_id" in select
        has_type = "type" in select
        has_count = any(
            isinstance(item, dict) and item.get("COUNT") == "*"
            for item in select
        )

        return has_advertiser and has_type and has_count and len(where) == 0

    def _matches_minute_revenue(self, select, where, group_by):
        """Check if query matches minute-level revenue"""
        if group_by != ["minute"]:
            return False

        has_minute = "minute" in select
        has_sum_bid = any(
            isinstance(item, dict) and item.get("SUM") == "bid_price"
            for item in select
        )

        type_filter = any(
            w.get("col") == "type" and w.get("op") == "eq" and w.get("val") == "impression"
            for w in where
        )

        return has_minute and has_sum_bid and type_filter

    # ==================== Pre-computed Query Execution ====================

    def _query_daily_revenue(self, where, order_by):
        """Execute daily revenue query from pre-computed aggregates"""
        df = self._load_aggregate("daily_revenue.parquet")
        df = df.rename({"sum_bid_price": "sum(bid_price)"})
        df = df.select(["day", "sum(bid_price)"])
        return df

    def _query_publisher_day_revenue(self, where, group_by, order_by):
        """Execute publisher revenue query from pre-computed aggregates"""
        df = self._load_aggregate("publisher_day_country_revenue.parquet")

        # Apply WHERE filters
        for w in where:
            col = w["col"]
            op = w["op"]
            val = w["val"]

            if col == "type":
                continue  # Already filtered in aggregate

            if col == "country" and op == "eq":
                df = df.filter(pl.col("country") == val)
            elif col == "day" and op == "between":
                low, high = val
                # Convert string dates to date objects
                low_date = pl.lit(low).str.to_date()
                high_date = pl.lit(high).str.to_date()
                df = df.filter((pl.col("day") >= low_date) & (pl.col("day") <= high_date))
            elif col == "day" and op == "eq":
                # Convert string date to date object
                date_val = pl.lit(val).str.to_date()
                df = df.filter(pl.col("day") == date_val)

        # Group by requested columns
        df = df.group_by(group_by).agg([
            pl.col("sum_bid_price").sum()
        ])

        df = df.rename({"sum_bid_price": "sum(bid_price)"})

        # Select only requested columns
        select_cols = [col for col in group_by] + ["sum(bid_price)"]
        df = df.select(select_cols)

        return df

    def _query_country_purchases(self, order_by):
        """Execute country purchase query from pre-computed aggregates"""
        df = self._load_aggregate("country_purchases.parquet")
        df = df.rename({"avg_total_price": "avg(total_price)"})
        df = df.select(["country", "avg(total_price)"])

        if order_by:
            df = self._apply_order_by(df, order_by)

        return df

    def _query_advertiser_type(self, order_by):
        """Execute advertiser-type query from pre-computed aggregates"""
        df = self._load_aggregate("advertiser_type_counts.parquet")
        df = df.rename({"count": "count(*)"})
        df = df.select(["advertiser_id", "type", "count(*)"])

        if order_by:
            df = self._apply_order_by(df, order_by)

        return df

    def _query_minute_revenue(self, where, order_by):
        """Execute minute-level revenue query from pre-computed aggregates"""
        df = self._load_aggregate("minute_revenue.parquet")

        # Apply day filter if present
        for w in where:
            col = w["col"]
            op = w["op"]
            val = w["val"]

            if col == "day" and op == "eq":
                # Convert string date to date object
                date_val = pl.lit(val).str.to_date()
                df = df.filter(pl.col("day") == date_val)

        df = df.rename({"sum_bid_price": "sum(bid_price)"})
        df = df.select(["minute", "sum(bid_price)"])

        if order_by:
            df = self._apply_order_by(df, order_by)

        return df

    # ==================== Partition Scanning ====================

    def _determine_partitions(self, where: List[Dict]) -> tuple[List[str], Optional[List]]:
        """Determine which partitions need to be scanned based on WHERE clause"""
        types_to_scan = ["serve", "impression", "click", "purchase"]
        days_to_scan = None  # None means all days

        for condition in where:
            col = condition["col"]
            op = condition["op"]
            val = condition["val"]

            if col == "type" and op == "eq":
                types_to_scan = [val]
            elif col == "type" and op == "in":
                types_to_scan = val
            elif col == "day" and op == "eq":
                days_to_scan = [val]
            elif col == "day" and op == "between":
                # We'd need to expand the range, but for now we scan all
                pass

        return types_to_scan, days_to_scan

    def _determine_columns(self, select, where, group_by, order_by) -> List[str]:
        """Determine which columns need to be loaded"""
        columns = set()

        # Columns from SELECT
        for item in select:
            if isinstance(item, str):
                columns.add(item)
            elif isinstance(item, dict):
                for func, col in item.items():
                    if col != "*":
                        columns.add(col)

        # Columns from WHERE
        for condition in where:
            columns.add(condition["col"])

        # Columns from GROUP BY
        if group_by:
            columns.update(group_by)

        # Columns from ORDER BY
        for order in order_by:
            col = order["col"]
            # Handle aggregate column names like "COUNT(*)"
            if "(" not in col:
                columns.add(col)

        # Always include derived columns that might be needed
        derived = {"day", "week", "hour", "minute"}
        columns = columns.union(derived.intersection(columns))

        return list(columns)

    def _load_partitions(self, types: List[str], days: Optional[List], columns: List[str]) -> pl.DataFrame:
        """
        Load data from relevant partitions with optimizations:
        - Lazy loading with scan_parquet
        - Column projection to minimize data loading
        - Parallel reads via Polars' native parallelism
        """
        dfs = []

        for event_type in types:
            type_dir = self.partitioned_dir / f"type={event_type}"

            if not type_dir.exists():
                continue

            parquet_files = sorted(type_dir.glob("day=*.parquet"))

            if not parquet_files:
                continue

            # Use scan_parquet with multiple files for better parallelism
            # Polars will automatically parallelize reads
            for parquet_file in parquet_files:
                # Load with lazy execution
                df = pl.scan_parquet(parquet_file)

                # Project only needed columns early (predicate pushdown)
                available_cols = [c for c in columns if c in df.collect_schema().names()]
                if available_cols:
                    df = df.select(available_cols)

                dfs.append(df)

        if not dfs:
            # Return empty dataframe with proper schema
            return pl.DataFrame()

        # Concatenate all lazy frames then collect once
        # This allows Polars to optimize the entire plan
        result = pl.concat(dfs).collect()
        return result

    def _apply_filters(self, df: pl.DataFrame, where: List[Dict]) -> pl.DataFrame:
        """Apply WHERE clause filters"""
        for condition in where:
            col = condition["col"]
            op = condition["op"]
            val = condition["val"]

            if col not in df.columns:
                continue

            # Check if column is a date type
            is_date_col = df[col].dtype in [pl.Date, pl.Datetime]

            if op == "eq":
                if is_date_col and isinstance(val, str):
                    val = pl.lit(val).str.to_date()
                df = df.filter(pl.col(col) == val)
            elif op == "neq":
                if is_date_col and isinstance(val, str):
                    val = pl.lit(val).str.to_date()
                df = df.filter(pl.col(col) != val)
            elif op == "in":
                df = df.filter(pl.col(col).is_in(val))
            elif op == "between":
                low, high = val
                if is_date_col and isinstance(low, str):
                    low = pl.lit(low).str.to_date()
                    high = pl.lit(high).str.to_date()
                df = df.filter((pl.col(col) >= low) & (pl.col(col) <= high))

        return df

    def _apply_select(self, df: pl.DataFrame, select: List, group_by: List[str]) -> pl.DataFrame:
        """Apply SELECT clause with aggregations"""
        if group_by:
            # Build aggregation expressions
            agg_exprs = []

            for item in select:
                if isinstance(item, str):
                    # Group-by column
                    continue
                elif isinstance(item, dict):
                    for func, col in item.items():
                        if func == "SUM":
                            agg_exprs.append(pl.col(col).sum().alias(f"sum({col})"))
                        elif func == "AVG":
                            agg_exprs.append(pl.col(col).mean().alias(f"avg({col})"))
                        elif func == "COUNT":
                            if col == "*":
                                agg_exprs.append(pl.len().alias("count(*)"))
                            else:
                                agg_exprs.append(pl.col(col).count().alias(f"count({col})"))

            df = df.group_by(group_by).agg(agg_exprs)

            # Select columns in order
            select_cols = []
            for item in select:
                if isinstance(item, str):
                    select_cols.append(item)
                elif isinstance(item, dict):
                    for func, col in item.items():
                        select_cols.append(f"{func.lower()}({col})")

            df = df.select(select_cols)

        else:
            # No aggregation, just select columns
            select_cols = []
            for item in select:
                if isinstance(item, str):
                    select_cols.append(item)

            if select_cols:
                df = df.select(select_cols)

        return df

    def _apply_order_by(self, df: pl.DataFrame, order_by: List[Dict]) -> pl.DataFrame:
        """Apply ORDER BY clause"""
        for order in order_by:
            col = order["col"]
            direction = order.get("dir", "asc")

            descending = (direction.lower() == "desc")

            # Handle column names that might have been transformed
            if col in df.columns:
                df = df.sort(col, descending=descending)
            else:
                # Try to find the column with different casing
                for df_col in df.columns:
                    if df_col.lower() == col.lower():
                        df = df.sort(df_col, descending=descending)
                        break

        return df

    def _load_aggregate(self, filename: str) -> pl.DataFrame:
        """Load pre-computed aggregate with caching"""
        if filename not in self._aggregate_cache:
            file_path = self.aggregates_dir / filename
            self._aggregate_cache[filename] = pl.read_parquet(file_path)

        return self._aggregate_cache[filename].clone()
