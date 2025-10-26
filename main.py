#!/usr/bin/env python3
"""
High-Performance Query Engine - Main Entry Point
-----------------------------------------------
Executes benchmark queries using optimized data structures

Usage:
  python main.py --optimized-dir ./optimized_data --out-dir ./out
"""

import argparse
from pathlib import Path
import csv
import sys

# Import queries
# JUDGES: To use evaluation queries, comment out the line below and uncomment the judges line
from inputs import queries as default_queries      # Default benchmark queries
# from judges import queries as default_queries   # Judges' evaluation queries (uncomment to use)

from query_engine import QueryEngine


def run_queries(queries, optimized_dir: Path, out_dir: Path):
    """Execute all queries and save results"""
    # Ensure output directory exists
    out_dir.mkdir(parents=True, exist_ok=True)

    # Initialize query engine
    engine = QueryEngine(optimized_dir)

    results = []
    total_time = 0

    print("🚀 Executing optimized queries...\n")

    for i, query in enumerate(queries, 1):
        print(f"🟦 Query {i}:")
        print(f"   {query}")

        try:
            # Execute query
            result_df, execution_time = engine.execute_query(query)

            # Convert to list of rows for CSV output
            columns = result_df.columns
            rows = result_df.rows()

            print(f"   ✅ Rows: {len(rows)} | Time: {execution_time:.3f}s\n")

            # Save to CSV
            out_path = out_dir / f"q{i}.csv"
            with out_path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)

            results.append({
                "query": i,
                "rows": len(rows),
                "time": execution_time
            })
            total_time += execution_time

        except Exception as e:
            print(f"   ❌ Error: {e}\n")
            results.append({
                "query": i,
                "rows": 0,
                "time": 0,
                "error": str(e)
            })

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    for r in results:
        if "error" in r:
            print(f"Q{r['query']}: ERROR - {r['error']}")
        else:
            print(f"Q{r['query']}: {r['time']:.3f}s ({r['rows']} rows)")

    print(f"\nTotal time: {total_time:.3f}s")
    print("="*60)


def main():
    parser = argparse.ArgumentParser(
        description="Execute queries using optimized query engine"
    )
    parser.add_argument(
        "--optimized-dir",
        type=Path,
        required=True,
        help="Directory containing optimized data (from prepare.py)"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Output directory for query results"
    )

    args = parser.parse_args()

    # Check if optimized data exists
    if not args.optimized_dir.exists():
        print(f"❌ Error: Optimized data directory not found: {args.optimized_dir}")
        print(f"Please run prepare.py first to create optimized data.")
        sys.exit(1)

    # Use queries from import
    queries = default_queries
    print(f"📋 Using queries from import ({len(queries)} queries)")

    run_queries(queries, args.optimized_dir, args.out_dir)


if __name__ == "__main__":
    main()
