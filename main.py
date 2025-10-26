#!/usr/bin/env python3
"""
High-Performance Query Engine - Main Entry Point
-----------------------------------------------
Executes benchmark queries using optimized data structures

Usage:
  python main.py --optimized-dir ./optimized_data --out-dir ./out
  python main.py --optimized-dir ./optimized_data --out-dir ./out --queries-file queries.json
"""

import argparse
from pathlib import Path
import csv
import sys
import json

# Import baseline queries
sys.path.insert(0, str(Path(__file__).parent / "baseline"))
from inputs import queries as default_queries

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


def load_queries_from_file(queries_file: Path) -> list:
    """Load queries from a JSON file"""
    try:
        with open(queries_file, 'r') as f:
            data = json.load(f)

        # Support both formats:
        # 1. Direct list of queries: [{"select": ..., "from": ...}, ...]
        # 2. Object with queries key: {"queries": [{"select": ..., "from": ...}, ...]}
        if isinstance(data, list):
            queries = data
        elif isinstance(data, dict) and "queries" in data:
            queries = data["queries"]
        else:
            raise ValueError("JSON file must contain a list of queries or an object with 'queries' key")

        print(f"📂 Loaded {len(queries)} queries from {queries_file}")
        return queries

    except FileNotFoundError:
        print(f"❌ Error: Queries file not found: {queries_file}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in queries file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error loading queries file: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Execute queries using optimized query engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run default benchmark queries
  python main.py --optimized-dir optimized_data_full --out-dir results_full

  # Run custom queries from JSON file
  python main.py --optimized-dir optimized_data_full --out-dir results_custom --queries-file my_queries.json

JSON file format (two options):

  Option 1 - Direct list:
  [
    {
      "select": ["day", {"SUM": "bid_price"}],
      "from": "events",
      "where": [{"col": "type", "op": "eq", "val": "impression"}],
      "group_by": ["day"]
    },
    ...
  ]

  Option 2 - Object with queries key:
  {
    "queries": [
      {
        "select": ["day", {"SUM": "bid_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "impression"}],
        "group_by": ["day"]
      },
      ...
    ]
  }
"""
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
    parser.add_argument(
        "--queries-file",
        type=Path,
        help="JSON file containing queries to execute (optional, uses default benchmark queries if not provided)"
    )

    args = parser.parse_args()

    # Check if optimized data exists
    if not args.optimized_dir.exists():
        print(f"❌ Error: Optimized data directory not found: {args.optimized_dir}")
        print(f"Please run prepare.py first to create optimized data.")
        sys.exit(1)

    # Load queries from file or use defaults
    if args.queries_file:
        queries = load_queries_from_file(args.queries_file)
    else:
        queries = default_queries
        print(f"📋 Using default benchmark queries ({len(queries)} queries)")

    run_queries(queries, args.optimized_dir, args.out_dir)


if __name__ == "__main__":
    main()
