# Instructions for Judges

## How to Run Evaluation Queries

Our system uses a simple Python import mechanism for query execution. Just modify two files to run your evaluation queries.

---

## Step 1: Add Your Queries

Edit `judges.py` in the main directory and replace the placeholder with your evaluation queries:

```python
#!/usr/bin/env python3

queries = [
    {
        "select": ["day", {"SUM": "bid_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "impression"}],
        "group_by": ["day"]
    },
    {
        "select": ["country", {"COUNT": "*"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "click"}],
        "group_by": ["country"]
    },
    # Add more queries here...
]
```

---

## Step 2: Switch the Import

Open `main.py` and modify lines 18-19:

**Change from:**
```python
from inputs import queries as default_queries      # Default benchmark queries
# from judges import queries as default_queries   # Judges' evaluation queries (uncomment to use)
```

**To:**
```python
# from inputs import queries as default_queries      # Default benchmark queries
from judges import queries as default_queries   # Judges' evaluation queries (uncomment to use)
```

---

## Step 3: Prepare Data

Run data preparation once (choose one option):

### Option 1: Optimized (Recommended)
```bash
python prepare_optimized.py --data-dir data/data-full --optimized-dir optimized_data
```
- **Time**: Moderate
- **Workers**: 6 parallel
- **Compression**: ZSTD level 3
- **Aggregates**: All 5 pre-computed

### Option 2: Ultra-Fast
```bash
python prepare_ultra_fast.py --data-dir data/data-full --optimized-dir optimized_data_ultra
```
- **Time**: <20 min target on M2 MacBook
- **Workers**: All CPU cores
- **Compression**: ZSTD level 1
- **Aggregates**: 3 essential (Q2 & Q5 computed on-demand)

---

## Step 4: Run Evaluation

```bash
# Run evaluation queries
python main.py --optimized-dir optimized_data --out-dir results_evaluation
```

Results will be saved as:
- `results_evaluation/q1.csv`
- `results_evaluation/q2.csv`
- `results_evaluation/q3.csv`
- etc.

---

## Query Format

Queries use a JSON-like Python dictionary structure:

```python
{
  "select": [columns and aggregates],
  "from": "events",
  "where": [filter conditions],
  "group_by": [grouping columns],
  "order_by": [sorting instructions]
}
```

### Supported Features

**SELECT:**
- Column names: `"day"`, `"country"`, `"type"`, `"advertiser_id"`, `"publisher_id"`, etc.
- Aggregates: `{"SUM": "bid_price"}`, `{"COUNT": "*"}`, `{"AVG": "total_price"}`

**WHERE:**
- Operators:
  - `"eq"` (equals): `{"col": "type", "op": "eq", "val": "impression"}`
  - `"neq"` (not equals): `{"col": "country", "op": "neq", "val": "US"}`
  - `"in"` (in list): `{"col": "country", "op": "in", "val": ["US", "UK", "JP"]}`
  - `"between"` (range): `{"col": "day", "op": "between", "val": ["2024-01-01", "2024-12-31"]}`

**GROUP BY:**
- List of column names: `["day"]`, `["country", "type"]`, `["advertiser_id"]`

**ORDER BY:**
- Example: `[{"col": "SUM(bid_price)", "dir": "desc"}]`
- Direction: `"asc"` or `"desc"`

---

## Example Queries

### Daily revenue
```python
{
    "select": ["day", {"SUM": "bid_price"}],
    "from": "events",
    "where": [{"col": "type", "op": "eq", "val": "impression"}],
    "group_by": ["day"]
}
```

### Clicks by country (sorted)
```python
{
    "select": ["country", {"COUNT": "*"}],
    "from": "events",
    "where": [{"col": "type", "op": "eq", "val": "click"}],
    "group_by": ["country"],
    "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
}
```

### Purchases in date range
```python
{
    "select": ["day", {"SUM": "total_price"}],
    "from": "events",
    "where": [
        {"col": "type", "op": "eq", "val": "purchase"},
        {"col": "day", "op": "between", "val": ["2024-01-01", "2024-01-31"]}
    ],
    "group_by": ["day"]
}
```

### Average purchase by country (filtered and sorted)
```python
{
    "select": ["country", {"AVG": "total_price"}],
    "from": "events",
    "where": [{"col": "type", "op": "eq", "val": "purchase"}],
    "group_by": ["country"],
    "order_by": [{"col": "AVG(total_price)", "dir": "desc"}]
}
```

### Publisher revenue in specific country
```python
{
    "select": ["publisher_id", {"SUM": "bid_price"}],
    "from": "events",
    "where": [
        {"col": "type", "op": "eq", "val": "impression"},
        {"col": "country", "op": "eq", "val": "JP"}
    ],
    "group_by": ["publisher_id"]
}
```

---

## Available Columns

From the `events` table:

| Column | Type | Description |
|--------|------|-------------|
| `ts` | long | Unix timestamp (milliseconds) |
| `type` | string | Event type: "serve", "impression", "click", "purchase" |
| `auction_id` | string | Unique auction identifier |
| `advertiser_id` | int | Advertiser identifier |
| `publisher_id` | int | Publisher identifier |
| `bid_price` | float | Bid price in USD (impressions only) |
| `user_id` | long | User identifier |
| `total_price` | float | Purchase amount in USD (purchases only) |
| `country` | string | Country code (e.g., "US", "JP", "DE") |
| `day` | date | Derived: Date (YYYY-MM-DD) |
| `week` | date | Derived: Week start date |
| `hour` | datetime | Derived: Hour timestamp |
| `minute` | string | Derived: Minute timestamp (YYYY-MM-DD HH:MM) |

---

## System Requirements

- **RAM**: 16GB (M2 MacBook)
- **Disk**: 100GB available space
- **Python**: 3.13+
- **Dependencies**: `polars`, `pyarrow` (install with `pip install -r requirements.txt`)

---

## Output Format

Query results are saved as CSV files:
- Header row with column names
- Data rows with query results
- One file per query: `q1.csv`, `q2.csv`, `q3.csv`, etc.

---

## Troubleshooting

### "Optimized data directory not found"
**Solution**: Run preparation first:
```bash
python prepare_optimized.py --data-dir data/data-full --optimized-dir optimized_data
```

### "Module not found: polars"
**Solution**: Install dependencies:
```bash
pip install -r requirements.txt
```

### Query takes longer than expected
**Note**:
- Queries matching pre-computed aggregates: 5-20ms
- Queries requiring partition scanning: 50-200ms
- Still significantly faster than DuckDB baseline (~24 seconds for 5 queries)

The system can handle arbitrary queries through intelligent partition pruning and lazy evaluation, even if they don't match pre-computed patterns.

---

## File Structure

```
attempt 0/
├── main.py                    # Main execution entry point
├── inputs.py                  # Default benchmark queries (5 queries)
├── judges.py                  # Your evaluation queries (edit this)
├── prepare_optimized.py       # Data preparation (recommended)
├── prepare_ultra_fast.py      # Ultra-fast preparation (<20 min)
├── query_engine.py            # Query execution engine
├── requirements.txt           # Python dependencies
├── README.md                  # Complete documentation
└── data/
    └── data-full/             # Input CSV data (245M rows)
```

---

## Quick Reference

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Prepare data (one-time)
python prepare_optimized.py --data-dir data/data-full --optimized-dir optimized_data

# 3. Edit judges.py with your queries

# 4. Edit main.py line 18-19 to use judges.py

# 5. Run evaluation
python main.py --optimized-dir optimized_data --out-dir results_evaluation
```

---

## Questions?

See `README.md` for complete architecture documentation and performance details.
