#!/usr/bin/env python3

"""
Judges' Query File
------------------
Judges will place their evaluation queries here.
The system will automatically use these queries when the import is changed.

To use judges' queries, change line 18 in main.py from:
    from inputs import queries as default_queries
to:
    from judges import queries as default_queries
"""

# Placeholder - judges will replace with their queries
queries = [
    {
        "select": ["day", {"SUM": "bid_price"}],
        "from": "events",
        "where": [ {"col": "type", "op": "eq", "val": "impression"} ],
        "group_by": ["day"],
    },
    # Judges will add their evaluation queries here
]
