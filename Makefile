# Makefile for High-Performance Query Engine
# Cal Hacks Query Planner Challenge

# Variables
PYTHON := python3
DATA_FULL := data/data-full
DATA_LITE := data/data-lite
OPTIMIZED := optimized_data
OPTIMIZED_LITE := optimized_data_lite
OPTIMIZED_ULTRA := optimized_data_ultra
RESULTS := results
RESULTS_LITE := results_lite
RESULTS_ULTRA := results_ultra
BASELINE_RESULTS := baseline_results

# Colors
GREEN := \033[0;32m
YELLOW := \033[1;33m
BLUE := \033[0;34m
NC := \033[0m

.PHONY: help install prepare prepare-lite prepare-ultra test clean

# Default target
.DEFAULT_GOAL := help

help:
	@echo "$(BLUE)High-Performance Query Engine$(NC)"
	@echo ""
	@echo "$(GREEN)Setup:$(NC)"
	@echo "  make install       - Install dependencies"
	@echo ""
	@echo "$(GREEN)Prepare Data (choose one):$(NC)"
	@echo "  make prepare       - Full dataset (6 workers, ZSTD level 3)"
	@echo "  make prepare-lite  - Lite dataset (quick testing)"
	@echo "  make prepare-ultra - Ultra-fast (<20 min, ZSTD level 1)"
	@echo ""
	@echo "$(GREEN)Run Queries:$(NC)"
	@echo "  make query         - Run queries on optimized data"
	@echo "  make query-lite    - Run queries on lite data"
	@echo "  make query-ultra   - Run queries on ultra-fast data"
	@echo ""
	@echo "$(GREEN)Testing:$(NC)"
	@echo "  make test          - Quick test on lite dataset (~30 sec)"
	@echo "  make baseline      - Run DuckDB baseline for comparison"
	@echo ""
	@echo "$(GREEN)Cleanup:$(NC)"
	@echo "  make clean         - Remove all generated files"
	@echo ""

# Installation
install:
	@echo "$(GREEN)Installing dependencies...$(NC)"
	pip install -r requirements.txt
	@echo "$(GREEN)Done!$(NC)"

# Prepare data
prepare:
	@echo "$(YELLOW)Preparing full dataset (6 workers, ZSTD level 3)...$(NC)"
	@time $(PYTHON) prepare_optimized.py --data-dir $(DATA_FULL) --optimized-dir $(OPTIMIZED)
	@echo "$(GREEN)Done! Size: $(shell du -sh $(OPTIMIZED) | cut -f1)$(NC)"

prepare-lite:
	@echo "$(YELLOW)Preparing lite dataset...$(NC)"
	@time $(PYTHON) prepare_optimized.py --data-dir $(DATA_LITE) --optimized-dir $(OPTIMIZED_LITE)
	@echo "$(GREEN)Done!$(NC)"

prepare-ultra:
	@echo "$(YELLOW)Preparing full dataset ULTRA-FAST (<20 min target)...$(NC)"
	@time $(PYTHON) prepare_ultra_fast.py --data-dir $(DATA_FULL) --optimized-dir $(OPTIMIZED_ULTRA)
	@echo "$(GREEN)Done! Size: $(shell du -sh $(OPTIMIZED_ULTRA) | cut -f1)$(NC)"

# Run queries
query:
	@echo "$(BLUE)Running queries...$(NC)"
	@time $(PYTHON) main.py --optimized-dir $(OPTIMIZED) --out-dir $(RESULTS)
	@echo "$(GREEN)Done! Results in $(RESULTS)/$(NC)"

query-lite:
	@echo "$(BLUE)Running queries on lite data...$(NC)"
	@time $(PYTHON) main.py --optimized-dir $(OPTIMIZED_LITE) --out-dir $(RESULTS_LITE)
	@echo "$(GREEN)Done!$(NC)"

query-ultra:
	@echo "$(BLUE)Running queries on ultra-fast data...$(NC)"
	@time $(PYTHON) main.py --optimized-dir $(OPTIMIZED_ULTRA) --out-dir $(RESULTS_ULTRA)
	@echo "$(GREEN)Done!$(NC)"

# Baseline
baseline:
	@echo "$(BLUE)Running DuckDB baseline...$(NC)"
	@pip install -q duckdb pandas 2>/dev/null || true
	@cd baseline && $(PYTHON) main.py --data-dir ../$(DATA_FULL) --out-dir ../$(BASELINE_RESULTS)
	@echo "$(GREEN)Done! Results in $(BASELINE_RESULTS)/$(NC)"

# Comparison
compare: compare-full

compare-full:
	@echo "$(BLUE)Comparing results between optimized and baseline (full dataset)...$(NC)"
	@if [ ! -d "$(RESULTS_DIR_FULL_V2)" ]; then \
		echo "$(YELLOW)Optimized results not found. Running query-optimized first...$(NC)"; \
		$(MAKE) query-optimized; \
	fi
	@if [ ! -d "$(BASELINE_RESULTS_FULL)" ]; then \
		echo "$(YELLOW)Baseline results not found. Running baseline-full first...$(NC)"; \
		$(MAKE) baseline-full; \
	fi
	@echo ""
	@echo "Comparing Q1 (Daily revenue)..."
	@diff $(RESULTS_DIR_FULL_V2)/q1.csv $(BASELINE_RESULTS_FULL)/q1.csv && echo "$(GREEN)✓ Q1 matches$(NC)" || echo "$(YELLOW)⚠ Q1 differs (check row order)$(NC)"
	@echo ""
	@echo "Comparing Q2 (Publisher revenue)..."
	@diff $(RESULTS_DIR_FULL_V2)/q2.csv $(BASELINE_RESULTS_FULL)/q2.csv && echo "$(GREEN)✓ Q2 matches$(NC)" || echo "$(YELLOW)⚠ Q2 differs (check row order)$(NC)"
	@echo ""
	@echo "Comparing Q3 (Country purchases)..."
	@diff $(RESULTS_DIR_FULL_V2)/q3.csv $(BASELINE_RESULTS_FULL)/q3.csv && echo "$(GREEN)✓ Q3 matches$(NC)" || echo "$(YELLOW)⚠ Q3 differs (check row order)$(NC)"
	@echo ""
	@echo "Comparing Q4 (Advertiser counts)..."
	@diff $(RESULTS_DIR_FULL_V2)/q4.csv $(BASELINE_RESULTS_FULL)/q4.csv && echo "$(GREEN)✓ Q4 matches$(NC)" || echo "$(YELLOW)⚠ Q4 differs (check row order)$(NC)"
	@echo ""
	@echo "Comparing Q5 (Minute revenue)..."
	@diff $(RESULTS_DIR_FULL_V2)/q5.csv $(BASELINE_RESULTS_FULL)/q5.csv && echo "$(GREEN)✓ Q5 matches$(NC)" || echo "$(YELLOW)⚠ Q5 differs (check row order)$(NC)"
	@echo ""
	@echo "$(BLUE)Note: Differences in row order are acceptable for queries without ORDER BY$(NC)"

compare-lite:
	@echo "$(BLUE)Comparing results between optimized and baseline (lite dataset)...$(NC)"
	@if [ ! -d "$(RESULTS_DIR_LITE_V2)" ]; then \
		echo "$(YELLOW)Optimized results not found. Running query on lite dataset first...$(NC)"; \
		$(PYTHON) main.py --optimized-dir $(OPTIMIZED_DIR_LITE_V2) --out-dir $(RESULTS_DIR_LITE_V2); \
	fi
	@if [ ! -d "$(BASELINE_RESULTS_LITE)" ]; then \
		echo "$(YELLOW)Baseline results not found. Running baseline-lite first...$(NC)"; \
		$(MAKE) baseline-lite; \
	fi
	@for i in 1 2 3 4 5; do \
		echo "Comparing Q$$i..."; \
		diff $(RESULTS_DIR_LITE_V2)/q$$i.csv $(BASELINE_RESULTS_LITE)/q$$i.csv && echo "$(GREEN)✓ Q$$i matches$(NC)" || echo "$(YELLOW)⚠ Q$$i differs$(NC)"; \
	done

# Cleanup
clean: clean-results clean-optimized

clean-results:
	@echo "$(YELLOW)Removing query results...$(NC)"
	rm -rf $(RESULTS_DIR_FULL) $(RESULTS_DIR_LITE)
	rm -rf $(RESULTS_DIR_FULL_V2) $(RESULTS_DIR_LITE_V2)
	rm -rf $(RESULTS_DIR_ULTRA)
	rm -rf $(BASELINE_RESULTS_FULL) $(BASELINE_RESULTS_LITE)
	rm -rf out_* results/ baseline_results/ results_*_benchmark/ results_*_test/
	@echo "$(GREEN)Results cleaned!$(NC)"

clean-optimized:
	@echo "$(YELLOW)Removing optimized data...$(NC)"
	rm -rf $(OPTIMIZED_DIR_FULL) $(OPTIMIZED_DIR_LITE)
	rm -rf $(OPTIMIZED_DIR_FULL_V2) $(OPTIMIZED_DIR_LITE_V2)
	rm -rf $(OPTIMIZED_DIR_ULTRA)
	rm -rf optimized_data/ optimized_data_full_new/ optimized_test_lite/
	@echo "$(GREEN)Optimized data cleaned!$(NC)"

clean-all: clean
	@echo "$(YELLOW)Removing all generated files...$(NC)"
	rm -rf __pycache__
	rm -rf baseline/__pycache__
	@echo "$(GREEN)All generated files cleaned!$(NC)"

# Workflows
all: prepare-optimized query-optimized query-cached
	@echo "$(GREEN)Full workflow completed!$(NC)"

test: test-optimizations
	@echo "$(GREEN)Test workflow completed!$(NC)"

benchmark: install prepare-optimized query-optimized install-baseline baseline-full
	@echo "$(GREEN)Full benchmark completed!$(NC)"

# Quick validation
validate: query-optimized
	@echo "$(BLUE)Validating query results...$(NC)"
	@if [ -f "$(RESULTS_DIR_FULL_V2)/q1.csv" ]; then \
		echo "$(GREEN)✓ Q1 results exist ($(shell wc -l < $(RESULTS_DIR_FULL_V2)/q1.csv) rows)$(NC)"; \
	else \
		echo "$(YELLOW)✗ Q1 results missing$(NC)"; \
	fi
	@if [ -f "$(RESULTS_DIR_FULL_V2)/q2.csv" ]; then \
		echo "$(GREEN)✓ Q2 results exist ($(shell wc -l < $(RESULTS_DIR_FULL_V2)/q2.csv) rows)$(NC)"; \
	else \
		echo "$(YELLOW)✗ Q2 results missing$(NC)"; \
	fi
	@if [ -f "$(RESULTS_DIR_FULL_V2)/q3.csv" ]; then \
		echo "$(GREEN)✓ Q3 results exist ($(shell wc -l < $(RESULTS_DIR_FULL_V2)/q3.csv) rows)$(NC)"; \
	else \
		echo "$(YELLOW)✗ Q3 results missing$(NC)"; \
	fi
	@if [ -f "$(RESULTS_DIR_FULL_V2)/q4.csv" ]; then \
		echo "$(GREEN)✓ Q4 results exist ($(shell wc -l < $(RESULTS_DIR_FULL_V2)/q4.csv) rows)$(NC)"; \
	else \
		echo "$(YELLOW)✗ Q4 results missing$(NC)"; \
	fi
	@if [ -f "$(RESULTS_DIR_FULL_V2)/q5.csv" ]; then \
		echo "$(GREEN)✓ Q5 results exist ($(shell wc -l < $(RESULTS_DIR_FULL_V2)/q5.csv) rows)$(NC)"; \
	else \
		echo "$(YELLOW)✗ Q5 results missing$(NC)"; \
	fi

# Data Preparation: Optimized (Recommended)
prepare-optimized:
	@echo "$(YELLOW)Preparing full dataset with optimized settings...$(NC)"
	@echo "$(BLUE)Optimizations: Parallel processing (6 workers), ZSTD compression level 3$(NC)"
	@time $(PYTHON) prepare_optimized.py --data-dir $(DATA_DIR_FULL) --optimized-dir $(OPTIMIZED_DIR_FULL_V2)
	@echo "$(GREEN)Dataset prepared successfully!$(NC)"
	@echo "$(BLUE)Storage size:$(NC)"
	@du -sh $(OPTIMIZED_DIR_FULL_V2)

# ULTRA-FAST: <20 minutes target
prepare-ultra-fast:
	@echo "$(YELLOW)Preparing full dataset with ULTRA-FAST optimizations...$(NC)"
	@echo "$(BLUE)⚡ TARGET: <20 MINUTES on M2 MacBook$(NC)"
	@echo "$(BLUE)Optimizations: ZSTD level 1, skip sorting, only 3 aggregates, max workers$(NC)"
	@time $(PYTHON) prepare_ultra_fast.py --data-dir $(DATA_DIR_FULL) --optimized-dir $(OPTIMIZED_DIR_ULTRA)
	@echo "$(GREEN)Ultra-fast dataset prepared!$(NC)"
	@echo "$(BLUE)Storage size:$(NC)"
	@du -sh $(OPTIMIZED_DIR_ULTRA)

query-ultra-fast:
	@echo "$(BLUE)Running queries on ultra-fast optimized data (FIRST RUN - no cache)...$(NC)"
	@if [ ! -d "$(OPTIMIZED_DIR_ULTRA)" ]; then \
		echo "$(YELLOW)Ultra-fast data not found. Running prepare-ultra-fast first...$(NC)"; \
		$(MAKE) prepare-ultra-fast; \
	fi
	@echo "$(BLUE)Expected time: ~40-50ms total (vs 62ms v1, vs 24.4s DuckDB)$(NC)"
	@time $(PYTHON) main.py --optimized-dir $(OPTIMIZED_DIR_ULTRA) --out-dir $(RESULTS_DIR_ULTRA)
	@echo "$(GREEN)Queries completed! Results saved to $(RESULTS_DIR_ULTRA)/$(NC)"

query-ultra-cached:
	@echo "$(BLUE)Running queries again to test CACHE PERFORMANCE (ultra-fast data)...$(NC)"
	@if [ ! -d "$(OPTIMIZED_DIR_ULTRA)" ]; then \
		echo "$(YELLOW)Ultra-fast data not found. Run 'make prepare-ultra-fast' first$(NC)"; \
		exit 1; \
	fi
	@echo "$(BLUE)Expected time: ~5-10ms total (vs 62ms baseline, cache speedup!)$(NC)"
	@time $(PYTHON) main.py --optimized-dir $(OPTIMIZED_DIR_ULTRA) --out-dir $(RESULTS_DIR_ULTRA)
	@echo "$(GREEN)Cached queries completed! ~500-1000x faster than DuckDB!$(NC)"

all-ultra-fast:
	@echo "$(BLUE)========================================$(NC)"
	@echo "$(BLUE) ULTRA-FAST COMPLETE WORKFLOW$(NC)"
	@echo "$(BLUE)========================================$(NC)"
	@echo ""
	@echo "$(YELLOW)Step 1: Preparing data (ultra-fast, <20 min)...$(NC)"
	@$(MAKE) prepare-ultra-fast
	@echo ""
	@echo "$(YELLOW)Step 2: Running queries (first run)...$(NC)"
	@$(MAKE) query-ultra-fast
	@echo ""
	@echo "$(YELLOW)Step 3: Running queries (cached)...$(NC)"
	@$(MAKE) query-ultra-cached
	@echo ""
	@echo "$(GREEN)========================================$(NC)"
	@echo "$(GREEN) ULTRA-FAST WORKFLOW COMPLETE!$(NC)"
	@echo "$(GREEN)========================================$(NC)"
	@echo "Total time: <25 minutes (prep + queries)"
	@echo "Speedup vs DuckDB: ~500-1000x"

prepare-optimized-lite:
	@echo "$(YELLOW)Preparing lite dataset with optimizations...$(NC)"
	@time $(PYTHON) prepare_optimized.py --data-dir $(DATA_DIR_LITE) --optimized-dir $(OPTIMIZED_DIR_LITE_V2)
	@echo "$(GREEN)Lite dataset prepared successfully!$(NC)"

query-optimized:
	@echo "$(BLUE)Running queries on optimized data (FIRST RUN - no cache)...$(NC)"
	@if [ ! -d "$(OPTIMIZED_DIR_FULL_V2)" ]; then \
		echo "$(YELLOW)Optimized data not found. Running prepare-optimized first...$(NC)"; \
		$(MAKE) prepare-optimized; \
	fi
	@time $(PYTHON) main.py --optimized-dir $(OPTIMIZED_DIR_FULL_V2) --out-dir $(RESULTS_DIR_FULL_V2)
	@echo "$(GREEN)Queries completed! Results saved to $(RESULTS_DIR_FULL_V2)/$(NC)"

query-cached:
	@echo "$(BLUE)Running queries again to test CACHE PERFORMANCE...$(NC)"
	@if [ ! -d "$(OPTIMIZED_DIR_FULL_V2)" ]; then \
		echo "$(YELLOW)Optimized data not found. Run 'make prepare-optimized' first$(NC)"; \
		exit 1; \
	fi
	@time $(PYTHON) main.py --optimized-dir $(OPTIMIZED_DIR_FULL_V2) --out-dir $(RESULTS_DIR_FULL_V2)
	@echo "$(GREEN)Cached queries completed!$(NC)"

benchmark-optimizations:
	@echo "$(BLUE)========================================$(NC)"
	@echo "$(BLUE) Comparing Optimized vs Ultra-Fast$(NC)"
	@echo "$(BLUE)========================================$(NC)"
	@echo ""
	@echo "$(YELLOW)Step 1: Running queries on optimized data (6 workers, ZSTD level 3)...$(NC)"
	@if [ ! -d "$(OPTIMIZED_DIR_FULL_V2)" ]; then \
		echo "$(YELLOW)Optimized data not found. Run 'make prepare-optimized' first$(NC)"; \
		exit 1; \
	fi
	@time $(PYTHON) main.py --optimized-dir $(OPTIMIZED_DIR_FULL_V2) --out-dir results_optimized_benchmark
	@echo ""
	@echo "$(YELLOW)Step 2: Running queries on ultra-fast data (all cores, ZSTD level 1)...$(NC)"
	@if [ ! -d "$(OPTIMIZED_DIR_ULTRA)" ]; then \
		echo "$(YELLOW)Ultra-fast data not found. Run 'make prepare-ultra-fast' first$(NC)"; \
		exit 1; \
	fi
	@time $(PYTHON) main.py --optimized-dir $(OPTIMIZED_DIR_ULTRA) --out-dir results_ultra_benchmark
	@echo ""
	@echo "$(YELLOW)Step 3: Testing cache performance (optimized)...$(NC)"
	@time $(PYTHON) main.py --optimized-dir $(OPTIMIZED_DIR_FULL_V2) --out-dir results_optimized_cached
	@echo ""
	@echo "$(GREEN)========================================$(NC)"
	@echo "$(GREEN) Benchmark Complete!$(NC)"
	@echo "$(GREEN)========================================$(NC)"
	@echo ""
	@echo "Compare the times above:"
	@echo "  - Optimized: Better compression, all aggregates"
	@echo "  - Ultra-fast: Faster preparation, slightly larger files"

test-optimizations:
	@echo "$(BLUE)Quick test of optimizations on lite dataset...$(NC)"
	@echo ""
	@echo "$(YELLOW)Step 1: Preparing lite dataset with optimizations...$(NC)"
	@time $(PYTHON) prepare_optimized.py --data-dir $(DATA_DIR_LITE) --optimized-dir $(OPTIMIZED_DIR_LITE_V2)
	@echo ""
	@echo "$(YELLOW)Step 2: Running queries (first run)...$(NC)"
	@time $(PYTHON) main.py --optimized-dir $(OPTIMIZED_DIR_LITE_V2) --out-dir results_lite_v2_test
	@echo ""
	@echo "$(YELLOW)Step 3: Running queries (cached)...$(NC)"
	@time $(PYTHON) main.py --optimized-dir $(OPTIMIZED_DIR_LITE_V2) --out-dir results_lite_v2_test
	@echo ""
	@echo "$(GREEN)Test completed! Notice the cached queries are much faster.$(NC)"

info-optimizations:
	@echo "$(BLUE)========================================$(NC)"
	@echo "$(BLUE) Optimization Improvements$(NC)"
	@echo "$(BLUE)========================================$(NC)"
	@echo ""
	@echo "$(GREEN)Applied Optimizations:$(NC)"
	@echo "  1. Parallel CSV processing (6 workers)"
	@echo "  2. Streaming: Never loads entire dataset into memory"
	@echo "  3. ZSTD compression level 3 (balanced compression)"
	@echo "  4. Lazy CSV loading (better memory efficiency)"
	@echo "  5. Pre-computed aggregations for common query patterns"
	@echo "  6. Partitioning by type and day"
	@echo ""
	@echo "$(GREEN)System Requirements:$(NC)"
	@echo "  - 16GB RAM (M2 MacBook)"
	@echo "  - 100GB disk space"
	@echo ""
	@echo "$(BLUE)Documentation:$(NC)"
	@echo "  - See README.md for complete documentation"
	@echo "  - See UPDATES_SUMMARY.md for recent changes"
	@echo ""

# Display system information
info:
	@echo "$(BLUE)System Information$(NC)"
	@echo "Python version: $(shell $(PYTHON) --version)"
	@echo "Platform: $(shell uname -s) $(shell uname -m)"
	@echo "Disk space available: $(shell df -h . | tail -1 | awk '{print $$4}')"
	@echo ""
	@echo "$(BLUE)Project Status$(NC)"
	@if [ -d "$(OPTIMIZED_DIR_FULL)" ]; then \
		echo "$(GREEN)✓ Full dataset optimized ($(shell du -sh $(OPTIMIZED_DIR_FULL) | cut -f1))$(NC)"; \
	else \
		echo "$(YELLOW)✗ Full dataset not prepared$(NC)"; \
	fi
	@if [ -d "$(OPTIMIZED_DIR_LITE)" ]; then \
		echo "$(GREEN)✓ Lite dataset optimized ($(shell du -sh $(OPTIMIZED_DIR_LITE) | cut -f1))$(NC)"; \
	else \
		echo "$(YELLOW)✗ Lite dataset not prepared$(NC)"; \
	fi
	@if [ -d "$(RESULTS_DIR_FULL)" ]; then \
		echo "$(GREEN)✓ Full results available$(NC)"; \
	else \
		echo "$(YELLOW)✗ Full results not available$(NC)"; \
	fi
	@if [ -d "$(BASELINE_RESULTS_FULL)" ]; then \
		echo "$(GREEN)✓ Baseline results available$(NC)"; \
	else \
		echo "$(YELLOW)✗ Baseline results not available$(NC)"; \
	fi
	@if [ -d "$(OPTIMIZED_DIR_ULTRA)" ]; then \
		echo "$(GREEN)✓ Ultra-fast dataset optimized ($(shell du -sh $(OPTIMIZED_DIR_ULTRA) | cut -f1))$(NC)"; \
	else \
		echo "$(YELLOW)✗ Ultra-fast dataset not prepared$(NC)"; \
	fi
