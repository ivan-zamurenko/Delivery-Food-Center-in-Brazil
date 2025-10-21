Professional Project Tasks — CV-ready

Purpose: Curated projects/tasks you can complete using this repo. Each task demonstrates specific professional skills (data engineering, analysis, visualization, testing, reproducibility) and maps to concrete deliverables for your CV.

Guidelines (for each task)
- SQL: provide one or more SQL queries (in `sql/` or in a .sql file) that answer the question.
- Script + Notebook: Python script in `scripts/` for reproducibility, plus a polished notebook in `notebooks/` used for presentation.
- Visuals: one or more static figures saved to `results/task-n/` and referenced in the notebook.
- Tests: at least one unit test in `tests/` covering transformation logic.
- README: a short `notebooks/task-n/README.md` describing inputs, steps, and conclusions.

Task A — Channel & Payment Mix Profitability (End-to-End)
- Goal: Identify the most profitable channels + payment methods; recommend optimizations.
- Skills: Joins/aggregation, revenue and fee calculations, plotting, story-telling.
- Deliverables:
  - SQL: revenue by channel+payment_method (use CTEs).
  - Script: `scripts/run_profitability.py` that outputs `results/task-A/profitability.csv`.
  - Notebook: interactive story + final plots (bar chart, heatmap of channel vs payment method revenue per order).
  - Test: check revenue calculations against sample input in `tests/test_profitability.py`.

Task B — Delivery Time Optimization (Driver Analysis)
- Goal: Quantify differences in delivery times by driver_type/modal and suggest operational changes.
- Skills: Time deltas, boxplots, ANOVA or non-parametric tests, outlier handling.
- Deliverables:
  - SQL: average delivery time per driver_type (see `problem_solve.sql` Q6 for pattern).
  - Script: `scripts/run_delivery_analysis.py` that saves summary and a CSV of per-driver stats.
  - Notebook: include boxplots, violin plots, and a short model estimation (e.g., linear regression of delivery_time ~ driver_modal + store + channel).
  - Test: validate delivery-time calculation logic.

Task C — Hub Coverage & Store Network (Mapping)
- Goal: Visualize hub coverage, find hubs with high potential (many stores, low revenue) and suggest consolidation or expansion.
- Skills: Spatial joins (if coords exist), geoplots, aggregations, map storytelling.
- Deliverables:
  - SQL: stores per hub, revenue per hub.
  - Notebook: map (scatter or choropleth) of hubs with store counts and revenue size-coded.
  - Script to generate `results/task-C/` outputs.

Task D — Data Quality & Pipeline (Data Engineering)
- Goal: Build a reproducible cleaning pipeline and show improvements in downstream metrics.
- Skills: Data validation, cleaning, idempotent scripts, DDL constraints, CI tests.
- Deliverables:
  - Script: `scripts/clean_data.py` to produce `data-cleaned/` (or store as `results/`), plus `sql/data_cleaning.sql` updated.
  - Tests: unit/integration tests verifying no nulls in key fields, deduplicated IDs.
  - README: cleaning steps + rationale.

Task E — Payment Methods Trend & Churn (Time Series)
- Goal: Detect trends and anomalies in payment methods over time; propose seasonal/marketing interpretations.
- Skills: Time series aggregation, anomaly detection, visual storytelling.
- Deliverables:
  - Script: `scripts/payment_trends.py` that outputs monthly shares and anomalies.
  - Notebook: stacked area charts, anomaly annotations, linked SQL queries.

Presentation-ready polish
- For each task, create a concise summary section in `README.md` linking to the notebook and results.
- Add an `examples/` folder with 1-2 PNGs and a short `presentation.md` bulleting key business insights per task.

Prioritization (order to do)
1) Task A (business impact, simple to implement)
2) Task D (shows you can clean & operate pipelines)
3) Task B (analysis + modeling)
4) Task E (time-series & anomaly detection)
5) Task C (mapping — optional but impressive)

Want me to scaffold any of the above (script, notebook, tests, CI)? Tell me which task to start with and I'll create the skeleton files.
