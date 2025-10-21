# Delivery-Food-Center-in-Brazil 🇧🇷 (⚙️ under maintenance)

This project is an analysis and visualization pipeline for a food delivery dataset (Brazil). The repository contains raw CSVs, SQL schema & queries, and Python plotting utilities.

Quick start
1) Create virtualenv and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-pinned.txt
```

2) Run the example script:

```bash
python scripts/run_task_1_missing_values.py
```

Project structure
- `data/` — raw CSVs
- `sql/` — schema and analytic SQL
- `analysis/` — primary analysis package (preferred, new name)
- `visualizations/` — legacy compatibility package (imports `analysis`)
- `notebooks/` — interactive analysis
- `scripts/` — reproducible scripts
- `results/` — generated outputs (ignored by git)

See `PROJECT_STRUCTURE.md` for more details.
