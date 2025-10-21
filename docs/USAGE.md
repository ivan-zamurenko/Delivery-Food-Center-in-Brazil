Usage & Quickstart

1) Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

2) If you want to run scripts that use Postgres, set up environment variables (see `config.example.env`):

```bash
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=changeme
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=food-delivery-brazil
```

3) Run an example script (missing-values task):

```bash
python scripts/run_task_1_missing_values.py
```

4) Run tests:

```bash
python -m pip install pytest
pytest -q
```
