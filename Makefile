install:
	python -m pip install -r requirements-pinned.txt

test:
	pytest -q

run-missing:
	python scripts/run_task_1_missing_values.py
