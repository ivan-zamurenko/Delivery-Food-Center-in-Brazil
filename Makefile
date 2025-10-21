install:
	python -m pip install -r requirements-pinned.txt

test:
	PYTHONPATH=. pytest -q

run-missing:
	python -m scripts.run_task_1_missing_values