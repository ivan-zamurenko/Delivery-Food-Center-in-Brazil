install:
	conda env update -f environment.yml

test:
	PYTHONPATH=. pytest -q

run-missing:
	python -m scripts.run_task_1_missing_values