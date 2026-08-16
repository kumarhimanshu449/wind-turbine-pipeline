.PHONY: install test lint run clean

install:
	python -m pip install -e ".[dev]"

test:
	pytest

lint:
	ruff check .

run:
	python -m wind_turbine_pipeline --input "data/raw/*.csv"

clean:
	rm -rf build .pytest_cache .ruff_cache .coverage htmlcov
