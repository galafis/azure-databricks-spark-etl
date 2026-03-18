.PHONY: install test lint format run docker-build docker-run clean

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v --cov=src --cov-report=term-missing

lint:
	flake8 src/ tests/ --max-line-length=88
	mypy src/ --ignore-missing-imports

format:
	black src/ tests/

run:
	python -m src.main

docker-build:
	docker-compose build

docker-run:
	docker-compose up

clean:
	rm -rf data/delta/ data/checkpoints/ mlruns/ .mypy_cache/ .pytest_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
