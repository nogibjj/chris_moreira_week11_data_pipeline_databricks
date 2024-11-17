install:
	pip install --upgrade pip && \
		pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main --cov=mylib test_*.py

format:
	black *.py mylib/*.py

lint:
	ruff check *.py mylib/*.py

refactor: format lint

all: install lint test format
