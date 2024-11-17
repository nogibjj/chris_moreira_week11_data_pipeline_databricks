install:
	pip install -r requirements.txt

lint:
	ruff check . --fix

test:
	pytest --cov=main --cov=mylib

format:
	black .
