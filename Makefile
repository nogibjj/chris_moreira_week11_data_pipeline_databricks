install:
    pip install -U pip setuptools wheel
    pip install -U pandas==1.5.3 numpy==1.23.5
    pip install -r requirements.txt

lint:
	ruff check . --fix

test:
	pytest --cov=main --cov=mylib

format:
	black .
