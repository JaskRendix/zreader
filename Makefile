APP=app.main:app
PORT=8000

.PHONY: install run dev test lint format clean build docker

install:
	pip install --upgrade pip
	pip install -e .
	pip install pytest pytest-asyncio httpx

run:
	uvicorn $(APP) --host 0.0.0.0 --port $(PORT)

dev:
	uvicorn $(APP) --host 0.0.0.0 --port $(PORT) --reload

test:
	pytest -q

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

docker:
	docker build -t zreader-service .
