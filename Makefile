.PHONY: generate demo-data test lint format

PYTHON ?= python

generate:
	$(PYTHON) -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. distributed_scheduler/distributed_jobs.proto

demo-data:
	$(PYTHON) -m examples.generate_demo_data

test: generate
	$(PYTHON) -m pytest

format:
	$(PYTHON) -m ruff format .

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .
