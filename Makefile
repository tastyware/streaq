.PHONY: install lint test docs cleanup

REDIS_VERSION ?= latest
PYTHON_VERSION ?= 3.11
COVERAGE_EXCLUDE ?= xreadgroup-no-max-count

install:
	uv sync --all-extras

lint:
	uv run ruff check --fix streaq/ tests/ example.py
	uv run ruff format streaq/ tests/
	uv run pyright streaq/ tests/ example.py
	uv run mypy --strict tests/ example.py

test:
	PYTHON_VERSION=$(PYTHON_VERSION) REDIS_VERSION=$(REDIS_VERSION) docker compose run --rm -e COVERAGE_EXCLUDE=$(COVERAGE_EXCLUDE) tests uv run --locked --all-extras --dev pytest -n auto --cov=streaq tests/

docs:
	uv run -m sphinx -T -b html -d docs/_build/doctrees -D language=en docs/ docs/_build/

cleanup:
	docker compose down --remove-orphans
