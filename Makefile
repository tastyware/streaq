.PHONY: install lint test docs

install:
	uv sync --all-extras

lint:
	uv run ruff check --select I --fix
	uv run ruff format streaq/ tests/
	uv run ruff check streaq/ tests/ example.py
	uv run pyright streaq/ tests/ example.py
	uv run mypy streaq/

test:
	uv run pytest --cov=streaq --cov-report=term-missing --cov-fail-under=95

docs:
	uv run -m sphinx -T -b html -d docs/_build/doctrees -D language=en docs/ docs/_build/
