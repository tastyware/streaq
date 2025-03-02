.PHONY: install lint test docs

install:
	uv sync --all-extras

lint:
	uv run ruff format streaq/ tests/
	uv run ruff check streaq/ tests/
	uv run pyright streaq/ tests/

test:
	uv run pytest --cov=streaq --cov-report=term-missing --cov-fail-under=90

docs:
	uv run -m sphinx -T -b html -d docs/_build/doctrees -D language=en docs/ docs/_build/
