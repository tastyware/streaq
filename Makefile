.PHONY: install lint test docs

install:
	uv sync --all-extras

lint:
	uv run ruff check --select I --fix
	uv run ruff format streaq/ tests/
	uv run ruff check streaq/ tests/ example.py
	uv run pyright streaq/ tests/ example.py

test:
	UV_PYTHON=3.11 docker compose run --rm tests uv run --locked --all-extras --dev pytest -n auto --dist=loadgroup --cov=streaq tests/

docs:
	uv run -m sphinx -T -b html -d docs/_build/doctrees -D language=en docs/ docs/_build/

cleanup:
	docker compose down --remove-orphans
