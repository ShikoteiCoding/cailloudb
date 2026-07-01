ruff:
	uv run ruff format . && uv run ruff check --fix .

test:
	cd src && PYTHONPATH=cailloudb uv run pytest