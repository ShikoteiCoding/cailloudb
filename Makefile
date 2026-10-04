ruff:
	uv run ruff format . && uv run ruff check --select I --fix .

test:
	cd src && PYTHONPATH=cailloudb uv run pytest -vv -s --cov cailloudb

diag:
	PYTHONPATH=./src/cailloudb/ uv run pyreverse -o mmd -f ALL -S src/cailloudb/