ruff:
	uv run ruff format . && uv run ruff check --select I --select F401 --fix .

test:
	cd src && PYTHONPATH=cailloudb uv run pytest -vv -s --cov cailloudb --cov-report=markdown

diag:
	PYTHONPATH=./src/cailloudb/ uv run pyreverse -o mmd -f ALL -S src/cailloudb/