clean:
	@rm -rf build dist .eggs *.egg-info
	@rm -rf .benchmarks .coverage coverage.xml htmlcov report.xml .tox
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type f -name "*.py[co]" -exec rm -rf {} +

test: clean ## Run the tests.
	@pytest 
	@echo "The tests pass! ✨ 🍰 ✨"

lint: ## Run the code linter.
	ruff ./


style:
	black .
	ruff ./ --fix
	@echo "The style pass! ✨ 🍰 ✨"	

check: test style
# check: test lint style mypy