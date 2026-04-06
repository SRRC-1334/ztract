.PHONY: install-dev test lint typecheck build-jar rebuild-jar build clean

install-dev:
	pip install -e ".[dev]"

test:
	pytest tests/ -v

test-quick:
	pytest tests/ -q

lint:
	ruff check ztract/ tests/

lint-fix:
	ruff check --fix ztract/ tests/

typecheck:
	mypy ztract/ --ignore-missing-imports

build-jar:
	cd engine-java && mvn clean package -q

rebuild-jar: build-jar
	cp engine-java/target/ztract-engine-0.1.0.jar ztract/engine/ztract-engine.jar
	@echo "JAR rebuilt and copied to ztract/engine/ztract-engine.jar"
	@echo "SHA256: $$(sha256sum ztract/engine/ztract-engine.jar | cut -d' ' -f1)"

jar-sha:
	@sha256sum ztract/engine/ztract-engine.jar 2>/dev/null || certutil -hashfile ztract/engine/ztract-engine.jar SHA256

build:
	python -m build

clean:
	rm -rf build/ dist/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf .mypy_cache .ruff_cache
	rm -rf engine-java/target
