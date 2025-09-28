# Release commands for StatsKita
# Updates version in: pyproject.toml, statskita/__init__.py, CITATION.cff

.PHONY: release-patch release-minor release-major release

# Quick releases
release-patch:
	@echo "Releasing patch version..."
	@uv run python scripts/release.py patch

release-minor:
	@echo "Releasing minor version..."
	@uv run python scripts/release.py minor

release-major:
	@echo "Releasing major version..."
	@uv run python scripts/release.py major

# Custom version
release:
	@test -n "$(VERSION)" || (echo "Error: VERSION not set" && exit 1)
	@echo "Releasing version $(VERSION)..."
	@uv run python scripts/release.py $(VERSION)