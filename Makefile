# Version management for StatsKita
# Main branch releases only: make release-patch/minor/major or make release VERSION=x.y.z

.PHONY: release release-patch release-minor release-major

# Release patch version (main branch only)
release-patch:
	@uv run python scripts/version.py release

# Release minor version (main branch only)
release-minor:
	@uv run python scripts/version.py release --minor

# Release major version (main branch only)
release-major:
	@uv run python scripts/version.py release --major

# Release specific version (e.g., make release VERSION=0.2.0)
release:
	@test -n "$(VERSION)" || (echo "Error: Set VERSION=x.y.z" && exit 1)
	@uv run python scripts/version.py release $(VERSION)