#!/usr/bin/env python
"""One-command release script for StatsKita."""

import re
import subprocess
import sys
from pathlib import Path


def get_current_version():
    """Read version from pyproject.toml."""
    content = Path("pyproject.toml").read_text()
    match = re.search(r'version = "([^"]+)"', content)
    if not match:
        print("❌ Could not find version in pyproject.toml")
        sys.exit(1)
    return match.group(1)


def bump_version(current, bump_type):
    """Calculate new version based on bump type."""
    parts = current.split(".")
    major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])

    if bump_type == "major":
        return f"{major + 1}.0.0"
    elif bump_type == "minor":
        return f"{major}.{minor + 1}.0"
    elif bump_type == "patch":
        return f"{major}.{minor}.{patch + 1}"
    else:
        # Assume it's a specific version
        return bump_type


def update_version_in_file(new_version):
    """Update version in all relevant files."""
    # Update pyproject.toml
    path = Path("pyproject.toml")
    content = path.read_text()
    updated = re.sub(r'version = "[^"]+"', f'version = "{new_version}"', content)
    path.write_text(updated)
    print(f"Updated pyproject.toml to {new_version}")

    # Update __init__.py
    init_path = Path("statskita/__init__.py")
    if init_path.exists():
        content = init_path.read_text()
        updated = re.sub(r'__version__ = "[^"]+"', f'__version__ = "{new_version}"', content)
        init_path.write_text(updated)
        print(f"Updated statskita/__init__.py to {new_version}")

    # Update CITATION.cff
    citation_path = Path("CITATION.cff")
    if citation_path.exists():
        content = citation_path.read_text()
        updated = re.sub(r'^version: .*$', f'version: {new_version}', content, flags=re.MULTILINE)
        citation_path.write_text(updated)
        print(f"Updated CITATION.cff to {new_version}")


def git_status_clean():
    """Check if git working directory is clean."""
    result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    return result.stdout.strip() == ""


def main():
    if len(sys.argv) < 2:
        print("Usage: python release.py [patch|minor|major|x.y.z]")
        sys.exit(1)

    bump_type = sys.argv[1]

    # Check current branch
    branch_result = subprocess.run(
        ["git", "branch", "--show-current"],
        capture_output=True,
        text=True
    )
    current_branch = branch_result.stdout.strip()

    if current_branch != "main":
        print(f"❌ Error: You're on branch '{current_branch}', not 'main'")
        print("\nReleases must be done from the main branch:")
        print("1. Commit your changes on this branch")
        print("2. Push and create a PR to main")
        print("3. After PR is merged, checkout main")
        print("4. Then run this release script")
        print("\nExample:")
        print("  git checkout main")
        print("  git pull origin main")
        print(f"  make release VERSION={bump_type}")
        sys.exit(1)

    # Check git status
    if not git_status_clean():
        print("❌ Git working directory not clean. Commit or stash changes first.")
        sys.exit(1)

    # Get versions
    current = get_current_version()
    new_version = bump_version(current, bump_type)

    print(f"Current version: {current}")
    print(f"New version: {new_version}")

    # Confirm
    response = input("\nProceed with release? (y/n): ")
    if response.lower() != "y":
        print("❌ Release cancelled")
        sys.exit(1)

    # Update version
    update_version_in_file(new_version)

    # Commit, tag, and push
    commands = [
        ["git", "add", "pyproject.toml", "statskita/__init__.py", "CITATION.cff"],
        ["git", "commit", "-m", f"bump version to {new_version}"],
        ["git", "tag", "-a", f"v{new_version}", "-m", f"release v{new_version}"],
        ["git", "push", "origin", "main"],  # Always push to main for releases
        ["git", "push", "origin", f"v{new_version}"],
    ]

    for cmd in commands:
        print(f"Running: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)

    print(f"\n✅ Released v{new_version}!")
    print("🚀 GitHub Actions will now publish to PyPI")
    print("📦 Monitor: https://github.com/okkymabruri/statskita/actions")


if __name__ == "__main__":
    main()
