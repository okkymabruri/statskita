#!/usr/bin/env python
"""Version management for StatsKita - handles both dev and release workflows."""

import argparse
import re
import subprocess
import sys
from pathlib import Path


def get_current_version():
    """Read version from pyproject.toml."""
    content = Path("pyproject.toml").read_text()
    match = re.search(r'version = "([^"]+)"', content)
    if not match:
        print("Error: Could not find version in pyproject.toml")
        sys.exit(1)
    return match.group(1)


def get_current_branch():
    """Get current git branch."""
    result = subprocess.run(["git", "branch", "--show-current"], capture_output=True, text=True)
    return result.stdout.strip()


def git_status_clean():
    """Check if git working directory is clean."""
    result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    return result.stdout.strip() == ""


def update_version_files(new_version):
    """Update version in all relevant files."""
    # Update pyproject.toml (only project version, preserve ruff target-version)
    path = Path("pyproject.toml")
    content = path.read_text()
    # Match only the version under [project] section
    lines = content.split('\n')
    in_project = False
    for i, line in enumerate(lines):
        if line.strip() == '[project]':
            in_project = True
        elif line.strip().startswith('[') and in_project:
            in_project = False
        elif in_project and line.startswith('version ='):
            lines[i] = f'version = "{new_version}"'
            break
    updated = '\n'.join(lines)
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
        updated = re.sub(r"^version: .*$", f"version: {new_version}", content, flags=re.MULTILINE)
        citation_path.write_text(updated)
        print(f"Updated CITATION.cff to {new_version}")


def parse_version(version):
    """Parse version into major, minor, patch components."""
    # Remove .dev suffix if present
    base_version = version.split(".dev")[0]
    parts = base_version.split(".")

    # Ensure we have at least 3 parts
    while len(parts) < 3:
        parts.append("0")

    return int(parts[0]), int(parts[1]), int(parts[2])


def cmd_dev(args):
    """Set development version (.dev0 suffix) for dev branches."""
    branch = get_current_branch()
    if branch == "main":
        print("Error: Use 'release' command on main branch")
        sys.exit(1)

    current = get_current_version()

    # If already a dev version, keep it
    if ".dev" in current:
        print(f"Already on dev version: {current}")
        return

    # Parse version - stay on current version with .dev0
    major, minor, patch = parse_version(current)
    new_version = f"{major}.{minor}.{patch}.dev0"

    print(f"Branch: {branch}")
    print(f"Current version: {current}")
    print(f"Dev version: {new_version}")

    if not args.yes:
        response = input("\nSet dev version? (y/n): ")
        if response.lower() != "y":
            print("Cancelled")
            sys.exit(1)

    update_version_files(new_version)

    if not args.no_commit:
        subprocess.run(
            ["git", "add", "pyproject.toml", "statskita/__init__.py", "CITATION.cff"], check=True
        )
        subprocess.run(["git", "commit", "-m", f"chore: set dev version {new_version}"], check=True)
        print(f"Set dev version {new_version} and committed")
    else:
        print(f"Set dev version {new_version} (not committed)")


def cmd_release(args):
    """Release a new version (main branch only)."""
    branch = get_current_branch()
    if branch != "main":
        print(f"Error: Must be on main branch to release (currently on {branch})")
        sys.exit(1)

    if not git_status_clean():
        print("Error: Git working directory not clean. Commit or stash changes first.")
        sys.exit(1)

    current = get_current_version()

    # Determine new version
    if args.version:
        # Explicit version provided
        new_version = args.version
    else:
        # Auto-bump based on type
        major, minor, patch = parse_version(current)

        if args.major:
            new_version = f"{major + 1}.0.0"
        elif args.minor:
            new_version = f"{major}.{minor + 1}.0"
        else:  # patch by default
            new_version = f"{major}.{minor}.{patch + 1}"

    print(f"Current version: {current}")
    print(f"New version: {new_version}")

    if not args.yes:
        response = input("\nProceed with release? (y/n): ")
        if response.lower() != "y":
            print("Release cancelled")
            sys.exit(1)

    # Update version files
    update_version_files(new_version)

    # Commit, tag, and push
    commands = [
        ["git", "add", "pyproject.toml", "statskita/__init__.py", "CITATION.cff"],
        ["git", "commit", "-m", f"release: version {new_version}"],
        ["git", "tag", "-a", f"v{new_version}", "-m", f"release v{new_version}"],
    ]

    if not args.no_push:
        commands.extend(
            [
                ["git", "push", "origin", "main"],
                ["git", "push", "origin", f"v{new_version}"],
            ]
        )

    for cmd in commands:
        print(f"Running: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)

    print(f"\nReleased v{new_version}!")

    if not args.no_push:
        print("GitHub Actions will now publish to PyPI")
        print("Monitor: https://github.com/okkymabruri/statskita/actions")
    else:
        print("Note: Not pushed (use --no-push to disable auto-push)")

    # Set next dev version (disabled by default)
    if args.dev:
        major, minor, patch = parse_version(new_version)
        next_dev = f"{major}.{minor}.{patch + 1}.dev0"
        print(f"\nSetting next dev version: {next_dev}")
        update_version_files(next_dev)
        subprocess.run(
            ["git", "add", "pyproject.toml", "statskita/__init__.py", "CITATION.cff"], check=True
        )
        subprocess.run(["git", "commit", "-m", f"chore: open {next_dev} development"], check=True)
        print(f"Opened development for {next_dev}")


def main():
    parser = argparse.ArgumentParser(
        description="StatsKita version management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # On dev branch: set to dev version
  python scripts/version.py dev

  # On main: release patch version
  python scripts/version.py release

  # On main: release specific version
  python scripts/version.py release 0.1.5

  # On main: release minor version
  python scripts/version.py release --minor
""",
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Dev command
    dev_parser = subparsers.add_parser("dev", help="Set dev version for development branches")
    dev_parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation")
    dev_parser.add_argument("--no-commit", action="store_true", help="Don't auto-commit")

    # Release command
    release_parser = subparsers.add_parser("release", help="Release new version (main branch only)")
    release_parser.add_argument("version", nargs="?", help="Specific version (e.g., 0.1.5)")
    release_parser.add_argument("--major", action="store_true", help="Major version bump")
    release_parser.add_argument("--minor", action="store_true", help="Minor version bump")
    release_parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation")
    release_parser.add_argument("--no-push", action="store_true", help="Don't push to remote")
    release_parser.add_argument("--dev", action="store_true", help="Set next dev version after release")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "dev":
        cmd_dev(args)
    elif args.command == "release":
        cmd_release(args)


if __name__ == "__main__":
    main()
