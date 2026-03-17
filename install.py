#!/usr/bin/env python3
"""Install the Lakebridge Reconcile skill for Claude Code.

Downloads skill files directly from GitHub and places them in
.claude/skills/databricks-lakebridge-reconcile/ in the current directory.

Usage:
    python3 install.py                      # install in current directory
    python3 install.py --global             # install in ~/.claude/skills/
    curl -sL <raw-url>/install.py | python3 # one-liner from GitHub
"""

import argparse
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

REPO = "lbruand-db/lakebridge-dev-kit"
BRANCH = "main"
SKILL_NAME = "databricks-lakebridge-reconcile"
SKILL_PATH = f"databricks-skills/{SKILL_NAME}"

FILES = [
    "SKILL.md",
    "configuration.md",
    "examples.md",
    "secret_scopes.md",
]


def download_file(repo: str, branch: str, filepath: str) -> str:
    """Download a file from GitHub raw content."""
    url = f"https://raw.githubusercontent.com/{repo}/{branch}/{filepath}"
    try:
        with urllib.request.urlopen(url) as resp:
            return resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        print(f"  ERROR: Failed to download {filepath}: {e}", file=sys.stderr)
        sys.exit(1)


def install_skill(target_dir: Path, repo: str, branch: str) -> None:
    """Download and install skill files to target directory."""
    skill_dir = target_dir / "skills" / SKILL_NAME
    skill_dir.mkdir(parents=True, exist_ok=True)

    print(f"Installing {SKILL_NAME} to {skill_dir}/")
    print()

    for filename in FILES:
        remote_path = f"{SKILL_PATH}/{filename}"
        print(f"  Downloading {filename}...", end=" ", flush=True)
        content = download_file(repo, branch, remote_path)
        (skill_dir / filename).write_text(content)
        print("OK")

    print()
    print(f"Installed {len(FILES)} files to {skill_dir}/")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Install the Lakebridge Reconcile skill for Claude Code."
    )
    parser.add_argument(
        "--global",
        dest="global_install",
        action="store_true",
        help="Install to ~/.claude/ instead of ./.claude/",
    )
    parser.add_argument(
        "--repo",
        default=REPO,
        help=f"GitHub repo (default: {REPO})",
    )
    parser.add_argument(
        "--branch",
        default=BRANCH,
        help=f"Git branch (default: {BRANCH})",
    )
    args = parser.parse_args()

    if args.global_install:
        target = Path.home() / ".claude"
    else:
        target = Path.cwd() / ".claude"

    install_skill(target, args.repo, args.branch)

    print()
    print("Done! The skill is now available in Claude Code.")
    if not args.global_install:
        print("Run Claude Code from this directory to use it.")


if __name__ == "__main__":
    main()
