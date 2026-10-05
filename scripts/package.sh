#!/usr/bin/env sh
# Build HookRelay.zip from the working tree (excludes caches, env files, node_modules, .git).
set -eu
cd "$(dirname "$0")/.."
rm -f ../HookRelay.zip
zip -r ../HookRelay.zip . -x '.git/*' '*/__pycache__/*' '*.pyc' '.env' '*/node_modules/*' '.pytest_cache/*' '.venv/*' '*.log'
echo "created ../HookRelay.zip"
