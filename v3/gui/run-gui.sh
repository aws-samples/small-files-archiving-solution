#!/bin/bash
# Launch the Small Files Archiver GUI. Creates .venv on first run (uv if
# available, else python3 -m venv) and installs requirements.txt.
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -x .venv/bin/python ]; then
  if command -v uv >/dev/null; then
    uv venv --python 3.12 .venv
    uv pip install --python .venv/bin/python -r requirements.txt
  else
    python3 -m venv .venv
    .venv/bin/pip install -r requirements.txt
  fi
fi
exec .venv/bin/python app.py "$@"
