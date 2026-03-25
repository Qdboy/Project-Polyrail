#!/bin/bash
set -euo pipefail

pip install -r requirements.txt

gunicorn --bind 0.0.0.0:${PORT:-8000} --workers 1 --threads 2 app:app
