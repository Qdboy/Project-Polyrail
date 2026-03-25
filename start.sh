#!/bin/bash
set -euo pipefail

pip install requests psycopg2-binary flask

python app.py
