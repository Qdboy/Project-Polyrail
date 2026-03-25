#!/bin/bash
set -euo pipefail

pip install requests psycopg2-binary

python polymarket_db_ingest.py
