#!/bin/bash
set -euo pipefail

pip install requests psycopg2-binary psycopg2

python polymarket_db_ingest.py
