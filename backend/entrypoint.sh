#!/usr/bin/env bash
set -e

echo "Waiting for database..."
python -c "
import time, sys, os, psycopg2

url = os.environ.get('DATABASE_URL', '').replace('postgresql+asyncpg', 'postgresql')
for i in range(30):
    try:
        psycopg2.connect(url)
        print('DB is ready')
        sys.exit(0)
    except Exception as e:
        print(f'DB not ready yet ({e}), retrying...')
        time.sleep(2)
sys.exit(1)
" || echo "Proceeding without confirmed DB (will retry inside app)"

echo "Running Alembic migrations..."
alembic upgrade head || echo "Alembic migration failed/skipped, continuing..."

echo "Starting FastAPI app..."
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
