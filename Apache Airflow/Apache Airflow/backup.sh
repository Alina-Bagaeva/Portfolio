#!/bin/bash
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="./backups"
mkdir -p "$BACKUP_DIR"

docker compose exec -T postgres \
  pg_dump -U airflow airflow > "$BACKUP_DIR/airflow_backup_${TIMESTAMP}.sql"

echo "Backup created: $BACKUP_DIR/airflow_backup_${TIMESTAMP}.sql"

find /root/Apache-Airflow/backups/ -name "*.sql" -mtime +30 -delete
find /root/Apache-Airflow/backups/ -name "*.log" -mtime +30 -delete
