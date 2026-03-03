#!/bin/bash
set -uo pipefail

set -a; source /etc/environment 2>/dev/null || true; set +a

JOB="$1"
TIMESTAMP=$(date -u +"%Y-%m-%d_%H-%M-%S")
LOGFILE="/app/logs/${JOB}_${TIMESTAMP}.log"
PIDFILE="/tmp/${JOB}.pid"

echo "══════════════════════════════════════════════════"
echo "🚀 [$TIMESTAMP] Starting: ${JOB}"
echo "══════════════════════════════════════════════════"

if [ -f "$PIDFILE" ] && kill -0 "$(cat $PIDFILE)" 2>/dev/null; then
    echo "⚠️  ${JOB} already running (PID $(cat $PIDFILE)) — skip"
    exit 0
fi

cd /app

case "$JOB" in
    scraper)
        python -u scraper.py 2>&1 | tee "$LOGFILE"
        ;;
    sync)
        python -u sync.py 2>&1 | tee "$LOGFILE"
        ;;
    *)
        echo "❌ Unknown job: ${JOB}. Use: scraper | sync"
        exit 1
        ;;
esac

EXIT_CODE=$?
rm -f "$PIDFILE"

[ $EXIT_CODE -eq 0 ] && echo "✅ ${JOB} done ($(date -u))" || echo "❌ ${JOB} failed (exit ${EXIT_CODE})"

find /app/logs -name "${JOB}_*.log" -mtime +30 -delete 2>/dev/null || true
exit $EXIT_CODE
