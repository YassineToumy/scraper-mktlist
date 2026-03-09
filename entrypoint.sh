#!/bin/bash


echo "══════════════════════════════════════════════════"
echo "🇨🇦 MktList Service Starting"
echo "   Time: $(date -u)"
echo "══════════════════════════════════════════════════"

# Export env vars with proper shell quoting (URIs with @ ? & break plain sourcing)
env | while IFS='=' read -r k v; do
    [[ "$k" == "no_proxy" || "$k" == "_" ]] && continue
    printf 'export %s=%q\n' "$k" "$v"
done > /app/.env_runtime
printenv | grep -v -E "^(no_proxy|_)=" >> /etc/environment

cat > /etc/cron.d/mktlist <<'CRON'
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin

# Scraper — every 24h at 04:00 UTC
0 4 * * * root /app/runner.sh scraper >> /app/logs/cron.log 2>&1

# Cleaner — every 24h at 06:00 UTC (after scraper)
0 6 * * * root /app/runner.sh cleaner >> /app/logs/cron.log 2>&1

# Sync to PostgreSQL — every 24h at 08:00 UTC (after cleaner)
0 8 * * * root /app/runner.sh sync >> /app/logs/cron.log 2>&1

CRON

chmod 0644 /etc/cron.d/mktlist

echo "✅ Cron schedule installed:"
echo "   04:00  🕷️  Scraper (daily)"
echo "   06:00  🧹 Cleaner (daily)"
echo "   08:00  🔄 Sync to PostgreSQL (daily)"
echo ""

echo "🔌 Testing MongoDB..."
python -c "
from pymongo import MongoClient
import os
c = MongoClient(os.environ['MONGODB_URI'], serverSelectionTimeoutMS=5000)
c.admin.command('ping')
print('  ✅ MongoDB OK')
c.close()
" || echo "  ❌ MongoDB connection failed"

echo "🔌 Testing PostgreSQL..."
python -c "
import psycopg2, os
conn = psycopg2.connect(os.environ['POSTGRES_DSN'])
cur = conn.cursor()
cur.execute('SELECT 1')
print('  ✅ PostgreSQL OK')
conn.close()
" || echo "  ❌ PostgreSQL connection failed"

echo ""
echo "🔄 Running initial jobs on startup..."

/app/runner.sh scraper  || echo "⚠️  Scraper startup failed — will retry via cron"
/app/runner.sh cleaner  || echo "⚠️  Cleaner startup failed — will retry via cron"
/app/runner.sh sync     || echo "⚠️  Sync startup failed — will retry via cron"

echo "══════════════════════════════════════════════════"
echo "✅ Startup complete — cron daemon starting"
echo "══════════════════════════════════════════════════"

exec cron -f
