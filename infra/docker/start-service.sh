#!/bin/sh
# Default start command for a LedgerMesh service image (LM-047).
#
# Logs the image revision first, so the deployed version can be identified from
# application logs as well as image labels. Migrations run only when asked
# (RUN_MIGRATIONS=true), so an image can start on its own and answer /health
# without a database.
set -eu

echo "starting ${SERVICE_NAME} revision=${LEDGERMESH_REVISION} version=${LEDGERMESH_VERSION}"

if [ "${RUN_MIGRATIONS}" = "true" ]; then
    alembic -c "services/${SERVICE}/alembic.ini" upgrade head
fi

exec python -m uvicorn "services.${SERVICE}.app.main:app" --host 0.0.0.0 --port "${PORT}"
