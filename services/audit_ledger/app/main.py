from fastapi import FastAPI

SERVICE_NAME = "audit-ledger"

app = FastAPI(title="LedgerMesh Audit Ledger Service", version="0.1.0")


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}
