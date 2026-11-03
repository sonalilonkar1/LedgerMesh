from fastapi import FastAPI

SERVICE_NAME = "coordinator"

app = FastAPI(title="LedgerMesh Redemption Coordinator", version="0.1.0")


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}
