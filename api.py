#!/usr/bin/env python3
import hmac
import os
from typing import Annotated

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException, Response, status


DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/analysis"

app = FastAPI(title="Analysis Lookup API")


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


def configured_api_keys() -> list[str]:
    raw_keys = os.environ.get("API_KEYS", "")
    return [key.strip() for key in raw_keys.split(",") if key.strip()]


def require_api_key(x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None) -> None:
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key",
        )

    api_keys = configured_api_keys()
    if not api_keys:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="API keys are not configured",
        )

    if not any(hmac.compare_digest(x_api_key, api_key) for api_key in api_keys):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )


def fetch_payload(record_id: str) -> bytes | None:
    with psycopg.connect(database_url()) as conn:
        with conn.cursor() as cur:
            row = cur.execute(
                "SELECT payload_gzip FROM analysis_records WHERE id = %s",
                (record_id,),
            ).fetchone()

    if row is None:
        return None
    return bytes(row[0])


@app.get("/health")
def health() -> dict[str, bool]:
    try:
        with psycopg.connect(database_url()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    except psycopg.Error as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        ) from exc

    return {"ok": True}


@app.get("/analysis/{record_id}", dependencies=[Depends(require_api_key)])
def get_analysis(record_id: str) -> Response:
    payload = fetch_payload(record_id)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Analysis not found")

    return Response(
        content=payload,
        media_type="application/json",
        headers={
            "Content-Encoding": "gzip",
            "Vary": "Accept-Encoding",
        },
    )
