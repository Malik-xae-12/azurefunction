"""
HubSpot Webhook Signature Verification — v3
Always enforced. No local bypass. Set HUBSPOT_APP_SECRET in .env.

Spec: https://developers.hubspot.com/docs/api/webhooks/validating-requests
"""

import base64
import hashlib
import hmac
import os
import time
from urllib.parse import unquote
import logging

from fastapi import HTTPException, Request

logger = logging.getLogger(__name__)


async def verify_hubspot_signature(request: Request) -> bool:
    """
    Dependency — validates every incoming HubSpot webhook request.

    HubSpot signs: METHOD + URL + body + timestamp (raw ms string)
    Algorithm    : HMAC-SHA256 → base64
    Header       : x-hubspot-signature-v3
    Timestamp    : x-hubspot-request-timestamp (milliseconds)
    Window       : ±5 minutes
    """
    app_secret = os.getenv("HUBSPOT_APP_SECRET", "").strip()
    if not app_secret:
        logger.critical(
            "HUBSPOT_APP_SECRET is not set. "
            "All webhook requests will be rejected until it is configured."
        )
        raise HTTPException(
            status_code=500,
            detail=(
                "Server misconfiguration: HUBSPOT_APP_SECRET is not set. "
                "Add it to your .env file and restart."
            ),
        )

    signature = request.headers.get("x-hubspot-signature-v3", "").strip()
    timestamp = request.headers.get("x-hubspot-request-timestamp", "").strip()

    if not signature:
        raise HTTPException(status_code=401, detail="Missing x-hubspot-signature-v3 header")
    if not timestamp:
        raise HTTPException(status_code=401, detail="Missing x-hubspot-request-timestamp header")

    # ── Timestamp validation ──────────────────────────────────────────────────
    try:
        timestamp_ms = float(timestamp)
        # HubSpot always sends milliseconds (13-digit epoch)
        timestamp_sec = timestamp_ms / 1000.0 if timestamp_ms > 1_000_000_000_000 else timestamp_ms
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid timestamp format")

    age_seconds = abs(time.time() - timestamp_sec)
    if age_seconds > 300:
        raise HTTPException(
            status_code=401,
            detail=f"Request timestamp is outside the 5-minute window (age: {age_seconds:.0f}s)",
        )

    # ── URI construction ──────────────────────────────────────────────────────
    # When behind a proxy (Azure Functions, Render, AWS ALB) the internal URL
    # differs from the URL HubSpot posted to. Use forwarded headers when present.
    forwarded_host = request.headers.get("x-forwarded-host", "").strip()
    if forwarded_host:
        scheme = request.headers.get("x-forwarded-proto", "https").strip()
        path = request.url.path
        query = f"?{request.url.query}" if request.url.query else ""
        uri = f"{scheme}://{forwarded_host}{path}{query}"
    else:
        uri = str(request.url)

    # HubSpot URL-decodes the URI before signing
    parsed_uri = unquote(uri)

    # ── Read raw body ─────────────────────────────────────────────────────────
    # FastAPI caches request.body() so downstream route handlers can still read it.
    body_bytes = await request.body()
    body_str = body_bytes.decode("utf-8")

    # ── Build source string (exact HubSpot spec) ──────────────────────────────
    # METHOD + URI + body + raw_timestamp_string (NOT the float)
    source_string = request.method + parsed_uri + body_str + timestamp

    # ── HMAC-SHA256 + base64 ──────────────────────────────────────────────────
    hash_bytes = hmac.new(
        app_secret.encode("utf-8"),
        source_string.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    expected_signature = base64.b64encode(hash_bytes).decode("utf-8")

    # ── Constant-time comparison ──────────────────────────────────────────────
    if not hmac.compare_digest(signature, expected_signature):
        logger.warning(
            "HubSpot signature mismatch — "
            "possible replay attack, wrong secret, or proxy URL mismatch. "
            "URI used for signing: %s",
            parsed_uri,
        )
        raise HTTPException(status_code=401, detail="Invalid HubSpot signature")

    logger.debug("HubSpot signature verified OK for %s %s", request.method, parsed_uri)
    return True