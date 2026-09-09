"""HTTP health-endpoint notifications for the incremental medallion pipelines.

The orchestrator pings a monitoring dashboard at three moments so the
dashboard can track liveness, detect missed runs (dead-man's switch), and
report failures:

    POST <PIPELINE_HEALTH_URL>/start   -> pipeline started
    POST <PIPELINE_HEALTH_URL>/fail    -> a stage failed
    POST <PIPELINE_HEALTH_URL>         -> pipeline finished successfully

Each POST carries a JSON body (pipeline, run_id, status, stage, error,
elapsed, timestamp) for dashboards that store request bodies.

This works with Healthchecks.io-style ping URLs, Uptime Kuma "Push"
monitors, or any HTTP endpoint that accepts POST requests.

Configuration (environment variables):

    PIPELINE_HEALTH_URL      base ping URL, e.g. https://hc-ping.com/<uuid>
    PIPELINE_HEALTH_TIMEOUT  request timeout in seconds (default: 10)

Health pings are disabled until PIPELINE_HEALTH_URL is set, and ping
failures are only logged -- they never fail the pipeline.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

_ENV_URL = "PIPELINE_HEALTH_URL"
_ENV_TIMEOUT = "PIPELINE_HEALTH_TIMEOUT"
_DEFAULT_TIMEOUT = 10.0
_MAX_ERROR_LENGTH = 2000


def _base_url() -> str:
    return os.getenv(_ENV_URL, "").strip()


def _timeout() -> float:
    try:
        return float(os.getenv(_ENV_TIMEOUT, _DEFAULT_TIMEOUT))
    except ValueError:
        return _DEFAULT_TIMEOUT


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _truncate(text: str | None, limit: int = _MAX_ERROR_LENGTH) -> str | None:
    if text is None:
        return None
    return text if len(text) <= limit else text[:limit] + "...(truncated)"


def _post(path: str, body: dict[str, object]) -> bool:
    """POST JSON to the configured health endpoint. Never raises."""
    base_url = _base_url()
    if not base_url:
        logger.debug("Health endpoint not configured (%s); ping skipped", _ENV_URL)
        return False

    target = f"{base_url}{path}"
    payload = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        target,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=_timeout()) as response:
            logger.info("Health ping sent: %s -> HTTP %s", target, response.status)
            return True
    except (urllib.error.URLError, OSError, ValueError) as exc:
        logger.warning("Health ping failed (%s): %s", target, exc)
        return False


def _message(
    *,
    pipeline: str,
    run_id: str,
    status: str,
    stage: str | None = None,
    error: str | None = None,
    elapsed: float | None = None,
) -> dict[str, object]:
    message: dict[str, object] = {
        "pipeline": pipeline,
        "run_id": run_id,
        "status": status,
        "message": f"{pipeline} pipeline {status}",
        "timestamp": _now_iso(),
    }
    if stage is not None:
        message["stage"] = stage
    if error is not None:
        message["error"] = _truncate(error)
    if elapsed is not None:
        message["elapsed_s"] = round(elapsed, 2)
    return message


def notify_start(pipeline: str, run_id: str) -> bool:
    """Signal that the pipeline run has started."""
    return _post("/start", _message(pipeline=pipeline, run_id=run_id, status="started"))


def notify_success(pipeline: str, run_id: str, elapsed: float) -> bool:
    """Signal that the pipeline completed successfully."""
    return _post(
        "",
        _message(pipeline=pipeline, run_id=run_id, status="success", elapsed=elapsed),
    )


def notify_failure(
    pipeline: str,
    run_id: str,
    stage: str,
    error: str,
    elapsed: float,
) -> bool:
    """Signal that a pipeline stage failed."""
    return _post(
        "/fail",
        _message(
            pipeline=pipeline,
            run_id=run_id,
            status="failure",
            stage=stage,
            error=error,
            elapsed=elapsed,
        ),
    )


# Health ping helpers for pipeline liveness dashboards.
__all__ = ["notify_failure", "notify_start", "notify_success"]
