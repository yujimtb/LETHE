#!/usr/bin/env python3
"""Shared building blocks for the v15 production-scale memory gate.

This module is imported by ``seed_gate_corpus.py`` and ``run_gate.py``. It
never talks to a production host: callers are responsible for pointing
``--base-url`` at a loopback (127.0.0.1) gate-only container.

Secrecy contract: values read from a deploy ``.env`` file (API tokens,
storage encryption keys) MUST NEVER be written to stdout/stderr, exception
messages, or the JSON report. Only *key names* may be logged.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import threading
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

# ---------------------------------------------------------------------------
# HTTP backend (httpx preferred, requests as a self-contained fallback)
# ---------------------------------------------------------------------------

try:
    import httpx as _httpx  # type: ignore

    _HTTP_BACKEND = "httpx"
except ImportError:  # pragma: no cover - environment dependent
    _httpx = None
    try:
        import requests as _requests  # type: ignore

        _HTTP_BACKEND = "requests"
    except ImportError as error:  # pragma: no cover
        raise ImportError(
            "gate_common requires either 'httpx' or 'requests'. "
            "Install one with: pip install httpx"
        ) from error


class GateError(Exception):
    """Raised for any gate-rig failure. Messages must never contain secrets."""


# ---------------------------------------------------------------------------
# (a) .env-file token loading — values never leave this process's memory
# ---------------------------------------------------------------------------


def read_env_file(path: Path) -> dict[str, str]:
    """Parse a deploy-style ``KEY=value`` file.

    Blank lines and ``#`` comments are ignored. Surrounding single or double
    quotes on the value are stripped. The returned dict's values must never
    be printed, logged, or embedded in an exception message — only the key
    names are safe to surface.
    """
    path = Path(path)
    if not path.is_file():
        raise GateError(f"env file not found: {path}")
    values: dict[str, str] = {}
    for lineno, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            raise GateError(f"env file line {lineno} is not KEY=value shaped")
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def require_env_value(env: dict[str, str], key: str) -> str:
    """Fetch ``key`` from a loaded env dict. Never include the value in errors."""
    if key not in env or env[key] == "":
        raise GateError(f"env file is missing a value for required key: {key}")
    return env[key]


# ---------------------------------------------------------------------------
# (b) discord-message shaped ObservationDraft generation (v1 import shape)
# ---------------------------------------------------------------------------
#
# Mirrors crates/adapters/discord/src/discord.rs::DiscordAdapter::map_message
# and crates/adapters/api/src/idempotency.rs::identity_key. The wire target
# is POST /api/import/observation-drafts with body
# {"source_instance_id": ..., "drafts": [...]}. This is *not* the harness in
# scripts/import_memory_harness.py, which drives a local CLI, not the HTTP
# import endpoint.

CANONICAL_IDENTITY_SOURCE = "discord"


def normalize_canonical_body(body: str) -> str:
    """Match Rust's normalize_canonical_body: CRLF -> LF, then NFC."""
    return unicodedata.normalize("NFC", body.replace("\r\n", "\n"))


def format_rfc3339(moment: datetime) -> str:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    moment = moment.astimezone(timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def canonical_identity(
    object_id: str, sender: str, body: str, event_time: datetime
) -> tuple[str, str]:
    """Return (canonical_json, idempotency_key) for a discord-message draft.

    The exact key ordering of canonical_json does not need to byte-match the
    Rust adapter: the v1 import endpoint stores whatever meta.canonical_json
    the client sends and only compares it against itself on retries (for
    duplicate vs. canonical-collision detection). What matters is that this
    function is deterministic for a given (object_id, sender, body,
    event_time) tuple so repeated runs reproduce the same key.
    """
    tuple_value = {
        "sender": sender,
        "body": normalize_canonical_body(body),
        "event_time": format_rfc3339(event_time),
    }
    canonical_json = json.dumps(
        tuple_value, separators=(",", ":"), ensure_ascii=False, sort_keys=True
    )
    digest = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    idempotency_key = f"{CANONICAL_IDENTITY_SOURCE}:{object_id}:{digest}"
    return canonical_json, idempotency_key


def build_discord_draft(
    *,
    source_instance_id: str,
    channel_id: str,
    message_id: str,
    content_prefix: str,
    index: int,
    published: datetime,
    pad_bytes: int = 0,
    author_id: str = "gate-probe-author",
    author_name: str = "gate-probe",
    is_dm: bool = True,
    guild_id: str | None = None,
    guild_name: str | None = None,
    channel_name: str | None = None,
    mentions: list[str] | None = None,
    referenced_message_id: str | None = None,
    client_ref: str | None = None,
) -> dict[str, Any]:
    """Build one ObservationDraft JSON object shaped like a discord message.

    ``source_instance_id`` is not embedded in the draft itself (the wire
    request carries it once, at the top level) — the self-host server
    namespaces the client-supplied idempotency_key with it
    (service_support.rs::namespace_draft). It is threaded through here only
    so callers cannot accidentally regenerate a draft with a different
    source_instance_id than the one it will be POSTed under, which would
    silently break duplicate detection. It is recorded in meta for
    traceability.
    """
    content = f"{content_prefix} {index}"
    if pad_bytes > 0:
        content = f"{content} " + ("x" * pad_bytes)

    object_id = f"{channel_id}:{message_id}"
    canonical_json, idempotency_key = canonical_identity(
        object_id, author_id, content, published
    )
    thread_ref = f"discord:thread:{referenced_message_id or message_id}"

    draft: dict[str, Any] = {
        "schema": "schema:discord-message",
        "schema_version": "1.0.0",
        "observer": "obs:discord-importer",
        "source_system": "sys:discord",
        "authority_model": "lake_authoritative",
        "capture_model": "event",
        "subject": f"message:discord:{channel_id}:{message_id}",
        "payload": {
            "channel_id": channel_id,
            "message_id": message_id,
            "timestamp": format_rfc3339(published),
            "author_id": author_id,
            "author_name": author_name,
            "content": content,
            "is_dm": is_dm,
            "guild_id": guild_id,
            "guild_name": guild_name,
            "channel_name": channel_name,
            "mentions": mentions or [],
            "referenced_message_id": referenced_message_id,
        },
        "attachments": [],
        "published": format_rfc3339(published),
        "idempotency_key": idempotency_key,
        "meta": {
            "sourceAdapterVersion": "gate-probe-1.0.0",
            "object_id": object_id,
            "canonical_json": canonical_json,
            "communication_channel_kind": "discord",
            "communication_channel_external_id": channel_id,
            "communication_sender_id": author_id,
            "communication_thread_ref": thread_ref,
            "gate_source_instance_id": source_instance_id,
        },
    }
    if client_ref is not None:
        draft["client_ref"] = client_ref
    return draft


# ---------------------------------------------------------------------------
# Deterministic gate corpus scheme shared by seed_gate_corpus.py and
# run_gate.py. Both scripts MUST use the same (tag, source_instance_id,
# index) to reproduce byte-identical drafts, since dup-only tests depend on
# reconstructing exactly what was seeded.
# ---------------------------------------------------------------------------

CORPUS_BASE_TIME = datetime(2020, 1, 1, tzinfo=timezone.utc)
DEFAULT_PAD_BYTES = 512


def corpus_channel_id(tag: str) -> str:
    return f"gate-probe:{tag}"


def corpus_message_id(index: int) -> str:
    return f"msg-{index:09d}"


def near_now_published(offset_minutes: float = 2.0) -> datetime:
    """A fixed "recent past" timestamp, independent of any corpus index.

    For probes whose index is not a small, bounded corpus offset (e.g. the
    ACK-convergence probe's ever-growing attempt counter), never derive
    ``published`` from the index — see corpus_published()'s docstring for
    why that goes wrong at large index values.
    """
    return datetime.now(timezone.utc) - timedelta(minutes=offset_minutes)


def corpus_published(index: int) -> datetime:
    """CORPUS_BASE_TIME + index seconds, clamped so it can never land in
    the future.

    This is only "safely in the past" while ``index`` stays within roughly
    CORPUS_BASE_TIME's distance from now (a couple hundred million seconds
    as of 2026) — the seeded corpus range and the small fixed offsets tests
    2/3 add to --seed-count are well within that. It is *not* safe for an
    unbounded/large index: e.g. the ACK-convergence probe's
    900_000_000+attempt indices push CORPUS_BASE_TIME (2020-01-01) into the
    2040s, which the server's clock-skew gate rejects with HTTP 400
    ("published is too far in the future"). Callers with an unbounded index
    space must pass an explicit published time instead (see
    near_now_published() / build_corpus_draft(published_override=...)).
    This clamp is a blanket safety net for every caller, audited or not.
    """
    candidate = CORPUS_BASE_TIME + timedelta(seconds=index)
    safety_ceiling = datetime.now(timezone.utc) - timedelta(minutes=5)
    return min(candidate, safety_ceiling)


def build_corpus_draft(
    *,
    tag: str,
    source_instance_id: str,
    index: int,
    pad_bytes: int = DEFAULT_PAD_BYTES,
    published_override: datetime | None = None,
) -> dict[str, Any]:
    """Build a corpus draft for ``index``.

    ``published_override``, when given, replaces the index-derived
    timestamp entirely — required for callers whose index is not a small,
    bounded corpus offset (see corpus_published()'s docstring).
    """
    published = published_override if published_override is not None else corpus_published(index)
    return build_discord_draft(
        source_instance_id=source_instance_id,
        channel_id=corpus_channel_id(tag),
        message_id=corpus_message_id(index),
        content_prefix="gate probe synthetic message",
        index=index,
        published=published,
        pad_bytes=pad_bytes,
        client_ref=str(index),
    )


# ---------------------------------------------------------------------------
# (c) Import submission + outcome tallying
# ---------------------------------------------------------------------------


@dataclass
class OutcomeCounts:
    ingested: int = 0
    duplicates: int = 0
    quarantined: int = 0
    rejected: int = 0

    @property
    def total(self) -> int:
        return self.ingested + self.duplicates + self.quarantined + self.rejected

    @classmethod
    def from_report(cls, report: dict[str, Any]) -> "OutcomeCounts":
        return cls(
            ingested=int(report.get("ingested", 0)),
            duplicates=int(report.get("duplicates", 0)),
            quarantined=int(report.get("quarantined", 0)),
            rejected=int(report.get("rejected", 0)),
        )

    def __add__(self, other: "OutcomeCounts") -> "OutcomeCounts":
        return OutcomeCounts(
            ingested=self.ingested + other.ingested,
            duplicates=self.duplicates + other.duplicates,
            quarantined=self.quarantined + other.quarantined,
            rejected=self.rejected + other.rejected,
        )

    def as_dict(self) -> dict[str, int]:
        return {
            "ingested": self.ingested,
            "duplicates": self.duplicates,
            "quarantined": self.quarantined,
            "rejected": self.rejected,
        }


@dataclass
class ImportBatchResult:
    report: dict[str, Any]
    elapsed_seconds: float
    counts: OutcomeCounts = field(init=False)

    def __post_init__(self) -> None:
        self.counts = OutcomeCounts.from_report(self.report)


class ImportRequestError(GateError):
    def __init__(self, status_code: int, body_text: str) -> None:
        super().__init__(
            f"import request failed with HTTP {status_code}: {body_text[:500]}"
        )
        self.status_code = status_code


class TransientImportError(GateError):
    """A network-level failure (timeout / connection error / 5xx) on a single
    import attempt.

    This is expected to happen — e.g. a fresh container doing backlog
    catch-up can take well over a minute to ACK a single import. Callers
    must treat this as "not converged yet" / "this attempt's latency
    exceeded", not as an unhandled crash. It is a GateError only so a
    caller that doesn't specifically handle it still gets a readable
    "GATE FAILED" message instead of the raw httpx/requests exception
    surfacing as "unexpected error".
    """

    def __init__(
        self,
        message: str,
        *,
        elapsed_seconds: float,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.elapsed_seconds = elapsed_seconds
        self.status_code = status_code


class ImportConcurrencyLimitError(TransientImportError):
    """HTTP 429 ``import_concurrency_limit`` — the server's bounded import
    permit pool (config ``limits.max_concurrent_imports``) is currently
    full.

    This commonly shows up as a *chain*: a client gives up on a slow import
    (timeout) while the server keeps processing it and holding a permit;
    the next probe/batch then gets 429 until that orphaned request finally
    finishes server-side and releases the permit — which has nothing to do
    with how long *this* request waits. Must be retried after
    ``retry_after_seconds`` (from the response body, matching
    apps/selfhost/src/self_host/import_client.rs's own 429 handling), never
    treated as a hard 4xx failure.
    """

    def __init__(
        self, message: str, *, elapsed_seconds: float, retry_after_seconds: float
    ) -> None:
        super().__init__(message, elapsed_seconds=elapsed_seconds, status_code=429)
        self.retry_after_seconds = retry_after_seconds


class BulkSessionConflictError(TransientImportError):
    """HTTP 409 from a bulk-session begin/end call whose error code is a
    known *transient* state, not a real usage error.

    Confirmed codes (apps/selfhost/src/self_host/app/bulk_import.rs):
    ``bulk_import_session_active`` — another session is open, which for our
    own strictly-serialized begin/import/end usage most often means an
    earlier ``begin`` whose *client* gave up on a timeout while the server
    went on to create the session anyway (the same orphan pattern as HTTP
    429 import_concurrency_limit — see ImportConcurrencyLimitError), or a
    brief lock hold-over from the immediately preceding ``end``.
    ``bulk_import_non_bulk_projection_active`` is included per a live-run
    report of this exact 409 code from the bulk-session endpoints; its
    construction site was not found in this checkout at the time this was
    written; a background (non-bulk-session) projection rebuild being
    briefly active is a plausible, transient cause consistent with its
    name. If the server ever emits it with different casing/spelling this
    allowlist needs a follow-up correction.

    These codes carry no ``retry_after`` in the response body (unlike HTTP
    429), so a short fixed wait is used instead. Retried up to a bounded
    count, never treated as a hard 4xx failure — any *other* 409 code
    (e.g. ``bulk_import_session_mismatch``, a real logic error) is not in
    this set and still raises ImportRequestError immediately.
    """

    def __init__(self, message: str, *, elapsed_seconds: float, conflict_code: str) -> None:
        super().__init__(message, elapsed_seconds=elapsed_seconds, status_code=409)
        self.conflict_code = conflict_code


BULK_SESSION_RETRYABLE_CONFLICT_CODES = {
    "bulk_import_session_active",
    "bulk_import_non_bulk_projection_active",
}


def _parse_error_code(body_text: str) -> str | None:
    """Read the ``error`` field from a JSON ErrorResponse body, if present."""
    try:
        payload = json.loads(body_text)
        value = payload.get("error") if isinstance(payload, dict) else None
        return value if isinstance(value, str) else None
    except (ValueError, TypeError, AttributeError):
        return None


def _parse_retry_after_seconds(body_text: str, default_seconds: float = 1.0) -> float:
    """Read ``retry_after`` (seconds) from a 429 response body, matching the
    server's ErrorResponse shape ({"error", "detail", "details",
    "retry_after"}). Falls back to ``default_seconds`` if absent/unparsable
    — mirrors import_client.rs::IMPORT_CONCURRENCY_DEFAULT_RETRY_AFTER_SECS.
    """
    try:
        payload = json.loads(body_text)
        value = payload.get("retry_after") if isinstance(payload, dict) else None
        if isinstance(value, (int, float)) and value >= 0:
            return float(value)
    except (ValueError, TypeError, AttributeError):
        pass
    return default_seconds


def _is_transient_network_error(error: BaseException) -> bool:
    """True for timeouts/connection-level failures from whichever HTTP
    backend is active (never for a clean non-2xx HTTP response, which is
    handled separately by status code)."""
    if _HTTP_BACKEND == "httpx":
        return isinstance(error, _httpx.TransportError)  # type: ignore[union-attr]
    return isinstance(
        error,
        (
            _requests.exceptions.Timeout,  # type: ignore[union-attr]
            _requests.exceptions.ConnectionError,  # type: ignore[union-attr]
            _requests.exceptions.ChunkedEncodingError,  # type: ignore[union-attr]
        ),
    )


@dataclass
class BulkSessionResult:
    """Response from POST /api/import/bulk-sessions/{begin,{id}/end}.

    ``report`` mirrors BulkImportSessionReport (session_id, state,
    base_append_seq, target_append_seq, target_observation_count) from
    apps/selfhost/src/self_host/app/bulk_import.rs.
    """

    report: dict[str, Any]
    elapsed_seconds: float

    @property
    def session_id(self) -> str:
        return self.report["session_id"]

    @property
    def state(self) -> str:
        return self.report.get("state", "<unknown>")


# HTTP statuses that indicate the bulk-session API family is unusable in
# this environment (auth/scope/routing), as opposed to a real business-logic
# conflict (e.g. a stray already-active session, which is a genuine failure
# worth surfacing, not a "skip"). Per gate policy, only this class of error
# causes bulk-session tests to be marked skipped instead of failed.
SESSION_API_UNAVAILABLE_STATUS_CODES = {401, 403, 404, 405, 501}


# Windows Docker Desktop's loopback port proxy has been observed to stall
# HTTP keep-alive connection reuse at the transport layer: a request whose
# server-side handler logged 349ms of actual work took 241s to return to
# the client, with zero server-side activity for the intervening ~4
# minutes — i.e. the stall was in transport, before the request ever
# reached the handler. Every request therefore forces a fresh connection
# (``Connection: close``, plus disabling httpx's keepalive pool outright)
# rather than reusing one. The performance cost of a fresh TCP+TLS-less
# loopback connection per request is negligible for this gate's purposes.
_NO_KEEPALIVE_HEADERS = {"Connection": "close"}
if _HTTP_BACKEND == "httpx":
    _NO_KEEPALIVE_LIMITS = _httpx.Limits(  # type: ignore[union-attr]
        max_keepalive_connections=0, max_connections=1
    )


def _http_post_json(
    url: str, json_body: dict[str, Any], headers: dict[str, str], timeout: float
) -> tuple[int, Any]:
    request_headers = {**headers, **_NO_KEEPALIVE_HEADERS}
    if _HTTP_BACKEND == "httpx":
        with _httpx.Client(  # type: ignore[union-attr]
            timeout=timeout, limits=_NO_KEEPALIVE_LIMITS
        ) as client:
            response = client.post(url, json=json_body, headers=request_headers)
            return response.status_code, response
    response = _requests.post(  # type: ignore[union-attr]
        url, json=json_body, headers=request_headers, timeout=timeout
    )
    return response.status_code, response


def _http_get(url: str, timeout: float) -> tuple[int, Any]:
    if _HTTP_BACKEND == "httpx":
        with _httpx.Client(  # type: ignore[union-attr]
            timeout=timeout, limits=_NO_KEEPALIVE_LIMITS
        ) as client:
            response = client.get(url, headers=_NO_KEEPALIVE_HEADERS)
            return response.status_code, response
    response = _requests.get(  # type: ignore[union-attr]
        url, headers=_NO_KEEPALIVE_HEADERS, timeout=timeout
    )
    return response.status_code, response


class ImportClient:
    """Thin client for POST /api/import/observation-drafts."""

    def __init__(self, base_url: str, write_token: str, default_timeout: float = 60.0):
        self.base_url = base_url.rstrip("/")
        self._token = write_token
        self.default_timeout = default_timeout

    def send_drafts(
        self,
        source_instance_id: str,
        drafts: list[dict[str, Any]],
        *,
        bulk_session_id: str | None = None,
        timeout: float | None = None,
    ) -> ImportBatchResult:
        url = f"{self.base_url}/api/import/observation-drafts"
        body: dict[str, Any] = {
            "source_instance_id": source_instance_id,
            "drafts": drafts,
        }
        if bulk_session_id is not None:
            body["bulk_session_id"] = bulk_session_id
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        request_timeout = timeout or self.default_timeout
        started = time.monotonic()
        try:
            status_code, response = _http_post_json(url, body, headers, request_timeout)
        except Exception as error:
            elapsed = time.monotonic() - started
            if _is_transient_network_error(error):
                raise TransientImportError(
                    f"{type(error).__name__} after {elapsed:.1f}s "
                    f"(request timeout was {request_timeout:.1f}s): {error}",
                    elapsed_seconds=elapsed,
                ) from error
            raise
        elapsed = time.monotonic() - started
        if status_code == 429:
            body_text = getattr(response, "text", "")
            retry_after = _parse_retry_after_seconds(body_text)
            raise ImportConcurrencyLimitError(
                f"HTTP 429 import_concurrency_limit after {elapsed:.1f}s "
                f"(retry_after={retry_after:.1f}s): {body_text[:300]}",
                elapsed_seconds=elapsed,
                retry_after_seconds=retry_after,
            )
        if status_code >= 500:
            body_text = getattr(response, "text", "<no body>")
            raise TransientImportError(
                f"HTTP {status_code} after {elapsed:.1f}s: {body_text[:300]}",
                elapsed_seconds=elapsed,
                status_code=status_code,
            )
        if status_code != 200:
            body_text = getattr(response, "text", "<no body>")
            raise ImportRequestError(status_code, body_text)
        report = response.json()
        return ImportBatchResult(report=report, elapsed_seconds=elapsed)

    def health(self, timeout: float = 5.0) -> tuple[bool, int | None]:
        try:
            status_code, _response = _http_get(f"{self.base_url}/health", timeout)
        except Exception:
            return False, None
        return status_code == 200, status_code

    def begin_bulk_session(self, timeout: float | None = None) -> BulkSessionResult:
        """POST /api/import/bulk-sessions/begin — no request body."""
        return self._bulk_session_call(
            f"{self.base_url}/api/import/bulk-sessions/begin", timeout
        )

    def end_bulk_session(
        self, session_id: str, timeout: float | None = None
    ) -> BulkSessionResult:
        """POST /api/import/bulk-sessions/{session_id}/end — no request body.

        This is the call that triggers the (potentially expensive)
        materialized-snapshot refresh / non-corpus-projection rebuild in
        apps/selfhost/src/self_host/app/bulk_import.rs::end_bulk_import_session
        when the session's target isn't already materialized — the code
        path the corpus-scale-proportional-rebuild regression lives in.
        """
        return self._bulk_session_call(
            f"{self.base_url}/api/import/bulk-sessions/{session_id}/end", timeout
        )

    def _bulk_session_call(self, url: str, timeout: float | None) -> BulkSessionResult:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        request_timeout = timeout or self.default_timeout
        started = time.monotonic()
        try:
            status_code, response = _http_post_json(url, {}, headers, request_timeout)
        except Exception as error:
            elapsed = time.monotonic() - started
            if _is_transient_network_error(error):
                raise TransientImportError(
                    f"{type(error).__name__} after {elapsed:.1f}s "
                    f"(request timeout was {request_timeout:.1f}s): {error}",
                    elapsed_seconds=elapsed,
                ) from error
            raise
        elapsed = time.monotonic() - started
        if status_code == 409:
            body_text = getattr(response, "text", "")
            conflict_code = _parse_error_code(body_text)
            if conflict_code in BULK_SESSION_RETRYABLE_CONFLICT_CODES:
                raise BulkSessionConflictError(
                    f"HTTP 409 {conflict_code} after {elapsed:.1f}s: {body_text[:300]}",
                    elapsed_seconds=elapsed,
                    conflict_code=conflict_code,
                )
            raise ImportRequestError(status_code, body_text)
        if status_code >= 500:
            body_text = getattr(response, "text", "<no body>")
            raise TransientImportError(
                f"HTTP {status_code} after {elapsed:.1f}s: {body_text[:300]}",
                elapsed_seconds=elapsed,
                status_code=status_code,
            )
        if status_code != 200:
            body_text = getattr(response, "text", "<no body>")
            raise ImportRequestError(status_code, body_text)
        return BulkSessionResult(report=response.json(), elapsed_seconds=elapsed)


@dataclass
class RetryOutcome:
    """Result of send_drafts_with_retry: the eventual success, plus a record
    of any transient (timeout/connection/5xx) and 429 concurrency-limit
    attempts that preceded it."""

    result: ImportBatchResult
    attempts: int
    transient_events: list[dict[str, Any]]
    concurrency_events: list[dict[str, Any]] = field(default_factory=list)


def send_drafts_with_retry(
    client: "ImportClient",
    source_instance_id: str,
    drafts: list[dict[str, Any]],
    *,
    bulk_session_id: str | None = None,
    timeout: float | None = None,
    max_consecutive_timeouts: int = 3,
    max_consecutive_concurrency_retries: int = 60,
    context: str,
    on_transient: Any = None,
    on_concurrency_limit: Any = None,
) -> RetryOutcome:
    """Send one import batch, retrying on transient network failures.

    Two independent, separately-counted failure streaks, each reset by any
    event of the *other* kind (or by success):

    - timeout / connection-error / 5xx: recorded as a latency spike for
      that batch (not an unhandled crash) and the batch is retried
      immediately. ``max_consecutive_timeouts`` in a row raises a
      GateError explaining the abort.
    - HTTP 429 ``import_concurrency_limit``: the server's bounded import
      permit pool is full, commonly because an earlier request whose
      *client* gave up via timeout is still being processed server-side
      and holding a permit. This is not counted against
      ``max_consecutive_timeouts`` — it is retried after the server's
      ``retry_after`` hint, up to ``max_consecutive_concurrency_retries``
      times (default 60), which raises a GateError if the permit never
      clears.

    ``on_transient`` / ``on_concurrency_limit`` (if given) are each called
    once per matching attempt, e.g. to log a single line before retrying.
    """
    transient_events: list[dict[str, Any]] = []
    concurrency_events: list[dict[str, Any]] = []
    timeout_streak = 0
    concurrency_streak = 0
    attempt = 0
    while True:
        attempt += 1
        try:
            result = client.send_drafts(
                source_instance_id,
                drafts,
                bulk_session_id=bulk_session_id,
                timeout=timeout,
            )
            return RetryOutcome(
                result=result,
                attempts=attempt,
                transient_events=transient_events,
                concurrency_events=concurrency_events,
            )
        except ImportConcurrencyLimitError as error:
            concurrency_streak += 1
            timeout_streak = 0
            event = {
                "attempt": attempt,
                "elapsed_seconds": error.elapsed_seconds,
                "retry_after_seconds": error.retry_after_seconds,
                "message": str(error),
            }
            concurrency_events.append(event)
            if on_concurrency_limit is not None:
                on_concurrency_limit(event)
            if concurrency_streak >= max_consecutive_concurrency_retries:
                raise GateError(
                    f"{context}: {concurrency_streak} consecutive HTTP 429 "
                    f"import_concurrency_limit responses, aborting (an orphaned "
                    f"import request is likely holding a concurrency permit and "
                    f"never clearing): {error}"
                ) from error
            time.sleep(max(error.retry_after_seconds, 0.1))
        except TransientImportError as error:
            timeout_streak += 1
            concurrency_streak = 0
            event = {
                "attempt": attempt,
                "elapsed_seconds": error.elapsed_seconds,
                "status_code": error.status_code,
                "message": str(error),
            }
            transient_events.append(event)
            if on_transient is not None:
                on_transient(event)
            if timeout_streak >= max_consecutive_timeouts:
                raise GateError(
                    f"{context}: {timeout_streak} consecutive transient import "
                    f"failures (timeout/connection-error/5xx), aborting: {error}"
                ) from error


@dataclass
class BulkSessionRetryOutcome:
    """Result of call_bulk_session_with_retry: the eventual success, plus a
    record of any transient (timeout/connection/5xx) and retryable-409
    attempts that preceded it."""

    result: BulkSessionResult
    attempts: int
    transient_events: list[dict[str, Any]]
    conflict_events: list[dict[str, Any]]


def call_bulk_session_with_retry(
    call: Any,
    *,
    max_consecutive_timeouts: int = 3,
    max_consecutive_conflict_retries: int = 10,
    conflict_wait_seconds: float = 1.5,
    context: str,
    on_transient: Any = None,
    on_conflict: Any = None,
) -> BulkSessionRetryOutcome:
    """Call a zero-argument bulk-session begin/end closure, retrying on
    transient failures.

    Same two-independent-streak policy as send_drafts_with_retry:

    - timeout / connection-error / 5xx: retried immediately, up to
      ``max_consecutive_timeouts`` in a row.
    - HTTP 409 with a known-transient conflict code (see
      BulkSessionConflictError / BULK_SESSION_RETRYABLE_CONFLICT_CODES):
      retried after a short fixed ``conflict_wait_seconds`` (these codes
      carry no server-provided retry_after), up to
      ``max_consecutive_conflict_retries`` in a row.

    Each streak resets on any event of the *other* kind (or on success). A
    409 with an unrecognized code, or any other non-2xx status, raises
    ImportRequestError immediately from inside ``call()`` — never retried.
    """
    transient_events: list[dict[str, Any]] = []
    conflict_events: list[dict[str, Any]] = []
    timeout_streak = 0
    conflict_streak = 0
    attempt = 0
    while True:
        attempt += 1
        try:
            result = call()
            return BulkSessionRetryOutcome(
                result=result,
                attempts=attempt,
                transient_events=transient_events,
                conflict_events=conflict_events,
            )
        except BulkSessionConflictError as error:
            conflict_streak += 1
            timeout_streak = 0
            event = {
                "attempt": attempt,
                "elapsed_seconds": error.elapsed_seconds,
                "conflict_code": error.conflict_code,
                "message": str(error),
            }
            conflict_events.append(event)
            if on_conflict is not None:
                on_conflict(event)
            if conflict_streak >= max_consecutive_conflict_retries:
                raise GateError(
                    f"{context}: {conflict_streak} consecutive HTTP 409 "
                    f"{error.conflict_code} responses, aborting: {error}"
                ) from error
            time.sleep(conflict_wait_seconds)
        except TransientImportError as error:
            timeout_streak += 1
            conflict_streak = 0
            event = {
                "attempt": attempt,
                "elapsed_seconds": error.elapsed_seconds,
                "status_code": error.status_code,
                "message": str(error),
            }
            transient_events.append(event)
            if on_transient is not None:
                on_transient(event)
            if timeout_streak >= max_consecutive_timeouts:
                raise GateError(
                    f"{context}: {timeout_streak} consecutive transient bulk-session "
                    f"call failures (timeout/connection-error/5xx), aborting: {error}"
                ) from error


def assert_no_failures(counts: OutcomeCounts, *, context: str) -> None:
    if counts.quarantined > 0 or counts.rejected > 0:
        raise GateError(
            f"{context}: unexpected failures "
            f"(quarantined={counts.quarantined}, rejected={counts.rejected})"
        )


def assert_all_ingested(counts: OutcomeCounts, expected: int, *, context: str) -> None:
    assert_no_failures(counts, context=context)
    if counts.ingested != expected or counts.duplicates != 0:
        raise GateError(
            f"{context}: expected {expected} ingested / 0 duplicates, "
            f"got ingested={counts.ingested} duplicates={counts.duplicates}"
        )


def assert_new_batch_outcome(
    counts: OutcomeCounts, expected: int, *, attempts: int, context: str
) -> int:
    """Validate a batch of *new* (never-before-seeded) drafts, tolerant of
    orphaned-retry duplicates.

    send_drafts_with_retry retries a batch on transient failure (timeout /
    connection-error / 5xx / 429), but the *previous* attempt's request can
    have actually completed server-side while the client gave up on it
    (the same orphan pattern as ImportConcurrencyLimitError) — the retry
    then legitimately comes back `duplicate` for those items, not
    `ingested`. Observed in practice: a 180s-timeout attempt whose retry
    then saw ingested=0/duplicates=25 for a batch of indices that had
    genuinely never been sent before.

    Requires ``ingested + duplicates == expected`` and no
    quarantined/rejected. A `duplicate` outcome is only accepted when
    ``attempts > 1`` (a retry actually happened); a duplicate on the very
    first attempt (no retry) is anomalous for a batch of indices that
    should never have existed before — that indicates real seed/index
    pollution, not an orphaned-retry artifact, and still fails.

    Returns the number of items resolved as `duplicate`, for reporting as
    ``retried_as_duplicates`` (0 on a normal all-`ingested` outcome).
    """
    assert_no_failures(counts, context=context)
    if counts.ingested + counts.duplicates != expected:
        raise GateError(
            f"{context}: expected {expected} ingested+duplicate total, "
            f"got ingested={counts.ingested} duplicates={counts.duplicates}"
        )
    if counts.duplicates > 0 and attempts == 1:
        raise GateError(
            f"{context}: {counts.duplicates} unexpected duplicate(s) on the "
            f"first attempt (no retry occurred) for a batch of indices that "
            f"should never have been seen before — this indicates real "
            f"seed/index pollution, not an orphaned-retry artifact"
        )
    return counts.duplicates


def assert_all_duplicate(counts: OutcomeCounts, expected: int, *, context: str) -> None:
    assert_no_failures(counts, context=context)
    if counts.duplicates != expected or counts.ingested != 0:
        raise GateError(
            f"{context}: expected {expected} duplicates / 0 ingested, "
            f"got ingested={counts.ingested} duplicates={counts.duplicates}"
        )


def batched(items: list[Any], batch_size: int) -> Iterable[list[Any]]:
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


# ---------------------------------------------------------------------------
# (d) docker stats RSS sampler
# ---------------------------------------------------------------------------

_MEM_UNIT_TO_MIB = {
    "B": 1.0 / 1024.0 / 1024.0,
    "KB": 1.0 / 1024.0,
    "KIB": 1.0 / 1024.0,
    "MB": 1.0,
    "MIB": 1.0,
    "GB": 1024.0,
    "GIB": 1024.0,
    "TB": 1024.0 * 1024.0,
    "TIB": 1024.0 * 1024.0,
}

_MEM_VALUE_RE = re.compile(r"^([0-9]*\.?[0-9]+)\s*([A-Za-z]+)$")


def parse_docker_mem_usage(raw: str) -> float:
    """Parse the 'used' half of docker stats MemUsage (e.g. '1.4GiB / 16GiB')."""
    used = raw.split("/")[0].strip()
    match = _MEM_VALUE_RE.match(used)
    if not match:
        raise GateError(f"unrecognized docker MemUsage value: {used!r}")
    value = float(match.group(1))
    unit = match.group(2).upper()
    if unit not in _MEM_UNIT_TO_MIB:
        raise GateError(f"unknown docker memory unit: {unit!r}")
    return value * _MEM_UNIT_TO_MIB[unit]


class DockerStatsSampler:
    """Background poller of `docker stats --no-stream` RSS for one container."""

    def __init__(self, container_name: str, poll_interval_seconds: float = 1.0):
        self.container_name = container_name
        self.poll_interval_seconds = poll_interval_seconds
        self._series: list[tuple[float, float]] = []  # (monotonic_ts, mib)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self.peak_mib = 0.0

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
        self._thread = None

    def _run(self) -> None:
        consecutive_errors = 0
        while not self._stop_event.is_set():
            try:
                completed = subprocess.run(
                    [
                        "docker",
                        "stats",
                        "--no-stream",
                        "--format",
                        "{{.MemUsage}}",
                        self.container_name,
                    ],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=True,
                )
                value_mib = parse_docker_mem_usage(completed.stdout.strip())
                now = time.monotonic()
                with self._lock:
                    self._series.append((now, value_mib))
                    if value_mib > self.peak_mib:
                        self.peak_mib = value_mib
                consecutive_errors = 0
            except Exception:
                consecutive_errors += 1
                if consecutive_errors >= 10:
                    break
            self._stop_event.wait(self.poll_interval_seconds)

    def mark(self) -> int:
        with self._lock:
            return len(self._series)

    def peak_since(self, marker: int) -> float:
        with self._lock:
            values = [v for _, v in self._series[marker:]]
        return max(values) if values else 0.0

    def average_recent(self, window_seconds: float) -> float:
        now = time.monotonic()
        with self._lock:
            values = [v for ts, v in self._series if now - ts <= window_seconds]
        if not values:
            raise GateError("no docker stats samples collected in the requested window")
        return sum(values) / len(values)

    def latest(self) -> float:
        with self._lock:
            if not self._series:
                raise GateError("no docker stats samples collected yet")
            return self._series[-1][1]

    def sample_count(self) -> int:
        with self._lock:
            return len(self._series)


def linear_regression_slope(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    if n < 2:
        raise GateError("linear_regression_slope requires at least 2 points")
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0:
        return 0.0
    return numerator / denominator


# ---------------------------------------------------------------------------
# docker CLI helpers (container lifecycle / crash detection)
# ---------------------------------------------------------------------------


def run_docker(
    args: list[str], *, timeout: float = 30.0, check: bool = True
) -> subprocess.CompletedProcess:
    completed = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout
    )
    if check and completed.returncode != 0:
        raise GateError(
            f"docker {' '.join(args[:2])} failed (exit {completed.returncode}): "
            f"{completed.stderr.strip()[:1000]}"
        )
    return completed


def docker_inspect_state(container_name: str) -> dict[str, Any]:
    completed = run_docker(
        ["inspect", "--format", "{{json .State}}", container_name],
        timeout=15.0,
        check=False,
    )
    if completed.returncode != 0:
        raise GateError(
            f"docker inspect failed for container {container_name}: "
            f"{completed.stderr.strip()[:500]}"
        )
    return json.loads(completed.stdout)


def assert_container_alive(container_name: str) -> dict[str, Any]:
    state = docker_inspect_state(container_name)
    if state.get("Running"):
        return state
    raise GateError(
        f"container {container_name} is not running "
        f"(status={state.get('Status')}, exit_code={state.get('ExitCode')}, "
        f"oom_killed={state.get('OOMKilled')})"
    )


def stop_and_remove_container(container_name: str) -> None:
    """Best-effort cleanup: never raise, this runs from a finally block."""
    run_docker(["stop", "-t", "10", container_name], timeout=30.0, check=False)
    run_docker(["rm", "-f", container_name], timeout=30.0, check=False)
