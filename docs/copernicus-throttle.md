# Copernicus throttle & retry

Read this before touching `copernicus.py`, `tile_cache.py`, or the
Copernicus path in `austria_processor.py`.

## The problem

Copernicus Data Space openEO answers **HTTP 402 PaymentRequired** both
when an account is genuinely out of credits and when it is merely
rate-limited for a while. The credential is often still healthy (OIDC
login works, the quota page shows credits) and recovery takes minutes to
hours. The processor therefore rotates through a small pool of
credentials and distinguishes *transient* from *genuine* exhaustion
before it pauses.

## Credentials

Credentials (CDSE OAuth `client_id` / `client_secret` pairs) are never
in the repository. They are read from
`config.COPERNICUS_CREDENTIALS_FILE` (default
`$SRTM_SECRETS_DIR/copernicus_credentials.json`, a JSON list of
`{"client_id": …, "client_secret": …}` entries) and can be added at
runtime through `POST /api/v1/credentials` on the operator API
(loopback only). Optionally `COPERNICUS_BOOTSTRAP_CREDS` (same JSON, as
an environment variable) seeds the list at import. <!-- VERIFY: file path
after the code pass; copernicus.py historically used
data/austria_processor/copernicus_credentials.json -->

The in-memory list is kept in a canonical order (sorted by `client_id`)
so that credential *indices* are stable across processes. Per-credential
usage counters (success/error per hour, last status) live next to the
credentials file (`copernicus_credential_usage.json`) and feed the
dashboard.

## Three-layer architecture

```
copernicus.py          tile_cache.py           austria_processor.py
(API layer)            (cache layer)           (orchestrator)
─────────────          ──────────────          ────────────────────
402 detected           re-raises               writes pause file
  │                    IPThrottledError        aborts tile loop
  ├─ probe credential  and                     re-queues KG
  │  passes? → rotate  CreditsExhaustedError   polls every 15 min
  │  + CredentialRotatedError  (never returns     until probe passes
  │                             None for these)
  ├─ probe fails? → mark
  │  exhausted + rotate
  │  + CredentialRotatedError
  │
  └─ all exhausted?
     → CreditsExhaustedError

@_retry_on_rotation decorator
  catches CredentialRotatedError
  → rebuilds connection + datacube with next credential
  → retries the entire function
  → after len(credentials)+1 attempts:
      all probes passed → IPThrottledError (transient)
      some probes failed → CreditsExhaustedError (genuine)
```

## Exception types (`copernicus.py`)

| Exception | Meaning | Caught by |
|---|---|---|
| `CredentialRotatedError` | one credential got 402, rotated to the next | `@_retry_on_rotation` (retries with the fresh credential) |
| `CreditsExhaustedError` | **all** credentials genuinely exhausted (probes failed) | `tile_cache` re-raises → processor writes the pause file |
| `IPThrottledError(RuntimeError)` | **all** credentials got a transient 402 (probes passed) | `tile_cache` re-raises → processor writes the pause file |

**Critical rule**: these three exceptions must NEVER be swallowed by a
generic `except Exception`. The public functions (`get_ndvi_composite`,
`get_land_cover`, `get_sar_backscatter`) have an explicit
`except (CredentialRotatedError, CreditsExhaustedError, IPThrottledError): raise`
before their generic handler, and so does every `_fetch_*_cell` method in
`tile_cache.py`.

## Credential probing

`_check_credits_error(exc)` runs on every 402:

1. authenticate the credential against the OIDC endpoint (not a data
   download);
2. auth succeeds → "transient 402" → rotate + `CredentialRotatedError`;
3. auth fails with 402 → "genuinely exhausted" → mark the credential,
   rotate + `CredentialRotatedError` (or `CreditsExhaustedError` when none
   is left).

The auth endpoint is separate from the processing endpoint, so a
credential can authenticate fine and still be refused on downloads. That
is why every credential is tried before giving up.

**Never probe from the request path.** Only the director's periodic
`_refresh_credentials_if_due()` may validate credentials; `/credentials`
GET and the dashboard are pure cache reads.

## Sync → batch fallback

Every product download tries a synchronous `/result` POST first
(`SYNC_DOWNLOAD_TIMEOUT` = 180 s; the openeo client's own 30-min
synchronous timeout is capped to 240 s at import and the executor is
shut down without joining) and falls back to a batch job (5–15 min, more
reliable). Because `@_retry_on_rotation` wraps the whole function, a 402
on the sync attempt with credential 1 means a fresh sync attempt (then
batch) with credential 2.

**NDVI time series** (`get_ndvi_timeseries` → `_download_month_sequential`)
downloads eight months (Mar–Oct) sequentially, each with its own
credential-tracking retry loop and a per-month cooldown
(`_FAILED_MONTH_COOLDOWNS`); an `IPThrottledError` /
`CreditsExhaustedError` for one month breaks the whole loop (cascade
breaker).

## `tile_cache.py` bridge

All `_fetch_*_cell` methods follow the same pattern:

```python
try:
    result = copernicus.get_*(cell_bbox, ...)
except (CreditsExhaustedError, IPThrottledError):
    raise                     # never swallowed
except server_error:
    retry with backoff
except other:
    return None               # soft failure for non-throttle errors
```

If the last exception text contains "IP-throttled", `IPThrottledError`
is raised even from the `return None` path.

## `austria_processor.py` response

* `_try_fetch_single(bbox)` bails early while `copernicus.ip_throttled`
  is set.
* `_fetch_copernicus_for_tile()` re-raises throttle exceptions
  immediately — the 2×2 / 4×4 quadrant fallback only runs on timeouts and
  server errors.
* Tile loop: on a throttle exception the subprocess sets
  `copernicus_exhausted=True`, writes
  `data/austria_processor/copernicus_paused` and aborts the tile loop (no
  GPKG/JSON build).
* Parent (`main()`): the KG goes to `retry_queue.json` (tile checkpoints
  preserved); the parent sleeps 15 min, runs `_copernicus_probe()` (resets
  the flags, clears cached connections, tries a tiny NDVI download) and
  resumes when it passes. The re-queued KG runs next and restores its
  completed tiles from the checkpoints.

Module globals (`ip_throttled`, `_exhausted_cred_indices`,
`_IP_THROTTLE_COOLDOWN` = 2 h) live in the per-KG subprocess and reset
with it; the pause file is the cross-process signal.

## Cache-only mode

`austria_processor.py --cache-only` (or `COPERNICUS_FORBIDDEN=1`) sets
`tile_cache.FORBID_REMOTE`: every `_fetch_*_cell` raises `CacheMissError`
instead of calling openEO/UMD, the tile loop marks the KG
`cache_incomplete` and re-queues it. Useful when a second machine should
only consume tiles already present in the local/Zenodo cache
(`docs/peer-director.md`).

## Credential health score

`copernicus.score_credential_health(meta)` → `[0, 1]` orders credential
assignment (best first). Signals: last OIDC status, error-recency ramp
(1 h → 24 h), 7-day error rate, rotation churn, recent-exhaustion
residual, freshness bonus for never-used credentials, and a
**recency-stall gate**: an account that produced `0` successes and `≥ 8`
errors in the last 24 h is clamped to ≤ 0.15 (label `stalled`) regardless
of older successes. This catches expired CDSE accounts (≈90-day lifetime)
that still issue OIDC logins — so the probe passes — but 402 on every
data request, and would otherwise oscillate between "warm" and
"degraded" forever.

## Key files & flags

| File / flag | Where | Purpose |
|---|---|---|
| `copernicus_paused` | `data/austria_processor/` | pause file — parent polls every 15 min while present |
| `openeo_circuit.json` | `data/austria_processor/` | circuit breaker — backs off on consecutive failures |
| `copernicus.ip_throttled` | module global | fast-bail flag after all credentials failed |
| `copernicus._exhausted_cred_indices` | module global | credentials confirmed exhausted |
| `_FAILED_MONTH_COOLDOWNS` | module global | per-(bbox, month) cooldowns for the NDVI series |

## Operations

```bash
cat data/austria_processor/copernicus_paused                 # paused?
rm -f data/austria_processor/copernicus_paused                # force resume (probe re-validates)
rm -f data/austria_processor/openeo_circuit.json              # reset the circuit breaker
grep -E 'Authenticated successfully|Rotated to credential|IP-throttled|transient 402' \
     data/austria_processor/logs/processor.log | tail -20
curl -s http://localhost:8000/api/v1/credentials | python3 -m json.tool   # usage per credential
```

Symptom of an upstream openEO degradation: `NDVI 2024-0x failed: … Read
timed out — skipping month` lines spaced minutes apart on the same KG and
`copernicus (…s) completed with warnings`; the per-hour error rate in
`/api/v1/credentials` is the fastest tell. Nothing to fix locally — the
sync-timeout cap only bounds how much wall-clock is lost.
