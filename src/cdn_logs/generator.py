"""Deterministic synthetic CDN access-log generator.

Each hour of logs is generated from a seed derived from the hour itself, so
re-running the same Airflow interval always produces the exact same file.
That property is what makes backfills and retries idempotent end to end.

The traffic model is intentionally realistic enough to make the marts
interesting:
  * diurnal traffic curve per edge POP, in the POP's local time
  * Zipf-like popularity of paths (a few hot objects, a long tail)
  * cache-hit probability that depends on content type
  * log-normal latency, slower on cache MISS and for distant clients
  * rare "incident" hours where one POP returns a burst of 5xx errors
"""

from __future__ import annotations

import hashlib
import math
import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

# --------------------------------------------------------------------------- #
# Static reference data (kept in sync with dbt/seeds/edge_pops.csv)
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class EdgePop:
    code: str
    utc_offset_hours: int
    weight: float  # share of global traffic
    home_countries: tuple[str, ...]


EDGE_POPS: tuple[EdgePop, ...] = (
    EdgePop("CMN", 1, 0.14, ("MA",)),
    EdgePop("ABJ", 0, 0.10, ("CI", "BF", "ML")),
    EdgePop("DKR", 0, 0.07, ("SN", "GN", "ML")),
    EdgePop("LOS", 1, 0.09, ("NG", "BJ", "TG")),
    EdgePop("CDG", 1, 0.20, ("FR", "BE", "CH")),
    EdgePop("FRA", 1, 0.15, ("DE", "NL", "AT")),
    EdgePop("IAD", -5, 0.15, ("US", "CA")),
    EdgePop("SIN", 8, 0.10, ("SG", "MY", "ID")),
)

HOSTS = ("www.example-media.com", "static.example-media.com", "api.example-media.com")

# (content_type, extension, cache_hit_probability, mean_bytes)
CONTENT_PROFILES = (
    ("image/webp", "webp", 0.96, 85_000),
    ("text/javascript", "js", 0.97, 140_000),
    ("text/css", "css", 0.97, 45_000),
    ("video/mp4", "mp4", 0.90, 2_400_000),
    ("application/json", "json", 0.35, 6_000),
    ("text/html", "html", 0.70, 28_000),
)
CONTENT_WEIGHTS = (0.32, 0.14, 0.08, 0.10, 0.22, 0.14)

USER_AGENTS = (
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36", 0.30),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Version/18.0 Safari/605.1.15", 0.12),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148", 0.20),
    ("Mozilla/5.0 (Linux; Android 15; SM-S921B) AppleWebKit/537.36 Chrome/131.0 Mobile Safari/537.36", 0.28),
    ("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", 0.06),
    ("curl/8.9.1", 0.04),
)

METHODS = (("GET", 0.93), ("HEAD", 0.03), ("POST", 0.035), ("OPTIONS", 0.005))

BASE_REQUESTS_PER_HOUR = 9_000
N_OBJECTS = 400  # distinct objects per content type
INCIDENT_PROBABILITY = 0.04  # chance that a given hour has an incident

COLUMNS = (
    "request_id",
    "event_ts",
    "edge_pop",
    "client_ip_hash",
    "client_country",
    "http_method",
    "host",
    "path",
    "http_status",
    "bytes_sent",
    "response_time_ms",
    "cache_status",
    "content_type",
    "user_agent",
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _seed_for(hour_start: datetime) -> int:
    digest = hashlib.sha256(hour_start.isoformat().encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _diurnal_factor(local_hour: int) -> float:
    """Traffic multiplier: trough ~04:00, peak ~21:00 local time."""
    return 0.55 + 0.45 * math.sin((local_hour - 9) / 24 * 2 * math.pi) + 0.1


def _weighted(rng: random.Random, items_with_weights):
    items, weights = zip(*items_with_weights, strict=True)
    return rng.choices(items, weights=weights, k=1)[0]


def normalize_hour(ts: datetime) -> datetime:
    """Truncate to the hour and return a naive UTC datetime."""
    if ts.tzinfo is not None:
        ts = ts.astimezone(UTC).replace(tzinfo=None)
    return ts.replace(minute=0, second=0, microsecond=0)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


def generate_hour(hour_start: datetime) -> list[dict]:
    """Return all log records for the hour starting at ``hour_start`` (UTC)."""
    hour_start = normalize_hour(hour_start)
    rng = random.Random(_seed_for(hour_start))

    incident_pop = rng.choice(EDGE_POPS).code if rng.random() < INCIDENT_PROBABILITY else None
    all_countries = sorted({c for p in EDGE_POPS for c in p.home_countries})

    records: list[dict] = []
    for pop in EDGE_POPS:
        local_hour = (hour_start.hour + pop.utc_offset_hours) % 24
        volume = int(
            BASE_REQUESTS_PER_HOUR * pop.weight * _diurnal_factor(local_hour) * rng.uniform(0.9, 1.1)
        )
        for _ in range(volume):
            records.append(
                _make_record(rng, hour_start, pop, all_countries, incident=(pop.code == incident_pop))
            )

    records.sort(key=lambda r: r["event_ts"])
    return records


def _make_record(
    rng: random.Random, hour_start: datetime, pop: EdgePop, all_countries: list[str], incident: bool
) -> dict:
    ctype, ext, hit_p, mean_bytes = rng.choices(CONTENT_PROFILES, weights=CONTENT_WEIGHTS, k=1)[0]

    # Zipf-ish popularity: low object ids are requested far more often.
    object_id = min(int(rng.paretovariate(1.1)), N_OBJECTS)
    host = HOSTS[2] if ctype == "application/json" else (HOSTS[0] if ext == "html" else HOSTS[1])
    path = f"/v1/items/{object_id}" if host == HOSTS[2] else f"/assets/{ext}/{object_id:04d}.{ext}"

    method = _weighted(rng, METHODS)
    if method in ("POST", "OPTIONS"):
        cache_status = "BYPASS"
    else:
        roll = rng.random()
        hot_bonus = 0.03 if object_id < 20 else 0.0
        if roll < min(hit_p + hot_bonus, 0.995):
            cache_status = "HIT"
        elif roll < min(hit_p + hot_bonus, 0.995) + 0.02:
            cache_status = "EXPIRED"
        else:
            cache_status = "MISS"

    status = _pick_status(rng, incident)
    if method == "HEAD" or status in (304, 204):
        bytes_sent = rng.randint(200, 600)
    elif status >= 400:
        bytes_sent = rng.randint(300, 2_000)
    else:
        bytes_sent = max(200, int(rng.lognormvariate(math.log(mean_bytes), 0.6)))

    foreign = rng.random() < 0.08
    country = rng.choice(all_countries) if foreign else rng.choice(pop.home_countries)

    base_latency = 18 if cache_status == "HIT" else 140
    latency = rng.lognormvariate(math.log(base_latency), 0.5)
    if foreign:
        latency += rng.uniform(60, 180)
    if incident and status >= 500:
        latency += rng.uniform(800, 4_000)

    octets = (rng.randint(1, 223), rng.randint(0, 255), rng.randint(0, 255), rng.randint(1, 254))
    ip_hash = hashlib.sha256(".".join(map(str, octets)).encode()).hexdigest()[:16]

    return {
        "request_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "event_ts": hour_start + timedelta(microseconds=rng.randrange(3_600_000_000)),
        "edge_pop": pop.code,
        "client_ip_hash": ip_hash,
        "client_country": country,
        "http_method": method,
        "host": host,
        "path": path,
        "http_status": status,
        "bytes_sent": bytes_sent,
        "response_time_ms": round(latency, 2),
        "cache_status": cache_status,
        "content_type": ctype,
        "user_agent": _weighted(rng, USER_AGENTS),
    }


def _pick_status(rng: random.Random, incident: bool) -> int:
    if incident and rng.random() < 0.25:
        return rng.choice((500, 502, 503, 504))
    roll = rng.random()
    if roll < 0.905:
        return 200
    if roll < 0.945:
        return 304
    if roll < 0.955:
        return 206
    if roll < 0.962:
        return 301
    if roll < 0.990:
        return 404
    if roll < 0.995:
        return 403
    if roll < 0.9985:
        return 429
    return rng.choice((500, 502, 503))


def iter_hours(start: datetime, end: datetime) -> Iterator[datetime]:
    """Yield hour starts in ``[start, end)``."""
    current = normalize_hour(start)
    end = normalize_hour(end)
    while current < end:
        yield current
        current += timedelta(hours=1)


def partition_key(hour_start: datetime) -> str:
    """Hive-style object key for one hour, e.g. ``dt=2026-11-07/hour=14/cdn_logs_2026110714.parquet``."""
    h = normalize_hour(hour_start)
    return f"dt={h:%Y-%m-%d}/hour={h:%H}/cdn_logs_{h:%Y%m%d%H}.parquet"
