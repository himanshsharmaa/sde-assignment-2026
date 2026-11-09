#!/usr/bin/env python3
"""Restartable HTTP-only controller for the Outcome-First Agent Loop simulator."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
import random
import sqlite3
import statistics
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

LOG = logging.getLogger("agentic_controller")
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
MIN_REGISTERED_TARGET_ARM_DIALS = 120
MIN_REGISTERED_TARGET_ARM_CONNECTED = 40
MIN_ARM_DIALS = MIN_REGISTERED_TARGET_ARM_DIALS
MIN_ARM_CONNECTED = MIN_REGISTERED_TARGET_ARM_CONNECTED
MIN_INCIDENT_CALLS = 100
CONFIDENCE_Z = 2.58
MAX_CONSECUTIVE_NO_PROGRESS_ADVANCES = 3


class RemoteError(RuntimeError):
    def __init__(self, status: int, code: str, detail: Any):
        super().__init__(f"simulator returned {status} {code}: {detail}")
        self.status = status
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class ArmStats:
    dials: int
    connected: int
    engaged: int
    hallucination_flags: int

    @property
    def pickup_rate(self) -> float:
        return self.connected / self.dials if self.dials else 0.0

    @property
    def engagement_rate(self) -> float:
        return self.engaged / self.connected if self.connected else 0.0


class SimClient:
    """Bounded async HTTP client with retries only for safe/idempotent operations."""

    def __init__(self, base_url: str, transport: httpx.AsyncBaseTransport | None = None):
        self._sem = asyncio.Semaphore(8)
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=httpx.Timeout(connect=5.0, read=45.0, write=10.0, pool=5.0),
            limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
            transport=transport,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def request(
        self, method: str, path: str, *, safe_retry: bool = True, **kwargs: Any
    ) -> Any:
        attempts = 4 if safe_retry else 1
        for attempt in range(attempts):
            try:
                async with self._sem:
                    response = await self._client.request(method, path, **kwargs)
                if response.status_code in RETRYABLE_STATUS and attempt + 1 < attempts:
                    await asyncio.sleep(min(2**attempt + random.random(), 8.0))
                    continue
                if response.is_error:
                    try:
                        body = response.json()
                    except ValueError:
                        body = response.text[:1000]
                    raise RemoteError(
                        response.status_code,
                        str(body.get("error", "http_error")) if isinstance(body, dict) else "http_error",
                        body.get("detail", body) if isinstance(body, dict) else body,
                    )
                if response.status_code == 204 or not response.content:
                    return None
                return response.json()
            except (httpx.TimeoutException, httpx.TransportError):
                if not safe_retry or attempt + 1 == attempts:
                    raise
                await asyncio.sleep(min(2**attempt + random.random(), 8.0))
        raise RuntimeError("unreachable retry state")

    async def advance(self, minutes: int) -> Any:
        # Advancing simulated time is not idempotent. On an ambiguous failure, inspect
        # state before deciding whether another advance is needed.
        before = await self.request("GET", "/sim/state")
        try:
            return await self.request(
                "POST", "/sim/advance", safe_retry=False, json={"minutes": minutes}
            )
        except (httpx.TimeoutException, httpx.TransportError):
            after = await self.request("GET", "/sim/state")
            if after["sim_minute"] > before["sim_minute"]:
                return {"sim_minute": after["sim_minute"], "ts": after["ts"],
                        "finished": after["finished"], "new_chunks": []}
            return await self.request(
                "POST", "/sim/advance", safe_retry=False, json={"minutes": minutes}
            )

    async def get_chunk(self, number: int) -> bytes:
        for attempt in range(4):
            try:
                async with self._sem:
                    response = await self._client.get(f"/events/chunks/{number}")
                if response.status_code in RETRYABLE_STATUS and attempt < 3:
                    await asyncio.sleep(min(2**attempt + random.random(), 8.0))
                    continue
                if response.is_error:
                    raise RemoteError(response.status_code, "chunk_fetch_failed", response.text[:1000])
                return response.content
            except (httpx.TimeoutException, httpx.TransportError):
                if attempt == 3:
                    raise
                await asyncio.sleep(min(2**attempt + random.random(), 8.0))
        raise RuntimeError("unreachable chunk retry state")


class Store:
    """SQLite state; each public method uses a short-lived connection/transaction."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    @contextmanager
    def _connection(self):
        conn = self._connect()
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _init(self) -> None:
        with self._connection() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk INTEGER PRIMARY KEY,
                    kind TEXT NOT NULL,
                    last_event_id INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS arm_totals (
                    workflow_id TEXT NOT NULL,
                    cohort_id TEXT NOT NULL,
                    config_version INTEGER NOT NULL,
                    sim_day INTEGER NOT NULL,
                    dimension TEXT NOT NULL,
                    variant TEXT NOT NULL,
                    dials INTEGER NOT NULL,
                    connected INTEGER NOT NULL,
                    engaged INTEGER NOT NULL,
                    hallucination_flag INTEGER NOT NULL,
                    PRIMARY KEY(workflow_id, cohort_id, config_version, sim_day, dimension, variant)
                );
                CREATE INDEX IF NOT EXISTS arm_totals_lookup
                    ON arm_totals(workflow_id, cohort_id, config_version, dimension, sim_day);
                CREATE TABLE IF NOT EXISTS proposals (
                    proposal_id TEXT PRIMARY KEY,
                    workflow_id TEXT NOT NULL,
                    base_version INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    approval_id TEXT,
                    payload TEXT NOT NULL,
                    application_attempted INTEGER NOT NULL DEFAULT 0,
                    eligible_at TEXT,
                    expires_at TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    workflow_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    detail TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS experiment_cursor (
                    workflow_id TEXT PRIMARY KEY,
                    dimension TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS proposal_cursor_advancements (
                    proposal_id TEXT PRIMARY KEY,
                    workflow_id TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN ('advanced', 'legacy_unverified')
                    ),
                    from_dimension TEXT,
                    to_dimension TEXT
                );
                CREATE TABLE IF NOT EXISTS store_migrations (
                    migration_id TEXT PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS experiment_evaluations (
                    workflow_id TEXT NOT NULL,
                    config_version INTEGER NOT NULL,
                    PRIMARY KEY(workflow_id, config_version)
                );
                CREATE TABLE IF NOT EXISTS experiment_families (
                    family_id TEXT PRIMARY KEY,
                    alpha_budget REAL NOT NULL,
                    hypothesis_count INTEGER NOT NULL,
                    look_count INTEGER NOT NULL,
                    look_fractions TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_sim_ts TEXT NOT NULL,
                    frozen_sim_ts TEXT,
                    validation_error TEXT
                );
                CREATE TABLE IF NOT EXISTS experiments (
                    experiment_id TEXT PRIMARY KEY,
                    family_id TEXT NOT NULL,
                    proposal_id TEXT UNIQUE,
                    workflow_id TEXT NOT NULL,
                    cohort_id TEXT NOT NULL,
                    config_version INTEGER NOT NULL,
                    dimension TEXT NOT NULL,
                    control_variant TEXT NOT NULL,
                    candidate_variant TEXT NOT NULL,
                    primary_kpi TEXT NOT NULL,
                    guardrail_kpi TEXT NOT NULL,
                    guardrail_margin REAL,
                    baseline_primary REAL,
                    baseline_guardrail REAL,
                    practical_effect REAL NOT NULL,
                    target_power REAL NOT NULL,
                    target_primary_n INTEGER,
                    target_guardrail_n INTEGER,
                    status TEXT NOT NULL,
                    start_event_id INTEGER,
                    end_event_id INTEGER,
                    design_json TEXT NOT NULL,
                    created_sim_ts TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS experiments_one_active_population
                    ON experiments(workflow_id, cohort_id)
                    WHERE status IN ('planned', 'awaiting_configuration', 'running');
                CREATE INDEX IF NOT EXISTS experiments_family_status
                    ON experiments(family_id, status);
                CREATE TABLE IF NOT EXISTS experiment_looks (
                    experiment_id TEXT NOT NULL,
                    look_number INTEGER NOT NULL,
                    information_fraction REAL NOT NULL,
                    cutoff_event_id INTEGER,
                    status TEXT NOT NULL,
                    result_json TEXT,
                    PRIMARY KEY(experiment_id, look_number)
                );
                CREATE TABLE IF NOT EXISTS experiment_observations (
                    experiment_id TEXT NOT NULL,
                    person_id TEXT NOT NULL,
                    event_id INTEGER NOT NULL,
                    ts TEXT NOT NULL,
                    workflow_id TEXT NOT NULL,
                    campaign_id TEXT,
                    lead_id TEXT,
                    cohort_id TEXT NOT NULL,
                    config_version INTEGER NOT NULL,
                    dimension TEXT NOT NULL,
                    variant TEXT NOT NULL,
                    variant_json TEXT NOT NULL,
                    attempt_no INTEGER NOT NULL,
                    connected INTEGER NOT NULL,
                    engaged INTEGER NOT NULL,
                    PRIMARY KEY(experiment_id, person_id),
                    UNIQUE(experiment_id, event_id)
                );
                CREATE INDEX IF NOT EXISTS experiment_observations_by_arm
                    ON experiment_observations(experiment_id, variant, event_id);
                CREATE TABLE IF NOT EXISTS prompt_checks (
                    workflow_id TEXT NOT NULL,
                    sim_day INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    PRIMARY KEY(workflow_id, sim_day)
                );
                """
            )
            proposal_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(proposals)")
            }
            if "application_attempted" not in proposal_columns:
                conn.execute(
                    "ALTER TABLE proposals ADD COLUMN application_attempted "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            if "eligible_at" not in proposal_columns:
                conn.execute("ALTER TABLE proposals ADD COLUMN eligible_at TEXT")
            if "expires_at" not in proposal_columns:
                conn.execute("ALTER TABLE proposals ADD COLUMN expires_at TEXT")
            family_columns = {
                row["name"] for row in conn.execute(
                    "PRAGMA table_info(experiment_families)"
                )
            }
            if "validation_error" not in family_columns:
                conn.execute(
                    "ALTER TABLE experiment_families ADD COLUMN validation_error TEXT"
                )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS proposals_deferred_eligibility "
                "ON proposals(status, eligible_at)"
            )
            conn.execute("BEGIN IMMEDIATE")
            migration = "proposal_cursor_advancements_v1"
            migration_done = conn.execute(
                "SELECT 1 FROM store_migrations WHERE migration_id=?",
                (migration,),
            ).fetchone()
            if migration_done is None:
                legacy_applied = conn.execute(
                    """SELECT proposal_id, workflow_id, payload FROM proposals
                       WHERE status='applied'"""
                ).fetchall()
                for proposal in legacy_applied:
                    payload = json.loads(proposal["payload"])
                    if str(payload.get("rationale", "")).startswith("Promote "):
                        conn.execute(
                            """INSERT OR IGNORE INTO proposal_cursor_advancements
                               (proposal_id, workflow_id, status)
                               VALUES (?, ?, 'legacy_unverified')""",
                            (proposal["proposal_id"], proposal["workflow_id"]),
                        )
                conn.execute(
                    "INSERT INTO store_migrations(migration_id) VALUES (?)",
                    (migration,),
                )

    def last_chunk(self) -> int:
        with self._connection() as conn:
            row = conn.execute("SELECT MAX(chunk) AS n FROM chunks").fetchone()
            return int(row["n"]) if row["n"] is not None else -1

    def process_chunk(self, chunk: dict[str, Any], raw: bytes) -> None:
        lines = raw.decode("utf-8").splitlines()
        totals: dict[tuple[str, str, int, int, str, str], list[int]] = {}
        calls: list[dict[str, Any]] = []
        for line in lines:
            if not line:
                continue
            event = json.loads(line)
            if event.get("type") != "call":
                continue
            calls.append(event)
            variants = event.get("variant") or {}
            qa = event.get("qa") or {}
            connected = int(bool(event["connected"]))
            engaged = int(bool(event["engaged"]))
            flagged = int(bool(qa.get("hallucination_flag")))
            prefix = (
                event["workflow_id"], event.get("cohort_id", "default"),
                int(event.get("config_version", 1)), _sim_day(event["ts"]),
            )
            for dimension, variant in variants.items():
                key = (*prefix, dimension, str(variant))
                counts = totals.setdefault(key, [0, 0, 0, 0])
                counts[0] += 1
                counts[1] += connected
                counts[2] += engaged
                counts[3] += flagged
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            exists = conn.execute(
                "SELECT 1 FROM chunks WHERE chunk=?", (chunk["chunk"],)
            ).fetchone()
            if exists:
                return
            active_experiments = conn.execute(
                """SELECT e.* FROM experiments e
                   JOIN experiment_families f ON f.family_id=e.family_id
                   WHERE e.status='running' AND f.status='frozen'"""
            ).fetchall()
            conn.executemany(
                """INSERT INTO arm_totals
                   (workflow_id, cohort_id, config_version, sim_day, dimension, variant,
                    dials, connected, engaged, hallucination_flag)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(workflow_id, cohort_id, config_version, sim_day, dimension, variant)
                   DO UPDATE SET dials=dials+excluded.dials,
                       connected=connected+excluded.connected, engaged=engaged+excluded.engaged,
                       hallucination_flag=hallucination_flag+excluded.hallucination_flag""",
                [(*key, *counts) for key, counts in totals.items()],
            )
            for event in calls:
                event_id = event.get("event_id")
                person_id = event.get("person_id")
                if event_id is None or person_id is None or not str(person_id):
                    continue
                for experiment in active_experiments:
                    if (
                        event["workflow_id"] != experiment["workflow_id"]
                        or event.get("cohort_id", "default") != experiment["cohort_id"]
                        or int(event.get("config_version", 1)) != experiment["config_version"]
                        or int(event_id) <= experiment["start_event_id"]
                    ):
                        continue
                    variant = (event.get("variant") or {}).get(experiment["dimension"])
                    if variant not in (
                        experiment["control_variant"],
                        experiment["candidate_variant"],
                    ):
                        continue
                    conn.execute(
                        """INSERT INTO experiment_observations
                           (experiment_id, person_id, event_id, ts, workflow_id,
                            campaign_id, lead_id, cohort_id, config_version, dimension,
                            variant, variant_json, attempt_no, connected, engaged)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(experiment_id, person_id) DO UPDATE SET
                               event_id=excluded.event_id, ts=excluded.ts,
                               workflow_id=excluded.workflow_id,
                               campaign_id=excluded.campaign_id,
                               lead_id=excluded.lead_id, cohort_id=excluded.cohort_id,
                               config_version=excluded.config_version,
                               dimension=excluded.dimension, variant=excluded.variant,
                               variant_json=excluded.variant_json,
                               attempt_no=excluded.attempt_no,
                               connected=excluded.connected, engaged=excluded.engaged
                           WHERE excluded.event_id < experiment_observations.event_id""",
                        (
                            experiment["experiment_id"], str(person_id), int(event_id),
                            event["ts"], event["workflow_id"], event.get("campaign_id"),
                            event.get("lead_id"),
                            event.get("cohort_id", "default"),
                            int(event.get("config_version", 1)),
                            experiment["dimension"], str(variant),
                            json.dumps(event.get("variant") or {}, sort_keys=True),
                            int(event.get("attempt_no", 1)),
                            int(bool(event["connected"])), int(bool(event["engaged"])),
                        ),
                    )
            conn.execute(
                "INSERT INTO chunks(chunk, kind, last_event_id) VALUES (?, ?, ?)",
                (chunk["chunk"], chunk["kind"], chunk["last_event_id"]),
            )

    def create_experiment_family(
        self, family_id: str, alpha_budget: float, hypothesis_count: int,
        created_sim_ts: str, look_fractions: tuple[float, ...] = (0.5, 0.75, 1.0),
    ) -> bool:
        if not family_id:
            raise ValueError("family_id must not be empty")
        if not 0 < alpha_budget < 1:
            raise ValueError("alpha_budget must be between zero and one")
        if hypothesis_count < 1:
            raise ValueError("hypothesis_count must be positive")
        if (
            len(look_fractions) != 3
            or tuple(sorted(set(look_fractions))) != look_fractions
            or look_fractions[-1] != 1.0
            or look_fractions[0] <= 0
        ):
            raise ValueError("exactly three increasing looks ending at 1.0 are required")
        fractions_json = json.dumps(look_fractions, separators=(",", ":"))
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM experiment_families WHERE family_id=?", (family_id,)
            ).fetchone()
            if row is not None:
                existing = (
                    float(row["alpha_budget"]), int(row["hypothesis_count"]),
                    row["look_fractions"], row["created_sim_ts"],
                )
                expected = (
                    float(alpha_budget), int(hypothesis_count), fractions_json,
                    created_sim_ts,
                )
                if existing != expected:
                    raise ValueError(f"experiment family {family_id} already has a different plan")
                return False
            conn.execute(
                """INSERT INTO experiment_families
                   (family_id, alpha_budget, hypothesis_count, look_count,
                    look_fractions, status, created_sim_ts)
                   VALUES (?, ?, ?, 3, ?, 'open', ?)""",
                (family_id, alpha_budget, hypothesis_count, fractions_json, created_sim_ts),
            )
        return True

    def register_experiment(
        self, experiment_id: str, family_id: str, workflow_id: str,
        cohort_id: str, config_version: int, dimension: str,
        control_variant: str, candidate_variant: str, primary_kpi: str,
        guardrail_kpi: str, created_sim_ts: str, *,
        proposal_id: str | None = None, guardrail_margin: float | None = None,
        baseline_primary: float | None = None, baseline_guardrail: float | None = None,
        practical_effect: float = 0.015, target_power: float = 0.8,
        target_primary_n: int | None = None, target_guardrail_n: int | None = None,
        design: dict[str, Any] | None = None,
    ) -> bool:
        if not experiment_id or not workflow_id or not cohort_id or not dimension:
            raise ValueError("experiment identity fields must not be empty")
        if control_variant == candidate_variant:
            raise ValueError("control and candidate variants must differ")
        if primary_kpi not in ("pickup_rate", "engagement_rate"):
            raise ValueError("unsupported primary KPI")
        if guardrail_kpi not in ("pickup_rate", "engagement_rate") or guardrail_kpi == primary_kpi:
            raise ValueError("guardrail KPI must be the other supported KPI")
        if config_version < 1 or practical_effect <= 0 or not 0 < target_power < 1:
            raise ValueError("invalid experiment version or statistical design")
        values = (
            family_id, proposal_id, workflow_id, cohort_id, int(config_version), dimension,
            control_variant, candidate_variant, primary_kpi, guardrail_kpi,
            guardrail_margin, baseline_primary, baseline_guardrail, practical_effect,
            target_power, target_primary_n, target_guardrail_n,
            json.dumps(design or {}, sort_keys=True), created_sim_ts,
        )
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            family = conn.execute(
                "SELECT status FROM experiment_families WHERE family_id=?", (family_id,)
            ).fetchone()
            if family is None:
                raise ValueError(f"unknown experiment family {family_id}")
            existing = conn.execute(
                "SELECT * FROM experiments WHERE experiment_id=?", (experiment_id,)
            ).fetchone()
            if existing is not None:
                existing_values = tuple(
                    existing[column] for column in (
                        "family_id", "proposal_id", "workflow_id", "cohort_id",
                        "config_version", "dimension", "control_variant",
                        "candidate_variant", "primary_kpi", "guardrail_kpi",
                        "guardrail_margin", "baseline_primary", "baseline_guardrail",
                        "practical_effect", "target_power", "target_primary_n",
                        "target_guardrail_n", "design_json", "created_sim_ts",
                    )
                )
                if existing_values != values:
                    raise ValueError(f"experiment {experiment_id} already has a different plan")
                return False
            if family["status"] != "open":
                raise ValueError("experiment family is frozen")
            conn.execute(
                """INSERT INTO experiments
                   (experiment_id, family_id, proposal_id, workflow_id, cohort_id,
                    config_version, dimension, control_variant, candidate_variant,
                    primary_kpi, guardrail_kpi, guardrail_margin, baseline_primary,
                    baseline_guardrail, practical_effect, target_power, target_primary_n,
                    target_guardrail_n, status, design_json, created_sim_ts)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           'planned', ?, ?)""",
                (experiment_id, *values),
            )
            conn.execute(
                "UPDATE experiment_families SET validation_error=NULL WHERE family_id=?",
                (family_id,),
            )
            family_row = conn.execute(
                "SELECT look_fractions FROM experiment_families WHERE family_id=?",
                (family_id,),
            ).fetchone()
            for look_number, fraction in enumerate(
                json.loads(family_row["look_fractions"]), start=1
            ):
                conn.execute(
                    """INSERT INTO experiment_looks
                       (experiment_id, look_number, information_fraction, status)
                       VALUES (?, ?, ?, 'planned')""",
                    (experiment_id, look_number, fraction),
                )
        return True

    def freeze_experiment_family(self, family_id: str, frozen_sim_ts: str) -> bool:
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status, hypothesis_count FROM experiment_families WHERE family_id=?",
                (family_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown experiment family {family_id}")
            if row["status"] == "frozen":
                return False
            if row["status"] != "open":
                raise ValueError(f"invalid experiment family status {row['status']}")
            actual_hypotheses = int(conn.execute(
                """SELECT COUNT(*) * 2 AS hypothesis_count FROM experiments
                   WHERE family_id=?""",
                (family_id,),
            ).fetchone()["hypothesis_count"])
            if actual_hypotheses == 0:
                raise ValueError("cannot freeze an empty experiment family")
            if actual_hypotheses != int(row["hypothesis_count"]):
                conn.execute(
                    """UPDATE experiment_families SET validation_error=?
                       WHERE family_id=? AND status='open'""",
                    (
                        f"declared {row['hypothesis_count']} hypotheses but "
                        f"registered {actual_hypotheses}",
                        family_id,
                    ),
                )
                conn.commit()
                raise ValueError(
                    f"experiment family hypothesis count mismatch: declared "
                    f"{row['hypothesis_count']}, registered {actual_hypotheses}"
                )
            conn.execute(
                """UPDATE experiment_families
                   SET status='frozen', frozen_sim_ts=?, validation_error=NULL
                   WHERE family_id=? AND status='open'""",
                (frozen_sim_ts, family_id),
            )
        return True

    def start_experiment(self, experiment_id: str, start_event_id: int) -> bool:
        if start_event_id < 0:
            raise ValueError("start_event_id must be non-negative")
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT e.status, e.start_event_id, f.status AS family_status,
                          p.status AS proposal_status
                   FROM experiments e JOIN experiment_families f
                     ON f.family_id=e.family_id
                   LEFT JOIN proposals p ON p.proposal_id=e.proposal_id
                   WHERE e.experiment_id=?""",
                (experiment_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown experiment {experiment_id}")
            if row["proposal_status"] == "conflict":
                return False
            if row["family_status"] != "frozen":
                raise ValueError("experiment family must be frozen before start")
            if row["status"] == "running":
                return False
            if row["status"] != "planned":
                raise ValueError(f"experiment cannot start from {row['status']}")
            conn.execute(
                """UPDATE experiments SET status='running', start_event_id=?
                   WHERE experiment_id=? AND status='planned'""",
                (start_event_id, experiment_id),
            )
        return True

    def pending_experiment_for_proposal(self, proposal_id: str) -> str | None:
        with self._connection() as conn:
            row = conn.execute(
                """SELECT experiment_id FROM experiments
                   WHERE proposal_id=? AND status IN ('planned', 'running')""",
                (proposal_id,),
            ).fetchone()
        return str(row["experiment_id"]) if row else None

    def cancel_experiment_for_proposal(self, proposal_id: str) -> bool:
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """UPDATE experiments SET status='cancelled'
                   WHERE proposal_id=? AND status='planned'""",
                (proposal_id,),
            )
        return cursor.rowcount == 1

    def set_experiment_look_cutoff(
        self, experiment_id: str, look_number: int, cutoff_event_id: int
    ) -> bool:
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT e.status, e.start_event_id, e.end_event_id, e.family_id,
                          l.cutoff_event_id, l.status AS look_status
                   FROM experiments e JOIN experiment_looks l
                     ON l.experiment_id=e.experiment_id
                   WHERE e.experiment_id=? AND l.look_number=?""",
                (experiment_id, look_number),
            ).fetchone()
            if row is None:
                raise ValueError("unknown experiment look")
            if row["status"] != "running":
                raise ValueError("look cutoff requires a running experiment")
            if cutoff_event_id < int(row["start_event_id"]):
                raise ValueError("look cutoff precedes experiment start")
            if row["cutoff_event_id"] is not None:
                if int(row["cutoff_event_id"]) != cutoff_event_id:
                    raise ValueError("look cutoff is immutable")
                return False
            if look_number > 1:
                previous = conn.execute(
                    """SELECT cutoff_event_id FROM experiment_looks
                       WHERE experiment_id=? AND look_number=?""",
                    (experiment_id, look_number - 1),
                ).fetchone()
                if previous is None or previous["cutoff_event_id"] is None:
                    raise ValueError("previous look cutoff must be established first")
                if cutoff_event_id <= int(previous["cutoff_event_id"]):
                    raise ValueError("look cutoffs must increase strictly")
            if look_number < 3:
                following = conn.execute(
                    """SELECT cutoff_event_id FROM experiment_looks
                       WHERE experiment_id=? AND look_number=?""",
                    (experiment_id, look_number + 1),
                ).fetchone()
                if following is not None and following["cutoff_event_id"] is not None:
                    raise ValueError("look cutoff ordering is immutable")
            cursor = conn.execute(
                """UPDATE experiment_looks SET cutoff_event_id=?, status='ready'
                   WHERE experiment_id=? AND look_number=? AND cutoff_event_id IS NULL""",
                (cutoff_event_id, experiment_id, look_number),
            )
            if cursor.rowcount == 1 and look_number == 3:
                conn.execute(
                    """UPDATE experiments SET end_event_id=?
                       WHERE experiment_id=? AND end_event_id IS NULL""",
                    (cutoff_event_id, experiment_id),
                )
        return cursor.rowcount == 1

    def experiment_family(self, family_id: str) -> dict[str, Any] | None:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM experiment_families WHERE family_id=?", (family_id,)
            ).fetchone()
            if row is not None:
                actual_hypotheses = int(conn.execute(
                    """SELECT COUNT(*) * 2 AS hypothesis_count FROM experiments
                       WHERE family_id=?""",
                    (family_id,),
                ).fetchone()["hypothesis_count"])
        if row is None:
            return None
        result = dict(row)
        result["look_fractions"] = json.loads(result["look_fractions"])
        if actual_hypotheses != int(result["hypothesis_count"]):
            if result["validation_error"] is None:
                raise ValueError(
                    f"experiment family hypothesis count mismatch: declared "
                    f"{result['hypothesis_count']}, registered {actual_hypotheses}"
                )
        elif result["status"] == "frozen":
            result["alpha_per_hypothesis_per_look"] = (
                result["alpha_budget"] / result["hypothesis_count"] / result["look_count"]
            )
        elif result["validation_error"] is None:
            raise ValueError("experiment family must be frozen before alpha allocation")
        return result

    def experiments(self, status: str | None = None) -> list[dict[str, Any]]:
        with self._connection() as conn:
            if status is None:
                rows = conn.execute(
                    "SELECT * FROM experiments ORDER BY created_sim_ts, experiment_id"
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM experiments WHERE status=?
                       ORDER BY created_sim_ts, experiment_id""",
                    (status,),
                ).fetchall()
        return [dict(row) for row in rows]

    def experiment_looks(self, experiment_id: str) -> list[dict[str, Any]]:
        with self._connection() as conn:
            rows = conn.execute(
                """SELECT * FROM experiment_looks WHERE experiment_id=?
                   ORDER BY look_number""",
                (experiment_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def experiment_observations(
        self, experiment_id: str, *, look_number: int | None = None
    ) -> list[dict[str, Any]]:
        with self._connection() as conn:
            if look_number is None:
                rows = conn.execute(
                    """SELECT * FROM experiment_observations WHERE experiment_id=?
                       ORDER BY event_id""",
                    (experiment_id,),
                ).fetchall()
            else:
                look = conn.execute(
                    """SELECT cutoff_event_id FROM experiment_looks
                       WHERE experiment_id=? AND look_number=?""",
                    (experiment_id, look_number),
                ).fetchone()
                if look is None:
                    raise ValueError("unknown experiment look")
                if look["cutoff_event_id"] is None:
                    raise ValueError("experiment look cutoff has not been established")
                experiment = conn.execute(
                    """SELECT start_event_id, end_event_id
                       FROM experiments WHERE experiment_id=?""",
                    (experiment_id,),
                ).fetchone()
                if experiment is None:
                    raise ValueError(f"unknown experiment {experiment_id}")
                cutoff_event_id = (
                    experiment["end_event_id"]
                    if look_number == 3 and experiment["end_event_id"] is not None
                    else look["cutoff_event_id"]
                )
                rows = conn.execute(
                    """SELECT * FROM experiment_observations
                       WHERE experiment_id=? AND event_id>? AND event_id<=?
                       ORDER BY event_id""",
                    (
                        experiment_id,
                        int(experiment["start_event_id"]),
                        int(cutoff_event_id),
                    ),
                ).fetchall()
        return [dict(row) for row in rows]

    def finalize_experiment_look(
        self, experiment_id: str, look_number: int, outcome: str,
        result: dict[str, Any], ts: str,
    ) -> bool:
        if outcome not in (
            "continue", "beneficial", "inconclusive", "guardrail_blocked"
        ):
            raise ValueError(f"unsupported experiment look outcome {outcome}")
        terminal = outcome != "continue"
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT e.workflow_id, e.status, l.status AS look_status,
                          l.cutoff_event_id
                   FROM experiments e JOIN experiment_looks l
                     ON l.experiment_id=e.experiment_id
                   WHERE e.experiment_id=? AND l.look_number=?""",
                (experiment_id, look_number),
            ).fetchone()
            if row is None:
                raise ValueError("unknown experiment look")
            if row["status"] != "running":
                return False
            if row["look_status"] == "evaluated":
                return False
            if row["look_status"] != "ready" or row["cutoff_event_id"] is None:
                raise ValueError("experiment look must have a persisted cutoff")
            cursor = conn.execute(
                """UPDATE experiment_looks SET status='evaluated', result_json=?
                   WHERE experiment_id=? AND look_number=? AND status='ready'""",
                (json.dumps(result, sort_keys=True), experiment_id, look_number),
            )
            if cursor.rowcount != 1:
                return False
            if terminal:
                conn.execute(
                    """UPDATE experiments SET status=?, end_event_id=COALESCE(
                           end_event_id, ?)
                       WHERE experiment_id=? AND status='running'""",
                    (outcome, row["cutoff_event_id"], experiment_id),
                )
            conn.execute(
                """INSERT INTO audit(ts, workflow_id, action, detail)
                   VALUES (?, ?, 'experiment_look_evaluated', ?)""",
                (
                    ts,
                    row["workflow_id"],
                    json.dumps(
                        {"experiment_id": experiment_id,
                         "look_number": look_number, "outcome": outcome,
                         **result},
                        sort_keys=True,
                    ),
                ),
            )
        return True

    def mark_experiment_underpowered(
        self, experiment_id: str, last_event_id: int, ts: str, reason: str,
        result: dict[str, Any],
    ) -> bool:
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT workflow_id, status FROM experiments
                   WHERE experiment_id=?""",
                (experiment_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown experiment {experiment_id}")
            if row["status"] != "running":
                return False
            conn.execute(
                """UPDATE experiments SET status='underpowered', end_event_id=?
                   WHERE experiment_id=? AND status='running'""",
                (last_event_id, experiment_id),
            )
            conn.execute(
                """UPDATE experiment_looks SET status='underpowered',
                       cutoff_event_id=COALESCE(cutoff_event_id, ?), result_json=?
                   WHERE experiment_id=? AND look_number=3
                     AND status IN ('planned', 'ready')""",
                (
                    last_event_id, json.dumps(
                        {"outcome": "underpowered", "reason": reason, **result},
                        sort_keys=True,
                    ), experiment_id,
                ),
            )
            conn.execute(
                """INSERT INTO audit(ts, workflow_id, action, detail)
                   VALUES (?, ?, 'experiment_underpowered', ?)""",
                (
                    ts, row["workflow_id"],
                    json.dumps(
                        {"experiment_id": experiment_id, "reason": reason, **result},
                        sort_keys=True,
                    ),
                ),
            )
        return True

    def arm_stats(
        self, workflow_id: str, cohort_id: str, version: int, dimension: str,
        *, since_day: int | None = None,
    ) -> dict[str, ArmStats]:
        sql = """SELECT variant, SUM(dials) AS dials, SUM(connected) AS connected,
                        SUM(engaged) AS engaged, SUM(hallucination_flag) AS flags
                 FROM arm_totals WHERE workflow_id=? AND cohort_id=? AND config_version=?
                   AND dimension=?"""
        args: list[Any] = [workflow_id, cohort_id, version, dimension]
        if since_day is not None:
            sql += " AND sim_day>=?"
            args.append(since_day)
        sql += " GROUP BY variant"
        result: dict[str, ArmStats] = {}
        with self._connection() as conn:
            for row in conn.execute(sql, args):
                result[str(row["variant"])] = ArmStats(
                    int(row["dials"] or 0), int(row["connected"] or 0),
                    int(row["engaged"] or 0), int(row["flags"] or 0),
                )
        return result

    def incident_stats(
        self, workflow_id: str, since_day: int, *,
        cohort_id: str = "default", config_version: int | None = None,
    ) -> dict[str, dict[str, int]]:
        sql = """SELECT variant, SUM(dials) AS dials, SUM(connected) AS connected,
                        SUM(engaged) AS engaged, SUM(hallucination_flag) AS flags
                 FROM arm_totals WHERE workflow_id=? AND cohort_id=? AND sim_day>=?
                   AND dimension='caller_id'"""
        args: list[Any] = [workflow_id, cohort_id, since_day]
        if config_version is not None:
            sql += " AND config_version=?"
            args.append(config_version)
        sql += " GROUP BY variant"
        out: dict[str, dict[str, int]] = {}
        with self._connection() as conn:
            for row in conn.execute(sql, args):
                out[str(row["variant"])] = {
                    "dials": int(row["dials"] or 0),
                    "connected": int(row["connected"] or 0),
                    "engaged": int(row["engaged"] or 0),
                    "flags": int(row["flags"] or 0),
                }
        return out

    def prompt_stats(self, workflow_id: str, since_day: int) -> dict[str, dict[str, int]]:
        sql = """SELECT variant, SUM(dials) AS dials, SUM(connected) AS connected,
                        SUM(hallucination_flag) AS flags
                 FROM arm_totals WHERE workflow_id=? AND sim_day>=?
                   AND dimension='prompt_version' GROUP BY variant"""
        out: dict[str, dict[str, int]] = {}
        with self._connection() as conn:
            for row in conn.execute(sql, (workflow_id, since_day)):
                out[str(row["variant"])] = {
                    "dials": int(row["dials"] or 0),
                    "connected": int(row["connected"] or 0),
                    "flags": int(row["flags"] or 0),
                }
        return out

    def prompt_checked(self, workflow_id: str, sim_day: int) -> bool:
        with self._connection() as conn:
            row = conn.execute(
                """SELECT status FROM prompt_checks
                   WHERE workflow_id=? AND sim_day=?""",
                (workflow_id, sim_day),
            ).fetchone()
        return row is not None and row["status"] != "below_threshold"

    def mark_prompt_checked(self, workflow_id: str, sim_day: int, status: str) -> None:
        with self._connection() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO prompt_checks(workflow_id, sim_day, status)
                   VALUES (?, ?, ?)""",
                (workflow_id, sim_day, status),
            )

    def experiment_dimension(self, workflow_id: str, default: str) -> str:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT dimension FROM experiment_cursor WHERE workflow_id=?", (workflow_id,)
            ).fetchone()
        return str(row["dimension"]) if row else default

    def set_experiment_dimension(self, workflow_id: str, dimension: str) -> None:
        with self._connection() as conn:
            conn.execute(
                """INSERT INTO experiment_cursor(workflow_id, dimension) VALUES (?, ?)
                   ON CONFLICT(workflow_id) DO UPDATE SET dimension=excluded.dimension""",
                (workflow_id, dimension),
            )

    def proposal_cursor_advancement(self, proposal_id: str) -> str | None:
        with self._connection() as conn:
            row = conn.execute(
                """SELECT status FROM proposal_cursor_advancements
                   WHERE proposal_id=?""",
                (proposal_id,),
            ).fetchone()
        return str(row["status"]) if row else None

    def experiment_evaluated(self, workflow_id: str, version: int) -> bool:
        with self._connection() as conn:
            row = conn.execute(
                """SELECT 1 FROM experiment_evaluations
                   WHERE workflow_id=? AND config_version=?""",
                (workflow_id, version),
            ).fetchone()
        return row is not None

    def mark_experiment_evaluated(self, workflow_id: str, version: int) -> None:
        with self._connection() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO experiment_evaluations(workflow_id, config_version)
                   VALUES (?, ?)""",
                (workflow_id, version),
            )

    def proposal(self, proposal_id: str) -> dict[str, Any] | None:
        with self._connection() as conn:
            row = conn.execute("SELECT * FROM proposals WHERE proposal_id=?", (proposal_id,)).fetchone()
        return dict(row) if row else None

    def proposals(self, workflow_id: str | None = None) -> list[dict[str, Any]]:
        with self._connection() as conn:
            if workflow_id:
                rows = conn.execute(
                    "SELECT * FROM proposals WHERE workflow_id=? ORDER BY created_at",
                    (workflow_id,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM proposals ORDER BY created_at").fetchall()
        return [dict(row) for row in rows]

    def save_proposal(
        self, proposal_id: str, workflow_id: str, base_version: int, kind: str,
        status: str, payload: dict[str, Any], approval_id: str | None = None,
        *, eligible_at: str | None = None, expires_at: str | None = None,
        audit_event: tuple[str, dict[str, Any]] | None = None,
        audit_ts: str | None = None,
    ) -> bool:
        if audit_event is not None and audit_ts is None:
            raise ValueError("audit_ts is required when saving an audit event")
        eligible_at = eligible_at or payload.get("eligible_at")
        expires_at = expires_at or payload.get("expires_at")
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """INSERT INTO proposals(proposal_id, workflow_id, base_version, kind,
                                          status, approval_id, payload, eligible_at, expires_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(proposal_id) DO UPDATE SET status=excluded.status,
                       approval_id=COALESCE(excluded.approval_id, proposals.approval_id),
                       payload=excluded.payload,
                       eligible_at=COALESCE(excluded.eligible_at, proposals.eligible_at),
                       expires_at=COALESCE(excluded.expires_at, proposals.expires_at)
                   WHERE proposals.status != 'applied'""",
                (proposal_id, workflow_id, base_version, kind, status, approval_id,
                 json.dumps(payload, sort_keys=True), eligible_at, expires_at),
            )
            if cursor.rowcount == 1:
                if status == "conflict":
                    conn.execute(
                        """UPDATE experiments SET status='cancelled'
                           WHERE proposal_id=?
                             AND status IN ('planned', 'running')""",
                        (proposal_id,),
                    )
                if audit_event is not None:
                    action, detail = audit_event
                    conn.execute(
                        """INSERT INTO audit(ts, workflow_id, action, detail)
                           VALUES (?, ?, ?, ?)""",
                        (audit_ts, workflow_id, action, json.dumps(detail, sort_keys=True)),
                    )
        return cursor.rowcount == 1

    def mark_application_attempted(
        self, proposal_id: str, approval_id: str
    ) -> bool:
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status, application_attempted FROM proposals WHERE proposal_id=?",
                (proposal_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown proposal {proposal_id}")
            if row["status"] not in ("pending", "submitted", "approved"):
                return False
            cursor = conn.execute(
                """UPDATE proposals SET status='approved',
                       approval_id=COALESCE(?, approval_id), application_attempted=1
                   WHERE proposal_id=? AND application_attempted=0
                     AND status IN ('pending', 'submitted', 'approved')""",
                (approval_id, proposal_id),
            )
        return cursor.rowcount == 1

    def finalize_applied_proposal(
        self, proposal_id: str, approval_id: str | None, ts: str, action: str,
        detail: dict[str, Any], *, advance_cursor: bool = False,
        primary_kpi: str | None = None,
    ) -> bool:
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT workflow_id, status FROM proposals
                   WHERE proposal_id=?""",
                (proposal_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"unknown proposal {proposal_id}")
            transitioned = row["status"] != "applied"
            if transitioned:
                conn.execute(
                    """UPDATE proposals SET status='applied',
                           approval_id=COALESCE(?, approval_id)
                       WHERE proposal_id=? AND status!='applied'""",
                    (approval_id, proposal_id),
                )
            applied_actions = (
                "auto_applied", "approval_applied", "approval_applied_reconciled"
            )
            existing_audits = conn.execute(
                """SELECT action, detail FROM audit
                   WHERE workflow_id=? AND action IN (?, ?, ?)""",
                (row["workflow_id"], *applied_actions),
            ).fetchall()
            audit_exists = any(
                json.loads(item["detail"]).get("proposal_id") == proposal_id
                for item in existing_audits
            )
            if not audit_exists:
                conn.execute(
                    """INSERT INTO audit(ts, workflow_id, action, detail)
                       VALUES (?, ?, ?, ?)""",
                    (ts, row["workflow_id"], action,
                     json.dumps(detail, sort_keys=True)),
                )
            if advance_cursor and primary_kpi == "engagement_rate":
                advancement = conn.execute(
                    """SELECT status FROM proposal_cursor_advancements
                       WHERE proposal_id=?""",
                    (proposal_id,),
                ).fetchone()
                if advancement is None:
                    current = conn.execute(
                        """SELECT dimension FROM experiment_cursor
                           WHERE workflow_id=?""",
                        (row["workflow_id"],),
                    ).fetchone()
                    dimension = (
                        str(current["dimension"])
                        if current else "welcome_message"
                    )
                    next_dimension = (
                        "voice" if dimension == "welcome_message"
                        else "welcome_message"
                    )
                    conn.execute(
                        """INSERT INTO experiment_cursor(workflow_id, dimension)
                           VALUES (?, ?)
                           ON CONFLICT(workflow_id) DO UPDATE SET
                               dimension=excluded.dimension""",
                        (row["workflow_id"], next_dimension),
                    )
                    conn.execute(
                        """INSERT INTO proposal_cursor_advancements
                           (proposal_id, workflow_id, status, from_dimension,
                            to_dimension)
                           VALUES (?, ?, 'advanced', ?, ?)""",
                        (proposal_id, row["workflow_id"], dimension, next_dimension),
                    )
                elif advancement["status"] == "legacy_unverified":
                    LOG.warning(
                        "Cursor advancement for legacy applied proposal %s "
                        "is ambiguous; reconcile it manually before changing "
                        "the experiment cursor",
                        proposal_id,
                    )
        return transitioned

    def mark_approval_applied(
        self, proposal_id: str, approval_id: str, ts: str, action: str,
        detail: dict[str, Any],
    ) -> bool:
        return self.finalize_applied_proposal(
            proposal_id, approval_id, ts, action, detail
        )

    def has_application_audit(self, proposal_id: str) -> bool:
        applied_actions = (
            "auto_applied", "approval_applied", "approval_applied_reconciled"
        )
        with self._connection() as conn:
            rows = conn.execute(
                """SELECT detail FROM audit WHERE action IN (?, ?, ?)""",
                applied_actions,
            ).fetchall()
        return any(
            json.loads(row["detail"]).get("proposal_id") == proposal_id
            for row in rows
        )

    def audit(self, ts: str, workflow_id: str, action: str, detail: dict[str, Any]) -> None:
        with self._connection() as conn:
            conn.execute(
                "INSERT INTO audit(ts, workflow_id, action, detail) VALUES (?, ?, ?, ?)",
                (ts, workflow_id, action, json.dumps(detail, sort_keys=True)),
            )

    def audit_rows(self, workflow_id: str | None = None) -> list[dict[str, Any]]:
        with self._connection() as conn:
            if workflow_id:
                rows = conn.execute(
                    """SELECT ts, workflow_id, action, detail FROM audit
                       WHERE workflow_id=? ORDER BY id""",
                    (workflow_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT ts, workflow_id, action, detail FROM audit ORDER BY id"
                ).fetchall()
        return [
            {**dict(row), "detail": json.loads(row["detail"])}
            for row in rows
        ]


def _sim_day(ts: str) -> int:
    # Simulator timestamps are ISO-8601 IST beginning 2027-01-04.
    from datetime import date
    day = date.fromisoformat(ts[:10]) - date(2027, 1, 4)
    return day.days


def _stable_id(
    kind: str, wid: str, version: int, config: dict[str, Any], salt: str = ""
) -> str:
    encoded = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(encoded + salt.encode()).hexdigest()[:16]
    return f"ctl-{kind}-{wid}-v{version}-{digest}"


def _proportion_z(a: ArmStats, b: ArmStats, metric: str) -> tuple[float, float] | None:
    def counts(s: ArmStats) -> tuple[int, int]:
        if metric == "pickup_rate":
            return s.connected, s.dials
        return s.engaged, s.connected

    x1, n1 = counts(a)
    x2, n2 = counts(b)
    minimum = MIN_ARM_DIALS if metric == "pickup_rate" else MIN_ARM_CONNECTED
    if n1 < minimum or n2 < minimum:
        return None
    return _two_proportion_z(x1, n1, x2, n2)


def _two_proportion_z(x1: int, n1: int, x2: int, n2: int) -> tuple[float, float]:
    p1, p2 = x1 / n1, x2 / n2
    pooled = (x1 + x2) / (n1 + n2)
    se = math.sqrt(max(pooled * (1 - pooled) * (1 / n1 + 1 / n2), 1e-12))
    return (p1 - p2) / se, p1 - p2


def _metric_counts(arm: ArmStats, metric: str) -> tuple[int, int]:
    if metric == "pickup_rate":
        return arm.connected, arm.dials
    if metric == "engagement_rate":
        return arm.engaged, arm.connected
    raise ValueError(f"unsupported KPI {metric}")


def _experiment_arm_stats(
    observations: list[dict[str, Any]], variant: str
) -> ArmStats:
    rows = [row for row in observations if row["variant"] == variant]
    return ArmStats(
        dials=len(rows),
        connected=sum(int(row["connected"]) for row in rows),
        engaged=sum(int(row["engaged"]) for row in rows),
        hallucination_flags=0,
    )


def _planned_look_cutoff(
    observations: list[dict[str, Any]], experiment: dict[str, Any], fraction: float
) -> int | None:
    primary_metric = experiment["primary_kpi"]
    guardrail_metric = experiment["guardrail_kpi"]
    primary_target = math.ceil(int(experiment["target_primary_n"]) * fraction)
    guardrail_target = math.ceil(int(experiment["target_guardrail_n"]) * fraction)
    counts = {
        experiment["control_variant"]: ArmStats(0, 0, 0, 0),
        experiment["candidate_variant"]: ArmStats(0, 0, 0, 0),
    }
    for row in observations:
        variant = row["variant"]
        current = counts.get(variant)
        if current is None:
            continue
        counts[variant] = ArmStats(
            dials=current.dials + 1,
            connected=current.connected + int(row["connected"]),
            engaged=current.engaged + int(row["engaged"]),
            hallucination_flags=0,
        )
        if all(
            _metric_counts(arm, primary_metric)[1] >= primary_target
            and _metric_counts(arm, guardrail_metric)[1] >= guardrail_target
            for arm in counts.values()
        ):
            return int(row["event_id"])
    return None


def _wilson_score_interval(
    successes: int, sample_size: int, critical_value: float
) -> tuple[float, float]:
    if sample_size <= 0 or not 0 <= successes <= sample_size:
        raise ValueError("Wilson interval requires valid binomial counts")
    rate = successes / sample_size
    z_squared = critical_value * critical_value
    denominator = 1 + z_squared / sample_size
    center = (rate + z_squared / (2 * sample_size)) / denominator
    half_width = (
        critical_value
        * math.sqrt(
            rate * (1 - rate) / sample_size
            + z_squared / (4 * sample_size * sample_size)
        )
        / denominator
    )
    return max(0.0, center - half_width), min(1.0, center + half_width)


def _newcombe_wilson_lower_bound(
    candidate_successes: int,
    candidate_sample_size: int,
    control_successes: int,
    control_sample_size: int,
    critical_value: float,
) -> float:
    candidate_rate = candidate_successes / candidate_sample_size
    control_rate = control_successes / control_sample_size
    candidate_lower, _ = _wilson_score_interval(
        candidate_successes, candidate_sample_size, critical_value
    )
    _, control_upper = _wilson_score_interval(
        control_successes, control_sample_size, critical_value
    )
    return (candidate_rate - control_rate) - math.hypot(
        candidate_rate - candidate_lower,
        control_upper - control_rate,
    )


def _evaluate_experiment_look(
    experiment: dict[str, Any], family: dict[str, Any],
    observations: list[dict[str, Any]], look: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    control = _experiment_arm_stats(
        observations, experiment["control_variant"]
    )
    candidate = _experiment_arm_stats(
        observations, experiment["candidate_variant"]
    )
    primary_candidate, primary_n_candidate = _metric_counts(
        candidate, experiment["primary_kpi"]
    )
    primary_control, primary_n_control = _metric_counts(
        control, experiment["primary_kpi"]
    )
    guard_candidate, guard_n_candidate = _metric_counts(
        candidate, experiment["guardrail_kpi"]
    )
    guard_control, guard_n_control = _metric_counts(
        control, experiment["guardrail_kpi"]
    )
    primary_z, primary_delta = _two_proportion_z(
        primary_candidate, primary_n_candidate,
        primary_control, primary_n_control,
    )
    guardrail_delta = guard_candidate / guard_n_candidate - guard_control / guard_n_control
    alpha = float(family["alpha_per_hypothesis_per_look"])
    primary_critical = statistics.NormalDist().inv_cdf(1 - alpha / 2)
    guardrail_critical = statistics.NormalDist().inv_cdf(1 - alpha)
    guardrail_lower = _newcombe_wilson_lower_bound(
        guard_candidate,
        guard_n_candidate,
        guard_control,
        guard_n_control,
        guardrail_critical,
    )
    margin = float(experiment["guardrail_margin"])
    primary_significant = primary_z >= primary_critical
    practical = primary_delta >= float(experiment["practical_effect"])
    guardrail_safe = guardrail_lower > -margin
    if primary_significant and practical and guardrail_safe:
        outcome = "beneficial"
    elif int(look["look_number"]) == int(family["look_count"]):
        outcome = "guardrail_blocked" if not guardrail_safe else "inconclusive"
    else:
        outcome = "continue"
    result = {
        "experiment_id": experiment["experiment_id"],
        "look_number": int(look["look_number"]),
        "cutoff_event_id": int(look["cutoff_event_id"]),
        "alpha_per_hypothesis_per_look": alpha,
        "primary_kpi": experiment["primary_kpi"],
        "primary_z": primary_z,
        "primary_critical_value": primary_critical,
        "primary_delta": primary_delta,
        "practical_effect": float(experiment["practical_effect"]),
        "primary_significant": primary_significant,
        "practical_effect_met": practical,
        "control": {
            "variant": experiment["control_variant"],
            "successes": primary_control,
            "sample_size": primary_n_control,
            "rate": primary_control / primary_n_control,
        },
        "candidate": {
            "variant": experiment["candidate_variant"],
            "successes": primary_candidate,
            "sample_size": primary_n_candidate,
            "rate": primary_candidate / primary_n_candidate,
        },
        "guardrail": {
            "kpi": experiment["guardrail_kpi"],
            "margin": margin,
            "candidate_minus_control": guardrail_delta,
            "lower_confidence_bound_method": "Newcombe-Wilson hybrid score",
            "confidence_level": 1 - alpha,
            "critical_value": guardrail_critical,
            "lower_confidence_bound": guardrail_lower,
            "safe": guardrail_safe,
            "control_sample_size": guard_n_control,
            "candidate_sample_size": guard_n_candidate,
        },
        "outcome": outcome,
    }
    return outcome, result


class Controller:
    def __init__(self, url: str, db_path: str, store: Store | None = None):
        self.client = SimClient(url)
        self.store = store if store is not None else Store(db_path)
        self._workflow_specs: dict[str, dict[str, Any]] = {}
        self._experiment_registration_skips: set[str] = set()

    async def _db(self, method, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(method, *args, **kwargs)

    async def run(self) -> None:
        try:
            state = await self.client.request("GET", "/sim/state")
            self._workflow_specs = {
                w["workflow_id"]: w for w in await self.client.request("GET", "/workflows")
            }
            await self._ingest_available()
            await self._register_eligible_experiment_plans()
            await self._evaluate_registered_experiments(state)
            await self._reconcile(state["ts"], plan=not state["finished"])
            no_progress_advances = 0
            while not state["finished"]:
                await self._ingest_available()
                state = await self.client.request("GET", "/sim/state")
                await self._register_eligible_experiment_plans()
                await self._evaluate_registered_experiments(state)
                await self._reconcile(state["ts"], plan=not state["finished"])
                if state["finished"]:
                    break
                previous_minute = int(state["sim_minute"])
                try:
                    await self.client.advance(60)
                except RemoteError as exc:
                    if exc.status == 409 and exc.code == "finished":
                        state = await self.client.request("GET", "/sim/state")
                        if not state["finished"]:
                            raise
                        break
                    raise
                state = await self.client.request("GET", "/sim/state")
                current_minute = int(state["sim_minute"])
                if current_minute < previous_minute:
                    raise RuntimeError(
                        "simulator clock moved backwards: "
                        f"{previous_minute} -> {current_minute} "
                        f"({state['ts']})"
                    )
                if state["finished"]:
                    break
                if current_minute == previous_minute:
                    no_progress_advances += 1
                    if no_progress_advances >= MAX_CONSECUTIVE_NO_PROGRESS_ADVANCES:
                        raise RuntimeError(
                            "simulator made no progress after "
                            f"{no_progress_advances} consecutive advances; "
                            f"last simulator time is {state['ts']} "
                            f"(sim_minute={current_minute})"
                        )
                else:
                    no_progress_advances = 0
            await self._ingest_available()
            await self._register_eligible_experiment_plans()
            await self._evaluate_registered_experiments(state)
            await self._reconcile(state["ts"], plan=False)
            LOG.info("scenario finished at %s", state["ts"])
        finally:
            await self.client.close()

    async def _ingest_available(self) -> None:
        after = await self._db(self.store.last_chunk)
        for kind in ("history", "events"):
            chunks = await self.client.request(
                "GET", "/events/chunks", params={"kind": kind, "after": after}
            )
            for chunk in sorted(chunks, key=lambda item: item["chunk"]):
                raw = await self.client.get_chunk(chunk["chunk"])
                await self._db(self.store.process_chunk, chunk, raw)
                after = max(after, int(chunk["chunk"]))

    async def _register_eligible_experiment_plans(self) -> None:
        proposals = await self._db(self.store.proposals)
        experiments = await self._db(self.store.experiments)
        registered_proposals = {
            experiment["proposal_id"]
            for experiment in experiments
            if experiment["proposal_id"] is not None
        }
        for proposal in proposals:
            proposal_id = proposal["proposal_id"]
            if (
                proposal["status"] != "applied"
                or proposal["kind"] not in ("approval", "auto")
                or proposal_id in registered_proposals
                or proposal_id in self._experiment_registration_skips
            ):
                continue
            payload = json.loads(proposal["payload"])
            if not str(payload.get("rationale", "")).startswith(
                "Start two-arm "
            ):
                continue
            self._experiment_registration_skips.add(proposal_id)
            LOG.warning(
                "Skipping fixed-horizon experiment registration for applied "
                "proposal %s (workflow %s): no compatible pre-treatment, "
                "person-level baseline exists in the Controller store for the "
                "experiment's configuration version and cohort; daily arm "
                "aggregates cannot supply that baseline or determine valid "
                "sample targets, the documented workflow response does not "
                "define a guardrail deterioration margin, and no experiment-"
                "family membership was persisted for this proposal. No "
                "experiment plan was created.",
                proposal_id,
                proposal["workflow_id"],
            )

    async def _evaluate_registered_experiments(
        self, simulator_state: dict[str, Any]
    ) -> None:
        running = await self._db(self.store.experiments, "running")
        for experiment in running:
            family = await self._db(
                self.store.experiment_family, experiment["family_id"]
            )
            if family is None:
                raise RuntimeError(
                    f"missing family {experiment['family_id']} for "
                    f"experiment {experiment['experiment_id']}"
                )
            primary_minimum = (
                MIN_REGISTERED_TARGET_ARM_DIALS
                if experiment["primary_kpi"] == "pickup_rate"
                else MIN_REGISTERED_TARGET_ARM_CONNECTED
            )
            guardrail_minimum = (
                MIN_REGISTERED_TARGET_ARM_DIALS
                if experiment["guardrail_kpi"] == "pickup_rate"
                else MIN_REGISTERED_TARGET_ARM_CONNECTED
            )
            plan_error = None
            if (
                experiment["target_primary_n"] is None
                or experiment["target_guardrail_n"] is None
            ):
                plan_error = "fixed sample targets were not registered"
            elif (
                int(experiment["target_primary_n"]) < primary_minimum
                or int(experiment["target_guardrail_n"]) < guardrail_minimum
            ):
                plan_error = "registered sample targets are below metric minimums"
            elif (
                experiment["baseline_primary"] is None
                or experiment["baseline_guardrail"] is None
                or not 0 <= float(experiment["baseline_primary"]) <= 1
                or not 0 <= float(experiment["baseline_guardrail"]) <= 1
            ):
                plan_error = "valid historical baseline rates were not registered"
            elif experiment["guardrail_margin"] is None:
                plan_error = "guardrail deterioration margin was not registered"

            if plan_error is not None:
                if simulator_state["finished"]:
                    await self._db(
                        self.store.mark_experiment_underpowered,
                        experiment["experiment_id"],
                        int(simulator_state["last_event_id"]),
                        simulator_state["ts"],
                        plan_error,
                        {"target_primary_n": experiment["target_primary_n"],
                         "target_guardrail_n": experiment["target_guardrail_n"]},
                    )
                continue

            observations = await self._db(
                self.store.experiment_observations, experiment["experiment_id"]
            )
            looks = await self._db(
                self.store.experiment_looks, experiment["experiment_id"]
            )
            for look in looks:
                if look["status"] == "evaluated":
                    if json.loads(look["result_json"]).get("outcome") == "beneficial":
                        break
                    continue
                if look["status"] not in ("planned", "ready"):
                    continue
                if look["cutoff_event_id"] is None:
                    cutoff = _planned_look_cutoff(
                        observations, experiment, float(look["information_fraction"])
                    )
                    if cutoff is None:
                        if simulator_state["finished"]:
                            await self._db(
                                self.store.mark_experiment_underpowered,
                                experiment["experiment_id"],
                                int(simulator_state["last_event_id"]),
                                simulator_state["ts"],
                                "registered sample target was not reached by the "
                                "simulator horizon",
                                {"target_primary_n": experiment["target_primary_n"],
                                 "target_guardrail_n": experiment["target_guardrail_n"],
                                 "observed_people": len(observations)},
                            )
                        break
                    await self._db(
                        self.store.set_experiment_look_cutoff,
                        experiment["experiment_id"],
                        int(look["look_number"]),
                        cutoff,
                    )
                    looks = await self._db(
                        self.store.experiment_looks, experiment["experiment_id"]
                    )
                    look = next(
                        item for item in looks
                        if item["look_number"] == look["look_number"]
                    )
                scoped = await self._db(
                    self.store.experiment_observations,
                    experiment["experiment_id"],
                    look_number=int(look["look_number"]),
                )
                outcome, result = _evaluate_experiment_look(
                    experiment, family, scoped, look
                )
                finalized = await self._db(
                    self.store.finalize_experiment_look,
                    experiment["experiment_id"],
                    int(look["look_number"]),
                    outcome,
                    result,
                    simulator_state["ts"],
                )
                if finalized and outcome in (
                    "beneficial", "inconclusive", "guardrail_blocked"
                ):
                    break
                if not finalized:
                    break

        for experiment in await self._db(self.store.experiments, "beneficial"):
            await self._submit_experiment_promotion(experiment, simulator_state["ts"])

    async def _submit_experiment_promotion(
        self, experiment: dict[str, Any], ts: str
    ) -> None:
        spec = self._workflow_specs.get(experiment["workflow_id"])
        if spec is None:
            return
        looks = await self._db(
            self.store.experiment_looks, experiment["experiment_id"]
        )
        successful_look = next(
            (
                look for look in reversed(looks)
                if look["status"] == "evaluated"
                and json.loads(look["result_json"]).get("outcome") == "beneficial"
            ),
            None,
        )
        if successful_look is None:
            raise RuntimeError(
                f"beneficial experiment {experiment['experiment_id']} "
                "has no persisted beneficial look"
            )
        result = json.loads(successful_look["result_json"])
        current = await self.client.request(
            "GET", f"/workflows/{experiment['workflow_id']}/config"
        )
        if int(current["version"]) != int(experiment["config_version"]):
            return
        revised = {
            key: value for key, value in current["config"].items()
        }
        revised["allocations"] = {
            cohort: dict(dimensions)
            for cohort, dimensions in current["config"]["allocations"].items()
        }
        population = revised["allocations"].get(experiment["cohort_id"])
        if population is None or experiment["dimension"] not in population:
            return
        allocation = population[experiment["dimension"]]
        frozen_pair = {
            experiment["control_variant"], experiment["candidate_variant"]
        }
        if set(allocation) != frozen_pair:
            return
        population[experiment["dimension"]] = {
            experiment["candidate_variant"]: 0.8,
            experiment["control_variant"]: 0.2,
        }
        revised["_evidence"] = {
            "registered_experiment": result,
            "family_id": experiment["family_id"],
            "workflow_id": experiment["workflow_id"],
            "cohort_id": experiment["cohort_id"],
            "config_version": experiment["config_version"],
        }
        reason = (
            f"Promote {experiment['candidate_variant']} after planned "
            f"look {successful_look['look_number']} passed the registered "
            f"{experiment['primary_kpi']} and guardrail criteria."
        )
        await self._submit_change(
            experiment["workflow_id"], spec, current, revised, reason, ts,
            proposal_id_salt=f"experiment:{experiment['experiment_id']}",
        )

    async def _record_applied_proposal(
        self, proposal_id: str, approval_id: str | None, ts: str,
        action: str, detail: dict[str, Any], payload: dict[str, Any],
    ) -> bool:
        rationale = str(payload.get("rationale", ""))
        spec = self._workflow_specs.get(detail.get("workflow_id", ""))
        if spec is None:
            proposal = await self._db(self.store.proposal, proposal_id)
            spec = self._workflow_specs.get(
                proposal["workflow_id"] if proposal else "", {}
            )
        primary_kpi = (
            spec.get("objective", {}).get("primary")
            if rationale.startswith("Promote ")
            else None
        )
        return await self._db(
            self.store.finalize_applied_proposal,
            proposal_id,
            approval_id,
            ts,
            action,
            detail,
            advance_cursor=rationale.startswith("Promote "),
            primary_kpi=primary_kpi,
        )

    async def _reconcile(self, ts: str, *, plan: bool = True) -> None:
        proposals = await self._db(self.store.proposals)
        for proposal in proposals:
            if proposal["status"] != "applied":
                continue
            payload = json.loads(proposal["payload"])
            has_audit = await self._db(
                self.store.has_application_audit, proposal["proposal_id"]
            )
            advancement = await self._db(
                self.store.proposal_cursor_advancement,
                proposal["proposal_id"],
            )
            rationale = str(payload.get("rationale", ""))
            spec = self._workflow_specs.get(proposal["workflow_id"], {})
            primary_kpi = spec.get("objective", {}).get("primary")
            if has_audit:
                if (
                    rationale.startswith("Promote ")
                    and primary_kpi == "engagement_rate"
                    and advancement is None
                ):
                    await self._record_applied_proposal(
                        proposal["proposal_id"],
                        proposal.get("approval_id"),
                        ts,
                        "approval_applied_reconciled",
                        {"proposal_id": proposal["proposal_id"],
                         "workflow_id": proposal["workflow_id"],
                         "rationale": rationale},
                        payload,
                    )
                elif advancement == "legacy_unverified":
                    LOG.warning(
                        "Applied legacy proposal %s has an ambiguous "
                        "cursor history; audit presence does not establish "
                        "whether its cursor advancement occurred",
                        proposal["proposal_id"],
                    )
                continue
            if proposal["kind"] == "auto":
                await self._reconcile_auto(proposal, payload, ts)
                continue
            history = await self.client.request(
                "GET",
                f"/workflows/{proposal['workflow_id']}/config/history",
            )
            applied = next(
                (
                    item for item in history
                    if item.get("proposal_id") == proposal["proposal_id"]
                    and int(item["version"]) > int(proposal["base_version"])
                ),
                None,
            )
            if applied is not None:
                aid = applied.get("approval_id") or proposal.get("approval_id")
                await self._start_experiment_for_proposal(
                    proposal["proposal_id"]
                )
                await self._record_applied_proposal(
                    proposal["proposal_id"],
                    aid,
                    applied.get("applied_at") or ts,
                    "approval_applied_reconciled",
                    {"approval_id": aid,
                     "proposal_id": proposal["proposal_id"],
                     "version": applied["version"],
                     "actor": applied.get("actor"),
                     "rationale": payload.get("rationale")},
                    payload,
                )

        approvals = await self.client.request("GET", "/approvals")
        approvals_by_pid = {item["proposal_id"]: item for item in approvals}
        for proposal in proposals:
            if proposal["status"] == "applied":
                continue
            if proposal["status"] in (
                "rejected", "expired", "lapsed", "conflict", "not_permitted"
            ):
                continue
            payload = json.loads(proposal["payload"])
            remote = approvals_by_pid.get(proposal["proposal_id"])
            if proposal["kind"] == "auto":
                await self._reconcile_auto(proposal, payload, ts)
                continue
            if remote is None:
                eligible_at = proposal.get("eligible_at") or payload.get("eligible_at")
                if proposal["status"] == "deferred" and not eligible_at:
                    continue
                if eligible_at and not _timestamp_is_due(ts, eligible_at):
                    continue
                if eligible_at:
                    spec = self._workflow_specs.get(proposal["workflow_id"], {})
                    approval_hours = spec.get("governance", {}).get(
                        "approver_hours", [10, 19]
                    )
                    if not _in_approver_hours(ts, approval_hours):
                        next_eligible = _next_approver_time(
                            ts, approval_hours
                        )
                        payload["eligible_at"] = next_eligible
                        await self._db(
                            self.store.save_proposal,
                            proposal["proposal_id"], proposal["workflow_id"],
                            proposal["base_version"], "approval", "deferred", payload,
                            eligible_at=next_eligible,
                        )
                        continue
                    current = await self.client.request(
                        "GET", f"/workflows/{proposal['workflow_id']}/config"
                    )
                    if int(current["version"]) != int(proposal["base_version"]):
                        await self._db(
                            self.store.save_proposal,
                            proposal["proposal_id"], proposal["workflow_id"],
                            proposal["base_version"], "approval", "conflict", payload,
                            audit_event=(
                                "approval_conflict",
                                {"proposal_id": proposal["proposal_id"],
                                 "reason": "base version changed before deferred submission",
                                 "base_version": proposal["base_version"],
                                 "current_version": current["version"]},
                            ),
                            audit_ts=ts,
                        )
                        continue
                    await self._db(
                        self.store.save_proposal,
                        proposal["proposal_id"], proposal["workflow_id"],
                        proposal["base_version"], "approval", "submitted", payload,
                        eligible_at=eligible_at,
                    )
                request_payload = {
                    key: value for key, value in payload.items()
                    if key not in ("eligible_at", "expires_at")
                }
                # The remote request may have succeeded before a process died.
                # Re-post exactly the same idempotency key and reconcile a duplicate.
                try:
                    remote = await self.client.request(
                        "POST", "/approvals", json=request_payload
                    )
                except RemoteError as exc:
                    if exc.status == 409 and exc.code == "duplicate_proposal_id":
                        approvals = await self.client.request("GET", "/approvals")
                        remote = next(
                            (item for item in approvals
                             if item["proposal_id"] == proposal["proposal_id"]),
                            None,
                        )
                        if remote is None:
                            raise RuntimeError(
                                f"proposal {proposal['proposal_id']} duplicate not found"
                            )
                    elif exc.status == 409 and exc.code == "stale_base_version":
                        await self._db(
                            self.store.save_proposal,
                            proposal["proposal_id"], proposal["workflow_id"],
                            proposal["base_version"], "approval", "conflict", payload,
                            audit_event=(
                                "approval_conflict",
                                {"proposal_id": proposal["proposal_id"],
                                 "reason": "base version changed during deferred submission",
                                 "base_version": proposal["base_version"]},
                            ),
                            audit_ts=ts,
                        )
                        continue
                    else:
                        raise
            if remote is None:
                continue
            aid = remote["approval_id"]
            status = remote["status"]
            if status == "approved" and proposal.get("application_attempted"):
                history_path = (
                    f"/workflows/{proposal['workflow_id']}/config/history"
                )
                try:
                    history = await self.client.request("GET", history_path)
                except (RemoteError, httpx.TimeoutException, httpx.TransportError) as exc:
                    await self._db(
                        self.store.audit, ts, proposal["workflow_id"],
                        "approval_application_recovery_history_failed",
                        {"proposal_id": proposal["proposal_id"], "approval_id": aid,
                         "error": str(exc)},
                    )
                    raise
                applied = next(
                    (
                        item for item in history
                        if item.get("proposal_id") == proposal["proposal_id"]
                        and item.get("approval_id") == aid
                        and item.get("actor") == "controller_approved"
                        and int(item["version"]) > int(proposal["base_version"])
                    ),
                    None,
                )
                if applied is None:
                    await self._db(
                        self.store.audit, ts, proposal["workflow_id"],
                        "approval_application_recovery_unconfirmed",
                        {"proposal_id": proposal["proposal_id"], "approval_id": aid},
                    )
                    continue
                await self._start_experiment_for_proposal(proposal["proposal_id"])
                await self._record_applied_proposal(
                    proposal["proposal_id"], aid, ts,
                    "approval_applied_reconciled",
                    {"approval_id": aid, "proposal_id": proposal["proposal_id"],
                     "version": applied["version"], "actor": applied["actor"],
                     "rationale": payload.get("rationale"),
                     "workflow_id": proposal["workflow_id"]},
                    payload,
                )
                continue
            if status == "approved":
                cfg = await self.client.request(
                    "GET", f"/workflows/{proposal['workflow_id']}/config"
                )
                if cfg["version"] != proposal["base_version"]:
                    await self._db(self.store.save_proposal,
                        proposal["proposal_id"], proposal["workflow_id"],
                        proposal["base_version"], "approval", "conflict", payload, aid,
                        audit_event=(
                            "approval_conflict",
                            {"approval_id": aid, "proposal_id": proposal["proposal_id"],
                             "reason": "base version changed before apply"},
                        ),
                        audit_ts=ts,
                    )
                    continue
                attempt_started = await self._db(
                    self.store.mark_application_attempted,
                    proposal["proposal_id"], aid,
                )
                if not attempt_started:
                    continue
                try:
                    result = await self.client.request(
                        "POST", f"/workflows/{proposal['workflow_id']}/apply",
                        json={"approval_id": aid}, safe_retry=False,
                    )
                    status = "applied"
                    await self._start_experiment_for_proposal(
                        proposal["proposal_id"]
                    )
                    await self._record_applied_proposal(
                        proposal["proposal_id"], aid, ts, "approval_applied",
                        {"approval_id": aid, "proposal_id": proposal["proposal_id"],
                         "result": result, "rationale": payload.get("rationale"),
                         "workflow_id": proposal["workflow_id"]},
                        payload,
                    )
                except RemoteError as exc:
                    if exc.status == 410:
                        status = "lapsed"
                    elif exc.status == 409 and exc.code == "stale_base_version":
                        status = "conflict"
                    else:
                        raise
                if status == "applied":
                    continue
            elif status == "applied":
                await self._start_experiment_for_proposal(
                    proposal["proposal_id"]
                )
                await self._record_applied_proposal(
                    proposal["proposal_id"], aid, ts,
                    "approval_applied_reconciled",
                    {"approval_id": aid, "proposal_id": proposal["proposal_id"],
                     "rationale": payload.get("rationale"),
                     "workflow_id": proposal["workflow_id"]},
                    payload,
                )
                continue
            terminal_audit = None
            if status in ("rejected", "expired", "lapsed"):
                await self._db(
                    self.store.cancel_experiment_for_proposal,
                    proposal["proposal_id"],
                )
                terminal_audit = (
                    f"approval_{status}",
                    {"approval_id": aid, "proposal_id": proposal["proposal_id"],
                     "reasons": remote.get("reasons", [])},
                )
            await self._db(self.store.save_proposal,
                proposal["proposal_id"], proposal["workflow_id"],
                proposal["base_version"], "approval", status, payload, aid,
                expires_at=remote.get("expires_at"),
                audit_event=terminal_audit,
                audit_ts=ts if terminal_audit is not None else None,
            )

        if plan:
            for wid, spec in self._workflow_specs.items():
                config = await self.client.request("GET", f"/workflows/{wid}/config")
                await self._detect_and_plan(wid, spec, config, ts)

    async def _reconcile_auto(
        self, proposal: dict[str, Any], payload: dict[str, Any], ts: str
    ) -> None:
        history = await self.client.request(
            "GET", f"/workflows/{proposal['workflow_id']}/config/history"
        )
        applied = next(
            (item for item in history if item.get("proposal_id") == proposal["proposal_id"]),
            None,
        )
        if applied:
            await self._start_experiment_for_proposal(proposal["proposal_id"])
            await self._record_applied_proposal(
                proposal["proposal_id"],
                None,
                applied.get("applied_at") or ts,
                "auto_applied",
                {"proposal_id": proposal["proposal_id"],
                 "version": applied["version"],
                 "rationale": payload.get("rationale"),
                 "workflow_id": proposal["workflow_id"]},
                payload,
            )
            return
        current = await self.client.request(
            "GET", f"/workflows/{proposal['workflow_id']}/config"
        )
        if current["version"] != proposal["base_version"]:
            await self._db(self.store.save_proposal,
                proposal["proposal_id"], proposal["workflow_id"],
                proposal["base_version"], "auto", "conflict", payload,
            )
            await self._db(
                self.store.audit, ts, proposal["workflow_id"], "auto_apply_conflict",
                {"proposal_id": proposal["proposal_id"], "reason": "base version changed"},
            )
            return
        try:
            result = await self.client.request(
                "PUT", f"/workflows/{proposal['workflow_id']}/config",
                json=payload, safe_retry=False,
            )
        except (httpx.TimeoutException, httpx.TransportError):
            history = await self.client.request(
                "GET", f"/workflows/{proposal['workflow_id']}/config/history"
            )
            if not any(item.get("proposal_id") == proposal["proposal_id"] for item in history):
                raise
            result = {"reconciled": True}
        except RemoteError as exc:
            if exc.status == 409 and exc.code == "duplicate_proposal_id":
                result = {"reconciled": True}
            elif exc.status == 409 and exc.code == "stale_base_version":
                await self._db(self.store.save_proposal,
                    proposal["proposal_id"], proposal["workflow_id"],
                    proposal["base_version"], "auto", "conflict", payload,
                )
                await self._db(
                    self.store.audit, ts, proposal["workflow_id"], "auto_apply_conflict",
                    {"proposal_id": proposal["proposal_id"], "detail": exc.detail},
                )
                return
            elif exc.status == 403:
                await self._db(
                    self.store.cancel_experiment_for_proposal,
                    proposal["proposal_id"],
                )
                await self._db(self.store.save_proposal,
                    proposal["proposal_id"], proposal["workflow_id"],
                    proposal["base_version"], "auto", "not_permitted", payload,
                )
                await self._db(
                    self.store.audit, ts, proposal["workflow_id"], "auto_apply_denied",
                    {"proposal_id": proposal["proposal_id"], "detail": exc.detail},
                )
                return
            else:
                raise
        await self._start_experiment_for_proposal(proposal["proposal_id"])
        await self._record_applied_proposal(
            proposal["proposal_id"],
            None,
            ts,
            "auto_applied",
            {"proposal_id": proposal["proposal_id"], "result": result,
             "rationale": payload.get("rationale"),
             "workflow_id": proposal["workflow_id"]},
            payload,
        )

    async def _start_experiment_for_proposal(self, proposal_id: str) -> None:
        experiment_id = await self._db(
            self.store.pending_experiment_for_proposal, proposal_id
        )
        if experiment_id is None:
            return
        state = await self.client.request("GET", "/sim/state")
        started = await self._db(
            self.store.start_experiment,
            experiment_id,
            int(state["last_event_id"]),
        )
        if not started:
            experiments = await self._db(self.store.experiments)
            existing = next(
                (item for item in experiments
                 if item["experiment_id"] == experiment_id),
                None,
            )
            if existing is None:
                raise RuntimeError(
                    f"experiment {experiment_id} disappeared during startup"
                )
            if existing["status"] == "running":
                return
            if existing["status"] == "cancelled":
                return
            raise RuntimeError(
                f"experiment {experiment_id} did not reach a terminal startup state"
            )

    async def _detect_and_plan(
        self, wid: str, spec: dict[str, Any], current: dict[str, Any], ts: str
    ) -> None:
        if any(p["workflow_id"] == wid and p["status"] in
               ("pending", "approved", "submitted", "deferred")
               for p in await self._db(self.store.proposals, wid)):
            return
        cfg = current["config"]
        version = int(current["version"])
        change: dict[str, Any] | None = None
        reason = ""
        prompt = await self._prompt_change(wid, spec, cfg, ts)
        if prompt:
            change, reason = prompt
        incident = await self._incident_change(
            wid, spec, cfg, _sim_day(ts), version
        )
        if change is None and incident:
            change, reason = incident
        if change is None:
            experiment = await self._experiment_change(wid, spec, cfg, version)
            if experiment:
                change, reason = experiment
        if change is not None:
            await self._submit_change(wid, spec, current, change, reason, ts)

    async def _incident_change(
        self, wid: str, spec: dict[str, Any], cfg: dict[str, Any],
        sim_day: int, config_version: int,
    ) -> tuple[dict[str, Any], str] | None:
        stats = await self._db(
            self.store.incident_stats,
            wid,
            sim_day - 2,
            cohort_id="default",
            config_version=config_version,
        )
        well_sampled = {
            did: values for did, values in stats.items() if values["dials"] >= MIN_INCIDENT_CALLS
        }
        if len(well_sampled) < 2:
            return None
        primary = spec["objective"]["primary"]
        guardrail = "engagement_rate" if primary == "pickup_rate" else "pickup_rate"

        def arm(values: dict[str, int]) -> ArmStats:
            return ArmStats(
                values["dials"], values["connected"], values["engaged"],
                values["flags"],
            )

        minimum = MIN_ARM_DIALS if primary == "pickup_rate" else MIN_ARM_CONNECTED
        if any(
            (
                values["dials"]
                if primary == "pickup_rate"
                else values["connected"]
            ) < minimum
            for values in well_sampled.values()
        ):
            return None

        def rate(values: dict[str, int], metric: str) -> float:
            numerator, denominator = (
                (values["connected"], values["dials"])
                if metric == "pickup_rate"
                else (values["engaged"], values["connected"])
            )
            return numerator / denominator if denominator else 0.0

        rates = {did: rate(values, primary) for did, values in well_sampled.items()}
        baseline = max(rates.values())
        worst = min(rates, key=rates.get)
        best_did = max(rates, key=rates.get)
        test = _proportion_z(
            arm(well_sampled[best_did]), arm(well_sampled[worst]), primary
        )
        if test is None or baseline - rates[worst] < 0.08 or test[0] < CONFIDENCE_Z:
            return None
        guardrail_test = _proportion_z(
            arm(well_sampled[best_did]), arm(well_sampled[worst]), guardrail
        )
        if guardrail_test is None:
            return None
        guardrail_z, guardrail_delta = guardrail_test
        max_drop = float(spec["objective"]["guardrail"].get(
            f"{guardrail}_max_drop", 0.02
        ))
        if guardrail_delta < -max_drop or (
            abs(guardrail_z) >= 1.96 and guardrail_delta < 0
        ):
            return None
        dist = cfg["allocations"]["default"].get("caller_id", {})
        if worst not in dist or dist[worst] < 0.05:
            return None
        revised = dict(cfg)
        revised["allocations"] = {k: dict(v) for k, v in cfg["allocations"].items()}
        revised["allocations"]["default"] = dict(cfg["allocations"]["default"])
        revised_dist = dict(dist)
        removed = revised_dist.pop(worst)
        remaining = list(revised_dist)
        if not remaining:
            return None
        add = removed / len(remaining)
        for did in remaining:
            revised_dist[did] += add
        revised["allocations"]["default"]["caller_id"] = revised_dist
        return revised, (
            f"Caller ID {worst} {primary} was {rates[worst]:.1%} vs "
            f"{baseline:.1%} best over a sufficient recent sample; "
            f"redistribute its allocation."
        )

    async def _experiment_change(
        self, wid: str, spec: dict[str, Any], cfg: dict[str, Any], version: int
    ) -> tuple[dict[str, Any], str] | None:
        primary = spec["objective"]["primary"]
        dim = (
            "caller_id" if primary == "pickup_rate"
            else await self._db(
                self.store.experiment_dimension, wid, "welcome_message"
            )
        )
        cohort = "default"
        options = spec["options"][dim]
        active = cfg["allocations"]["default"].get(dim, {})
        stats = await self._db(
            self.store.arm_stats, wid, cohort, version, dim
        )
        if not stats:
            return None
        positive = [key for key, value in stats.items() if value.dials > 0]
        if len(positive) < 2:
            tried = set(positive)
            candidates = [value for value in options if value not in tried]
            if not candidates:
                candidates = [value for value in options if value not in active]
            if not candidates:
                return None
            candidate = candidates[0]
            # Keep two active options and avoid a high-variance, many-arm launch.
            incumbent = max(active, key=active.get) if active else options[0]
            if candidate == incumbent:
                candidates = [value for value in options if value != incumbent]
                if not candidates:
                    return None
                candidate = candidates[0]
            revised = dict(cfg)
            revised["allocations"] = {k: dict(v) for k, v in cfg["allocations"].items()}
            revised["allocations"]["default"] = dict(cfg["allocations"]["default"])
            revised["allocations"]["default"][dim] = {incumbent: 0.5, candidate: 0.5}
            return revised, f"Start two-arm {dim} exploration ({incumbent}, {candidate}) for {primary}."
        return None

    async def _prompt_change(
        self, wid: str, spec: dict[str, Any], cfg: dict[str, Any], ts: str
    ) -> tuple[dict[str, Any], str] | None:
        sim_day = _sim_day(ts)
        if await self._db(self.store.prompt_checked, wid, sim_day):
            return None
        metrics = await self._db(self.store.prompt_stats, wid, sim_day - 2)
        all_metrics = await self._db(self.store.prompt_stats, wid, -100_000)
        current = metrics.get(cfg["prompt_version"], {"connected": 0, "flags": 0})
        total, flags = current["connected"], current["flags"]
        historical = {
            key: value for key, value in all_metrics.items() if key != cfg["prompt_version"]
        }
        baseline_n = sum(value["connected"] for value in historical.values())
        baseline_flags = sum(value["flags"] for value in historical.values())
        flag_test = (
            _two_proportion_z(flags, total, baseline_flags, baseline_n)
            if total >= MIN_INCIDENT_CALLS and baseline_n >= MIN_INCIDENT_CALLS
            else None
        )
        if (flag_test is None or flag_test[0] < CONFIDENCE_Z
                or flag_test[1] < 0.025):
            await self._db(self.store.mark_prompt_checked, wid, sim_day, "below_threshold")
            return None
        await self._db(self.store.mark_prompt_checked, wid, sim_day, "started")
        try:
            suggestions = await self.client.request(
                "POST", "/llm/suggest-fixes", safe_retry=False,
                json={"workflow_id": wid, "problem": f"Recent QA hallucination flags: {flags}/{total}"},
            )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            await self._db(
                self.store.audit, ts, wid, "prompt_suggestion_transport_error",
                {"error": str(exc)},
            )
            await self._db(self.store.mark_prompt_checked, wid, sim_day, "transport_error")
            return None
        except RemoteError as exc:
            if exc.status != 402 and exc.status not in RETRYABLE_STATUS:
                raise
            await self._db(
                self.store.audit, ts, wid, "prompt_suggestion_failed",
                {"status": exc.status, "code": exc.code, "detail": exc.detail},
            )
            await self._db(self.store.mark_prompt_checked, wid, sim_day, "credits_exhausted")
            return None
        for item in suggestions.get("suggestions", []):
            candidate = item["prompt_version"]
            try:
                validation = await self.client.request(
                    "POST", "/validation/runs", safe_retry=False,
                    json={"workflow_id": wid, "prompt_version": candidate, "n": 400},
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                await self._db(
                    self.store.audit, ts, wid, "prompt_validation_transport_error",
                    {"prompt_version": candidate, "error": str(exc)},
                )
                await self._db(
                    self.store.mark_prompt_checked, wid, sim_day, "transport_error"
                )
                return None
            except RemoteError as exc:
                if exc.status != 402 and exc.status not in RETRYABLE_STATUS:
                    raise
                await self._db(
                    self.store.audit, ts, wid, "prompt_validation_failed",
                    {"prompt_version": candidate, "status": exc.status,
                     "code": exc.code, "detail": exc.detail},
                )
                await self._db(
                    self.store.mark_prompt_checked, wid, sim_day, "credits_exhausted"
                )
                return None
            if validation["pass_rate"] < spec["governance"].get("min_validation_pass_rate", 0.97):
                continue
            revised = dict(cfg)
            revised["prompt_version"] = candidate
            revised["_evidence"] = {"validation_run_id": validation["validation_run_id"]}
            await self._db(self.store.mark_prompt_checked, wid, sim_day, "candidate_validated")
            return revised, (
                f"Hallucination flags {flags}/{total} exceed the 3% incident threshold; "
                f"{candidate} passed validation at {validation['pass_rate']:.1%}."
            )
        await self._db(
            self.store.audit, ts, wid, "prompt_remediation_no_safe_candidate",
            {"flags": flags, "connected": total, "suggestions": suggestions},
        )
        await self._db(self.store.mark_prompt_checked, wid, sim_day, "no_safe_candidate")
        return None

    async def _submit_change(
        self, wid: str, spec: dict[str, Any], current: dict[str, Any],
        revised: dict[str, Any], reason: str, ts: str, *,
        proposal_id_salt: str | None = None,
    ) -> None:
        revised = dict(revised)
        evidence = revised.pop("_evidence", None)
        version = int(current["version"])
        delta = _diff(current["config"], revised)
        if not delta:
            return
        proposal_id = _stable_id(
            "proposal", wid, version, revised,
            proposal_id_salt or ts[:10],
        )
        existing = await self._db(self.store.proposal, proposal_id)
        if existing:
            return
        payload: dict[str, Any] = {
            "proposal_id": proposal_id, "base_version": version, "proposed_config": revised,
            "rationale": reason,
        }
        if evidence is not None:
            payload["evidence"] = evidence
        changed = _changed_dims(current["config"], revised)
        policy = spec["governance"]["auto_apply"]
        if ("prompt_version" not in changed
                and policy.get("enabled")
                and all(d in policy.get("allowed_dims", []) for d in changed)
                and _allocation_shift(current["config"], revised) <= policy.get("max_shift", 0) + 1e-9):
            await self._db(self.store.save_proposal,
                proposal_id, wid, version, "auto", "submitted", payload
            )
            await self._reconcile_auto(
                {"proposal_id": proposal_id, "workflow_id": wid,
                 "base_version": version, "kind": "auto"}, payload, ts,
            )
            detail = {
                "proposal_id": proposal_id, "diff": delta, "rationale": reason,
            }
            if evidence is not None:
                detail["evidence"] = evidence
            await self._db(self.store.audit, ts, wid, "proposal_auto", detail)
            return
        stats = await self._db(self.store.arm_stats, wid, "default", version,
                               next(iter(changed), "voice"))
        sample = sum(item.dials for item in stats.values())
        per_lead = abs(max(0.0, min(1.0, _estimated_lift(reason))))
        impact = max(0.0, min(1.0, per_lead * (0.25 + 0.75 * min(sample / 500, 1))))
        body = {
            **payload,
            "workflow_id": wid,
            "change_summary": reason[:180],
            "diff": delta,
            "affected_leads": max(0, int(spec["daily_batch"]["size"])),
            "dry_run": {
                "cohort_id": "default",
                "historical_calls": sample,
                "estimated_delta": per_lead,
                "method": "two-proportion comparison or thresholded incident evidence",
            },
            "impact_score": round(impact, 4),
        }
        governance = spec["governance"]
        approval_hours = governance.get("approver_hours", [10, 19])
        ttl_hours = governance.get("proposal_ttl_hours")
        if ttl_hours is not None:
            body["expires_in_hours"] = ttl_hours
        if not _in_approver_hours(ts, approval_hours):
            eligible_at = _next_approver_time(ts, approval_hours)
            body["eligible_at"] = eligible_at
            await self._db(
                self.store.save_proposal,
                proposal_id, wid, version, "approval", "deferred", body,
                eligible_at=eligible_at,
            )
            detail = {
                "proposal_id": proposal_id, "eligible_at": eligible_at,
                "expires_in_hours": ttl_hours, "diff": delta, "rationale": reason,
            }
            if evidence is not None:
                detail["evidence"] = evidence
            await self._db(
                self.store.audit, ts, wid, "proposal_deferred_outside_approver_hours",
                detail,
            )
            return
        await self._db(self.store.save_proposal,
                       proposal_id, wid, version, "approval", "submitted", body)
        try:
            request_body = {
                key: value for key, value in body.items()
                if key not in ("eligible_at", "expires_at")
            }
            response = await self.client.request("POST", "/approvals", json=request_body)
        except RemoteError as exc:
            if exc.status == 409 and exc.code == "duplicate_proposal_id":
                await self._db(self.store.save_proposal,
                               proposal_id, wid, version, "approval", "pending", body)
                return
            if exc.status == 409 and exc.code == "stale_base_version":
                await self._db(self.store.save_proposal,
                               proposal_id, wid, version, "approval", "conflict", body)
                return
            raise
        await self._db(self.store.save_proposal,
            proposal_id, wid, version, "approval", response["status"], body,
            response.get("approval_id"),
            expires_at=response.get("expires_at"),
        )
        detail = {
            "proposal_id": proposal_id, "approval_id": response.get("approval_id"),
            "diff": delta, "affected_leads": body["affected_leads"],
            "impact_score": body["impact_score"], "rationale": reason,
        }
        if evidence is not None:
            detail["evidence"] = evidence
        await self._db(self.store.audit, ts, wid, "approval_requested", detail)


def _in_approver_hours(ts: str, hours: list[int]) -> bool:
    current = datetime.fromisoformat(ts)
    if current.tzinfo is None:
        raise ValueError("simulator timestamps must include a timezone")
    return int(hours[0]) <= current.hour < int(hours[1])


def _next_approver_time(ts: str, hours: list[int]) -> str:
    current = datetime.fromisoformat(ts)
    if current.tzinfo is None:
        raise ValueError("simulator timestamps must include a timezone")
    start, end = int(hours[0]), int(hours[1])
    if start <= current.hour < end:
        return current.isoformat()
    day = current.date()
    if current.hour >= end:
        day += timedelta(days=1)
    eligible = current.replace(
        year=day.year, month=day.month, day=day.day,
        hour=start, minute=0, second=0, microsecond=0,
    )
    return eligible.isoformat()


def _timestamp_is_due(ts: str, eligible_at: str) -> bool:
    current = datetime.fromisoformat(ts)
    eligible = datetime.fromisoformat(eligible_at)
    if current.tzinfo is None or eligible.tzinfo is None:
        raise ValueError("simulator timestamps must include a timezone")
    return current >= eligible


def _changed_dims(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    result: list[str] = []
    if old.get("cohorts", []) != new.get("cohorts", []):
        result.append("cohorts")
    for dim in ("welcome_message", "voice", "caller_id"):
        if (old.get("allocations", {}).get("default", {}).get(dim)
                != new.get("allocations", {}).get("default", {}).get(dim)):
            result.append(dim)
    if old.get("prompt_version") != new.get("prompt_version"):
        result.append("prompt_version")
    if old.get("schedule", {}) != new.get("schedule", {}):
        result.append("schedule")
    return result


def _allocation_shift(old: dict[str, Any], new: dict[str, Any]) -> float:
    worst = 0.0
    old_alloc = old.get("allocations", {})
    new_alloc = new.get("allocations", {})
    keys = set(old_alloc) | set(new_alloc)
    for key in keys:
        old_dims = old_alloc.get(key, {})
        new_dims = new_alloc.get(key, {})
        for dim in ("welcome_message", "voice", "caller_id"):
            before, after = old_dims.get(dim), new_dims.get(dim)
            if before is None:
                before = old_alloc.get("default", {}).get(dim, {})
            if after is None:
                after = new_alloc.get("default", {}).get(dim, {})
            tv = 0.5 * sum(
                abs(before.get(option, 0) - after.get(option, 0))
                for option in set(before) | set(after)
            )
            worst = max(worst, tv)
    return worst


def _diff(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    return {
        key: {"from": old.get(key), "to": new.get(key)}
        for key in ("cohorts", "allocations", "prompt_version", "schedule")
        if old.get(key) != new.get(key)
    }


def _estimated_lift(reason: str) -> float:
    # Conservative default for impact ranking when the evidence is an incident threshold.
    for token in reason.replace(",", " ").split():
        try:
            value = float(token)
            if 0 < abs(value) < 1:
                return abs(value)
        except ValueError:
            continue
    return 0.02


async def _async_main(args: argparse.Namespace) -> None:
    store = await asyncio.to_thread(Store, args.db)
    controller = Controller(args.url, args.db, store)
    await controller.run()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8700")
    parser.add_argument(
        "--db", default=os.environ.get("CONTROLLER_DB", ".controller/controller.sqlite3")
    )
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    parser.add_argument("--audit", action="store_true", help="print the durable decision audit")
    parser.add_argument("--workflow", help="restrict --audit output to one workflow")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        if args.audit:
            print(json.dumps(Store(args.db).audit_rows(args.workflow), indent=2))
            return 0
        asyncio.run(_async_main(args))
    except KeyboardInterrupt:
        LOG.info("shutdown requested; durable state is available for restart")
    except Exception:
        LOG.exception("controller stopped with an unrecoverable error")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
