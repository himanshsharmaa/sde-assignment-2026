import asyncio
import copy
from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3
import statistics
import threading
import time

import httpx
import pytest

from agentic_controller import (
    ArmStats,
    Controller,
    RemoteError,
    SimClient,
    Store,
    _allocation_shift,
    _changed_dims,
    _evaluate_experiment_look,
    _planned_look_cutoff,
    _proportion_z,
    _sim_day,
)


class _ApprovalApi:
    def __init__(self, *, config_version=7, approvals=None):
        self.config_version = config_version
        self.approvals = list(approvals or [])
        self.submissions = []
        self.applies = []

    async def request(self, method, path, **kwargs):
        if method == "GET" and path == "/approvals":
            return copy.deepcopy(self.approvals)
        if method == "POST" and path == "/approvals":
            payload = copy.deepcopy(kwargs["json"])
            self.submissions.append(payload)
            approval = {
                "approval_id": f"approval-{len(self.submissions)}",
                "proposal_id": payload["proposal_id"],
                "status": "pending",
            }
            self.approvals.append(approval)
            return copy.deepcopy(approval)
        if method == "GET" and path == "/workflows/wf/config":
            return {"version": self.config_version, "config": {}}
        if method == "POST" and path == "/workflows/wf/apply":
            self.applies.append(copy.deepcopy(kwargs["json"]))
            self.config_version += 1
            return {"version": self.config_version}
        raise AssertionError(f"Unexpected simulator request: {method} {path}")


class _ApprovalRecoveryApi:
    def __init__(
        self, approvals, *, config_version=7, approvals_error_once=None,
        history_error=None, history_entries=None,
    ):
        self.approvals = copy.deepcopy(approvals)
        self.config_version = config_version
        self.approvals_error_once = approvals_error_once
        self.history_error = history_error
        self.history_entries = copy.deepcopy(history_entries or [])
        self.approval_gets = 0
        self.history_gets = 0
        self.application_posts = []

    async def request(self, method, path, **kwargs):
        if method == "GET" and path == "/approvals":
            self.approval_gets += 1
            if self.approvals_error_once is not None:
                error = self.approvals_error_once
                self.approvals_error_once = None
                raise error
            return copy.deepcopy(self.approvals)
        if method == "GET" and path == "/workflows/wf/config":
            return {"version": self.config_version, "config": {}}
        if method == "GET" and path == "/workflows/wf/config/history":
            self.history_gets += 1
            if self.history_error is not None:
                raise self.history_error
            return copy.deepcopy(self.history_entries)
        if method == "POST" and path == "/workflows/wf/apply":
            self.application_posts.append(copy.deepcopy(kwargs["json"]))
            self.config_version += 1
            return {"workflow_id": "wf", "version": self.config_version,
                    "changed_dims": ["voice"]}
        raise AssertionError(f"Unexpected simulator request: {method} {path}")


class _ConcurrentApprovalApi:
    def __init__(self, store_path, proposal_id):
        self.store_path = store_path
        self.proposal_id = proposal_id
        self.config_version = 7
        self.approvals_barrier = threading.Barrier(2)
        self.config_barrier = threading.Barrier(2)
        self.lock = threading.Lock()
        self.application_posts = []

    async def request(self, method, path, **kwargs):
        if method == "GET" and path == "/approvals":
            self.approvals_barrier.wait(timeout=5)
            return [{
                "approval_id": "approval-original",
                "proposal_id": self.proposal_id,
                "workflow_id": "wf",
                "base_version": 7,
                "status": "approved",
            }]
        if method == "GET" and path == "/workflows/wf/config":
            self.config_barrier.wait(timeout=5)
            return {"version": 7, "config": {}}
        if method == "POST" and path == "/workflows/wf/apply":
            with self.lock:
                self.application_posts.append(copy.deepcopy(kwargs["json"]))
                if self.config_version == 7:
                    self.config_version = 8
                    return {"workflow_id": "wf", "version": 8}

            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                row = Store(self.store_path).proposal(self.proposal_id)
                if row["status"] == "applied":
                    break
                time.sleep(0.01)
            else:
                raise AssertionError("winning application was not persisted")
            raise RemoteError(
                409, "stale_base_version", "configuration was already applied"
            )
        raise AssertionError(f"Unexpected simulator request: {method} {path}")


class _AutoApplyApi:
    def __init__(self, *, config_version=7, put_error=None):
        self.config_version = config_version
        self.put_error = put_error

    async def request(self, method, path, **kwargs):
        if method == "GET" and path == "/workflows/wf/config/history":
            return []
        if method == "GET" and path == "/workflows/wf/config":
            return {"version": self.config_version, "config": {}}
        if method == "PUT" and path == "/workflows/wf/config":
            if self.put_error is not None:
                raise self.put_error
            raise AssertionError("This test expects the PUT to fail")
        raise AssertionError(f"Unexpected simulator request: {method} {path}")


def _proposal_payload(proposal_id="proposal-original", base_version=7, eligible_at=None):
    payload = {
        "proposal_id": proposal_id,
        "workflow_id": "wf",
        "base_version": base_version,
        "proposed_config": {
            "allocations": {"default": {"voice": {"V1": 0.5, "V2": 0.5}}},
            "prompt_version": "P1",
        },
        "rationale": "Promote V2 based on the measured experiment.",
        "change_summary": "Increase the share of voice V2.",
        "diff": {"voice": {"V1": 0.5, "V2": 0.5}},
        "affected_leads": 300,
        "dry_run": {"historical_calls": 600, "estimated_delta": 0.04},
        "impact_score": 0.42,
        "evidence": {"experiment": {"z": 2.8, "delta": 0.04}},
    }
    if eligible_at is not None:
        payload["eligible_at"] = eligible_at
    return payload


def _save_deferred_proposal(store, payload):
    store.save_proposal(
        payload["proposal_id"],
        payload["workflow_id"],
        payload["base_version"],
        "approval",
        "deferred",
        payload,
    )


def _save_approved_proposal(store, payload, approval_id="approval-original"):
    store.save_proposal(
        payload["proposal_id"],
        payload["workflow_id"],
        payload["base_version"],
        "approval",
        "approved",
        payload,
        approval_id,
    )


def _save_applied_exploration(store, proposal_id="proposal-exploration"):
    payload = _proposal_payload(proposal_id)
    payload["rationale"] = (
        "Start two-arm voice exploration (V1, V2) for engagement_rate."
    )
    _save_approved_proposal(store, payload)
    store.finalize_applied_proposal(
        proposal_id,
        "approval-original",
        "2027-01-04T10:00:00+05:30",
        "approval_applied",
        {"proposal_id": proposal_id, "workflow_id": payload["workflow_id"]},
    )
    return payload


def _call_event(event_id=1):
    return {
        "type": "call",
        "event_id": event_id,
        "ts": "2027-01-04T10:00:00+05:30",
        "workflow_id": "wf",
        "cohort_id": "default",
        "config_version": 2,
        "connected": True,
        "engaged": False,
        "qa": {"hallucination_flag": True},
        "variant": {
            "welcome_message": "W1",
            "voice": "V1",
            "caller_id": "D1",
            "prompt_version": "P1",
        },
    }


def test_chunk_ingestion_is_atomic_and_idempotent(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    chunk = {"chunk": 3, "kind": "events", "last_event_id": 1}
    encoded = (json.dumps(_call_event()) + "\n").encode()

    store.process_chunk(chunk, encoded)
    store.process_chunk(chunk, encoded)

    result = store.arm_stats("wf", "default", 2, "welcome_message")
    assert result["W1"] == ArmStats(dials=1, connected=1, engaged=0, hallucination_flags=1)
    assert store.prompt_stats("wf", 0)["P1"] == {"dials": 1, "connected": 1, "flags": 1}
    assert store.last_chunk() == 3


def test_concurrent_replay_does_not_double_count(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    chunk = {"chunk": 3, "kind": "events", "last_event_id": 1}
    encoded = (json.dumps(_call_event()) + "\n").encode()

    async def ingest():
        await asyncio.gather(*[
            asyncio.to_thread(store.process_chunk, chunk, encoded) for _ in range(8)
        ])

    asyncio.run(ingest())
    assert store.arm_stats("wf", "default", 2, "voice")["V1"].dials == 1


def test_minimum_sample_and_effect_direction():
    assert _proportion_z(
        ArmStats(20, 10, 5, 0), ArmStats(20, 10, 6, 0), "engagement_rate"
    ) is None
    result = _proportion_z(
        ArmStats(300, 240, 180, 0), ArmStats(300, 240, 120, 0), "engagement_rate"
    )
    assert result is not None
    z, difference = result
    assert z > 0
    assert difference == 0.25


def test_config_diffs_and_total_variation_shift():
    old = {
        "cohorts": [],
        "allocations": {"default": {"voice": {"V1": 1.0}}},
        "prompt_version": "P1",
        "schedule": {},
    }
    new = {
        "cohorts": [],
        "allocations": {"default": {"voice": {"V1": 0.5, "V2": 0.5}}},
        "prompt_version": "P1",
        "schedule": {},
    }
    assert _changed_dims(old, new) == ["voice"]
    assert _allocation_shift(old, new) == 0.5


def test_time_conversion_includes_history_days():
    assert _sim_day("2027-01-04T00:00:00+05:30") == 0
    assert _sim_day("2027-01-03T23:45:00+05:30") == -1


@pytest.mark.parametrize(
    ("advance_mode", "expected_advances"),
    [("progress", 2), ("finish_without_clock_change", 1)],
)
def test_controller_run_exits_on_progress_or_simulator_completion(
    tmp_path, advance_mode, expected_advances
):
    controller = Controller(
        "http://simulator.test", str(tmp_path / "state.sqlite3")
    )
    api = _SimulatorLoopApi(advance_mode=advance_mode)
    controller.client = api

    asyncio.run(controller.run())

    assert api.advance_calls == expected_advances
    assert api.close_calls == 1


def test_controller_run_fails_after_bounded_no_progress_advances(tmp_path):
    controller = Controller(
        "http://simulator.test", str(tmp_path / "state.sqlite3")
    )
    api = _SimulatorLoopApi(advance_mode="no_progress")
    controller.client = api

    with pytest.raises(RuntimeError, match="simulator made no progress"):
        asyncio.run(controller.run())

    assert api.advance_calls == 3
    assert api.close_calls == 1


def test_controller_run_rejects_backward_simulator_clock(tmp_path):
    controller = Controller(
        "http://simulator.test", str(tmp_path / "state.sqlite3")
    )
    api = _SimulatorLoopApi(advance_mode="backwards")
    controller.client = api

    with pytest.raises(RuntimeError, match="simulator clock moved backwards"):
        asyncio.run(controller.run())

    assert api.advance_calls == 1
    assert api.close_calls == 1


def test_controller_run_attempts_registration_before_evaluation_and_skips_safely(
    tmp_path, caplog
):
    store = Store(str(tmp_path / "state.sqlite3"))
    _save_applied_exploration(store)
    api = _SimulatorLoopApi(workflows=[{
        "workflow_id": "wf",
        "objective": {
            "primary": "engagement_rate",
            "guardrail": {"kpi": "pickup_rate"},
        },
    }])
    api.state["finished"] = True
    controller = Controller(
        "http://simulator.test", str(store.path), store=store
    )
    controller.client = api
    order = []
    register = controller._register_eligible_experiment_plans
    evaluate = controller._evaluate_registered_experiments

    async def record_registration():
        order.append("register")
        await register()

    async def record_evaluation(state):
        order.append("evaluate")
        await evaluate(state)

    controller._register_eligible_experiment_plans = record_registration
    controller._evaluate_registered_experiments = record_evaluation

    asyncio.run(controller.run())

    assert order == ["register", "evaluate", "register", "evaluate"]
    assert store.experiments() == []
    assert "Skipping fixed-horizon experiment registration" in caplog.text
    assert "person-level baseline" in caplog.text
    assert "sample targets" in caplog.text
    assert "guardrail deterioration margin" in caplog.text
    assert {path for method, path, _ in api.requests} <= {
        "/sim/state",
        "/workflows",
        "/events/chunks",
        "/approvals",
    }
    assert api.close_calls == 1


def test_unapproved_or_ineligible_proposals_cannot_register_experiments(tmp_path, caplog):
    store = Store(str(tmp_path / "state.sqlite3"))
    statuses = ("pending", "submitted", "approved", "deferred", "rejected")
    for status in statuses:
        payload = _proposal_payload(f"proposal-{status}")
        payload["rationale"] = (
            "Start two-arm voice exploration (V1, V2) for engagement_rate."
        )
        store.save_proposal(
            payload["proposal_id"],
            payload["workflow_id"],
            payload["base_version"],
            "approval",
            status,
            payload,
        )
    non_experiment = _proposal_payload("proposal-applied-remediation")
    non_experiment["rationale"] = "Fix an observed incident."
    store.save_proposal(
        non_experiment["proposal_id"],
        non_experiment["workflow_id"],
        non_experiment["base_version"],
        "approval",
        "applied",
        non_experiment,
    )
    controller = Controller(
        "http://simulator.test", str(store.path), store=store
    )

    asyncio.run(controller._register_eligible_experiment_plans())

    assert store.experiments() == []
    assert "Skipping fixed-horizon experiment registration" not in caplog.text


def test_registration_skip_does_not_change_existing_plan_or_look_progress(tmp_path):
    db_path = str(tmp_path / "state.sqlite3")
    store = Store(db_path)
    payload = _save_applied_exploration(store)
    _register_experiment(store, proposal_id=payload["proposal_id"])
    assert store.set_experiment_look_cutoff("exp-1", 1, 120)
    assert store.finalize_experiment_look(
        "exp-1", 1, "continue", {"outcome": "continue"},
        "2027-01-04T10:15:00+05:30",
    )
    before_experiments = store.experiments()
    before_looks = store.experiment_looks("exp-1")

    first_controller = Controller(
        "http://simulator.test", db_path, store=store
    )
    asyncio.run(first_controller._register_eligible_experiment_plans())
    asyncio.run(first_controller._register_eligible_experiment_plans())

    restarted_store = Store(db_path)
    restarted_controller = Controller(
        "http://simulator.test", db_path, store=restarted_store
    )
    asyncio.run(restarted_controller._register_eligible_experiment_plans())

    assert restarted_store.experiments() == before_experiments
    assert restarted_store.experiment_looks("exp-1") == before_looks
    assert restarted_store.experiments()[0]["status"] == "running"
    assert restarted_store.experiment_looks("exp-1")[0]["cutoff_event_id"] == 120
    assert len(restarted_store.experiments()) == 1


def test_http_retry_is_bounded_and_post_can_disable_retry():
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        if calls in (1, 3):
            return httpx.Response(503, json={"error": "busy"}, request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    async def exercise():
        client = SimClient("http://simulator.test", transport=httpx.MockTransport(handler))
        try:
            assert await client.request("GET", "/health") == {"ok": True}
            assert calls == 2
            with pytest.raises(RemoteError):
                await client.request("POST", "/advance", safe_retry=False)
            assert calls == 3
        finally:
            await client.close()

    asyncio.run(exercise())


def test_http_connection_cap_is_enforced():
    active = 0
    peak = 0

    async def handler(request):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return httpx.Response(200, json={"ok": True}, request=request)

    async def exercise():
        client = SimClient("http://simulator.test", transport=httpx.MockTransport(handler))
        try:
            await asyncio.gather(*[
                client.request("GET", f"/health/{index}") for index in range(24)
            ])
        finally:
            await client.close()

    asyncio.run(exercise())
    assert 1 < peak <= 8


def test_out_of_hours_change_is_persisted_with_original_proposal_data(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    controller = Controller("http://simulator.test", str(store.path), store=store)
    api = _ApprovalApi()
    controller.client = api
    current = {
        "version": 7,
        "config": {
            "allocations": {"default": {"voice": {"V1": 1.0}}},
            "prompt_version": "P1",
        },
    }
    revised = {
        "allocations": {"default": {"voice": {"V1": 0.5, "V2": 0.5}}},
        "prompt_version": "P1",
    }
    test_evidence = {
        "source": "test_fixture",
        "primary_kpi": "engagement_rate",
        "statistical_test": {"name": "two_proportion_z", "z_score": 2.8},
        "absolute_lift": 0.04,
    }
    proposed_change = {**revised, "_evidence": test_evidence}
    spec = {
        "governance": {
            "auto_apply": {"enabled": False},
            "approver_hours": [10, 19],
        },
        "daily_batch": {"size": 300},
    }

    asyncio.run(controller._submit_change(
        "wf", spec, current, proposed_change,
        "Measured winner with statistical evidence.", "2027-01-04T09:30:00+05:30",
    ))

    proposals = store.proposals("wf")
    assert len(proposals) == 1
    row = proposals[0]
    payload = json.loads(row["payload"])
    assert row["status"] == "deferred"
    assert payload["proposal_id"] == row["proposal_id"]
    assert row["base_version"] == 7
    assert payload["base_version"] == 7
    assert payload["proposed_config"] == revised
    assert payload["rationale"] == "Measured winner with statistical evidence."
    assert payload["workflow_id"] == "wf"
    assert payload["diff"]
    assert payload["eligible_at"] == "2027-01-04T10:00:00+05:30"
    assert payload["evidence"] == test_evidence
    deferred_audit = store.audit_rows("wf")[0]
    assert deferred_audit["detail"]["evidence"] == test_evidence
    assert api.submissions == []


@pytest.mark.parametrize(
    ("include_evidence", "evidence"),
    [(False, None), (True, {})],
)
def test_submit_change_preserves_empty_and_missing_evidence(
    tmp_path, include_evidence, evidence
):
    store = Store(str(tmp_path / "state.sqlite3"))
    controller = Controller("http://simulator.test", str(store.path), store=store)
    revised = {
        "allocations": {"default": {"voice": {"V1": 0.5, "V2": 0.5}}},
        "prompt_version": "P1",
    }
    proposed_change = dict(revised)
    if include_evidence:
        proposed_change["_evidence"] = evidence
    spec = {
        "governance": {
            "auto_apply": {"enabled": False},
            "approver_hours": [10, 19],
        },
        "daily_batch": {"size": 300},
    }

    asyncio.run(controller._submit_change(
        "wf",
        spec,
        {
            "version": 7,
            "config": {
                "allocations": {"default": {"voice": {"V1": 1.0}}},
                "prompt_version": "P1",
            },
        },
        proposed_change,
        "Test-only evidence handling.",
        "2027-01-04T09:30:00+05:30",
    ))

    row = store.proposals("wf")[0]
    payload = json.loads(row["payload"])
    assert payload["proposed_config"] == revised
    if include_evidence:
        assert payload["evidence"] == evidence
    else:
        assert "evidence" not in payload


def test_unregistered_aggregate_comparison_cannot_promote(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    controller = Controller("http://simulator.test", str(store.path), store=store)
    store.set_experiment_dimension("wf", "voice")
    events = []
    for variant, engaged_count in (("V1", 160), ("V2", 120)):
        for index in range(200):
            event = _call_event(event_id=len(events) + 1)
            event["config_version"] = 7
            event["variant"]["voice"] = variant
            event["connected"] = index < 160
            event["engaged"] = index < engaged_count
            events.append(json.dumps(event))
    store.process_chunk(
        {"chunk": 1, "kind": "events", "last_event_id": len(events)},
        ("\n".join(events) + "\n").encode(),
    )
    config = {
        "allocations": {"default": {"voice": {"V1": 0.5, "V2": 0.5}}},
        "prompt_version": "P1",
    }
    experiment_spec = {
        "objective": {"primary": "engagement_rate", "guardrail": {}},
        "options": {"voice": ["V1", "V2"]},
    }

    result = asyncio.run(
        controller._experiment_change("wf", experiment_spec, config, 7)
    )

    assert result is None
    assert store.proposals("wf") == []
    assert store.audit_rows("wf") == []


@pytest.mark.parametrize(
    ("config_version", "put_error", "expected_status", "expected_action"),
    [
        (8, None, "conflict", "auto_apply_conflict"),
        (
            7,
            RemoteError(409, "stale_base_version", "version changed"),
            "conflict",
            "auto_apply_conflict",
        ),
        (
            7,
            RemoteError(403, "auto_apply_not_permitted", "not permitted"),
            "not_permitted",
            "auto_apply_denied",
        ),
    ],
    ids=["pre_put_version_conflict", "stale_version_response", "http_403_denial"],
)
def test_auto_apply_failure_audit_uses_simulator_timestamp(
    tmp_path, config_version, put_error, expected_status, expected_action
):
    store = Store(str(tmp_path / "state.sqlite3"))
    controller = Controller("http://simulator.test", str(store.path), store=store)
    controller.client = _AutoApplyApi(
        config_version=config_version, put_error=put_error
    )
    proposal = {
        "proposal_id": "auto-proposal",
        "workflow_id": "wf",
        "base_version": 7,
        "kind": "auto",
    }
    payload = {
        "proposal_id": "auto-proposal",
        "workflow_id": "wf",
        "base_version": 7,
        "proposed_config": {"allocations": {"default": {"voice": {"V1": 1.0}}}},
        "rationale": "Test-only auto-apply failure.",
    }
    simulator_ts = "2027-01-04T16:30:00+05:30"

    asyncio.run(controller._reconcile_auto(proposal, payload, simulator_ts))

    assert store.proposal("auto-proposal")["status"] == expected_status
    audit = store.audit_rows("wf")[-1]
    assert audit["action"] == expected_action
    assert audit["ts"] == simulator_ts


def test_deferred_proposal_waits_for_window_and_resumes_once_after_restart(tmp_path):
    db_path = str(tmp_path / "state.sqlite3")
    store = Store(db_path)
    payload = _proposal_payload(eligible_at="2027-01-04T10:00:00+05:30")
    _save_deferred_proposal(store, payload)
    api = _ApprovalApi()

    first_controller = Controller("http://simulator.test", db_path, store=store)
    first_controller.client = api
    asyncio.run(first_controller._reconcile("2027-01-04T09:45:00+05:30", plan=False))
    assert api.submissions == []
    deferred = store.proposal("proposal-original")
    assert deferred["status"] == "deferred"
    assert deferred["eligible_at"] == "2027-01-04T10:00:00+05:30"

    restarted_store = Store(db_path)
    restarted_controller = Controller("http://simulator.test", db_path, store=restarted_store)
    restarted_controller.client = api
    assert restarted_store.proposal("proposal-original")["status"] == "deferred"
    asyncio.run(restarted_controller._reconcile("2027-01-04T10:00:00+05:30", plan=False))
    assert len(api.submissions) == 1
    asyncio.run(restarted_controller._reconcile("2027-01-04T10:15:00+05:30", plan=False))

    assert len(api.submissions) == 1
    submitted = api.submissions[0]
    assert submitted == {
        key: value for key, value in payload.items() if key != "eligible_at"
    }
    assert submitted["proposal_id"] == "proposal-original"
    assert "eligible_at" not in submitted
    assert restarted_store.proposal("proposal-original")["status"] == "pending"


def test_expired_remote_proposal_stays_terminal_and_is_not_resubmitted(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    store.save_proposal(
        payload["proposal_id"], "wf", 7, "approval", "pending", payload, "approval-1"
    )
    api = _ApprovalApi(approvals=[{
        "approval_id": "approval-1",
        "proposal_id": payload["proposal_id"],
        "status": "expired",
    }])
    controller = Controller("http://simulator.test", str(store.path), store=store)
    controller.client = api

    asyncio.run(controller._reconcile("2027-01-04T10:00:00+05:30", plan=False))
    asyncio.run(controller._reconcile("2027-01-04T10:15:00+05:30", plan=False))

    assert store.proposal(payload["proposal_id"])["status"] == "expired"
    assert api.submissions == []
    assert api.applies == []


def test_deferred_proposal_does_not_overwrite_supervisor_config(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload(eligible_at="2027-01-04T10:00:00+05:30")
    _save_deferred_proposal(store, payload)
    api = _ApprovalApi(config_version=8)
    controller = Controller("http://simulator.test", str(store.path), store=store)
    controller.client = api

    asyncio.run(controller._reconcile("2027-01-04T10:00:00+05:30", plan=False))

    assert store.proposal(payload["proposal_id"])["status"] == "conflict"
    assert api.submissions == []
    assert api.applies == []
    assert api.config_version == 8


def test_restart_reconciles_remote_applied_approval_without_reapplying(tmp_path):
    db_path = str(tmp_path / "state.sqlite3")
    store = Store(db_path)
    payload = _proposal_payload()
    approval_id = "approval-original"
    _save_approved_proposal(store, payload, approval_id)
    api = _ApprovalRecoveryApi([{
        "approval_id": approval_id,
        "proposal_id": payload["proposal_id"],
        "workflow_id": payload["workflow_id"],
        "base_version": payload["base_version"],
        "status": "applied",
    }])

    restarted_store = Store(db_path)
    controller = Controller("http://simulator.test", db_path, store=restarted_store)
    controller.client = api
    controller._workflow_specs["wf"] = {"objective": {"primary": "pickup_rate"}}
    asyncio.run(controller._reconcile("2027-01-04T10:30:00+05:30", plan=False))

    row = restarted_store.proposal(payload["proposal_id"])
    assert row["status"] == "applied"
    assert row["proposal_id"] == payload["proposal_id"]
    assert row["approval_id"] == approval_id
    assert api.application_posts == []
    assert api.history_gets == 0
    audits = restarted_store.audit_rows("wf")
    assert len(audits) == 1
    assert audits[0]["action"] == "approval_applied_reconciled"
    assert audits[0]["ts"] == "2027-01-04T10:30:00+05:30"
    assert audits[0]["detail"]["proposal_id"] == payload["proposal_id"]
    assert audits[0]["detail"]["approval_id"] == approval_id


def test_repeated_reconcile_of_remote_applied_approval_does_not_duplicate_work(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    approval_id = "approval-original"
    _save_approved_proposal(store, payload, approval_id)
    api = _ApprovalRecoveryApi([{
        "approval_id": approval_id,
        "proposal_id": payload["proposal_id"],
        "workflow_id": payload["workflow_id"],
        "base_version": payload["base_version"],
        "status": "applied",
    }])
    controller = Controller("http://simulator.test", str(store.path), store=store)
    controller.client = api
    controller._workflow_specs["wf"] = {"objective": {"primary": "pickup_rate"}}

    asyncio.run(controller._reconcile("2027-01-04T10:30:00+05:30", plan=False))
    asyncio.run(controller._reconcile("2027-01-04T10:45:00+05:30", plan=False))

    assert store.proposal(payload["proposal_id"])["status"] == "applied"
    assert store.proposal(payload["proposal_id"])["approval_id"] == approval_id
    assert api.application_posts == []
    audits = store.audit_rows("wf")
    assert [row["action"] for row in audits] == ["approval_applied_reconciled"]


def test_approval_list_failure_preserves_proposal_and_allows_later_reconcile(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    approval_id = "approval-original"
    _save_approved_proposal(store, payload, approval_id)
    api = _ApprovalRecoveryApi(
        [{
            "approval_id": approval_id,
            "proposal_id": payload["proposal_id"],
            "workflow_id": payload["workflow_id"],
            "base_version": payload["base_version"],
            "status": "applied",
        }],
        approvals_error_once=RemoteError(503, "temporarily_unavailable", "retry later"),
    )
    controller = Controller("http://simulator.test", str(store.path), store=store)
    controller.client = api
    controller._workflow_specs["wf"] = {"objective": {"primary": "pickup_rate"}}
    original_row = store.proposal(payload["proposal_id"])

    with pytest.raises(RemoteError):
        asyncio.run(controller._reconcile("2027-01-04T10:30:00+05:30", plan=False))

    after_failure = store.proposal(payload["proposal_id"])
    assert after_failure["status"] == "approved"
    assert after_failure["proposal_id"] == original_row["proposal_id"]
    assert after_failure["approval_id"] == original_row["approval_id"]
    assert after_failure["payload"] == original_row["payload"]
    assert api.application_posts == []
    assert store.audit_rows("wf") == []

    asyncio.run(controller._reconcile("2027-01-04T10:45:00+05:30", plan=False))

    assert store.proposal(payload["proposal_id"])["status"] == "applied"
    assert store.proposal(payload["proposal_id"])["approval_id"] == approval_id
    assert api.application_posts == []
    assert [row["action"] for row in store.audit_rows("wf")] == [
        "approval_applied_reconciled"
    ]


def test_ambiguous_approved_status_fails_closed_when_history_unavailable(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    approval_id = "approval-original"
    _save_approved_proposal(store, payload, approval_id)
    assert store.mark_application_attempted(payload["proposal_id"], approval_id)
    api = _ApprovalRecoveryApi(
        [{
            "approval_id": approval_id,
            "proposal_id": payload["proposal_id"],
            "workflow_id": payload["workflow_id"],
            "base_version": payload["base_version"],
            "status": "approved",
        }],
        history_error=RemoteError(503, "temporarily_unavailable", "retry later"),
    )
    controller = Controller("http://simulator.test", str(store.path), store=store)
    controller.client = api
    controller._workflow_specs["wf"] = {"objective": {"primary": "pickup_rate"}}
    original_row = store.proposal(payload["proposal_id"])

    with pytest.raises(RemoteError):
        asyncio.run(controller._reconcile("2027-01-04T10:30:00+05:30", plan=False))

    current_row = store.proposal(payload["proposal_id"])
    assert api.history_gets == 1
    assert api.application_posts == []
    assert current_row["status"] == "approved"
    assert current_row["proposal_id"] == original_row["proposal_id"]
    assert current_row["approval_id"] == approval_id
    assert current_row["payload"] == original_row["payload"]
    assert current_row["application_attempted"] == 1
    audits = store.audit_rows("wf")
    assert [row["action"] for row in audits] == [
        "approval_application_recovery_history_failed"
    ]


def test_ambiguous_approved_status_with_matching_history_is_reconciled(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    approval_id = "approval-original"
    _save_approved_proposal(store, payload, approval_id)
    assert store.mark_application_attempted(payload["proposal_id"], approval_id)
    api = _ApprovalRecoveryApi(
        [{
            "approval_id": approval_id,
            "proposal_id": payload["proposal_id"],
            "workflow_id": payload["workflow_id"],
            "base_version": payload["base_version"],
            "status": "approved",
        }],
        config_version=8,
        history_entries=[{
            "version": 8,
            "actor": "controller_approved",
            "changed_dims": ["voice"],
            "applied_at": "2027-01-04T10:20:00+05:30",
            "proposal_id": payload["proposal_id"],
            "approval_id": approval_id,
        }],
    )
    controller = Controller("http://simulator.test", str(store.path), store=store)
    controller.client = api
    controller._workflow_specs["wf"] = {"objective": {"primary": "pickup_rate"}}

    asyncio.run(controller._reconcile("2027-01-04T10:30:00+05:30", plan=False))
    asyncio.run(controller._reconcile("2027-01-04T10:45:00+05:30", plan=False))

    row = store.proposal(payload["proposal_id"])
    assert row["status"] == "applied"
    assert row["application_attempted"] == 1
    assert row["proposal_id"] == payload["proposal_id"]
    assert row["approval_id"] == approval_id
    assert api.history_gets == 1
    assert api.application_posts == []
    audits = store.audit_rows("wf")
    assert [item["action"] for item in audits] == [
        "approval_applied_reconciled"
    ]
    assert audits[0]["detail"]["version"] == 8
    assert audits[0]["detail"]["actor"] == "controller_approved"


def test_ambiguous_approved_status_without_history_match_is_not_retried(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    store = Store(str(db_path))
    payload = _proposal_payload()
    approval_id = "approval-original"
    _save_approved_proposal(store, payload, approval_id)
    assert store.mark_application_attempted(payload["proposal_id"], approval_id)
    api = _ApprovalRecoveryApi(
        [{
            "approval_id": approval_id,
            "proposal_id": payload["proposal_id"],
            "workflow_id": payload["workflow_id"],
            "base_version": payload["base_version"],
            "status": "approved",
        }],
        history_entries=[{
            "version": 8,
            "actor": "supervisor",
            "changed_dims": ["voice"],
            "applied_at": "2027-01-04T10:20:00+05:30",
            "proposal_id": payload["proposal_id"],
            "approval_id": approval_id,
        }],
    )
    controller = Controller("http://simulator.test", str(store.path), store=store)
    controller.client = api
    controller._workflow_specs["wf"] = {"objective": {"primary": "pickup_rate"}}

    asyncio.run(controller._reconcile("2027-01-04T10:30:00+05:30", plan=False))
    store = Store(str(db_path))
    controller = Controller("http://simulator.test", str(db_path), store=store)
    controller.client = api
    controller._workflow_specs["wf"] = {"objective": {"primary": "pickup_rate"}}
    asyncio.run(controller._reconcile("2027-01-04T10:45:00+05:30", plan=False))

    row = store.proposal(payload["proposal_id"])
    assert row["status"] == "approved"
    assert row["application_attempted"] == 1
    assert row["approval_id"] == approval_id
    assert api.application_posts == []
    assert api.history_gets == 2
    assert [item["action"] for item in store.audit_rows("wf")] == [
        "approval_application_recovery_unconfirmed",
        "approval_application_recovery_unconfirmed",
    ]


def test_application_attempt_migration_preserves_existing_proposal(tmp_path):
    db_path = tmp_path / "legacy.sqlite3"
    payload = _proposal_payload()
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """CREATE TABLE proposals (
                   proposal_id TEXT PRIMARY KEY,
                   workflow_id TEXT NOT NULL,
                   base_version INTEGER NOT NULL,
                   kind TEXT NOT NULL,
                   status TEXT NOT NULL,
                   approval_id TEXT,
                   payload TEXT NOT NULL,
                   created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
               )"""
        )
        connection.execute(
            """INSERT INTO proposals
               (proposal_id, workflow_id, base_version, kind, status, approval_id, payload)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                payload["proposal_id"], payload["workflow_id"],
                payload["base_version"], "approval", "approved",
                "approval-original", json.dumps(payload, sort_keys=True),
            ),
        )

    store = Store(str(db_path))

    migrated = store.proposal(payload["proposal_id"])
    assert migrated["status"] == "approved"
    assert migrated["approval_id"] == "approval-original"
    assert json.loads(migrated["payload"]) == payload
    assert migrated["application_attempted"] == 0
    assert migrated["eligible_at"] is None
    assert migrated["expires_at"] is None


def test_concurrent_application_attempt_claim_has_one_winner(tmp_path):
    db_path = str(tmp_path / "state.sqlite3")
    setup_store = Store(db_path)
    payload = _proposal_payload()
    _save_approved_proposal(setup_store, payload)

    stores = [Store(db_path), Store(db_path)]
    start = threading.Barrier(2)

    def claim(store):
        start.wait(timeout=5)
        return store.mark_application_attempted(
            payload["proposal_id"], "approval-original"
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(claim, store) for store in stores]
        results = [future.result(timeout=10) for future in futures]

    final_row = Store(db_path).proposal(payload["proposal_id"])
    assert results.count(True) == 1
    assert results.count(False) == 1
    assert final_row["application_attempted"] == 1
    assert final_row["status"] == "approved"
    assert final_row["approval_id"] == "approval-original"


def test_saving_conflict_cannot_overwrite_applied_proposal_or_audit(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    approval_id = "approval-original"
    _save_approved_proposal(store, payload, approval_id)

    assert store.mark_approval_applied(
        payload["proposal_id"],
        approval_id,
        "2027-01-04T10:30:00+05:30",
        "approval_applied",
        {"proposal_id": payload["proposal_id"], "approval_id": approval_id},
    )
    existing_audit = store.audit_rows("wf")

    store.save_proposal(
        payload["proposal_id"],
        payload["workflow_id"],
        payload["base_version"],
        "approval",
        "conflict",
        payload,
        approval_id,
    )

    assert store.audit_rows("wf") == existing_audit
    assert [row["action"] for row in store.audit_rows("wf")] == [
        "approval_applied"
    ]
    assert store.proposal(payload["proposal_id"])["status"] == "applied"


def test_applied_finalization_rolls_back_status_audit_and_cursor_together(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    _save_approved_proposal(store, payload, "approval-original")
    store.set_experiment_dimension("wf", "voice")
    with store._connection() as connection:
        connection.execute(
            """CREATE TRIGGER reject_applied_audit
               BEFORE INSERT ON audit
               WHEN NEW.action='approval_applied'
               BEGIN SELECT RAISE(ABORT, 'audit insert rejected'); END"""
        )

    with pytest.raises(sqlite3.IntegrityError, match="audit insert rejected"):
        store.finalize_applied_proposal(
            payload["proposal_id"],
            "approval-original",
            "2027-01-04T10:00:00+05:30",
            "approval_applied",
            {"proposal_id": payload["proposal_id"]},
            advance_cursor=True,
            primary_kpi="engagement_rate",
        )

    assert store.proposal(payload["proposal_id"])["status"] == "approved"
    assert store.experiment_dimension("wf", "welcome_message") == "voice"
    assert store.proposal_cursor_advancement(payload["proposal_id"]) is None
    assert store.audit_rows("wf") == []


def test_cursor_update_failure_rolls_back_applied_audit_and_ledger(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    _save_approved_proposal(store, payload, "approval-original")
    store.set_experiment_dimension("wf", "voice")
    with store._connection() as connection:
        connection.execute(
            """CREATE TRIGGER reject_cursor_update
               BEFORE UPDATE ON experiment_cursor
               BEGIN SELECT RAISE(ABORT, 'cursor update rejected'); END"""
        )

    with pytest.raises(sqlite3.IntegrityError, match="cursor update rejected"):
        store.finalize_applied_proposal(
            payload["proposal_id"],
            "approval-original",
            "2027-01-04T10:00:00+05:30",
            "approval_applied",
            {"proposal_id": payload["proposal_id"]},
            advance_cursor=True,
            primary_kpi="engagement_rate",
        )

    assert store.proposal(payload["proposal_id"])["status"] == "approved"
    assert store.experiment_dimension("wf", "welcome_message") == "voice"
    assert store.proposal_cursor_advancement(payload["proposal_id"]) is None
    assert store.audit_rows("wf") == []


def test_applied_finalization_is_idempotent_for_audit_and_cursor(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    _save_approved_proposal(store, payload, "approval-original")
    store.set_experiment_dimension("wf", "voice")
    detail = {"proposal_id": payload["proposal_id"], "workflow_id": "wf"}

    assert store.finalize_applied_proposal(
        payload["proposal_id"],
        "approval-original",
        "2027-01-04T10:00:00+05:30",
        "approval_applied",
        detail,
        advance_cursor=True,
        primary_kpi="engagement_rate",
    )
    assert not store.finalize_applied_proposal(
        payload["proposal_id"],
        "approval-original",
        "2027-01-04T10:15:00+05:30",
        "approval_applied_reconciled",
        detail,
        advance_cursor=True,
        primary_kpi="engagement_rate",
    )

    assert store.proposal(payload["proposal_id"])["status"] == "applied"
    assert store.experiment_dimension("wf", "welcome_message") == "welcome_message"
    assert [row["action"] for row in store.audit_rows("wf")] == [
        "approval_applied"
    ]
    assert store.proposal_cursor_advancement(payload["proposal_id"]) == "advanced"


def test_applied_cursor_advancement_survives_restart(tmp_path):
    db_path = str(tmp_path / "state.sqlite3")
    store = Store(db_path)
    payload = _proposal_payload()
    _save_approved_proposal(store, payload, "approval-original")
    store.set_experiment_dimension("wf", "voice")

    assert store.finalize_applied_proposal(
        payload["proposal_id"],
        "approval-original",
        "2027-01-04T10:00:00+05:30",
        "approval_applied",
        {"proposal_id": payload["proposal_id"]},
        advance_cursor=True,
        primary_kpi="engagement_rate",
    )
    restarted_store = Store(db_path)

    assert restarted_store.proposal_cursor_advancement(
        payload["proposal_id"]
    ) == "advanced"
    assert restarted_store.experiment_dimension("wf", "welcome_message") == "welcome_message"
    assert not restarted_store.finalize_applied_proposal(
        payload["proposal_id"],
        "approval-original",
        "2027-01-04T10:15:00+05:30",
        "approval_applied_reconciled",
        {"proposal_id": payload["proposal_id"]},
        advance_cursor=True,
        primary_kpi="engagement_rate",
    )
    assert restarted_store.experiment_dimension("wf", "welcome_message") == "welcome_message"


def test_legacy_applied_promotion_is_marked_ambiguous_on_migration(tmp_path):
    db_path = str(tmp_path / "state.sqlite3")
    store = Store(db_path)
    payload = _proposal_payload()
    payload["rationale"] = "Promote V2 after a completed experiment."
    _save_approved_proposal(store, payload, "approval-original")
    with store._connection() as connection:
        connection.execute(
            "UPDATE proposals SET status='applied' WHERE proposal_id=?",
            (payload["proposal_id"],),
        )
        connection.execute(
            """INSERT INTO audit(ts, workflow_id, action, detail)
               VALUES (?, ?, ?, ?)""",
            ("2027-01-04T10:00:00+05:30", "wf", "approval_applied",
             json.dumps({"proposal_id": payload["proposal_id"]})),
        )
        connection.execute("DROP TABLE proposal_cursor_advancements")
        connection.execute("DROP TABLE store_migrations")
    store.set_experiment_dimension("wf", "voice")

    migrated_store = Store(db_path)

    assert migrated_store.proposal_cursor_advancement(
        payload["proposal_id"]
    ) == "legacy_unverified"
    assert migrated_store.experiment_dimension("wf", "welcome_message") == "voice"
    controller = Controller(
        "http://simulator.test", db_path, store=migrated_store
    )
    controller.client = _ApprovalRecoveryApi([])
    controller._workflow_specs["wf"] = {
        "objective": {"primary": "engagement_rate"}
    }

    asyncio.run(controller._reconcile("2027-01-04T10:30:00+05:30", plan=False))

    assert migrated_store.experiment_dimension("wf", "welcome_message") == "voice"
    assert migrated_store.proposal_cursor_advancement(
        payload["proposal_id"]
    ) == "legacy_unverified"


def test_reconcile_repairs_applied_promotion_without_audit_or_cursor(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    payload["rationale"] = "Promote V2 after planned look 3."
    _save_approved_proposal(store, payload, "approval-original")
    with store._connection() as connection:
        connection.execute(
            "UPDATE proposals SET status='applied' WHERE proposal_id=?",
            (payload["proposal_id"],),
        )
    store.set_experiment_dimension("wf", "voice")
    api = _ApprovalRecoveryApi([], history_entries=[{
        "version": payload["base_version"] + 1,
        "proposal_id": payload["proposal_id"],
        "approval_id": "approval-original",
        "actor": "controller_approved",
    }])
    controller = Controller("http://simulator.test", str(store.path), store=store)
    controller.client = api
    controller._workflow_specs["wf"] = {
        "objective": {"primary": "engagement_rate"}
    }

    asyncio.run(controller._reconcile("2027-01-04T10:30:00+05:30", plan=False))

    assert store.proposal(payload["proposal_id"])["status"] == "applied"
    assert store.proposal_cursor_advancement(payload["proposal_id"]) == "advanced"
    assert store.experiment_dimension("wf", "welcome_message") == "welcome_message"
    assert [row["action"] for row in store.audit_rows("wf")] == [
        "approval_applied_reconciled"
    ]


def test_reconcile_repairs_applied_promotion_with_audit_but_no_cursor(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    payload["rationale"] = "Promote V2 after planned look 3."
    _save_approved_proposal(store, payload, "approval-original")
    with store._connection() as connection:
        connection.execute(
            "UPDATE proposals SET status='applied' WHERE proposal_id=?",
            (payload["proposal_id"],),
        )
        connection.execute(
            """INSERT INTO audit(ts, workflow_id, action, detail)
               VALUES (?, ?, ?, ?)""",
            ("2027-01-04T10:00:00+05:30", "wf", "approval_applied",
             json.dumps({"proposal_id": payload["proposal_id"]})),
        )
    store.set_experiment_dimension("wf", "voice")
    api = _ApprovalRecoveryApi(
        [], approvals_error_once=RemoteError(
            503, "temporarily_unavailable", "approval list unavailable"
        )
    )
    controller = Controller("http://simulator.test", str(store.path), store=store)
    controller.client = api
    controller._workflow_specs["wf"] = {
        "objective": {"primary": "engagement_rate"}
    }

    with pytest.raises(RemoteError):
        asyncio.run(controller._reconcile(
            "2027-01-04T10:30:00+05:30", plan=False
        ))

    assert api.history_gets == 0
    assert store.proposal_cursor_advancement(payload["proposal_id"]) == "advanced"
    assert store.experiment_dimension("wf", "welcome_message") == "welcome_message"
    assert len(store.audit_rows("wf")) == 1
    asyncio.run(controller._reconcile("2027-01-04T10:45:00+05:30", plan=False))
    assert store.experiment_dimension("wf", "welcome_message") == "welcome_message"


def test_repeated_reconcile_does_not_toggle_cursor_repeatedly(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    payload["rationale"] = "Promote V2 after planned look 3."
    _save_approved_proposal(store, payload, "approval-original")
    with store._connection() as connection:
        connection.execute(
            "UPDATE proposals SET status='applied' WHERE proposal_id=?",
            (payload["proposal_id"],),
        )
        connection.execute(
            """INSERT INTO audit(ts, workflow_id, action, detail)
               VALUES (?, ?, ?, ?)""",
            ("2027-01-04T10:00:00+05:30", "wf", "approval_applied",
             json.dumps({"proposal_id": payload["proposal_id"]})),
        )
    store.set_experiment_dimension("wf", "voice")
    controller = Controller("http://simulator.test", str(store.path), store=store)
    controller.client = _ApprovalRecoveryApi([])
    controller._workflow_specs["wf"] = {
        "objective": {"primary": "engagement_rate"}
    }

    asyncio.run(controller._reconcile("2027-01-04T10:30:00+05:30", plan=False))
    asyncio.run(controller._reconcile("2027-01-04T10:45:00+05:30", plan=False))

    assert store.experiment_dimension("wf", "welcome_message") == "welcome_message"
    assert store.proposal_cursor_advancement(payload["proposal_id"]) == "advanced"
    assert len(store.audit_rows("wf")) == 1


def test_reconcile_never_applies_pending_remote_approval(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    store.save_proposal(
        payload["proposal_id"],
        payload["workflow_id"],
        payload["base_version"],
        "approval",
        "submitted",
        payload,
        "approval-original",
    )
    api = _ApprovalRecoveryApi([{
        "approval_id": "approval-original",
        "proposal_id": payload["proposal_id"],
        "workflow_id": payload["workflow_id"],
        "base_version": payload["base_version"],
        "status": "pending",
    }])
    controller = Controller("http://simulator.test", str(store.path), store=store)
    controller.client = api

    asyncio.run(controller._reconcile("2027-01-04T10:00:00+05:30", plan=False))

    assert store.proposal(payload["proposal_id"])["status"] == "pending"
    assert api.application_posts == []
    assert store.audit_rows("wf") == []


def test_failed_applied_audit_finalization_recovers_from_remote_history(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload()
    _save_approved_proposal(store, payload, "approval-original")
    assert store.mark_application_attempted(
        payload["proposal_id"], "approval-original"
    )
    with store._connection() as connection:
        connection.execute(
            """CREATE TRIGGER reject_reconciled_audit
               BEFORE INSERT ON audit
               WHEN NEW.action='approval_applied_reconciled'
               BEGIN SELECT RAISE(ABORT, 'audit insert rejected'); END"""
        )
    api = _ApprovalRecoveryApi(
        [{
            "approval_id": "approval-original",
            "proposal_id": payload["proposal_id"],
            "workflow_id": "wf",
            "base_version": 7,
            "status": "approved",
        }],
        config_version=8,
        history_entries=[{
            "version": 8,
            "actor": "controller_approved",
            "applied_at": "2027-01-04T10:20:00+05:30",
            "proposal_id": payload["proposal_id"],
            "approval_id": "approval-original",
        }],
    )
    controller = Controller(
        "http://simulator.test", str(store.path), store=store
    )
    controller.client = api

    with pytest.raises(sqlite3.IntegrityError, match="audit insert rejected"):
        asyncio.run(controller._reconcile(
            "2027-01-04T10:30:00+05:30", plan=False
        ))

    assert store.proposal(payload["proposal_id"])["status"] == "approved"
    assert store.proposal(payload["proposal_id"])["application_attempted"] == 1
    assert store.audit_rows("wf") == []
    assert api.application_posts == []

    with store._connection() as connection:
        connection.execute("DROP TRIGGER reject_reconciled_audit")
    asyncio.run(controller._reconcile(
        "2027-01-04T10:45:00+05:30", plan=False
    ))

    assert store.proposal(payload["proposal_id"])["status"] == "applied"
    assert [row["action"] for row in store.audit_rows("wf")] == [
        "approval_applied_reconciled"
    ]
    assert api.history_gets == 2
    assert api.application_posts == []


def test_concurrent_reconcile_does_not_duplicate_or_conflict_application(tmp_path):
    db_path = str(tmp_path / "state.sqlite3")
    store = Store(db_path)
    payload = _proposal_payload()
    _save_approved_proposal(store, payload)
    api = _ConcurrentApprovalApi(db_path, payload["proposal_id"])
    controllers = []
    for _ in range(2):
        controller = Controller("http://simulator.test", db_path, store=Store(db_path))
        controller.client = api
        controller._workflow_specs["wf"] = {
            "objective": {"primary": "pickup_rate"}
        }
        controllers.append(controller)

    def reconcile(controller):
        asyncio.run(
            controller._reconcile("2027-01-04T10:30:00+05:30", plan=False)
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(reconcile, controller) for controller in controllers]
        for future in futures:
            future.result(timeout=15)

    final_row = Store(db_path).proposal(payload["proposal_id"])
    audit_actions = [
        row["action"] for row in Store(db_path).audit_rows("wf")
    ]
    observed = (
        len(api.application_posts),
        final_row["status"],
        "approval_conflict" in audit_actions or "approval_lapsed" in audit_actions,
        audit_actions.count("approval_applied"),
    )
    assert observed == (1, "applied", False, 1)


def _register_experiment(
    store, *, experiment_id="exp-1", proposal_id=None, start=True
):
    store.create_experiment_family(
        "family-1", 0.01, 2, "2027-01-04T09:00:00+05:30"
    )
    store.register_experiment(
        experiment_id,
        "family-1",
        "wf",
        "default",
        3,
        "voice",
        "V1",
        "V2",
        "pickup_rate",
        "engagement_rate",
        "2027-01-04T09:00:00+05:30",
        proposal_id=proposal_id,
        guardrail_margin=0.02,
        baseline_primary=0.25,
        baseline_guardrail=0.5,
        target_primary_n=20000,
        target_guardrail_n=25000,
        design={"target_effect": 0.015, "target_power": 0.8},
    )
    store.freeze_experiment_family(
        "family-1", "2027-01-04T09:15:00+05:30"
    )
    if start:
        store.start_experiment(experiment_id, 10)


def _register_evaluation_experiment(
    store, *, experiment_id="evaluation", primary_kpi="engagement_rate",
    guardrail_kpi="pickup_rate", target_primary_n=80, target_guardrail_n=120,
):
    family_id = f"family-{experiment_id}"
    store.create_experiment_family(
        family_id, 0.01, 2, "2027-01-04T09:00:00+05:30"
    )
    store.register_experiment(
        experiment_id,
        family_id,
        "wf",
        "default",
        3,
        "voice",
        "V1",
        "V2",
        primary_kpi,
        guardrail_kpi,
        "2027-01-04T09:00:00+05:30",
        guardrail_margin=0.25,
        baseline_primary=0.5,
        baseline_guardrail=0.8,
        practical_effect=0.015,
        target_power=0.8,
        target_primary_n=target_primary_n,
        target_guardrail_n=target_guardrail_n,
        design={"target_effect": 0.015, "target_power": 0.8},
    )
    store.freeze_experiment_family(
        family_id, "2027-01-04T09:15:00+05:30"
    )
    store.start_experiment(experiment_id, 0)


def _planned_experiment_events(
    *, start_event_id=1, count_per_variant=120, mode="inconclusive",
    primary_kpi="engagement_rate",
):
    events = []
    for index in range(count_per_variant):
        for variant in ("V1", "V2"):
            event_id = start_event_id + len(events)
            event = _call_event(event_id)
            event.update({
                "person_id": f"person-{event_id}",
                "attempt_no": 1,
                "config_version": 3,
            })
            event["variant"]["voice"] = variant

            if primary_kpi == "pickup_rate":
                connected_ranges = (
                    ((0, 42), (60, 81), (90, 111))
                    if variant == "V1"
                    else ((0, 54), (60, 87), (90, 117))
                )
                connected = any(
                    start <= index < end for start, end in connected_ranges
                )
                engaged_ranges = (
                    ((0, 21), (60, 70), (90, 100))
                    if variant == "V1"
                    else ((0, 27), (60, 73), (90, 103))
                )
                engaged = any(
                    start <= index < end for start, end in engaged_ranges
                )
            else:
                if mode == "guardrail_failure":
                    connected_ranges = (
                        ((0, 54), (60, 87), (90, 117))
                        if variant == "V1"
                        else ((0, 42), (60, 81), (90, 111))
                    )
                else:
                    connected_ranges = ((0, 48), (60, 84), (90, 114))
                connected = any(
                    start <= index < end for start, end in connected_ranges
                )
                if mode == "guardrail_failure":
                    engaged_ranges = (
                        ((0, 27), (60, 74), (90, 104))
                        if variant == "V1"
                        else ((0, 34), (60, 77), (90, 107))
                    )
                elif mode == "beneficial" and variant == "V2":
                    engaged_ranges = ((0, 36), (60, 78), (90, 114))
                else:
                    engaged_ranges = ((0, 24), (60, 72), (90, 102))
                engaged = any(
                    start <= index < end for start, end in engaged_ranges
                ) and connected
            event["connected"] = connected
            event["engaged"] = engaged
            events.append(json.dumps(event))
    return events


def _guardrail_score_result(
    candidate_successes, control_successes, sample_size
):
    observations = []
    for variant, successes in (
        ("V2", candidate_successes),
        ("V1", control_successes),
    ):
        for index in range(sample_size):
            observations.append({
                "event_id": len(observations) + 1,
                "variant": variant,
                "connected": 1,
                "engaged": int(index < successes),
            })
    experiment = {
        "experiment_id": "score-bound",
        "candidate_variant": "V2",
        "control_variant": "V1",
        "primary_kpi": "pickup_rate",
        "guardrail_kpi": "engagement_rate",
        "guardrail_margin": 0.02,
        "practical_effect": 0.015,
    }
    family = {
        "alpha_per_hypothesis_per_look": 0.01 / 6,
        "look_count": 3,
    }
    look = {"look_number": 3, "cutoff_event_id": 2 * sample_size}
    return _evaluate_experiment_look(
        experiment, family, observations, look
    )[1]["guardrail"]


@pytest.mark.parametrize(
    ("candidate_successes", "control_successes", "sample_size"),
    [
        (0, 0, 100),
        (100, 100, 100),
        (0, 25, 100),
        (2, 4, 100),
        (96, 98, 100),
    ],
)
def test_guardrail_score_bound_rejects_probability_boundary_approval(
    candidate_successes, control_successes, sample_size
):
    guardrail = _guardrail_score_result(
        candidate_successes, control_successes, sample_size
    )

    assert guardrail["safe"] is False
    assert guardrail["lower_confidence_bound"] <= -guardrail["margin"]
    assert guardrail["lower_confidence_bound_method"] == (
        "Newcombe-Wilson hybrid score"
    )
    assert guardrail["confidence_level"] == pytest.approx(1 - 0.01 / 6)
    assert guardrail["critical_value"] == pytest.approx(
        statistics.NormalDist().inv_cdf(1 - 0.01 / 6)
    )


def test_guardrail_score_bound_accepts_clear_safe_case():
    guardrail = _guardrail_score_result(80000, 80000, 100000)

    assert guardrail["safe"] is True
    assert guardrail["lower_confidence_bound"] > -guardrail["margin"]


def test_guardrail_score_bound_rejects_clear_unsafe_case():
    guardrail = _guardrail_score_result(70000, 85000, 100000)

    assert guardrail["safe"] is False
    assert guardrail["lower_confidence_bound"] <= -guardrail["margin"]


def _ingest_planned_events(store, events, chunk_number):
    if not events:
        return
    last_event_id = json.loads(events[-1])["event_id"]
    store.process_chunk(
        {"chunk": chunk_number, "kind": "events",
         "last_event_id": last_event_id},
        ("\n".join(events) + "\n").encode(),
    )


class _ExperimentPromotionApi:
    def __init__(self):
        self.submissions = []
        self.config = {
            "allocations": {
                "default": {"voice": {"V1": 0.5, "V2": 0.5}}
            },
            "prompt_version": "P1",
        }

    async def request(self, method, path, **kwargs):
        if method == "GET" and path == "/workflows/wf/config":
            return {"version": 3, "config": copy.deepcopy(self.config)}
        if method == "POST" and path == "/approvals":
            self.submissions.append(copy.deepcopy(kwargs["json"]))
            return {
                "approval_id": f"approval-{len(self.submissions)}",
                "proposal_id": kwargs["json"]["proposal_id"],
                "status": "pending",
            }
        raise AssertionError(f"Unexpected simulator request: {method} {path}")


class _SimulatorLoopApi:
    def __init__(
        self, *, initial_minute=0, advance_mode="progress", workflows=None
    ):
        self.state = {
            "sim_minute": initial_minute,
            "ts": "2027-01-04T10:00:00+05:30",
            "last_event_id": 0,
            "finished": False,
        }
        self.advance_mode = advance_mode
        self.workflows = copy.deepcopy(workflows or [])
        self.advance_calls = 0
        self.close_calls = 0
        self.requests = []

    async def request(self, method, path, **kwargs):
        self.requests.append((method, path, copy.deepcopy(kwargs)))
        if method == "GET" and path == "/sim/state":
            return copy.deepcopy(self.state)
        if method == "GET" and path == "/workflows":
            return copy.deepcopy(self.workflows)
        if method == "GET" and path == "/events/chunks":
            return []
        if method == "GET" and path == "/approvals":
            return []
        raise AssertionError(f"Unexpected simulator request: {method} {path}")

    async def advance(self, minutes):
        self.advance_calls += 1
        if self.advance_mode == "progress":
            self.state["sim_minute"] += minutes
            self.state["finished"] = self.state["sim_minute"] >= 120
        elif self.advance_mode == "backwards":
            self.state["sim_minute"] -= 1
        elif self.advance_mode == "finish_without_clock_change":
            self.state["finished"] = True
        return copy.deepcopy(self.state)

    async def get_chunk(self, chunk_number):
        raise AssertionError("No simulator chunks are expected")

    async def close(self):
        self.close_calls += 1


def _experiment_simulator_state(last_event_id, *, finished=False):
    return {
        "last_event_id": last_event_id,
        "finished": finished,
        "ts": "2027-01-04T10:00:00+05:30",
    }


def test_registered_experiment_uses_planned_samples_and_recovers_after_restart(
    tmp_path,
):
    db_path = str(tmp_path / "state.sqlite3")
    store = Store(db_path)
    _register_evaluation_experiment(store)
    events = _planned_experiment_events()

    _ingest_planned_events(store, events[:80], 1)
    controller = Controller("http://simulator.test", db_path, store=store)
    asyncio.run(controller._evaluate_registered_experiments(
        _experiment_simulator_state(80)
    ))
    assert [look["status"] for look in store.experiment_looks("evaluation")] == [
        "planned", "planned", "planned"
    ]

    _ingest_planned_events(store, events[80:120], 2)
    asyncio.run(controller._evaluate_registered_experiments(
        _experiment_simulator_state(120)
    ))
    first_look = store.experiment_looks("evaluation")[0]
    assert first_look["status"] == "evaluated"
    assert first_look["cutoff_event_id"] == 120
    assert json.loads(first_look["result_json"])["outcome"] == "continue"
    assert store.experiments("running")[0]["status"] == "running"

    restarted_store = Store(db_path)
    restarted_controller = Controller(
        "http://simulator.test", db_path, store=restarted_store
    )
    asyncio.run(restarted_controller._evaluate_registered_experiments(
        _experiment_simulator_state(120)
    ))
    assert len([
        row for row in restarted_store.audit_rows("wf")
        if row["action"] == "experiment_look_evaluated"
    ]) == 1

    _ingest_planned_events(store, events[120:180], 3)
    asyncio.run(restarted_controller._evaluate_registered_experiments(
        _experiment_simulator_state(180)
    ))
    assert store.experiment_looks("evaluation")[1]["status"] == "evaluated"
    assert len(store.experiment_observations("evaluation")) == 180

    _ingest_planned_events(store, events[180:], 4)
    asyncio.run(restarted_controller._evaluate_registered_experiments(
        _experiment_simulator_state(240)
    ))
    looks = store.experiment_looks("evaluation")
    experiment = store.experiments()[0]
    assert [look["status"] for look in looks] == [
        "evaluated", "evaluated", "evaluated"
    ]
    assert [look["cutoff_event_id"] for look in looks] == [120, 180, 240]
    assert json.loads(looks[2]["result_json"])["outcome"] == "inconclusive"
    assert experiment["status"] == "inconclusive"
    assert experiment["end_event_id"] == 240
    totals = store.arm_stats("wf", "default", 3, "voice")
    assert totals["V1"].dials == totals["V2"].dials == 120
    assert len([
        row for row in store.audit_rows("wf")
        if row["action"] == "experiment_look_evaluated"
    ]) == 3


def test_registered_experiment_promotes_beneficial_final_look_once(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    _register_evaluation_experiment(store)
    _ingest_planned_events(
        store, _planned_experiment_events(mode="beneficial"), 1
    )
    controller = Controller(
        "http://simulator.test", str(store.path), store=store
    )
    controller.client = _ExperimentPromotionApi()
    controller._workflow_specs["wf"] = {
        "governance": {
            "auto_apply": {"enabled": False},
            "approver_hours": [10, 19],
        },
        "daily_batch": {"size": 300},
    }

    asyncio.run(controller._evaluate_registered_experiments(
        _experiment_simulator_state(240)
    ))
    final_look = store.experiment_looks("evaluation")[2]
    result = json.loads(final_look["result_json"])
    assert final_look["status"] == "evaluated"
    assert result["outcome"] == "beneficial"
    assert result["primary_kpi"] == "engagement_rate"
    assert result["primary_delta"] >= 0.015
    assert result["primary_significant"]
    assert result["guardrail"]["safe"]
    assert len(controller.client.submissions) == 1

    asyncio.run(controller._evaluate_registered_experiments(
        _experiment_simulator_state(240)
    ))
    assert len(controller.client.submissions) == 1
    proposals = store.proposals("wf")
    assert len(proposals) == 1
    assert len([
        row for row in store.audit_rows("wf")
        if row["action"] == "approval_requested"
    ]) == 1


def test_registered_experiment_guardrail_blocks_primary_improvement(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    _register_evaluation_experiment(store)
    _ingest_planned_events(
        store, _planned_experiment_events(mode="guardrail_failure"), 1
    )
    controller = Controller(
        "http://simulator.test", str(store.path), store=store
    )
    controller.client = _ExperimentPromotionApi()
    controller._workflow_specs["wf"] = {
        "governance": {
            "auto_apply": {"enabled": False},
            "approver_hours": [10, 19],
        },
        "daily_batch": {"size": 300},
    }

    asyncio.run(controller._evaluate_registered_experiments(
        _experiment_simulator_state(240)
    ))

    result = json.loads(store.experiment_looks("evaluation")[2]["result_json"])
    assert result["outcome"] == "guardrail_blocked"
    assert result["primary_delta"] >= 0.015
    assert result["guardrail"]["safe"] is False
    assert store.proposals("wf") == []
    assert controller.client.submissions == []


def test_registered_experiment_can_test_pickup_as_primary_kpi(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    _register_evaluation_experiment(
        store,
        primary_kpi="pickup_rate",
        guardrail_kpi="engagement_rate",
        target_primary_n=120,
        target_guardrail_n=80,
    )
    _ingest_planned_events(
        store,
        _planned_experiment_events(primary_kpi="pickup_rate"),
        1,
    )
    controller = Controller(
        "http://simulator.test", str(store.path), store=store
    )

    asyncio.run(controller._evaluate_registered_experiments(
        _experiment_simulator_state(240)
    ))

    looks = store.experiment_looks("evaluation")
    result = json.loads(looks[1]["result_json"])
    assert looks[1]["status"] == "evaluated"
    assert result["outcome"] == "beneficial"
    assert result["primary_kpi"] == "pickup_rate"
    assert result["primary_significant"]
    assert result["primary_delta"] >= 0.015
    assert result["guardrail"]["kpi"] == "engagement_rate"


def test_registered_look_fractions_scale_minimum_final_targets(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    _register_evaluation_experiment(
        store,
        primary_kpi="engagement_rate",
        guardrail_kpi="pickup_rate",
        target_primary_n=40,
        target_guardrail_n=120,
    )
    events = []
    for index in range(120):
        for variant in ("V1", "V2"):
            event = _call_event(len(events) + 1)
            event.update({
                "person_id": f"minimum-person-{len(events) + 1}",
                "attempt_no": 1,
                "config_version": 3,
                "connected": index < 20 or 60 <= index < 80,
                "engaged": False,
            })
            event["variant"]["voice"] = variant
            events.append(json.dumps(event))
    _ingest_planned_events(store, events, 1)
    experiment = store.experiments()[0]
    observations = store.experiment_observations("evaluation")
    assert [
        _planned_look_cutoff(observations, experiment, fraction)
        for fraction in (0.5, 0.75, 1.0)
    ] == [120, 180, 240]
    controller = Controller(
        "http://simulator.test", str(store.path), store=store
    )

    asyncio.run(controller._evaluate_registered_experiments(
        _experiment_simulator_state(240)
    ))

    experiment = store.experiments()[0]
    looks = store.experiment_looks("evaluation")
    assert experiment["target_primary_n"] == 40
    assert experiment["target_guardrail_n"] == 120
    assert [look["cutoff_event_id"] for look in looks] == [120, 180, 240]
    first_look = json.loads(looks[0]["result_json"])
    assert first_look["control"]["sample_size"] == 20
    assert first_look["candidate"]["sample_size"] == 20


def test_experiment_plan_and_looks_survive_store_restart(tmp_path):
    db_path = str(tmp_path / "state.sqlite3")
    store = Store(db_path)
    _register_experiment(store, proposal_id="proposal-experiment")

    assert store.set_experiment_look_cutoff("exp-1", 1, 120)
    restarted_store = Store(db_path)

    family = restarted_store.experiment_family("family-1")
    experiment = restarted_store.experiments("running")[0]
    looks = restarted_store.experiment_looks("exp-1")
    assert family["status"] == "frozen"
    assert family["look_fractions"] == [0.5, 0.75, 1.0]
    assert family["alpha_per_hypothesis_per_look"] == pytest.approx(0.01 / 6)
    assert experiment["proposal_id"] == "proposal-experiment"
    assert experiment["workflow_id"] == "wf"
    assert experiment["cohort_id"] == "default"
    assert experiment["config_version"] == 3
    assert (experiment["dimension"], experiment["control_variant"],
            experiment["candidate_variant"]) == ("voice", "V1", "V2")
    assert experiment["start_event_id"] == 10
    assert [look["information_fraction"] for look in looks] == [0.5, 0.75, 1.0]
    assert looks[0]["cutoff_event_id"] == 120


def test_reconcile_starts_registered_experiment_at_public_event_boundary(tmp_path):
    class Api(_ApprovalRecoveryApi):
        async def request(self, method, path, **kwargs):
            if method == "GET" and path == "/sim/state":
                return {"last_event_id": 37}
            return await super().request(method, path, **kwargs)

    db_path = str(tmp_path / "state.sqlite3")
    store = Store(db_path)
    payload = _proposal_payload()
    _save_approved_proposal(store, payload, "approval-original")
    _register_experiment(store, proposal_id=payload["proposal_id"], start=False)
    api = Api([{
        "proposal_id": payload["proposal_id"],
        "approval_id": "approval-original",
        "status": "applied",
    }])
    controller = Controller("http://simulator.test", db_path, store=store)
    controller.client = api
    controller._workflow_specs["wf"] = {
        "objective": {"primary": "pickup_rate"}
    }

    asyncio.run(
        controller._reconcile("2027-01-04T10:30:00+05:30", plan=False)
    )

    experiment = store.experiments("running")[0]
    assert experiment["start_event_id"] == 37
    assert store.proposal(payload["proposal_id"])["status"] == "applied"


def test_chunk_ingestion_enrolls_first_eligible_person_call_atomically(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    _register_experiment(store)

    events = []
    for event_id, person_id, variant, version in (
        (10, "person-boundary", "V1", 3),
        (11, "person-1", "V1", 3),
        (12, "person-1", "V2", 3),
        (13, "person-2", "V2", 3),
        (14, "person-wrong-version", "V1", 4),
    ):
        event = _call_event(event_id)
        event.update({
            "person_id": person_id,
            "attempt_no": 1,
            "config_version": version,
            "campaign_id": "wf:campaign-1",
            "lead_id": f"wf:lead-{event_id}",
            "connected": event_id % 2 == 1,
            "engaged": False,
        })
        event["variant"]["voice"] = variant
        events.append(json.dumps(event))

    chunk = {"chunk": 1, "kind": "events", "last_event_id": 14}
    encoded = ("\n".join(events) + "\n").encode()
    store.process_chunk(chunk, encoded)
    store.process_chunk(chunk, encoded)

    observations = store.experiment_observations("exp-1")
    assert [(row["person_id"], row["event_id"], row["variant"]) for row in observations] == [
        ("person-1", 11, "V1"),
        ("person-2", 13, "V2"),
    ]
    assert observations[0]["campaign_id"] == "wf:campaign-1"
    assert observations[0]["lead_id"] == "wf:lead-11"
    assert json.loads(observations[0]["variant_json"])["caller_id"] == "D1"
    assert store.arm_stats("wf", "default", 3, "voice")["V1"].dials == 2
    assert store.last_chunk() == 1


def test_concurrent_duplicate_chunk_enrolls_person_once(tmp_path):
    db_path = str(tmp_path / "state.sqlite3")
    store = Store(db_path)
    _register_experiment(store)
    event = _call_event(11)
    event.update({"person_id": "person-1", "attempt_no": 1, "config_version": 3})
    encoded = (json.dumps(event) + "\n").encode()
    chunk = {"chunk": 1, "kind": "events", "last_event_id": 11}
    stores = [Store(db_path), Store(db_path)]

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(candidate.process_chunk, chunk, encoded)
            for candidate in stores
        ]
        for future in futures:
            future.result(timeout=10)

    assert len(Store(db_path).experiment_observations("exp-1")) == 1
    assert Store(db_path).arm_stats("wf", "default", 3, "voice")["V1"].dials == 1


def test_out_of_order_chunk_replay_keeps_earliest_person_event(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    _register_experiment(store)
    later = _call_event(12)
    later.update({"person_id": "person-1", "attempt_no": 2, "config_version": 3})
    later["variant"]["voice"] = "V2"
    earlier = _call_event(11)
    earlier.update({"person_id": "person-1", "attempt_no": 1, "config_version": 3})
    earlier["variant"]["voice"] = "V1"

    store.process_chunk(
        {"chunk": 2, "kind": "events", "last_event_id": 12},
        (json.dumps(later) + "\n").encode(),
    )
    store.process_chunk(
        {"chunk": 1, "kind": "events", "last_event_id": 11},
        (json.dumps(earlier) + "\n").encode(),
    )

    observation = store.experiment_observations("exp-1")
    assert len(observation) == 1
    assert (observation[0]["event_id"], observation[0]["attempt_no"],
            observation[0]["variant"]) == (11, 1, "V1")


def test_experiment_observation_failure_rolls_back_aggregate_and_chunk(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    _register_experiment(store)
    with store._connection() as connection:
        connection.execute(
            """CREATE TRIGGER reject_experiment_observation
               BEFORE INSERT ON experiment_observations
               BEGIN SELECT RAISE(ABORT, 'test failure'); END"""
        )
    event = _call_event(11)
    event.update({"person_id": "person-1", "attempt_no": 1, "config_version": 3})

    with pytest.raises(sqlite3.IntegrityError, match="test failure"):
        store.process_chunk(
            {"chunk": 1, "kind": "events", "last_event_id": 11},
            (json.dumps(event) + "\n").encode(),
        )

    assert store.last_chunk() == -1
    assert store.arm_stats("wf", "default", 3, "voice") == {}
    assert store.experiment_observations("exp-1") == []


def test_caller_id_incident_uses_primary_and_isolated_current_population(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    controller = Controller("http://simulator.test", str(store.path), store=store)
    event_lines = []
    event_id = 0
    for version, cohort, did, dials, connected in (
        (7, "default", "D1", 300, 180),
        (7, "default", "D2", 300, 120),
        (6, "default", "D1", 300, 100),
        (6, "default", "D2", 300, 200),
        (7, "other", "D1", 300, 100),
        (7, "other", "D2", 300, 200),
    ):
        for index in range(dials):
            event_id += 1
            event = _call_event(event_id)
            event["config_version"] = version
            event["cohort_id"] = cohort
            event["variant"]["caller_id"] = did
            event["connected"] = index < connected
            event["engaged"] = event["connected"] and index % 2 == 0
            event_lines.append(json.dumps(event))

    store.process_chunk(
        {"chunk": 1, "kind": "events", "last_event_id": event_id},
        ("\n".join(event_lines) + "\n").encode(),
    )
    spec = {
        "objective": {
            "primary": "pickup_rate",
            "guardrail": {"engagement_rate_max_drop": 0.02},
        }
    }
    config = {
        "allocations": {
            "default": {"caller_id": {"D1": 0.5, "D2": 0.5}}
        }
    }

    result = asyncio.run(
        controller._incident_change("wf", spec, config, 0, 7)
    )

    assert result is not None
    revised, rationale = result
    assert revised["allocations"]["default"]["caller_id"] == {"D1": 1.0}
    assert "pickup_rate" in rationale


def test_caller_id_incident_uses_engagement_primary_with_pickup_guardrail(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    controller = Controller("http://simulator.test", str(store.path), store=store)
    event_lines = []
    event_id = 0
    for did, engaged_count in (("D1", 150), ("D2", 90)):
        for index in range(300):
            event_id += 1
            event = _call_event(event_id)
            event["config_version"] = 4
            event["variant"]["caller_id"] = did
            event["connected"] = index < 180
            event["engaged"] = index < engaged_count
            event_lines.append(json.dumps(event))

    store.process_chunk(
        {"chunk": 1, "kind": "events", "last_event_id": event_id},
        ("\n".join(event_lines) + "\n").encode(),
    )
    spec = {
        "objective": {
            "primary": "engagement_rate",
            "guardrail": {"pickup_rate_max_drop": 0.02},
        }
    }
    config = {
        "allocations": {
            "default": {"caller_id": {"D1": 0.5, "D2": 0.5}}
        }
    }

    result = asyncio.run(
        controller._incident_change("wf", spec, config, 0, 4)
    )

    assert result is not None
    revised, rationale = result
    assert revised["allocations"]["default"]["caller_id"] == {"D1": 1.0}
    assert "engagement_rate" in rationale


def _register_family_test_experiment(
    store, experiment_id, workflow_id, config_version, created_sim_ts
):
    store.register_experiment(
        experiment_id,
        "family-test",
        workflow_id,
        "default",
        config_version,
        "voice",
        "V1",
        "V2",
        "pickup_rate",
        "engagement_rate",
        created_sim_ts,
    )


def test_experiment_family_rejects_mismatched_registered_hypothesis_count(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    created = "2027-01-04T09:00:00+05:30"
    store.create_experiment_family("family-test", 0.01, 1, created)
    _register_family_test_experiment(store, "exp-1", "wf-1", 3, created)
    _register_family_test_experiment(store, "exp-2", "wf-2", 4, created)

    with pytest.raises(ValueError, match="hypothes"):
        store.freeze_experiment_family(
            "family-test", "2027-01-04T09:15:00+05:30"
        )

    assert store.experiment_family("family-test")["status"] == "open"


def test_experiment_family_alpha_requires_valid_frozen_hypothesis_count(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    created = "2027-01-04T09:00:00+05:30"
    store.create_experiment_family("family-test", 0.01, 1, created)
    _register_family_test_experiment(store, "exp-1", "wf-1", 3, created)
    _register_family_test_experiment(store, "exp-2", "wf-2", 4, created)

    with pytest.raises(ValueError, match="hypothes"):
        store.experiment_family("family-test")


def test_correct_experiment_family_hypothesis_count_freezes_and_allocates_alpha(
    tmp_path,
):
    store = Store(str(tmp_path / "state.sqlite3"))
    created = "2027-01-04T09:00:00+05:30"
    store.create_experiment_family("family-test", 0.01, 4, created)
    _register_family_test_experiment(store, "exp-1", "wf-1", 3, created)
    _register_family_test_experiment(store, "exp-2", "wf-2", 4, created)

    assert store.freeze_experiment_family(
        "family-test", "2027-01-04T09:15:00+05:30"
    )
    family = store.experiment_family("family-test")
    assert family["status"] == "frozen"
    assert family["alpha_per_hypothesis_per_look"] == pytest.approx(0.01 / (4 * 3))


@pytest.mark.parametrize("look_number", [2, 3])
def test_experiment_look_cannot_be_completed_before_previous_look(
    tmp_path, look_number
):
    store = Store(str(tmp_path / "state.sqlite3"))
    _register_experiment(store)

    with pytest.raises(ValueError, match="previous|preceding|order"):
        store.set_experiment_look_cutoff("exp-1", look_number, 120)


def test_experiment_look_cutoffs_must_increase_strictly(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    _register_experiment(store)
    assert store.set_experiment_look_cutoff("exp-1", 1, 120)

    with pytest.raises(ValueError, match="increase|cutoff|order"):
        store.set_experiment_look_cutoff("exp-1", 2, 120)


def test_experiment_looks_finalize_immutably_in_order(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    _register_experiment(store)

    assert store.set_experiment_look_cutoff("exp-1", 1, 120)
    assert store.set_experiment_look_cutoff("exp-1", 2, 160)
    assert store.set_experiment_look_cutoff("exp-1", 3, 200)
    assert not store.set_experiment_look_cutoff("exp-1", 3, 200)
    with pytest.raises(ValueError, match="immutable"):
        store.set_experiment_look_cutoff("exp-1", 3, 201)


def test_look_scoped_observations_respect_each_event_cutoff(tmp_path):
    store = Store(str(tmp_path / "state.sqlite3"))
    created = "2027-01-04T09:00:00+05:30"
    store.create_experiment_family("family-test", 0.01, 4, created)
    _register_family_test_experiment(store, "exp-1", "wf", 3, created)
    _register_family_test_experiment(store, "exp-2", "other-wf", 4, created)
    store.freeze_experiment_family(
        "family-test", "2027-01-04T09:15:00+05:30"
    )
    store.start_experiment("exp-1", 10)
    store.start_experiment("exp-2", 10)
    for experiment_id in ("exp-1", "exp-2"):
        for look_number, cutoff in ((1, 12), (2, 14), (3, 16)):
            store.set_experiment_look_cutoff(
                experiment_id, look_number, cutoff
            )

    events = []
    for event_id, workflow_id, config_version, person_id in (
        (11, "wf", 3, "person-1"),
        (12, "wf", 3, "person-2"),
        (13, "wf", 3, "person-3"),
        (14, "other-wf", 4, "other-workflow-person"),
        (15, "wf", 4, "wrong-version-person"),
        (16, "wf", 3, "person-4"),
        (17, "wf", 3, "after-final-boundary"),
    ):
        event = _call_event(event_id)
        event.update({
            "workflow_id": workflow_id,
            "config_version": config_version,
            "person_id": person_id,
            "attempt_no": 1,
        })
        event["variant"]["voice"] = "V1" if event_id % 2 else "V2"
        events.append(json.dumps(event))

    store.process_chunk(
        {"chunk": 1, "kind": "events", "last_event_id": 17},
        ("\n".join(events) + "\n").encode(),
    )

    all_exp1 = store.experiment_observations("exp-1")
    assert [row["event_id"] for row in all_exp1] == [11, 12, 13, 16, 17]
    assert [row["event_id"] for row in store.experiment_observations(
        "exp-1", look_number=1
    )] == [11, 12]
    assert [row["event_id"] for row in store.experiment_observations(
        "exp-1", look_number=2
    )] == [11, 12, 13]
    assert [row["event_id"] for row in store.experiment_observations(
        "exp-1", look_number=3
    )] == [11, 12, 13, 16]
    assert [row["event_id"] for row in store.experiment_observations(
        "exp-2", look_number=3
    )] == [14]


class _ExperimentConflictApi:
    def __init__(self, path):
        self.path = path
        self.submissions = []

    async def request(self, method, request_path, **kwargs):
        if method == "GET" and request_path == "/approvals":
            if self.path == "approved_pre_application":
                return [{
                    "approval_id": "approval-original",
                    "proposal_id": "proposal-original",
                    "workflow_id": "wf",
                    "base_version": 7,
                    "status": "approved",
                }]
            return []
        if method == "GET" and request_path == "/workflows/wf/config":
            version = 8 if self.path in (
                "deferred_pre_submit", "approved_pre_application"
            ) else 7
            return {"version": version, "config": {}}
        if method == "POST" and request_path == "/approvals":
            self.submissions.append(copy.deepcopy(kwargs["json"]))
            if self.path == "deferred_stale_response":
                raise RemoteError(
                    409, "stale_base_version", "configuration version changed"
                )
            raise AssertionError("Unexpected approval submission")
        raise AssertionError(f"Unexpected simulator request: {method} {request_path}")


@pytest.mark.parametrize(
    "path",
    [
        "deferred_pre_submit",
        "deferred_stale_response",
        "approved_pre_application",
    ],
    ids=[
        "deferred_pre_submit_version_conflict",
        "deferred_stale_version_response",
        "approved_pre_application_version_conflict",
    ],
)
def test_reconcile_conflicts_cancel_experiment_and_release_population(
    tmp_path, path
):
    store = Store(str(tmp_path / "state.sqlite3"))
    payload = _proposal_payload(
        eligible_at="2027-01-04T10:00:00+05:30"
    )
    if path == "approved_pre_application":
        _save_approved_proposal(store, payload)
    else:
        _save_deferred_proposal(store, payload)
    _register_experiment(store, proposal_id=payload["proposal_id"], start=False)

    controller = Controller("http://simulator.test", str(store.path), store=store)
    controller.client = _ExperimentConflictApi(path)
    controller._workflow_specs["wf"] = {
        "governance": {"approver_hours": [10, 19]},
        "objective": {"primary": "pickup_rate"},
    }
    simulator_ts = "2027-01-04T10:00:00+05:30"
    asyncio.run(controller._reconcile(simulator_ts, plan=False))
    store = Store(str(store.path))

    proposal = store.proposal(payload["proposal_id"])
    experiment = store.experiments()[0]
    audit = [
        row for row in store.audit_rows("wf")
        if row["action"] == "approval_conflict"
    ]

    replacement_registered = False
    store.create_experiment_family(
        "replacement-family", 0.01, 2, "2027-01-04T10:00:00+05:30"
    )
    try:
        store.register_experiment(
            "replacement-experiment",
            "replacement-family",
            "wf",
            "default",
            3,
            "voice",
            "V1",
            "V2",
            "pickup_rate",
            "engagement_rate",
            "2027-01-04T10:00:00+05:30",
        )
        replacement_registered = True
    except sqlite3.IntegrityError:
        pass

    applied_payload = _proposal_payload("already-applied-proposal")
    _save_approved_proposal(store, applied_payload, "already-applied-approval")
    assert store.mark_approval_applied(
        applied_payload["proposal_id"],
        "already-applied-approval",
        simulator_ts,
        "approval_applied",
        {"proposal_id": applied_payload["proposal_id"]},
    )
    audit_before_stale_conflict = store.audit_rows("wf")
    store.save_proposal(
        applied_payload["proposal_id"],
        applied_payload["workflow_id"],
        applied_payload["base_version"],
        "approval",
        "conflict",
        applied_payload,
        "already-applied-approval",
        audit_event=(
            "approval_conflict",
            {"proposal_id": applied_payload["proposal_id"]},
        ),
        audit_ts=simulator_ts,
    )
    applied_proposal_preserved = (
        store.proposal(applied_payload["proposal_id"])["status"] == "applied"
        and store.audit_rows("wf") == audit_before_stale_conflict
    )

    observed = {
        "proposal_status": proposal["status"],
        "experiment_status": experiment["status"],
        "conflict_audit_count": len(audit),
        "audit_proposal_id": audit[0]["detail"]["proposal_id"] if audit else None,
        "audit_ts": audit[0]["ts"] if audit else None,
        "replacement_registered": replacement_registered,
        "applied_proposal_preserved": applied_proposal_preserved,
    }
    assert observed == {
        "proposal_status": "conflict",
        "experiment_status": "cancelled",
        "conflict_audit_count": 1,
        "audit_proposal_id": payload["proposal_id"],
        "audit_ts": simulator_ts,
        "replacement_registered": True,
        "applied_proposal_preserved": True,
    }


class _ExperimentStartupApi:
    def __init__(self, event_boundary):
        self.event_boundary = event_boundary
        self.boundaries_returned = []

    async def request(self, method, path, **kwargs):
        if method == "GET" and path == "/approvals":
            return [{
                "approval_id": "approval-original",
                "proposal_id": "proposal-original",
                "workflow_id": "wf",
                "base_version": 7,
                "status": "applied",
            }]
        if method == "GET" and path == "/sim/state":
            self.boundaries_returned.append(self.event_boundary)
            return {"last_event_id": self.event_boundary}
        raise AssertionError(f"Unexpected simulator request: {method} {path}")


def test_concurrent_reconcile_starts_experiment_at_one_winning_boundary(tmp_path):
    db_path = str(tmp_path / "state.sqlite3")
    setup_store = Store(db_path)
    payload = _proposal_payload()
    _save_approved_proposal(setup_store, payload)
    _register_experiment(
        setup_store, proposal_id=payload["proposal_id"], start=False
    )

    proposal_snapshot_barrier = threading.Barrier(2)
    pending_experiment_barrier = threading.Barrier(2)
    start_outcomes = []
    losing_controller_boundaries = []
    outcome_lock = threading.Lock()
    controllers = []
    apis = []
    for boundary in (37, 42):
        instance_store = Store(db_path)
        original_proposals = instance_store.proposals

        def synchronized_proposals(
            workflow_id=None, original=original_proposals
        ):
            result = original(workflow_id)
            proposal_snapshot_barrier.wait(timeout=5)
            return result

        instance_store.proposals = synchronized_proposals
        original_pending = instance_store.pending_experiment_for_proposal

        def synchronized_pending(proposal_id, original=original_pending):
            result = original(proposal_id)
            pending_experiment_barrier.wait(timeout=5)
            return result

        instance_store.pending_experiment_for_proposal = synchronized_pending
        original_experiments = instance_store.experiments
        local_start_outcomes = []

        def tracked_experiments(status=None, original=original_experiments):
            result = original(status)
            if (
                status is None
                and local_start_outcomes
                and local_start_outcomes[-1] is False
            ):
                matching = next(
                    (item for item in result if item["experiment_id"] == "exp-1"),
                    None,
                )
                if matching is not None and matching["status"] == "running":
                    with outcome_lock:
                        losing_controller_boundaries.append(
                            matching["start_event_id"]
                        )
            return result

        instance_store.experiments = tracked_experiments
        original_start = instance_store.start_experiment

        def tracked_start(experiment_id, event_id, original=original_start):
            try:
                result = original(experiment_id, event_id)
            except Exception as exc:
                with outcome_lock:
                    start_outcomes.append(exc)
                raise
            with outcome_lock:
                start_outcomes.append(result)
                local_start_outcomes.append(result)
            return result

        instance_store.start_experiment = tracked_start

        controller = Controller(
            "http://simulator.test", db_path, store=instance_store
        )
        api = _ExperimentStartupApi(boundary)
        controller.client = api
        controller._workflow_specs["wf"] = {
            "objective": {"primary": "pickup_rate"}
        }
        controllers.append(controller)
        apis.append(api)

    def reconcile(controller):
        try:
            asyncio.run(
                controller._reconcile("2027-01-04T10:30:00+05:30", plan=False)
            )
        except Exception as exc:
            return exc
        return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(reconcile, controller) for controller in controllers]
        reconcile_errors = [future.result(timeout=15) for future in futures]

    persisted_store = Store(db_path)
    running = persisted_store.experiments("running")
    winning_boundary = running[0]["start_event_id"] if running else None
    observed = {
        "reconcile_errors": reconcile_errors,
        "requested_boundaries": sorted(
            boundary
            for api in apis
            for boundary in api.boundaries_returned
        ),
        "successful_starts": start_outcomes.count(True),
        "unsuccessful_starts": start_outcomes.count(False),
        "start_errors": sum(isinstance(item, Exception) for item in start_outcomes),
        "losing_controller_boundaries": losing_controller_boundaries,
        "running_count": len(running),
        "winning_boundary": winning_boundary,
        "winning_boundary_is_proposed": winning_boundary in (37, 42),
        "proposal_status": persisted_store.proposal(
            payload["proposal_id"]
        )["status"],
        "controllers_read_winning_boundary": all(
            controller.store.experiments("running")[0]["start_event_id"]
            == winning_boundary
            for controller in controllers
        ) if running else False,
    }
    assert observed == {
        "reconcile_errors": [None, None],
        "requested_boundaries": [37, 42],
        "successful_starts": 1,
        "unsuccessful_starts": 1,
        "start_errors": 0,
        "losing_controller_boundaries": [winning_boundary],
        "running_count": 1,
        "winning_boundary": winning_boundary,
        "winning_boundary_is_proposed": True,
        "proposal_status": "applied",
        "controllers_read_winning_boundary": True,
    }
    assert not persisted_store.start_experiment("exp-1", 999)
    assert persisted_store.experiments("running")[0]["start_event_id"] == winning_boundary
