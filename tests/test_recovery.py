from __future__ import annotations

from typing import Any

from roxchaos.harness import ChaosHarness
from roxchaos.report import ChaosReport


def test_two_parallel_workflows_recover_after_service_crashes(
    chaos_harness: ChaosHarness,
) -> None:
    report = ChaosReport(chaos_harness.settings.report_dir)
    try:
        report.add("provenance", **chaos_harness.provenance())
        report.add("scheduler_startup", **chaos_harness.scheduler_startup)
        assert chaos_harness.scheduler_startup["initial_lock"] == "absent"
        assert chaos_harness.scheduler_startup["after_training_lock"] == "present"
        assert all(chaos_harness.scheduler_startup["heartbeat_refresh"].values())
        assert all(chaos_harness.scheduler_startup["consumers"].values())
        assert chaos_harness.scheduler_startup["training_queue_empty"] is True
        arm_result = chaos_harness.arm_scheduled_workflows()
        report.add("workflow_schedules_armed", **arm_result)
        scheduled = chaos_harness.wait_for_scheduled_runs(arm_result)
        runs = scheduled["runs"]
        run_ids = [run["id"] for run in runs]
        assert len(run_ids) == 2
        assert len({run["list"]["name"] for run in runs}) == 2
        assert all(run["trigger_type"] == "scheduled" for run in runs)
        assert all(entry["count"] == 1 for entry in scheduled["scheduled_period_row_counts"])
        assert len(scheduled["deliveries"]) == 2
        report.add("scheduled_workflows_started", **scheduled)

        fault_state = chaos_harness.wait_for_fault_window(run_ids)
        report.add(
            "fault_window_reached",
            statuses=_run_statuses(fault_state),
            external_jobs=sum(
                len(run["external_jobs"]) for run in fault_state["runs"]
            ),
        )

        chaos_harness.crash_processing_services()
        pending_after_crash = chaos_harness.pending_summary()
        victim = fault_state["fault_victim"]
        assert pending_after_crash[0] > 0
        assert chaos_harness.redis_value(f"result:{victim['job_id']}") is None
        report.add(
            "services_killed", pending=pending_after_crash, victim=victim
        )

        restart_evidence = chaos_harness.restart_processing_services()
        report.add("services_restarted", **restart_evidence)

        final_state = chaos_harness.wait_for_terminal_runs(run_ids)
        report.final_state = final_state
        api_jobs = _assert_recovery(chaos_harness, final_state)
        scheduler_cycle = chaos_harness.wait_for_scheduler_cycle_after_inference()
        assert scheduler_cycle["lock_transition"] == "present -> absent -> present"
        assert scheduler_cycle["lock_absence_observed"] is True
        assert scheduler_cycle["training_heartbeat_gap_observed"] is True
        assert scheduler_cycle["new_training_consumers"]
        final_state["api_jobs"] = api_jobs
        final_state["redis"] = {
            "roxinfer": {
                "pending": chaos_harness.pending_summary(),
                "stream_length": chaos_harness.stream_length(),
            },
            "roxtrain": chaos_harness.assert_training_queue_empty(),
        }
        final_state["scheduler"] = {
            "startup": chaos_harness.scheduler_startup,
            "post_recovery_cycle": scheduler_cycle,
            "scheduled_trigger": scheduled,
        }
        final_state["services"] = chaos_harness.service_status()
        report.add("scheduler_cycle_completed", **scheduler_cycle)
        report.add("workflows_completed", statuses=_run_statuses(final_state))
        report.pass_test(final_state)
    except BaseException as error:
        report.fail_test(error)
        raise
    finally:
        report.write()


def _assert_recovery(
    harness: ChaosHarness, final_state: dict[str, Any]
) -> list[dict[str, Any]]:
    runs = final_state["runs"]
    expectations = harness.execution_expectations()
    assert len(runs) == 2
    assert {run["list"]["name"] for run in runs} == set(harness.settings.workflows)

    all_jobs: list[dict[str, Any]] = []
    for run in runs:
        expected = expectations[run["list"]["name"]]
        assert run["status"] == "completed", run["message"]
        assert run["trigger_type"] == "scheduled"
        assert run["expected_items_count"] == expected["documents"]
        assert run["processed_items_count"] == expected["documents"]
        assert run["counts"]["analyses"] == expected["analyses"]
        assert (
            run["counts"]["workflow_run_items"]
            == expected["workflow_run_items"]
        )
        assert run["counts"]["external_jobs"] == expected["external_jobs"]
        assert (
            run["counts"]["task_logs_by_state"].get("SUCCESS", 0)
            == expected["successful_task_logs"]
        )
        assert run["counts"]["reference_tags"] == expected["reference_tags"]
        assert (
            run["counts"]["workflow_document_logs"]
            == expected["workflow_document_logs"]
        )
        assert run["counts"]["task_logs"] == expected["successful_task_logs"]
        assert set(run["counts"]["task_logs_by_state"]) == {"SUCCESS"}
        assert run["duplicate_workflow_run_item_groups"] == []
        assert run["duplicate_success_task_log_groups"] == []
        assert run["duplicate_reference_tag_groups"] == []
        assert run["duplicate_workflow_document_log_groups"] == []
        assert all(item["status"] == "succeeded" for item in run["workflow_run_items"])
        assert all(job["status"] == "succeeded" for job in run["external_jobs"])
        document_item_counts: dict[str, int] = {}
        for item in run["workflow_run_items"]:
            reference = item["document_reference"]
            document_item_counts[reference] = document_item_counts.get(reference, 0) + 1
        assert len(document_item_counts) == expected["documents"]
        assert set(document_item_counts.values()) == {expected["items_per_document"]}

        success_logs_by_step: dict[int, int] = {}
        for log in run["task_logs"]:
            step = log["task"]["step_order"]
            success_logs_by_step[step] = success_logs_by_step.get(step, 0) + 1
        assert success_logs_by_step[2] == 1
        assert set(success_logs_by_step) == set(range(2, 23))
        assert all(
            success_logs_by_step[step] == expected["documents"]
            for step in range(3, 23)
        )

        assert len({analysis["reference"] for analysis in run["analyses"]}) == expected[
            "documents"
        ]
        assert {analysis["unique_id_sha256"] for analysis in run["analyses"]} == set(
            expected["analysis_identity_sha256"]
        )
        expected_audit = run["list"]["name"].endswith(" - AUDIT")
        assert all(analysis["is_audit"] == expected_audit for analysis in run["analyses"])
        assert all(log["status"] == "detected" for log in run["workflow_document_logs"])
        assert run["async_schedule"]["active"] is True
        assert run["async_schedule"]["recurrence_type"] == "daily"
        all_jobs.extend(run["external_jobs"])

    assert len({job["job_id"] for job in all_jobs}) == len(all_jobs)
    assert len({job["idempotency_key"] for job in all_jobs}) == len(all_jobs)
    assert len(all_jobs) == sum(
        expected["external_jobs"] for expected in expectations.values()
    )

    api_jobs = [harness.api_job(job["job_id"]) for job in all_jobs]
    assert all(job["status"] == "success" for job in api_jobs)
    assert all(job["name"] == "roxinfer" for job in api_jobs)
    assert {job["job_id"] for job in harness.api_jobs()} == {
        job["job_id"] for job in all_jobs
    }
    for job in all_jobs:
        assert harness.idempotency_target(job["idempotency_key"]) == job["job_id"]

    pending = harness.pending_summary()
    assert pending[0] == 0
    assert harness.stream_length() == 0
    return [
        {
            "job_id": job["job_id"],
            "name": job["name"],
            "status": job["status"],
            "source": job.get("source"),
        }
        for job in api_jobs
    ]


def _run_statuses(state: dict[str, Any]) -> dict[str, str]:
    return {run["list"]["name"]: run["status"] for run in state["runs"]}
