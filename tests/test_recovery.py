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
        runs = chaos_harness.start_runs()
        run_ids = [run["id"] for run in runs]
        assert len(run_ids) == 2
        assert len({run["list"]["name"] for run in runs}) == 2
        report.add("workflows_started", runs=runs)

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

        chaos_harness.restart_processing_services()
        report.add("services_restarted")

        final_state = chaos_harness.wait_for_terminal_runs(run_ids)
        api_jobs = _assert_recovery(chaos_harness, final_state)
        final_state["api_jobs"] = api_jobs
        final_state["redis"] = {
            "pending": chaos_harness.pending_summary(),
            "stream_length": chaos_harness.stream_length(),
        }
        final_state["services"] = chaos_harness.service_status()
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
        assert run["expected_items_count"] == 1
        assert run["processed_items_count"] == 1
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
        assert run["duplicate_workflow_run_item_groups"] == []
        assert run["duplicate_success_task_log_groups"] == []
        assert run["duplicate_reference_tag_groups"] == []
        assert run["duplicate_workflow_document_log_groups"] == []
        assert all(item["status"] == "succeeded" for item in run["workflow_run_items"])
        assert all(job["status"] == "succeeded" for job in run["external_jobs"])
        all_jobs.extend(run["external_jobs"])

    assert len({job["job_id"] for job in all_jobs}) == len(all_jobs)
    assert len({job["idempotency_key"] for job in all_jobs}) == len(all_jobs)

    api_jobs = [harness.api_job(job["job_id"]) for job in all_jobs]
    assert all(job["status"] == "success" for job in api_jobs)
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
