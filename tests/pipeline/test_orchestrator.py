"""Tests for pipeline/orchestrator.py and pipeline/step_context.py."""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from ztract.pipeline.orchestrator import PipelineOrchestrator
from ztract.pipeline.step_context import StepContext


# ---------------------------------------------------------------------------
# StepContext tests
# ---------------------------------------------------------------------------


class TestStepContext:
    def test_expose_and_resolve_ref(self, tmp_path):
        ctx = StepContext()
        output_path = tmp_path / "out.csv"
        ctx.expose("customers", "csv", output_path)
        resolved = ctx.resolve_ref("$ref:customers.csv")
        assert resolved == output_path

    def test_resolve_ref_without_type_suffix(self, tmp_path):
        ctx = StepContext()
        output_path = tmp_path / "out.csv"
        ctx.expose("orders", "csv", output_path)
        resolved = ctx.resolve_ref("$ref:orders")
        assert resolved == output_path

    def test_resolve_ref_unknown_raises_key_error(self):
        ctx = StepContext()
        with pytest.raises(KeyError, match="unknown_output"):
            ctx.resolve_ref("$ref:unknown_output.csv")

    def test_resolve_ref_invalid_prefix_raises_value_error(self):
        ctx = StepContext()
        with pytest.raises(ValueError, match="\\$ref:"):
            ctx.resolve_ref("noprefix:foo")

    def test_timing_records_elapsed(self):
        ctx = StepContext()
        ctx.start_step("step1")
        time.sleep(0.01)
        ctx.end_step("step1")
        elapsed = ctx.get_elapsed("step1")
        assert elapsed >= 0.005  # at least some time passed

    def test_get_elapsed_before_end_raises(self):
        ctx = StepContext()
        ctx.start_step("step2")
        with pytest.raises(KeyError, match="step2"):
            ctx.get_elapsed("step2")

    def test_end_step_without_start_raises(self):
        ctx = StepContext()
        with pytest.raises(KeyError):
            ctx.end_step("never_started")

    def test_reject_count_aggregation(self):
        ctx = StepContext()
        ctx.add_rejects("step1", 3)
        ctx.add_rejects("step2", 7)
        ctx.add_rejects("step1", 2)  # additional rejects for step1
        assert ctx.total_rejects == 12

    def test_total_rejects_zero_initially(self):
        ctx = StepContext()
        assert ctx.total_rejects == 0

    def test_get_connector_caches(self):
        ctx = StepContext()
        factory = MagicMock(side_effect=lambda uri: MagicMock())
        c1 = ctx.get_connector("ftp://host", factory)
        c2 = ctx.get_connector("ftp://host", factory)
        assert c1 is c2
        factory.assert_called_once()

    def test_get_connector_different_uris(self):
        ctx = StepContext()
        factory = MagicMock(side_effect=lambda uri: MagicMock())
        c1 = ctx.get_connector("ftp://a", factory)
        c2 = ctx.get_connector("ftp://b", factory)
        assert c1 is not c2
        assert factory.call_count == 2

    def test_close_calls_connector_close(self):
        ctx = StepContext()
        mock_conn = MagicMock()
        ctx._connections["x"] = mock_conn
        ctx.close()
        mock_conn.close.assert_called_once()

    def test_close_deletes_temp_files(self, tmp_path):
        ctx = StepContext()
        tmp_file = tmp_path / "temp.bin"
        tmp_file.write_bytes(b"data")
        ctx.register_temp(tmp_file)
        ctx.close()
        assert not tmp_file.exists()

    def test_close_tolerates_missing_temp(self, tmp_path):
        ctx = StepContext()
        ctx.register_temp(tmp_path / "nonexistent.bin")
        ctx.close()  # should not raise


# ---------------------------------------------------------------------------
# PipelineOrchestrator tests
# ---------------------------------------------------------------------------


def _make_config(steps=None, continue_on_error=False, job_name="test-job") -> dict:
    cfg: dict = {"job": {"name": job_name, "steps": steps or []}}
    if continue_on_error:
        cfg["job"]["continue_on_error"] = True
    return cfg


class TestPipelineOrchestrator:
    def test_empty_steps_returns_success(self):
        orc = PipelineOrchestrator(_make_config(steps=[]))
        assert orc.run() == 0

    def test_dry_run_returns_zero_without_executing(self):
        called = []

        config = _make_config(
            steps=[{"name": "s1", "action": "convert", "copybook": "c.cpy"}]
        )
        orc = PipelineOrchestrator(config, dry_run=True)
        with patch.object(orc, "_execute_step", side_effect=called.append):
            exit_code = orc.run()
        assert exit_code == 0
        assert called == []  # _execute_step never called in dry_run

    def test_single_convert_step_runs(self):
        config = _make_config(
            steps=[{"name": "extract", "action": "convert", "copybook": "c.cpy"}]
        )
        orc = PipelineOrchestrator(config)
        exit_code = orc.run()
        assert exit_code == 0

    def test_step_filter_skips_non_matching(self):
        executed = []
        config = _make_config(
            steps=[
                {"name": "step-a", "action": "convert", "copybook": "c.cpy"},
                {"name": "step-b", "action": "convert", "copybook": "c.cpy"},
            ]
        )
        orc = PipelineOrchestrator(config, step_filter="step-b")
        original = orc._execute_step

        def _spy(step):
            executed.append(step["name"])
            return original(step)

        orc._execute_step = _spy
        exit_code = orc.run()
        assert exit_code == 0
        assert executed == ["step-b"]

    def test_failed_step_returns_exit_code_1(self):
        config = _make_config(
            steps=[{"name": "bad", "action": "convert", "copybook": "c.cpy"}]
        )
        orc = PipelineOrchestrator(config)
        orc._execute_step = MagicMock(side_effect=RuntimeError("boom"))
        assert orc.run() == 1

    def test_continue_on_error_runs_all_steps(self):
        executed = []
        config = _make_config(
            steps=[
                {"name": "s1", "action": "convert", "copybook": "c.cpy"},
                {"name": "s2", "action": "convert", "copybook": "c.cpy"},
            ],
            continue_on_error=True,
        )
        orc = PipelineOrchestrator(config)

        def _side(step):
            executed.append(step["name"])
            if step["name"] == "s1":
                raise RuntimeError("first step fails")

        orc._execute_step = _side
        exit_code = orc.run()
        assert exit_code == 1
        assert executed == ["s1", "s2"]  # both steps ran

    def test_rejects_return_exit_code_2(self):
        config = _make_config(
            steps=[{"name": "s", "action": "convert", "copybook": "c.cpy"}]
        )
        orc = PipelineOrchestrator(config)

        def _step_with_rejects(step):
            orc._ctx.add_rejects(step["name"], 5)

        orc._execute_step = _step_with_rejects
        assert orc.run() == 2

    def test_diff_and_generate_stubs_run(self):
        config = _make_config(
            steps=[
                {"name": "d", "action": "diff", "copybook": "c.cpy"},
                {"name": "g", "action": "generate", "copybook": "c.cpy"},
            ]
        )
        orc = PipelineOrchestrator(config)
        assert orc.run() == 0

    def test_unknown_action_raises_then_returns_1(self):
        config = _make_config(
            steps=[{"name": "x", "action": "unknown_action", "copybook": "c.cpy"}]
        )
        orc = PipelineOrchestrator(config)
        assert orc.run() == 1

    def test_dry_run_with_step_filter_still_returns_zero(self):
        config = _make_config(
            steps=[{"name": "s", "action": "convert", "copybook": "c.cpy"}]
        )
        orc = PipelineOrchestrator(config, step_filter="s", dry_run=True)
        assert orc.run() == 0
