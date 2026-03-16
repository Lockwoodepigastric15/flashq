"""Comprehensive tests for CLI interface covering all commands."""

from __future__ import annotations

import argparse
import logging
from unittest.mock import MagicMock, patch

import pytest

from flashq.cli import (
    _import_app,
    _setup_logging,
    build_parser,
    cmd_info,
    cmd_purge,
    cmd_version,
    main,
)


class TestImportApp:
    def test_no_colon_no_dot_exits(self):
        with pytest.raises(SystemExit):
            _import_app("badpath")

    def test_nonexistent_module_exits(self):
        with pytest.raises(SystemExit):
            _import_app("nonexistent_xyz_module:app")

    def test_missing_attribute_exits(self):
        with pytest.raises(SystemExit):
            _import_app("os:nonexistent_attr_xyz")

    def test_valid_import(self):
        result = _import_app("os:path")
        import os
        assert result is os.path

    def test_dot_notation_import(self):
        result = _import_app("os.path:join")
        import os.path
        assert result is os.path.join


class TestSetupLogging:
    def test_debug_level(self):
        _setup_logging("debug")
        logging.getLogger()
        # We just verify no exception. basicConfig may not override if already configured.

    def test_warning_level(self):
        _setup_logging("warning")
        # No crash is the test

    def test_unknown_defaults_gracefully(self):
        _setup_logging("nonexistent_level_xyz")
        # No crash is the test


class TestBuildParser:
    def test_parser_creation(self):
        parser = build_parser()
        assert parser is not None

    def test_worker_args(self):
        parser = build_parser()
        args = parser.parse_args(["worker", "myapp:app"])
        assert args.command == "worker"
        assert args.app == "myapp:app"
        assert args.concurrency == 4
        assert args.queues is None
        assert args.poll_interval == 1.0
        assert args.log_level == "info"

    def test_worker_all_options(self):
        parser = build_parser()
        args = parser.parse_args([
            "worker", "myapp:app",
            "-c", "16",
            "-q", "high,low",
            "--poll-interval", "0.25",
            "-n", "my-worker",
            "-l", "debug",
        ])
        assert args.concurrency == 16
        assert args.queues == "high,low"
        assert args.poll_interval == 0.25
        assert args.name == "my-worker"
        assert args.log_level == "debug"

    def test_info_args(self):
        parser = build_parser()
        args = parser.parse_args(["info", "myapp:app"])
        assert args.command == "info"

    def test_purge_force(self):
        parser = build_parser()
        args = parser.parse_args(["purge", "myapp:app", "-f"])
        assert args.command == "purge"
        assert args.force is True

    def test_purge_no_force(self):
        parser = build_parser()
        args = parser.parse_args(["purge", "myapp:app"])
        assert args.force is False

    def test_version_flag(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--version"])
        assert exc_info.value.code == 0

    def test_no_command(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.command is None


class TestCmdWorker:
    def test_worker_parser_accepts_args(self):
        """Verify worker command parses all options correctly."""
        parser = build_parser()
        args = parser.parse_args([
            "worker", "myapp:app",
            "-c", "8", "-q", "default,emails",
            "--poll-interval", "0.5", "-n", "test-worker",
        ])
        assert args.app == "myapp:app"
        assert args.concurrency == 8
        assert args.queues == "default,emails"
        assert args.poll_interval == 0.5
        assert args.name == "test-worker"

    def test_worker_default_values(self):
        parser = build_parser()
        args = parser.parse_args(["worker", "myapp:app"])
        assert args.queues is None
        assert args.concurrency == 4
        assert args.poll_interval == 1.0
        assert args.name is None


class TestCmdInfo:
    @patch("flashq.cli._import_app")
    def test_info_output(self, mock_import, capsys):
        mock_app = MagicMock()
        mock_app.name = "test-app"
        mock_app.backend.__class__.__name__ = "SQLiteBackend"
        mock_app.registry = {}
        mock_app.backend.queue_size.return_value = 5
        mock_app.backend.schedule_size.return_value = 2
        mock_import.return_value = mock_app

        args = argparse.Namespace(app="myapp:app", queue=None, log_level="info")
        cmd_info(args)

        out = capsys.readouterr().out
        assert "test-app" in out
        assert "5" in out
        assert "2" in out

    @patch("flashq.cli._import_app")
    def test_info_with_tasks(self, mock_import, capsys):
        mock_task = MagicMock()
        mock_task.queue = "default"
        mock_task.max_retries = 3

        mock_app = MagicMock()
        mock_app.name = "test-app"
        mock_app.registry = {"my_task": mock_task}
        mock_app.backend.queue_size.return_value = 0
        mock_app.backend.schedule_size.return_value = 0
        mock_import.return_value = mock_app

        args = argparse.Namespace(app="myapp:app", queue="high", log_level="info")
        cmd_info(args)

        out = capsys.readouterr().out
        assert "my_task" in out
        assert "high" in out


class TestCmdPurge:
    @patch("flashq.cli._import_app")
    def test_purge_force(self, mock_import, capsys):
        mock_app = MagicMock()
        mock_app.backend.flush_queue.return_value = 10
        mock_import.return_value = mock_app

        args = argparse.Namespace(
            app="myapp:app", queue=None, force=True, log_level="info",
        )
        cmd_purge(args)

        mock_app.backend.flush_queue.assert_called_once_with("default")
        out = capsys.readouterr().out
        assert "10" in out

    @patch("builtins.input", return_value="y")
    @patch("flashq.cli._import_app")
    def test_purge_confirm_yes(self, mock_import, mock_input, capsys):
        mock_app = MagicMock()
        mock_app.backend.queue_size.return_value = 3
        mock_app.backend.flush_queue.return_value = 3
        mock_import.return_value = mock_app

        args = argparse.Namespace(
            app="myapp:app", queue=None, force=False, log_level="info",
        )
        cmd_purge(args)

        mock_app.backend.flush_queue.assert_called_once()
        out = capsys.readouterr().out
        assert "3" in out

    @patch("builtins.input", return_value="n")
    @patch("flashq.cli._import_app")
    def test_purge_confirm_no(self, mock_import, mock_input, capsys):
        mock_app = MagicMock()
        mock_app.backend.queue_size.return_value = 5
        mock_import.return_value = mock_app

        args = argparse.Namespace(
            app="myapp:app", queue=None, force=False, log_level="info",
        )
        cmd_purge(args)

        mock_app.backend.flush_queue.assert_not_called()
        out = capsys.readouterr().out
        assert "Aborted" in out


class TestCmdVersion:
    def test_version_output(self, capsys):
        from flashq._version import __version__

        args = argparse.Namespace()
        cmd_version(args)

        out = capsys.readouterr().out
        assert __version__ in out


class TestMain:
    @patch("flashq.cli.build_parser")
    def test_no_command_exits(self, mock_build):
        mock_parser = MagicMock()
        mock_parser.parse_args.return_value = argparse.Namespace(command=None)
        mock_build.return_value = mock_parser

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 0

    @patch("flashq.cli.build_parser")
    @patch("flashq.cli._setup_logging")
    def test_with_command_calls_func(self, mock_logging, mock_build):
        mock_func = MagicMock()
        mock_args = argparse.Namespace(
            command="version", func=mock_func, log_level="info",
        )
        mock_parser = MagicMock()
        mock_parser.parse_args.return_value = mock_args
        mock_build.return_value = mock_parser

        main()

        mock_logging.assert_called_once_with("info")
        mock_func.assert_called_once_with(mock_args)
