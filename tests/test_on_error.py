"test error handling"

from pathlib import Path

from pytest import CaptureFixture
from snowflake.connector import SnowflakeConnection

from sfrun.batch import ErrorAction, run


def test_error_stop(tmp_path: Path, cnx: SnowflakeConnection, capsys: CaptureFixture[str]) -> None:
    sqlf = tmp_path / "test.sql"
    sqlf.write_text("select 'one'; selec current_u(); select 'two';")
    run(cnx, [sqlf], on_error=ErrorAction.STOP)

    actual = capsys.readouterr().out
    assert "one" in actual
    assert "two" not in actual


def test_error_continue(tmp_path: Path, cnx: SnowflakeConnection, capsys: CaptureFixture[str]) -> None:
    sqlf = tmp_path / "test.sql"
    sqlf.write_text("select 'one'; selec current_u(); select 'two';")

    run(cnx, [sqlf], on_error=ErrorAction.CONTINUE)

    actual = capsys.readouterr().out
    assert "one" in actual
    assert "two" in actual


def test_error_skip_file(tmp_path: Path, cnx: SnowflakeConnection, capsys: CaptureFixture[str]) -> None:
    sqlf = tmp_path / "test.sql"
    sqlf.write_text("select 'one'; selec current_u(); select 'two';")

    sqlf2 = tmp_path / "test2.sql"
    sqlf2.write_text("select 'three';")

    run(cnx, [sqlf, sqlf2], on_error=ErrorAction.SKIP_FILE)

    actual = capsys.readouterr().out
    assert "one" in actual
    assert "two" not in actual
    assert "three" in actual
