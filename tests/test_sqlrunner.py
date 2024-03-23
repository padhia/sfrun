"test error handling"

from pathlib import Path

from pytest import CaptureFixture
from snowflake.connector import SnowflakeConnection

from sfrun import SqlScript


def test_skip_on_error(tmp_path: Path, cnx: SnowflakeConnection, capsys: CaptureFixture[str]) -> None:
    sqlf = tmp_path / "test.sql"
    sqlf.write_text("select 'one'; selec current_u(); select 'two';")

    runner = SqlScript(sqlf, stop_on_error=False)
    assert not runner.run(cnx)

    output = capsys.readouterr().out
    assert "one" in output
    assert "two" in output


def test_dont_skip_on_error(tmp_path: Path, cnx: SnowflakeConnection, capsys: CaptureFixture[str]) -> None:
    sqlf = tmp_path / "test.sql"
    sqlf.write_text("select 'one'; selec current_u(); select 'two';")

    runner = SqlScript(sqlf, stop_on_error=True)
    assert not runner.run(cnx)

    output = capsys.readouterr().out
    assert "one" in output
    assert "two" not in output
