"test error logging"

from pathlib import Path

from pytest import LogCaptureFixture
from snowflake.connector import SnowflakeConnection

from sfrun.batch import ErrorAction, run


def test_error_log(cnx: SnowflakeConnection, tmp_path: Path, caplog: LogCaptureFixture) -> None:
    sqlf = tmp_path / "test_error.sql"
    sqlf.write_text("selec current_u()")
    run(cnx, [sqlf], on_error=ErrorAction.STOP)
    assert caplog.records[0].levelname == "ERROR"
    assert "001003 (42000): SQL compilation error" in caplog.records[0].message
