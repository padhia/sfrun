"test multiple sqls"

from pathlib import Path

from pytest import CaptureFixture
from snowflake.connector import SnowflakeConnection

from sfrun.batch import run


def test_two_rows(tmp_path: Path, cnx: SnowflakeConnection, capsys: CaptureFixture[str]) -> None:
    sqlf = tmp_path / "test.sql"
    sqlf.write_text("select $1 from values (1), (2), (3) order by 1;")
    run(cnx, [sqlf], limit=2)
    actual = capsys.readouterr().out
    expected = """\
select $1 from values (1), (2), (3) order by 1;
+----+
| $1 |
+----+
|  1 |
|  2 |
+----+
"""
    assert actual == expected
