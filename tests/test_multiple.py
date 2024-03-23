"test multiple sqls"

from pathlib import Path
from textwrap import dedent

from pytest import CaptureFixture
from snowflake.connector import SnowflakeConnection

from sfrun.batch import run


def test_mix(tmp_path: Path, cnx: SnowflakeConnection, capsys: CaptureFixture[str]) -> None:
    sqlf = tmp_path / "test.sql"
    sqlf.write_text(
        dedent(
            """\
            create temporary table t1(
                c1 int,
                c2 varchar(5)
            );

            describe table t1;

            insert into t1 values (1, 'hello'), (2, 'world');

            update t1
                set c1 = 0
            ;

            select * from t1;
            """
        )
    )
    run(cnx, [sqlf])

    expected = """\
create temporary table t1(
    c1 int,
    c2 varchar(5)
);
+--------------------------------+
| status                         |
+--------------------------------+
| Table T1 successfully created. |
+--------------------------------+

describe table t1;
+------+--------------+--------+-------+---------+-------------+------------+-------+------------+---------+-------------+----------------+
| name | type         | kind   | null? | default | primary key | unique key | check | expression | comment | policy name | privacy domain |
+------+--------------+--------+-------+---------+-------------+------------+-------+------------+---------+-------------+----------------+
| C1   | NUMBER(38,0) | COLUMN | Y     |         | N           | N          |       |            |         |             |                |
| C2   | VARCHAR(5)   | COLUMN | Y     |         | N           | N          |       |            |         |             |                |
+------+--------------+--------+-------+---------+-------------+------------+-------+------------+---------+-------------+----------------+

insert into t1 values (1, 'hello'), (2, 'world');
+-------------------------+
| number of rows inserted |
+-------------------------+
|                       2 |
+-------------------------+

update t1
    set c1 = 0
;
+------------------------+-------------------------------------+
| number of rows updated | number of multi-joined rows updated |
+------------------------+-------------------------------------+
|                      2 |                                   0 |
+------------------------+-------------------------------------+

select * from t1;
+----+-------+
| C1 | C2    |
+----+-------+
|  0 | hello |
|  0 | world |
+----+-------+
"""
    assert expected == capsys.readouterr().out
