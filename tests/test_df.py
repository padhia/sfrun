from itertools import zip_longest
from textwrap import dedent

from pytest import CaptureFixture
from snowflake.snowpark import Session

from sfrun import Format, run_df

from .sample import main


def compare_lines(expected: str, actual: str):
    for e, a in zip_longest(expected.splitlines(), actual.splitlines()):
        assert e.rstrip() == a.rstrip()


def test_fmt(sess: Session, capsys: CaptureFixture[str]):
    run_df(main(sess), format=Format.FMT)

    compare_lines(
        capsys.readouterr().out,
        dedent(
            """\
            ┌────┬───────┬──────┬──────┬────────────┬──────────┬─────────────────────┐
            │ C1 │ C2    │   C3 │   C4 │     C5     │    C6    │          C7         │
            ├────┼───────┼──────┼──────┼────────────┼──────────┼─────────────────────┤
            │  1 │ one   │ 1.10 │ 1.10 │ 2000-01-01 │ 11:01:01 │ 2000-01-01 11:01:01 │
            │  2 │ two   │ 2.22 │ 2.22 │ 2000-02-02 │ 12:02:22 │ 2000-01-01 12:02:22 │
            │  3 │ three │ 3.33 │ 3.33 │ 2000-03-03 │ 13:03:33 │ 2000-01-03 13:03:44 │
            └────┴───────┴──────┴──────┴────────────┴──────────┴─────────────────────┘
            """
        ),
    )


def test_csv(sess: Session, capsys: CaptureFixture[str]):
    run_df(main(sess), format=Format.CSV)

    compare_lines(
        capsys.readouterr().out,
        dedent(
            """\
            C1,C2,C3,C4,C5,C6,C7
            1,one,1.1000,1.1,2000-01-01,11:01:01,2000-01-01 11:01:01
            2,two,2.2200,2.22,2000-02-02,12:02:22,2000-01-01 12:02:22
            3,three,3.3330,3.333,2000-03-03,13:03:33,2000-01-03 13:03:44
            """
        ),
    )


def test_md(sess: Session, capsys: CaptureFixture[str]):
    run_df(main(sess), format=Format.MD)

    compare_lines(
        capsys.readouterr().out,
        dedent(
            """\
            | C1 | C2    |   C3 |   C4 |     C5     |    C6    |          C7         |
            | -: | :---- | ---: | ---: | :--------: | :------: | :-----------------: |
            |  1 | one   | 1.10 | 1.10 | 2000-01-01 | 11:01:01 | 2000-01-01 11:01:01 |
            |  2 | two   | 2.22 | 2.22 | 2000-02-02 | 12:02:22 | 2000-01-01 12:02:22 |
            |  3 | three | 3.33 | 3.33 | 2000-03-03 | 13:03:33 | 2000-01-03 13:03:44 |
            """
        ),
    )


def test_raw(sess: Session, capsys: CaptureFixture[str]):
    run_df(main(sess), format=Format.RAW)

    compare_lines(
        capsys.readouterr().out,
        dedent(
            """\
            1\tone\t1.1000\t1.1\t2000-01-01\t11:01:01\t2000-01-01 11:01:01
            2\ttwo\t2.2200\t2.22\t2000-02-02\t12:02:22\t2000-01-01 12:02:22
            3\tthree\t3.3330\t3.333\t2000-03-03\t13:03:33\t2000-01-03 13:03:44
            """
        ),
    )
