from datetime import date, datetime, time
from decimal import Decimal, getcontext
from typing import NamedTuple

import snowflake.snowpark.types as T
from snowflake.snowpark import DataFrame, Session

getcontext().prec = 6


class Row(NamedTuple):
    c1: int
    c2: str
    c3: Decimal
    c4: float
    c5: date
    c6: time
    c7: datetime


schema = T.StructType(
    [
        T.StructField("C1", T.LongType(), nullable=False),
        T.StructField("C2", T.StringType(10), nullable=False),
        T.StructField("C3", T.DecimalType(6, 4), nullable=False),
        T.StructField("C4", T.DoubleType(), nullable=False),
        T.StructField("C5", T.DateType(), nullable=False),
        T.StructField("C6", T.TimeType(), nullable=False),
        T.StructField("C7", T.TimestampType(timezone=T.TimestampTimeZone.NTZ), nullable=False),
    ]
)

rows = [
    Row(1, "one", Decimal("1.1"), 1.1, date(2000, 1, 1), time(11, 1, 1), datetime(2000, 1, 1, 11, 1, 1)),
    Row(2, "two", Decimal("2.22"), 2.22, date(2000, 2, 2), time(12, 2, 22), datetime(2000, 1, 1, 12, 2, 22)),
    Row(3, "three", Decimal("3.333"), 3.333, date(2000, 3, 3), time(13, 3, 33), datetime(2000, 1, 3, 13, 3, 44)),
]


def main(session: Session) -> DataFrame:
    df = session.create_dataframe(rows, schema=schema)
    return df


def sql(session: Session) -> str:
    return main(session).queries["queries"][0]


def return_int(session: Session) -> int:
    return 0


def extra_arg(session: Session, _: int = 0) -> str:
    return ""


def no_sess_arg(account: str) -> str:
    return ""


def no_types(sess):
    return ""


not_a_function = "this is not a function"
