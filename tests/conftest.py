from pytest import fixture
from sfconn import getconn, getsess


@fixture(scope="session")
def sess():
    with getsess() as session:
        yield session


@fixture(scope="session")
def cnx():
    with getconn() as conn:
        yield conn
