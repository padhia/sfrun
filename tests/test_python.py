from pathlib import Path

import pytest

from sfrun.df import load_py

test_file = Path(__file__).parent / "sample.py"


def test_main():
    assert load_py(test_file).__name__ == "main"


def test_sql():
    assert load_py(test_file, "sql").__name__ == "sql"


def test_wrong_rettype():
    with pytest.raises(TypeError):
        load_py(test_file, "return_int")


def test_wrong_argtype():
    with pytest.raises(TypeError):
        load_py(test_file, "no_sess_arg")


def test_wrong_argcount():
    with pytest.raises(TypeError):
        load_py(test_file, "extra_arg")


def test_no_types():
    assert callable(load_py(test_file, "no_types"))


def test_non_existent():
    with pytest.raises(ValueError):
        load_py(test_file, "non_existent")


def test_non_function():
    with pytest.raises(TypeError):
        load_py(test_file, "not_a_function")


def test_load_error():
    with pytest.raises(TypeError):
        load_py(Path("/dev/null"))
