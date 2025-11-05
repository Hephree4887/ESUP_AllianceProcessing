import sys
import types
from pathlib import Path

import pytest


class FakeCursor:
    def execute(self, *args, **kwargs):
        pass


class FakeConnection:
    def __init__(self, *args, **kwargs):
        self._cursor = FakeCursor()

    def cursor(self):
        return self._cursor


# Stub mysql.connector before importing JSONtoMySQL
fake_mysql_module = types.ModuleType("mysql")
fake_connector_module = types.ModuleType("mysql.connector")


def _fake_connect(**kwargs):
    return FakeConnection()


fake_connector_module.connect = _fake_connect
fake_mysql_module.connector = fake_connector_module

sys.modules.setdefault("mysql", fake_mysql_module)
sys.modules.setdefault("mysql.connector", fake_connector_module)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import JSONtoMySQL


@pytest.fixture
def importer():
    return JSONtoMySQL.JSONtoMySQL(
        host="localhost",
        user="user",
        password="password",
        database="test_db",
    )


@pytest.mark.parametrize(
    "values,expected",
    [
        pytest.param([True, False, None], "BOOLEAN", id="booleans"),
        pytest.param([2147483648, -2147483649, None], "BIGINT", id="large-integers"),
        pytest.param([3.14, 2.718, None], "DOUBLE", id="floats"),
        pytest.param(["a" * 256, "b" * 300], "TEXT", id="long-strings"),
        pytest.param([{"nested": 1}, [1, 2, 3], None], "JSON", id="nested-structures"),
        pytest.param([None, None, None], "TEXT", id="all-none"),
    ],
)
def test_determine_column_type(values, expected, importer):
    assert importer._determine_column_type(values) == expected
