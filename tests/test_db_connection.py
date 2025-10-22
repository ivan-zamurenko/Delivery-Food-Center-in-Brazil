
import pytest
from analysis.db_connection import get_engine

pytestmark = pytest.mark.skip(reason="Database connection tests are skipped in CI.")


def test_get_engine_returns_engine():
    engine = get_engine()
    assert engine is not None
