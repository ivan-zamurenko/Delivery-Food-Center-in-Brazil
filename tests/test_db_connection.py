import pytest
from analysis.db_connection import get_engine


def test_get_engine_returns_engine():
    engine = get_engine()
    assert engine is not None
