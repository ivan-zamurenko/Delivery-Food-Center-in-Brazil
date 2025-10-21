from analysis.db_connection import get_engine
from analysis.queries import fetch_orders


def test_orders_missing_values():
    """Test for missing values in the orders DataFrame."""

    # ? Fetch orders data
    engine = get_engine()
    df = fetch_orders(engine)
    missing = df.isnull().sum()
    print(missing)

    # ? Assertions for missing values
    assert missing["order_id"] == 0
    assert missing["store_id"] == 0
    assert missing["channel_id"] == 0
    assert missing["order_status"] == 0


if __name__ == "__main__":
    test_orders_missing_values()
