import analysis.queries as queries
from analysis.db_connection import get_engine


def test_orders_missing_values():
    """Test for missing values in the orders DataFrame."""

    # ? Fetch orders data
    engine = get_engine()
    df = queries.fetch_orders(engine)
    missing = df.isnull().sum()
    print(missing)

    # ? Assertions for missing values
    assert missing["order_id"] == 0
    assert missing["store_id"] == 0
    assert missing["channel_id"] == 0
    assert missing["order_status"] == 0


def test_payments_missing_values():
    """Test for missing values in the payments DataFrame."""

    # ? Fetch payments data
    engine = get_engine()
    df = queries.fetch_payments(engine)
    missing = df.isnull().sum()
    print(missing)

    # ? Assertions for missing values
    assert missing["payment_id"] == 0
    assert missing["payment_order_id"] == 0
    assert missing["payment_method"] == 0
    assert missing["payment_amount"] == 0


def test_deliveries_missing_values():
    """Test for missing values in the deliveries DataFrame."""

    # ? Fetch deliveries data
    engine = get_engine()
    df = queries.fetch_deliveries(engine)
    missing = df.isnull().sum()
    print(missing)

    # ? Assertions for missing values
    assert missing["delivery_id"] == 0
    assert missing["delivery_order_id"] == 0
    assert missing["driver_id"] == 0


def test_drivers_missing_values():
    """Test for missing values in the drivers DataFrame."""

    # ? Fetch drivers data
    engine = get_engine()
    df = queries.fetch_drivers(engine)
    missing = df.isnull().sum()
    print(missing)

    # ? Assertions for missing values
    assert missing["driver_id"] == 0


def test_stores_missing_values():
    """Test for missing values in the stores DataFrame."""

    # ? Fetch stores data
    engine = get_engine()
    df = queries.fetch_stores(engine)
    missing = df.isnull().sum()
    print(missing)

    # ? Assertions for missing values
    assert missing["store_id"] == 0
    assert missing["hub_id"] == 0


def test_hubs_missing_values():
    """Test for missing values in the hubs DataFrame."""

    # ? Fetch hubs data
    engine = get_engine()
    df = queries.fetch_hubs(engine)
    missing = df.isnull().sum()
    print(missing)

    # ? Assertions for missing values
    assert missing["hub_id"] == 0
    assert missing["hub_name"] == 0


def test_channels_missing_values():
    """Test for missing values in the channels DataFrame."""

    # ? Fetch channels data
    engine = get_engine()
    df = queries.fetch_channels(engine)
    missing = df.isnull().sum()
    print(missing)

    # ? Assertions for missing values
    assert missing["channel_id"] == 0
    assert missing["channel_name"] == 0
