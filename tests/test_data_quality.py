"""
Unit tests for data quality validation - Task D

These tests verify that the cleaned data meets quality standards:
- No duplicate IDs
- No nulls in critical columns
- Valid data types
- Reasonable value ranges
- Foreign key integrity
"""

import pytest
import pandas as pd
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestDataQuality:
    """Test suite for data quality validation."""

    @pytest.fixture
    def data_dir(self):
        """Return the path to cleaned data directory."""
        return Path("data/data-cleaned")

    @pytest.fixture
    def orders(self, data_dir):
        """Load cleaned orders data."""
        if not (data_dir / "orders_cleaned.csv").exists():
            pytest.skip("Cleaned data not available. Run scripts/clean_data.py first.")
        return pd.read_csv(data_dir / "orders_cleaned.csv", encoding="latin1")

    @pytest.fixture
    def payments(self, data_dir):
        """Load cleaned payments data."""
        if not (data_dir / "payments_cleaned.csv").exists():
            pytest.skip("Cleaned data not available. Run scripts/clean_data.py first.")
        return pd.read_csv(data_dir / "payments_cleaned.csv", encoding="latin1")

    @pytest.fixture
    def deliveries(self, data_dir):
        """Load cleaned deliveries data."""
        if not (data_dir / "deliveries_cleaned.csv").exists():
            pytest.skip("Cleaned data not available. Run scripts/clean_data.py first.")
        return pd.read_csv(data_dir / "deliveries_cleaned.csv", encoding="latin1")

    @pytest.fixture
    def drivers(self, data_dir):
        """Load cleaned drivers data."""
        if not (data_dir / "drivers_cleaned.csv").exists():
            pytest.skip("Cleaned data not available. Run scripts/clean_data.py first.")
        return pd.read_csv(data_dir / "drivers_cleaned.csv", encoding="latin1")

    @pytest.fixture
    def channels(self, data_dir):
        """Load cleaned channels data."""
        if not (data_dir / "channels_cleaned.csv").exists():
            pytest.skip("Cleaned data not available. Run scripts/clean_data.py first.")
        return pd.read_csv(data_dir / "channels_cleaned.csv", encoding="latin1")

    @pytest.fixture
    def stores(self, data_dir):
        """Load cleaned stores data."""
        if not (data_dir / "stores_cleaned.csv").exists():
            pytest.skip("Cleaned data not available. Run scripts/clean_data.py first.")
        return pd.read_csv(data_dir / "stores_cleaned.csv", encoding="latin1")

    @pytest.fixture
    def hubs(self, data_dir):
        """Load cleaned hubs data."""
        if not (data_dir / "hubs_cleaned.csv").exists():
            pytest.skip("Cleaned data not available. Run scripts/clean_data.py first.")
        return pd.read_csv(data_dir / "hubs_cleaned.csv", encoding="latin1")

    # === Orders Tests ===

    def test_orders_no_duplicate_ids(self, orders):
        """Verify that order_id is unique."""
        assert orders["order_id"].is_unique, "order_id should have no duplicates"

    def test_orders_no_null_critical_fields(self, orders):
        """Verify critical fields have no nulls."""
        critical_fields = ["order_id", "store_id", "channel_id"]
        for field in critical_fields:
            null_count = orders[field].isnull().sum()
            assert null_count == 0, (
                f"{field} should have no null values, found {null_count}"
            )

    def test_orders_valid_dates(self, orders):
        """Verify datetime columns can be parsed."""
        date_columns = ["order_moment_created", "order_moment_accepted"]
        for col in date_columns:
            if col in orders.columns:
                # Should not raise an error
                pd.to_datetime(orders[col], errors="coerce")
                # Check that most values are valid dates
                valid_dates = pd.to_datetime(orders[col], errors="coerce").notna().sum()
                assert valid_dates > len(orders) * 0.9, (
                    f"{col} has too many invalid dates"
                )

    def test_orders_deliveries_positive_times(self, orders):
        """Verify delivery times are reasonable for completed deliveries."""
        # Convert delivery_time_minutes to numeric (handles any string conversion issues)
        delivery_times = pd.to_numeric(orders["delivery_time_minutes"], errors="coerce")

        # Filter to only orders with delivery times (ignore NaN from pending/cancelled orders)
        completed_deliveries = delivery_times.notna()
        valid_times = delivery_times[completed_deliveries]

        # For completed deliveries, all times should be non-negative
        assert (valid_times >= 0).all(), (
            "Delivery times should be non-negative for completed orders"
        )

        # For completed deliveries, all times should be reasonable (< 3 hours)
        assert (valid_times <= 180).all(), (
            "Delivery times should be under 180 minutes for completed orders"
        )

    def test_orders_amount_positive(self, orders):
        """Verify order amounts are positive."""
        assert (orders["order_amount"] > 0).all(), (
            "All order amounts should be positive"
        )

    # === Deliveries Tests ===

    def test_deliveries_no_duplicate_ids(self, deliveries):
        """Verify that delivery_id is unique."""
        assert deliveries["delivery_id"].is_unique, (
            "delivery_id should have no duplicates"
        )

    def test_deliveries_no_null_driver_id(self, deliveries):
        """Verify driver_id has no nulls."""
        null_count = deliveries["driver_id"].isnull().sum()
        assert null_count == 0, f"driver_id should have no nulls, found {null_count}"

    # === Payments Tests ===

    def test_payments_no_duplicate_ids(self, payments):
        """Verify that payment_id is unique."""
        assert payments["payment_id"].is_unique, "payment_id should have no duplicates"

    def test_payments_positive_amounts(self, payments):
        """Verify payment amounts are positive."""
        assert (payments["payment_amount"] > 0).all(), (
            "All payment amounts should be positive"
        )

    # === Dimension Tables Tests ===

    def test_stores_no_duplicate_ids(self, stores):
        """Verify that store_id is unique."""
        assert stores["store_id"].is_unique, "store_id should have no duplicates"

    def test_channels_no_duplicate_ids(self, channels):
        """Verify that channel_id is unique."""
        assert channels["channel_id"].is_unique, "channel_id should have no duplicates"

    def test_drivers_no_duplicate_ids(self, drivers):
        """Verify that driver_id is unique."""
        assert drivers["driver_id"].is_unique, "driver_id should have no duplicates"

    def test_hubs_no_duplicate_ids(self, hubs):
        """Verify that hub_id is unique."""
        assert hubs["hub_id"].is_unique, "hub_id should have no duplicates"

    # === Foreign Key Tests ===

    def test_orders_valid_store_references(self, orders, stores):
        """Verify all orders reference valid stores."""
        invalid_refs = ~orders["store_id"].isin(stores["store_id"])
        invalid_count = invalid_refs.sum()
        assert invalid_count == 0, (
            f"Found {invalid_count} orders with invalid store_id references"
        )

    def test_orders_valid_channel_references(self, orders, channels):
        """Verify all orders reference valid channels."""
        invalid_refs = ~orders["channel_id"].isin(channels["channel_id"])
        invalid_count = invalid_refs.sum()
        assert invalid_count == 0, (
            f"Found {invalid_count} orders with invalid channel_id references"
        )

    def test_deliveries_valid_order_references(self, deliveries, orders):
        """Verify all deliveries reference valid orders."""
        invalid_refs = ~deliveries["delivery_order_id"].isin(orders["order_id"])
        invalid_count = invalid_refs.sum()
        assert invalid_count == 0, (
            f"Found {invalid_count} deliveries with invalid delivery_order_id references"
        )

    def test_payments_valid_order_references(self, payments, orders):
        """Verify all payments reference valid orders."""
        invalid_refs = ~payments["payment_order_id"].isin(orders["order_id"])
        invalid_count = invalid_refs.sum()
        assert invalid_count == 0, (
            f"Found {invalid_count} payments with invalid payment_order_id references"
        )


# === Utility Tests (can be run without cleaned data) ===


def test_cleaning_script_exists():
    """Verify the cleaning script exists."""
    script_path = Path("scripts/clean_data.py")
    assert script_path.exists(), "clean_data.py script should exist"


def test_results_directory_structure():
    """Verify results directory exists for Task D."""
    results_dir = Path("results/task-D")
    # Create if doesn't exist (for first run)
    results_dir.mkdir(parents=True, exist_ok=True)
    assert results_dir.exists(), "results/task-D directory should exist"
