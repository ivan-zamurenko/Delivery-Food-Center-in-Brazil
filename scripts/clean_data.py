"""
Data Cleaning Pipeline - Task D

This script implements a data cleaning pipeline.
It processes raw CSV data files from. data/ and produced cleaned version in data/data-cleaned


Cleaning steps applied:
- Remove duplicates records (by ID columns)
- Handle missing values (drop or impute based on business rules)
- Fix data types and parse dates
- Remove outliers (based on business logic)
- Validate foreign key relationships
- Log all changes for auditability

Usage:
    python scripts/clean_data.py

Outputs:
    - Cleaned CSV files in data/data-cleaned/
    - Cleaning report in results/task-D/cleaning_report.txt

"""

import sys
from pathlib import Path
import logging
import pandas as pd
from datetime import datetime


# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataCleaner:
    """Handles data cleaning operations for the delivery food center dataset."""

    def __init__(self, input_dir="data/raw", output_dir="data/data-cleaned"):
        # Get project root directory (parent of scripts/)
        project_root = Path(__file__).parent.parent

        # Use absolute paths based on project root
        self.input_dir = project_root / input_dir
        self.output_dir = project_root / output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Create results directory for reports
        self.results_dir = project_root / "results/task-d"
        self.results_reports_dir = self.results_dir / "reports"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.results_reports_dir.mkdir(parents=True, exist_ok=True)

        self.cleaning_stats = {}

    def load_data(self):
        """Load raw data files from the input directory."""
        logger.info(f" -> Loading data from {self.input_dir}")

        # use latin1 encoding for Brazilian Portuguese data with special characters
        encoding = "latin1"

        self.orders = pd.read_csv(self.input_dir / "orders.csv", encoding=encoding)
        self.payments = pd.read_csv(self.input_dir / "payments.csv", encoding=encoding)
        self.deliveries = pd.read_csv(
            self.input_dir / "deliveries.csv", encoding=encoding
        )
        self.drivers = pd.read_csv(self.input_dir / "drivers.csv", encoding=encoding)
        self.channels = pd.read_csv(self.input_dir / "channels.csv", encoding=encoding)
        self.stores = pd.read_csv(self.input_dir / "stores.csv", encoding=encoding)
        self.hubs = pd.read_csv(self.input_dir / "hubs.csv", encoding=encoding)

        logger.info("✓ All files loaded successfully")

    def clean_orders(self):
        """
        Clean orders dataset by removing duplicates, parsing dates, and validating data.

        Cleaning steps:
        1. Remove duplicate order records (keep first occurrence)
        2. Parse 13 datetime columns to proper datetime format
        3. Calculate delivery time and remove invalid values (negative or >180 min)
        4. Remove orders with non-positive amounts
        5. Drop orders missing critical IDs (order_id, store_id, channel_id)

        Note: Only filters delivery times for completed orders (ignores pending/cancelled).
        """
        logger.info(" -> Cleaning orders dataset...")

        df = self.orders.copy()
        initial_rows = len(df)

        # STEP 1: Remove duplicates by order_id (keep first occurrence)
        df = df.drop_duplicates(subset=["order_id"], keep="first")
        duplicates_removed = initial_rows - len(df)

        # STEP 2: Parse datetime columns to proper datetime objects
        # Note: Only parse order_moment_* columns (these are timestamp strings)
        # DO NOT parse order_created_hour/minute/day/month/year (these are already integers)
        datetime_cols = [
            "order_moment_created",
            "order_moment_accepted",
            "order_moment_ready",
            "order_moment_collected",
            "order_moment_in_expedition",
            "order_moment_delivering",
            "order_moment_delivered",
            "order_moment_finished",
        ]
        for col in datetime_cols:
            # format='mixed' handles various datetime formats, errors='coerce' converts invalid dates to NaT
            df[col] = pd.to_datetime(df[col], format="mixed", errors="coerce")

        # STEP 3: Calculate delivery time metric (for completed deliveries)
        df["delivery_time_minutes"] = (
            df["order_moment_delivered"] - df["order_moment_delivering"]
        ).dt.total_seconds() / 60

        # Remove ONLY invalid delivery times (negative or >180 min)
        # IMPORTANT: We keep orders with NaN delivery times (pending/cancelled orders)
        # Using has_delivery_time mask prevents removing 95% of dataset (orders not yet delivered)
        before_time_filter = len(df)
        has_delivery_time = df[
            "delivery_time_minutes"
        ].notna()  # True if delivery completed
        invalid_delivery_times = has_delivery_time & (
            (df["delivery_time_minutes"] < 0) | (df["delivery_time_minutes"] > 180)
        )
        df = df[~invalid_delivery_times]  # Remove only truly invalid delivery times
        invalid_times_removed = before_time_filter - len(df)

        # STEP 4: Validate order amounts (business rule: must be positive)
        df = df[df["order_amount"] > 0]
        invalid_order_amounts_removed = (
            initial_rows - duplicates_removed - invalid_times_removed - len(df)
        )

        # STEP 5: Handle missing values in critical fields
        nulls_before = df.isnull().sum().sum()
        # Drop orders missing required foreign keys (can't link to stores/channels without these)
        df = df.dropna(subset=["order_id", "store_id", "channel_id"])
        nulls_after = df.isnull().sum().sum()

        # VALIDATION: Ensure order_id uniqueness (primary key constraint)
        assert df["order_id"].is_unique, "order_id should be unique after cleaning"

        # Store cleaned data and statistics for reporting
        self.orders_cleaned = df
        self.cleaning_stats["orders"] = {
            "initial_rows": initial_rows,
            "final_rows": len(df),
            "duplicates_removed": duplicates_removed,
            "invalid_times_removed": int(invalid_times_removed),
            "invalid_order_amounts_removed": int(invalid_order_amounts_removed),
            "nulls_before": int(nulls_before),
            "nulls_after": int(nulls_after),
        }

        logger.info(
            f"✓ Orders cleaned: {len(df)} rows (removed {duplicates_removed} duplicates)"
        )

    def clean_payments(self):
        """
        Clean payments dataset by removing duplicates and validating amounts.

        Cleaning steps:
        1. Remove duplicate payment records (keep first occurrence)
        2. Drop payments missing required IDs (payment_id, payment_order_id)
        3. Remove payments with non-positive amounts (invalid transactions)

        Note: Orphaned payments (referencing deleted orders) are removed later in validate_relationships().
        """
        logger.info(" -> Cleaning payments dataset...")

        df = self.payments.copy()
        initial_rows = len(df)

        # STEP 1: Remove duplicate payments by payment_id
        df = df.drop_duplicates(subset=["payment_id"], keep="first")
        duplicates_removed = initial_rows - len(df)

        # STEP 2: Handle missing values in critical fields
        nulls_before = df.isnull().sum().sum()
        # Drop payments without IDs (can't identify payment or link to order)
        df = df.dropna(subset=["payment_id", "payment_order_id"])
        nulls_after = df.isnull().sum().sum()

        # STEP 3: Validate payment amounts (business rule: must be positive)
        # Negative or zero payments are data errors
        df = df[df["payment_amount"] > 0]
        invalid_payment_amounts_removed = initial_rows - duplicates_removed - len(df)

        # VALIDATION: Ensure payment_id uniqueness (primary key constraint)
        assert df["payment_id"].is_unique, "payment_id should be unique after cleaning"

        # Store cleaned data and statistics
        self.payments_cleaned = df
        self.cleaning_stats["payments"] = {
            "initial_rows": initial_rows,
            "final_rows": len(df),
            "duplicates_removed": duplicates_removed,
            "invalid_payment_amounts_removed": int(invalid_payment_amounts_removed),
            "nulls_before": int(nulls_before),
            "nulls_after": int(nulls_after),
        }

        logger.info(
            f"✓ Payments cleaned: {len(df)} rows (removed {duplicates_removed} duplicates)"
        )

    def clean_deliveries(self):
        """
        Clean deliveries dataset with special handling for missing driver data.

        Cleaning steps:
        1. Remove duplicate delivery records (keep first occurrence)
        2. Handle NULL driver_id values (business decision: keep as valid pattern)
        3. Replace NULL driver_id with -1 (dimensional modeling: "unknown driver")
        4. Create has_driver_data flag for analytics
        5. Drop deliveries missing critical IDs

        Business Context:
        - 4.19% of deliveries (15,886) have NULL driver_id
        - Of these, 8,601 are successfully DELIVERED
        - Indicates valid business models: customer pickups, third-party delivery services
        - Decision: Keep these deliveries but flag them for analytical purposes
        """
        logger.info(" -> Cleaning deliveries dataset...")

        df = self.deliveries.copy()
        initial_rows = len(df)

        # STEP 1: Remove duplicate deliveries by delivery_id
        df = df.drop_duplicates(subset=["delivery_id"], keep="first")
        duplicates_removed = initial_rows - len(df)

        # STEP 2: Analyze NULL driver pattern BEFORE making decisions
        null_driver_mask = df["driver_id"].isnull()
        null_count = null_driver_mask.sum()
        null_pct = (null_count / len(df)) * 100
        logger.info(
            f"      Found {null_count} deliveries ({null_pct:.2f}%) without driver_id"
        )

        # Check if these are legitimate successful deliveries
        delivered_orders = df[
            (df["driver_id"].isnull())
            & (df["delivery_status"].str.upper() == "DELIVERED")
        ]
        logger.info(f"      Of these, {len(delivered_orders)} are marked as DELIVERED")

        # BUSINESS DECISION: Keep all deliveries, replace NULL with -1
        # Rationale: 8,601 successful deliveries indicate this is NOT data quality issue
        #           but rather a valid business pattern (customer pickups, third-party services)
        # Dimensional modeling: Use -1 as surrogate key for "unknown driver"
        df["driver_id"] = df["driver_id"].fillna(-1).astype(int)

        # STEP 3: Create analytical flag for downstream analysis
        # Allows analysts to filter/segment by driver presence
        df["has_driver_data"] = df["driver_id"] != -1

        # STEP 4: Handle missing values in critical fields
        nulls_before = df.isnull().sum().sum()
        # Drop deliveries without IDs (can't identify delivery or link to order)
        df = df.dropna(subset=["delivery_id", "delivery_order_id"])
        nulls_after = df.isnull().sum().sum()

        # VALIDATION: Ensure delivery_id uniqueness (primary key constraint)
        assert df["delivery_id"].is_unique, (
            "delivery_id should be unique after cleaning"
        )

        # Store cleaned data and statistics
        self.deliveries_cleaned = df
        self.cleaning_stats["deliveries"] = {
            "initial_rows": initial_rows,
            "final_rows": len(df),
            "duplicates_removed": duplicates_removed,
            "nulls_before": int(nulls_before),
            "nulls_after": int(nulls_after),
            "null_drivers_replaced": int(null_count),
        }

        logger.info(
            f"✓ Deliveries cleaned: {len(df)} rows (removed {duplicates_removed} duplicates)"
        )
        logger.info(f"   Replaced {null_count} null driver_id with -1 (unknown driver)")

    def clean_dimension_tables(self):
        """
        Clean dimension tables by removing duplicates.

        Dimension tables: channels, drivers, stores, hubs
        - These are reference/lookup tables used for foreign key relationships
        - Primary cleaning: Remove duplicate records by primary key
        - No complex validation needed (fact tables reference these)

        Note: In a real data warehouse, these would be slowly changing dimensions (SCD)
        with proper versioning and surrogate keys.
        """
        logger.info(" -> Cleaning dimension tables...")

        # Remove duplicates from each dimension table (keep first occurrence)
        self.channels_cleaned = self.channels.drop_duplicates(subset=["channel_id"])
        self.drivers_cleaned = self.drivers.drop_duplicates(subset=["driver_id"])
        self.stores_cleaned = self.stores.drop_duplicates(subset=["store_id"])
        self.hubs_cleaned = self.hubs.drop_duplicates(subset=["hub_id"])

        logger.info("✓ Dimension tables cleaned.")

    def validate_relationships(self):
        """
        Validate and enforce foreign key relationships (referential integrity).

        Data Model:
        - orders.store_id → stores.store_id (N:1)
        - orders.channel_id → channels.channel_id (N:1)
        - payments.payment_order_id → orders.order_id (N:1)
        - deliveries.delivery_order_id → orders.order_id (N:1)

        Process:
        1. Validate parent relationships (orders → stores/channels)
        2. Remove orphaned child records (payments/deliveries referencing deleted orders)

        Important: This implements CASCADE DELETE behavior
        - When we remove invalid orders, their payments/deliveries become orphaned
        - We remove orphaned records to maintain referential integrity
        - Expected behavior: ~0.1-0.3% orphaned records after cleaning ~1000 invalid orders

        Technical Note: .astype(str) conversion prevents type mismatch in .isin() comparisons
        """
        logger.info(" -> Validating foreign key relationships...")

        # STEP 1: Validate Orders → Dimension Tables (parent relationships)
        # Check orders.store_id exists in stores table
        orphaned_stores = ~self.orders_cleaned["store_id"].isin(
            self.stores_cleaned["store_id"]
        )

        # Check orders.channel_id exists in channels table
        orphaned_channels = ~self.orders_cleaned["channel_id"].isin(
            self.channels_cleaned["channel_id"]
        )

        # Remove orders with invalid foreign keys
        if orphaned_stores.any():
            logger.warning(
                f"✗ Found {orphaned_stores.sum()} orders with invalid store_id."
            )
            self.orders_cleaned = self.orders_cleaned[~orphaned_stores]

        if orphaned_channels.any():
            logger.warning(
                f"✗ Found {orphaned_channels.sum()} orders with invalid channel_id."
            )
            self.orders_cleaned = self.orders_cleaned[~orphaned_channels]

        # STEP 2: Calculate total orders removed (for context on orphaned child records)
        orders_removed = self.cleaning_stats["orders"]["initial_rows"] - len(
            self.orders_cleaned
        )
        logger.info(
            f"   Note: {orders_removed} orders removed during cleaning may have orphaned child records"
        )

        # STEP 3: Validate Child Tables → Orders (remove orphaned payments/deliveries)
        # This implements CASCADE DELETE: remove child records when parent is deleted

        # Find payments referencing non-existent orders
        # .astype(str) ensures type matching (prevents int/str comparison issues)
        orphaned_payments = ~self.payments_cleaned["payment_order_id"].astype(str).isin(
            self.orders_cleaned["order_id"].astype(str)
        )

        if orphaned_payments.any():
            orphan_count = orphaned_payments.sum()
            orphan_pct = (orphan_count / len(self.payments_cleaned)) * 100
            logger.warning(
                f"✗ Found {orphan_count} payments ({orphan_pct:.1f}%) with invalid order references."
            )
            # Remove orphaned payments to maintain referential integrity
            self.payments_cleaned = self.payments_cleaned[~orphaned_payments]

        # Find deliveries referencing non-existent orders
        orphaned_deliveries = ~self.deliveries_cleaned["delivery_order_id"].astype(
            str
        ).isin(self.orders_cleaned["order_id"].astype(str))

        if orphaned_deliveries.any():
            orphan_count = orphaned_deliveries.sum()
            orphan_pct = (orphan_count / len(self.deliveries_cleaned)) * 100
            logger.warning(
                f"✗ Found {orphan_count} deliveries ({orphan_pct:.1f}%) with invalid order references."
            )
            # Remove orphaned deliveries to maintain referential integrity
            self.deliveries_cleaned = self.deliveries_cleaned[~orphaned_deliveries]

        logger.info("✓ All foreign key relationships validated successfully.")

    def save_cleaned_data(self):
        """
        Save all cleaned datasets to CSV files in the output directory.

        Output location: data/data-cleaned/
        Encoding: latin1 (preserves Brazilian Portuguese characters like "São Paulo")
        Format: CSV without index column

        Files saved:
        - orders_cleaned.csv
        - payments_cleaned.csv
        - deliveries_cleaned.csv
        - drivers_cleaned.csv
        - channels_cleaned.csv
        - stores_cleaned.csv
        - hubs_cleaned.csv
        """
        logger.info(f" -> Saving cleaned data to {self.output_dir}")

        # Save all cleaned tables to CSV files
        # latin1 encoding preserves special characters in Brazilian data (São Paulo, etc.)
        # index=False prevents adding row numbers as a column
        self.orders_cleaned.to_csv(
            self.output_dir / "orders_cleaned.csv", index=False, encoding="latin1"
        )
        self.payments_cleaned.to_csv(
            self.output_dir / "payments_cleaned.csv", index=False, encoding="latin1"
        )
        self.deliveries_cleaned.to_csv(
            self.output_dir / "deliveries_cleaned.csv", index=False, encoding="latin1"
        )
        self.drivers_cleaned.to_csv(
            self.output_dir / "drivers_cleaned.csv", index=False, encoding="latin1"
        )
        self.channels_cleaned.to_csv(
            self.output_dir / "channels_cleaned.csv", index=False, encoding="latin1"
        )
        self.stores_cleaned.to_csv(
            self.output_dir / "stores_cleaned.csv", index=False, encoding="latin1"
        )
        self.hubs_cleaned.to_csv(
            self.output_dir / "hubs_cleaned.csv", index=False, encoding="latin1"
        )

        logger.info("✓ Cleaned data saved successfully.")

    def generate_report(self):
        """
        Generate comprehensive cleaning report with before/after statistics.

        Outputs:
        1. Text report saved to results/task-D/cleaning_report.txt
        2. Summary table printed to console

        Report includes:
        - Per-table cleaning statistics (rows removed, duplicates, nulls, etc.)
        - Overall data retention rate
        - Timestamp for audit trail
        """
        logger.info(" -> Generating cleaning report...")

        # Generate detailed text report
        report_path = self.results_reports_dir / "cleaning_report.txt"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=" * 70 + "\n")
            f.write("Data Cleaning Report - Task D\n")
            f.write("=" * 70 + "\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Write detailed statistics for each table
            for table_name, stats in self.cleaning_stats.items():
                f.write(f"{table_name.upper()}\n")
                f.write("-" * 40 + "\n")
                for key, value in stats.items():
                    f.write(f"   {key}: {value}\n")
                f.write("\n")

            # Write overall summary
            f.write("=" * 70 + "\n")
            f.write("Cleaning Summary:\n")
            f.write("=" * 70 + "\n")
            total_initial = sum(s["initial_rows"] for s in self.cleaning_stats.values())
            total_final = sum(s["final_rows"] for s in self.cleaning_stats.values())
            f.write(f"Total rows before cleaning: {total_initial}\n")
            f.write(f"Total rows after cleaning: {total_final}\n")
            f.write(f"Total rows removed: {total_initial - total_final}\n")
            f.write(
                f"Data quality improvement: {((total_final / total_initial) * 100):.2f}% retained\n"
            )

        logger.info(f"✓ Report saved to {report_path}.")

        # Print formatted summary to console for quick review
        print("\n" + "=" * 70)
        print("CLEANING SUMMARY")
        print("=" * 70)
        for table_name, stats in self.cleaning_stats.items():
            removed = stats["initial_rows"] - stats["final_rows"]
            print(
                f"{table_name:15} | Initial: {stats['initial_rows']:6} | Final: {stats['final_rows']:6} | Removed: {removed:4}"
            )
        print("=" * 70 + "\n")

    def run(self):
        """
        Execute the complete data cleaning pipeline.

        Pipeline stages (in order):
        1. load_data() - Load raw CSV files from data/raw/
        2. clean_orders() - Clean fact table: orders
        3. clean_payments() - Clean fact table: payments
        4. clean_deliveries() - Clean fact table: deliveries (special NULL driver handling)
        5. clean_dimension_tables() - Clean dimension tables: channels, drivers, stores, hubs
        6. validate_relationships() - Enforce referential integrity (CASCADE DELETE orphaned records)
        7. save_cleaned_data() - Save cleaned CSVs to data/data-cleaned/
        8. generate_report() - Generate cleaning summary report

        Order is important:
        - Clean fact tables before validating relationships
        - Validate relationships before saving (ensures referential integrity)
        - Generate report last (captures final statistics)
        """

        self.load_data()
        self.clean_orders()
        self.clean_payments()
        self.clean_deliveries()
        self.clean_dimension_tables()
        self.validate_relationships()
        self.save_cleaned_data()
        self.generate_report()

        logger.info("✓ Data cleaning pipeline completed successfully!")


# Entry point for script execution
if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.run()
