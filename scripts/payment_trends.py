# Task E — Payment Methods Trend & Churn (Time Series)
# - Goal: Detect trends and anomalies in payment methods over time; propose seasonal/marketing interpretations.
# - Skills: Time series aggregation, anomaly detection, visual storytelling.
# - Deliverables:
#   - Script: `scripts/payment_trends.py` that outputs monthly shares and anomalies.
#   - Notebook: stacked area charts, anomaly annotations, linked SQL queries.


import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
from pathlib import Path
from datetime import datetime


#! Adjust the system path to include the parent directory
sys.path.insert(0, str(Path(__file__).parent.parent))

#! Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PaymentTrendsAnalyzer:
    """Class to analyze payment method trends and detect anomalies over time."""

    def __init__(self, input_dir="data/data-cleaned", output_dir="results/task-e"):
        # Get project root directory)
        project_root = Path(__file__).parent.parent

        self.input_dir = project_root / input_dir
        self.output_dir = project_root / output_dir

        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ========================================================================
    # STEP 1: LOAD AND PREPARE DATA
    # ========================================================================
    # TODO: Load orders_cleaned.csv and payments_cleaned.csv with latin1 encoding
    # TODO: Check for missing values in payment_fee column
    # TODO: Fill missing payment_fee values with 0 (business decision: assume no fee if missing)

    def load_data(self):
        """Load cleaned data for orders and payments."""
        logger.info(" -> Loading cleaned data...")

        self.orders_df = pd.read_csv(
            f"{self.input_dir}/orders_cleaned.csv", dtype=str, encoding="latin1"
        )
        self.payments_df = pd.read_csv(
            f"{self.input_dir}/payments_cleaned.csv", dtype=str, encoding="latin1"
        )

        logger.info("✓ All files loaded successfully")

    def prepare_data(self):
        """Prepare and clean data for analysis."""
        logger.info(" -> Preparing data...")

        # Check how many missing values exists
        missing_count = self.payments_df["payment_fee"].isna().sum()
        logger.info(f"   • Missing payment_fee values: {missing_count}")

        # Fill missing payment_fee with 0 (business decision)
        self.payments_df["payment_fee"] = self.payments_df["payment_fee"].fillna(0)

        # Recalculate missing count after filling
        missing_count = self.payments_df["payment_fee"].isna().sum()
        logger.info(f"   • Missing payment_fee values after filling: {missing_count}")

        logger.info("✓ Data preparation complete")

    # ========================================================================
    # STEP 2: MERGE ORDERS AND PAYMENTS FOR TIME SERIES ANALYSIS
    # ========================================================================
    # TODO: Merge orders and payments DataFrames on order_id and payment_order_id
    # TODO: Keep only these columns: order_id, order_moment_created, payment_method, payment_amount, payment_status
    # TODO: Convert order_moment_created to datetime format (use pd.to_datetime with format="mixed")
    # TODO: Drop rows where order_moment_created is NaN (invalid dates)
    # HINT: You need the order timestamp to analyze trends over time!
    def merge_orders_payments(self):
        """Merge orders and payments data for time series analysis."""
        logger.info(" -> Merging orders and payments data...")

        self.merged_df = self.orders_df.merge(
            self.payments_df, left_on="order_id", right_on="payment_order_id"
        )[
            [
                "order_id",
                "order_moment_created",
                "payment_method",
                "payment_amount",
                "payment_status",
            ]
        ]

        # Convert order_moment_created to datetime
        self.merged_df["order_moment_created"] = pd.to_datetime(
            self.merged_df["order_moment_created"], errors="coerce", format="mixed"
        )

        # Check for missing order_moment_created
        logger.info(
            f"   • Missing order_moment_created: {self.merged_df['order_moment_created'].isna().sum()}"
        )

        logger.info("✓ Merging complete")

    # ========================================================================
    # STEP 3: EXTRACT TIME COMPONENTS FOR ANALYSIS
    # ========================================================================
    # TODO: Create a new column 'year_month' by extracting year-month from order_moment_created
    # TODO: Create a new column 'year' by extracting year from order_moment_created
    # TODO: Create a new column 'month' by extracting month from order_moment_created
    # HINT: Use .dt.to_period('M') for year_month, .dt.year for year, .dt.month for month
    def extract_time_components(self):
        """Extract time components from order_moment_created."""
        logger.info(" -> Extracting time components...")

        # Extract year-month (e.g., "2021-01", "2021-02", etc.)
        self.merged_df["year_month"] = self.merged_df[
            "order_moment_created"
        ].dt.to_period("M")

        # Extract year and month separately
        self.merged_df["year"] = self.merged_df["order_moment_created"].dt.year
        self.merged_df["month"] = self.merged_df["order_moment_created"].dt.month

        logger.info("✓ Time components extracted")

    # ========================================================================
    # STEP 4: CALCULATE MONTHLY PAYMENT METHOD MARKET SHARE
    # ========================================================================
    # TODO: Filter to only PAID payments (payment_status == "PAID")
    # TODO: Group by year_month and payment_method, count transactions
    # TODO: Calculate total transactions per month (group by year_month only)
    # TODO: Merge the two results above
    # TODO: Calculate market share percentage: (transactions per method / total transactions) * 100
    # TODO: Save result to a new DataFrame called 'monthly_shares'
    # BUSINESS QUESTION: What percentage of transactions use ONLINE vs CREDIT each month?
    def calculate_monthly_payment_shares(self):
        """Calculate monthly payment method market share."""
        logger.info(" -> Calculating monthly payment method market share...")

        # Filter to only PAID payments
        paid_df = self.merged_df[self.merged_df["payment_status"] == "PAID"].copy()
        logger.info(
            f"   • Total PAID transactions: {len(paid_df)} out of {len(self.merged_df)} total"
        )

        # Count transactions per payment method per year_month
        method_counts = (
            paid_df.groupby(["year_month", "payment_method"])
            .agg(total_transactions=("order_id", "count"))
            .reset_index()
        )

        # Count total transactions per year_month
        total_counts = (
            paid_df.groupby("year_month")
            .agg(monthly_total_transactions=("order_id", "count"))
            .reset_index()
        )

        # Merge method_counts with total_counts
        merged_counts = (
            method_counts.merge(total_counts, on="year_month")
            .assign(
                share_pct=lambda x: (
                    x["total_transactions"] / x["monthly_total_transactions"]
                )
                * 100
            )
            .round(3)
        )

        # Store the result
        self.monthly_shares = merged_counts.reset_index()

        logger.info("✓ Monthly payment method market share calculated")

    # ========================================================================
    # STEP 5: DETECT ANOMALIES IN PAYMENT METHOD USAGE
    # ========================================================================
    # TODO: For each payment_method, calculate the mean and standard deviation of its monthly market share
    # TODO: Calculate z-score for each month: (actual_share - mean_share) / std_share
    # TODO: Flag as anomaly if z-score > 2.5 or z-score < -2.5 (statistical outliers)
    # TODO: Add columns 'zscore' and 'is_anomaly' to monthly_shares DataFrame
    # BUSINESS QUESTION: Were there any unusual spikes or drops in payment method usage?
    # HINT: A z-score of 2.5 means 2.5 standard deviations from the mean (very unusual)
    def calculate_anomalies(self):
        """Detect anomalies in payment method usage over time."""
        logger.info(" -> Detecting anomalies in payment method usage... ")

        # Calculate mean and std dev of share_pct per payment_method
        stats = (
            self.monthly_shares.groupby("payment_method")["share_pct"]
            .agg(["mean", "std"])
            .round(3)
        )

        # Merge stats back to monthly_shares
        self.monthly_shares = self.monthly_shares.merge(stats, on="payment_method")

        # Calculate z-score for each row in monthly_shares
        self.monthly_shares["zscore"] = (
            self.monthly_shares["share_pct"] - self.monthly_shares["mean"]
        ) / self.monthly_shares["std"]

        # Flag anomalies where |z-score| > 2.5
        self.monthly_shares["is_anomaly"] = self.monthly_shares["zscore"].abs() > 2.5

        logger.info("✓ Anomaly detection complete")

    # ========================================================================
    # STEP 6: VISUALIZE PAYMENT METHOD TRENDS OVER TIME
    # ========================================================================
    # TODO: Create a line chart showing market share % over time for top 5 payment methods
    # TODO: Mark anomalies with red 'X' markers on the line chart
    # TODO: Use figsize=(14, 8) for readability
    # TODO: Add title: "Payment Method Market Share Trends Over Time"
    # TODO: Add labels for x-axis (Month) and y-axis (Market Share %)
    # TODO: Show legend with payment method names
    # TODO: Save the plot to results/task-e/payment_trends_line_chart.png
    def visualize_payment_trends(self):
        """Visualize payment method trends over time."""
        logger.info(" -> Visualizing payment method trends over time...")

        # Identify top 5 payment methods by total transactions
        top_n = 5
        top_payment_methods = (
            self.monthly_shares.groupby("payment_method")["total_transactions"]
            .sum()
            .nlargest(top_n)
            .index
        )

        # Keep only rows where payment_method is in top_5_methods
        top_n_data = self.monthly_shares[
            self.monthly_shares["payment_method"].isin(top_payment_methods)
        ]

        # Define anomalies FIRST (from top_n_data)
        anomalies = top_n_data[top_n_data["is_anomaly"] == True]

        # Create figure with 2 subplots (one above the other)
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

        # Top plot: High-share methods (ONLINE, VOUCHER)
        high_share_methods = ["ONLINE", "VOUCHER"]
        for method in high_share_methods:
            method_data = top_n_data[top_n_data["payment_method"] == method]
            ax1.plot(
                method_data["year_month"].astype(str),
                method_data["share_pct"],
                marker="o",
                label=method,
            )
        ax1.set_ylabel("Market Share (%)")
        ax1.set_title("High Share Payment Methods (ONLINE, VOUCHER)")
        ax1.legend()
        ax1.grid(True, alpha=0.3, linestyle="--")

        # Bottom plot: Low-share methods (DEBIT, MEAL_BENEFIT, STORE_DIRECT_PAYMENT)
        low_share_methods = ["DEBIT", "MEAL_BENEFIT", "STORE_DIRECT_PAYMENT"]
        for method in low_share_methods:
            method_data = top_n_data[top_n_data["payment_method"] == method]
            ax2.plot(
                method_data["year_month"].astype(str),
                method_data["share_pct"],
                marker="o",
                label=method,
            )
        ax2.set_ylabel("Market Share (%)")
        ax2.set_xlabel("Month")
        ax2.set_title(
            "Low Share Payment Methods (DEBIT, MEAL_BENEFIT, STORE_DIRECT_PAYMENT)"
        )
        ax2.legend()
        ax2.grid(True, alpha=0.3, linestyle="--")

        # Add anomalies to both plots
        anomalies_high = anomalies[anomalies["payment_method"].isin(high_share_methods)]
        anomalies_low = anomalies[anomalies["payment_method"].isin(low_share_methods)]

        ax1.scatter(
            anomalies_high["year_month"].astype(str),
            anomalies_high["share_pct"],
            color="red",
            marker="x",
            s=200,
            label="Anomaly",
        )
        ax2.scatter(
            anomalies_low["year_month"].astype(str),
            anomalies_low["share_pct"],
            color="red",
            marker="x",
            s=200,
            label="Anomaly",
        )

        plt.xticks(rotation=45, ha="right")  # Rotate x-axis labels for readability
        plt.tight_layout()

        # Save the plot
        plt.savefig(
            f"{self.output_dir}/payment_trends_line_chart.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

        logger.info("✓ Payment method trends visualization complete")

    # ========================================================================
    # STEP 7: CREATE STACKED AREA CHART FOR ALL PAYMENT METHODS
    # ========================================================================
    # TODO: Pivot the monthly_shares DataFrame so:
    #       - Index = year_month (time on x-axis)
    #       - Columns = payment_method (different payment types)
    #       - Values = share_pct (market share percentage)
    # TODO: Fill missing values with 0 (months where a payment method wasn't used)
    # TODO: Create a stacked area chart using .plot(kind='area', stacked=True)
    # TODO: Use figsize=(14, 8) and alpha=0.7 for transparency
    # TODO: Add title: "Payment Method Market Share Evolution (Stacked)"
    # TODO: Save to results/task-e/payment_trends_stacked_area.png
    # BUSINESS INSIGHT: This shows how payment method mix changed over time!
    def visualize_stacked_area_chart(self):
        """Create stacked area chart for all payment methods."""
        logger.info(" -> Creating stacked area chart for all payment methods...")

        # Pivot the DataFrame
        pivot_df = self.monthly_shares.pivot(
            index="year_month", columns="payment_method", values="share_pct"
        )

        # Group minor payment methods into "Other"
        top_n = 5
        top_methods = pivot_df.sum().nlargest(top_n).index
        pivot_df["Other"] = pivot_df.drop(columns=top_methods).sum(axis=1)
        pivot_df_simplified = pivot_df[list(top_methods) + ["Other"]]

        # Fill missing values with 0
        pivot_df_simplified = pivot_df_simplified.fillna(0)

        # Create stacked area chart
        ax = pivot_df_simplified.plot(
            kind="area", stacked=True, figsize=(14, 8), alpha=0.7
        )
        ax.set_title("Payment Method Market Share Evolution (Stacked)")
        ax.set_xlabel("Month")
        ax.set_ylabel("Payment Method Market Share (%)")

        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        # Save the plot
        plt.savefig(
            f"{self.output_dir}/payment_trends_stacked_area.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

        logger.info("✓ Stacked area chart created")

    # ========================================================================
    # STEP 8: ANALYZE PAYMENT AMOUNT DISTRIBUTION BY METHOD
    # ========================================================================
    # TODO: Filter to only PAID payments
    # TODO: For top 5 payment methods (by transaction count), create a boxplot
    # TODO: X-axis: payment_method, Y-axis: payment_amount
    # TODO: Use figsize=(12, 8)
    # TODO: Add title: "Payment Amount Distribution by Method"
    # TODO: Rotate x-axis labels 45 degrees for readability
    # TODO: Save to results/task-e/payment_amount_boxplot.png
    # BUSINESS QUESTION: Which payment methods have higher average order values?
    # HINT: Boxplots show median, quartiles, and outliers - great for comparing distributions!
    def analyze_payment_amount_distribution(self):
        """Analyze payment amount distribution by method."""
        logger.info(" -> Analyzing payment amount distribution by method...")

        # Filter to only PAID payments
        paid_df = self.merged_df[self.merged_df["payment_status"] == "PAID"].copy()

        # Identify top 5 payment methods by transaction count
        top_n = 5
        top_payment_methods = (
            paid_df["payment_method"].value_counts().nlargest(top_n).index
        )

        # Filter data to only top payment methods
        top_data = paid_df[paid_df["payment_method"].isin(top_payment_methods)].copy()
        # Convert payment_amount to float
        top_data["payment_amount"] = top_data["payment_amount"].astype(float)

        # Remove extreme outliers (keep 99% of data for better visualization)
        percentile_99 = top_data["payment_amount"].quantile(0.99)
        top_data_filtered = top_data[top_data["payment_amount"] <= percentile_99]
        logger.info(f"   • Filtered to 99th percentile: ${percentile_99:.2f}")

        # Create boxplot using filtered data
        fig, (ay1, ay2, ay3) = plt.subplots(1, 3, figsize=(18, 6))

        # Prepare data for boxplot
        high_share_methods = ["ONLINE", "DEBIT"]
        middle_share_methods = ["VOUCHER"]
        low_share_methods = ["MEAL_BENEFIT", "STORE_DIRECT_PAYMENT"]

        # Left plot: High-share methods
        data_to_plot_high = [
            top_data_filtered[top_data_filtered["payment_method"] == method][
                "payment_amount"
            ]
            for method in high_share_methods
        ]
        ay1.boxplot(data_to_plot_high, labels=high_share_methods)
        ay1.set_xlabel("Payment Method")
        ay1.set_ylabel("Payment Amount ($)")
        ay1.set_title("Payment Amount Distribution by Method - High Share Methods")
        ay1.grid(True, alpha=0.3, linestyle="--")

        data_to_plot_medium = [
            top_data_filtered[top_data_filtered["payment_method"] == method][
                "payment_amount"
            ]
            for method in middle_share_methods
        ]
        ay2.boxplot(data_to_plot_medium, labels=middle_share_methods, showfliers=False)
        ay2.set_xlabel("Payment Method")
        ay2.set_ylabel("Payment Amount ($)")
        ay2.set_title("Payment Amount Distribution by Method - Medium Share Methods")
        ay2.grid(True, alpha=0.3, linestyle="--")

        # Right plot: Low-share methods
        data_to_plot_low = [
            top_data_filtered[top_data_filtered["payment_method"] == method][
                "payment_amount"
            ]
            for method in low_share_methods
        ]
        ay3.boxplot(data_to_plot_low, labels=low_share_methods)
        ay3.set_xlabel("Payment Method")
        ay3.set_ylabel("Payment Amount ($)")
        ay3.set_title("Payment Amount Distribution by Method - Low Share Methods")
        ay3.grid(True, alpha=0.3, linestyle="--")
        ay3.tick_params(axis="x", rotation=45)

        plt.tight_layout()

        # Save the plot
        plt.savefig(
            f"{self.output_dir}/payment_amount_boxplot.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

        logger.info("✓ Payment amount distribution analysis complete")

    def run(self):
        """Main method to run the payment trends analysis."""
        self.load_data()
        self.prepare_data()
        self.merge_orders_payments()
        self.extract_time_components()
        self.calculate_monthly_payment_shares()
        self.calculate_anomalies()
        self.visualize_payment_trends()
        self.visualize_stacked_area_chart()
        self.analyze_payment_amount_distribution()


# ========================================================================
# STEP 9: SAVE RESULTS TO CSV FILES
# ========================================================================
# TODO: Create directory results/task-e/ if it doesn't exist (use Path.mkdir with parents=True, exist_ok=True)
# TODO: Save monthly_shares DataFrame to results/task-e/monthly_payment_shares.csv
# TODO: Save only anomalies (where is_anomaly == True) to results/task-e/payment_anomalies.csv
# TODO: Create a summary DataFrame with:
#       - payment_method
#       - total_transactions (sum across all months)
#       - avg_market_share (mean of share_pct)
#       - anomaly_count (count of anomalies for that method)
# TODO: Save summary to results/task-e/payment_method_summary.csv

# ========================================================================
# STEP 10: PRINT BUSINESS INSIGHTS SUMMARY
# ========================================================================
# TODO: Print analysis period (min and max date from year_month)
# TODO: Print total number of transactions analyzed
# TODO: Print top 3 payment methods by total transaction count
# TODO: Print total number of anomalies detected
# TODO: Print 3-5 sample anomalies with details (payment_method, month, market_share, z-score)
# TODO: Print a business recommendation based on trends observed
# EXAMPLE OUTPUT:
# ======================================================================
# PAYMENT TRENDS ANALYSIS SUMMARY
# ======================================================================
# Analysis Period: Jan 2021 to Dec 2021
# Total Transactions: 400,377
# Top 3 Payment Methods:
#   1. ONLINE: 293,845 (73.4%)
#   2. DEBIT: 12,452 (3.1%)
#   3. VOUCHER: 45,946 (11.5%)
# Anomalies Detected: 8
#   • CREDIT in Mar 2021: 15.2% (z-score: 3.1) - Unusual spike
#   • ONLINE in Jul 2021: 68.5% (z-score: -2.8) - Unusual drop
# Recommendation: Investigate Mar 2021 CREDIT spike - possible promotion?
# ======================================================================

# ========================================================================
# BONUS STEP 11 (OPTIONAL): SEASONAL ANALYSIS
# ========================================================================
# TODO: Group transactions by month (1-12, ignoring year) to find seasonal patterns
# TODO: Calculate average market share per calendar month for each payment method
# TODO: Create a heatmap showing payment_method (y-axis) vs month (x-axis) with share_pct as color
# TODO: Save to results/task-e/seasonal_payment_patterns.png
# BUSINESS QUESTION: Do certain payment methods spike during holidays (Dec) or tax season (Apr)?

# ========================================================================
# BONUS STEP 12 (OPTIONAL): CORRELATION ANALYSIS
# ========================================================================
# TODO: Pivot monthly_shares to create a correlation matrix between payment methods
# TODO: Calculate correlation: when ONLINE usage goes up, does CREDIT go down?
# TODO: Create a heatmap of correlations using seaborn (sns.heatmap)
# TODO: Save to results/task-e/payment_method_correlations.png
# BUSINESS INSIGHT: Negative correlation = substitution effect (users switch between methods)
#                   Positive correlation = methods used together or by similar customer segments

# ? Entry point for payment trends analysis
if __name__ == "__main__":
    analyzer = PaymentTrendsAnalyzer()
    analyzer.run()

    print("✅ Task E: Payment Trends Analysis Complete!")
    print("📁 Check results/task-e/ for CSV files and visualizations")
