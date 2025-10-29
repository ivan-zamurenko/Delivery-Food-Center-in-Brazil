"""
Task E — Payment Methods Trend Analysis & Anomaly Detection

Business Goal:
    Analyze payment method trends over time to identify market shifts, anomalies,
    and seasonal patterns that inform marketing strategy and payment partnerships.

Technical Approach:
    - Time series aggregation of 400K+ payment transactions
    - Z-score based anomaly detection (threshold: 2.5 standard deviations)
    - Statistical visualization (line charts, heatmaps, correlation analysis)
    - Automated business insights generation

Deliverables:
    - Monthly payment method market share data (CSV)
    - Anomaly detection reports with recommendations
    - 5 publication-quality visualizations
    - Statistical correlation analysis between payment methods

Author: Ivan Zamurenko
Date: October 2025
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from pathlib import Path

# Adjust system path to include parent directory
sys.path.insert(0, str(Path(__file__).parent.parent))

# Initialize logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PaymentTrendsAnalyzer:
    """
    Analyzes payment method trends and detects statistical anomalies over time.

    This class implements a complete time series analysis pipeline:
    1. Data loading and preparation (orders + payments merge)
    2. Monthly market share calculation
    3. Z-score based anomaly detection
    4. Statistical visualizations (5 charts)
    5. Correlation and seasonal analysis

    Attributes:
        input_dir (Path): Directory containing cleaned CSV data
        output_dir (Path): Root directory for analysis outputs
        output_report_dir (Path): Directory for CSV reports
        output_plots_dir (Path): Directory for visualizations
        monthly_shares (DataFrame): Main analysis dataset with market shares
    """

    def __init__(self, input_dir="data/data-cleaned", output_dir="results/task-e"):
        """
        Initialize analyzer with input/output directory paths.

        Args:
            input_dir (str): Path to cleaned data directory
            output_dir (str): Path to results directory
        """
        # Get project root directory
        project_root = Path(__file__).parent.parent

        self.input_dir = project_root / input_dir
        self.output_dir = project_root / output_dir
        self.output_report_dir = self.output_dir / "reports"
        self.output_plots_dir = self.output_dir / "plots"

        # Create output directories if they don't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_report_dir.mkdir(parents=True, exist_ok=True)
        self.output_plots_dir.mkdir(parents=True, exist_ok=True)

    # ========================================================================
    # DATA LOADING & PREPARATION
    # ========================================================================

    def load_data(self):
        """
        Load cleaned orders and payments data from CSV files.

        Reads:
            - orders_cleaned.csv: 367K+ order records
            - payments_cleaned.csv: 400K+ payment transactions

        Note: Uses latin1 encoding to handle special characters in Brazilian data.
        """
        logger.info(" -> Loading cleaned data...")

        self.orders_df = pd.read_csv(
            f"{self.input_dir}/orders_cleaned.csv", dtype=str, encoding="latin1"
        )
        self.payments_df = pd.read_csv(
            f"{self.input_dir}/payments_cleaned.csv", dtype=str, encoding="latin1"
        )

        logger.info("✓ All files loaded successfully")

    def prepare_data(self):
        """
        Clean and prepare data for time series analysis.

        Business Logic:
            - Fills missing payment_fee values with 0 (assume no fee when not recorded)
            - Logs data quality metrics for transparency
        """
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

    def merge_orders_payments(self):
        """
        Merge orders and payments data for time series analysis.

        Joins on order_id to combine transaction details with payment information.
        Converts timestamps to datetime format for temporal analysis.
        """
        logger.info(" -> Merging orders and payments data...")

        # Join orders and payments, keeping only essential columns for analysis
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

        # Convert timestamps to datetime (handles mixed formats)
        self.merged_df["order_moment_created"] = pd.to_datetime(
            self.merged_df["order_moment_created"], errors="coerce", format="mixed"
        )

        logger.info(
            f"   • Missing order_moment_created: {self.merged_df['order_moment_created'].isna().sum()}"
        )
        logger.info("✓ Merging complete")

    def extract_time_components(self):
        """
        Extract time components for temporal aggregation.

        Creates year_month periods for monthly grouping and separate year/month columns.
        """
        logger.info(" -> Extracting time components...")

        # Extract year-month (e.g., "2021-01", "2021-02", etc.)
        self.merged_df["year_month"] = self.merged_df[
            "order_moment_created"
        ].dt.to_period("M")

        # Extract year and month separately
        self.merged_df["year"] = self.merged_df["order_moment_created"].dt.year
        self.merged_df["month"] = self.merged_df["order_moment_created"].dt.month

        logger.info("✓ Time components extracted")

    def calculate_monthly_payment_shares(self):
        """
        Calculate monthly market share percentage for each payment method.

        Market share = (method transactions / total monthly transactions) × 100
        Only includes successfully completed (PAID) transactions.
        """
        logger.info(" -> Calculating monthly payment method market share...")

        # Filter to only successfully completed payments
        paid_df = self.merged_df[self.merged_df["payment_status"] == "PAID"].copy()
        logger.info(
            f"   • Total PAID transactions: {len(paid_df)} out of {len(self.merged_df)} total"
        )

        # Count transactions per payment method per month
        method_counts = (
            paid_df.groupby(["year_month", "payment_method"])
            .agg(total_transactions=("order_id", "count"))
            .reset_index()
        )

        # Calculate monthly totals (denominator for market share)
        total_counts = (
            paid_df.groupby("year_month")
            .agg(monthly_total_transactions=("order_id", "count"))
            .reset_index()
        )

        # Calculate market share percentage: (method transactions / total) × 100
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

        self.monthly_shares = merged_counts.reset_index()
        logger.info("✓ Monthly payment method market share calculated")

    def calculate_anomalies(self):
        """
        Detect statistical anomalies using z-score analysis.

        Flags observations where |z-score| > 2.5 (99.38% confidence interval).
        Anomalies indicate unusual spikes or drops in payment method usage.
        """
        logger.info(" -> Detecting anomalies in payment method usage... ")

        # Calculate historical statistics per payment method
        stats = (
            self.monthly_shares.groupby("payment_method")["share_pct"]
            .agg(["mean", "std"])
            .round(3)
        )

        self.monthly_shares = self.monthly_shares.merge(stats, on="payment_method")

        # Calculate z-score: (actual - mean) / std_dev
        self.monthly_shares["zscore"] = (
            self.monthly_shares["share_pct"] - self.monthly_shares["mean"]
        ) / self.monthly_shares["std"]

        # Flag anomalies beyond 2.5 standard deviations (99.38% confidence)
        self.monthly_shares["is_anomaly"] = self.monthly_shares["zscore"].abs() > 2.5

        logger.info("✓ Anomaly detection complete")

    def visualize_payment_trends(self):
        """
        Create dual-axis line chart showing payment method trends over time.

        Separates high-share and low-share methods for readability.
        Red 'X' markers indicate detected anomalies.
        """
        logger.info(" -> Visualizing payment method trends over time...")

        # Filter to top 5 payment methods by transaction volume
        top_n = 5
        top_payment_methods = (
            self.monthly_shares.groupby("payment_method")["total_transactions"]
            .sum()
            .nlargest(top_n)
            .index
        )

        top_n_data = self.monthly_shares[
            self.monthly_shares["payment_method"].isin(top_payment_methods)
        ]
        anomalies = top_n_data[top_n_data["is_anomaly"]]

        # Create dual-axis figure (high-share and low-share methods separated)
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

        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(
            f"{self.output_plots_dir}/payment_trends_line_chart.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

        logger.info("✓ Payment method trends visualization complete")

    def visualize_stacked_area_chart(self):
        """
        Create stacked area chart showing market share composition over time.

        Groups minor payment methods into 'Other' category for clarity.
        Shows relative proportions and market mix evolution.
        """
        logger.info(" -> Creating stacked area chart for all payment methods...")

        # Pivot to wide format: months × payment methods
        pivot_df = self.monthly_shares.pivot(
            index="year_month", columns="payment_method", values="share_pct"
        )

        # Group minor methods into "Other" category for readability
        top_n = 5
        top_methods = pivot_df.sum().nlargest(top_n).index
        pivot_df["Other"] = pivot_df.drop(columns=top_methods).sum(axis=1)
        pivot_df_simplified = pivot_df[list(top_methods) + ["Other"]].fillna(0)

        # Create stacked area chart (100% composition)
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
            f"{self.output_plots_dir}/payment_trends_stacked_area.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

        logger.info("✓ Stacked area chart created")

    def analyze_payment_amount_distribution(self):
        """
        Analyze payment amount distribution across payment methods using boxplots.

        Creates three-panel visualization (high, medium, low share methods).
        Filters to 99th percentile to remove extreme outliers.
        """
        logger.info(" -> Analyzing payment amount distribution by method...")

        paid_df = self.merged_df[self.merged_df["payment_status"] == "PAID"].copy()

        # Get top 5 payment methods
        top_n = 5
        top_payment_methods = (
            paid_df["payment_method"].value_counts().nlargest(top_n).index
        )

        top_data = paid_df[paid_df["payment_method"].isin(top_payment_methods)].copy()
        top_data["payment_amount"] = top_data["payment_amount"].astype(float)

        # Filter outliers: keep 99th percentile for clearer visualization
        percentile_99 = top_data["payment_amount"].quantile(0.99)
        top_data_filtered = top_data[top_data["payment_amount"] <= percentile_99]
        logger.info(f"   • Filtered to 99th percentile: ${percentile_99:.2f}")

        # Create three-panel boxplot (separated by market share level)
        fig, (ay1, ay2, ay3) = plt.subplots(1, 3, figsize=(18, 6))

        high_share_methods = ["ONLINE", "DEBIT"]
        middle_share_methods = ["VOUCHER"]
        low_share_methods = ["MEAL_BENEFIT", "STORE_DIRECT_PAYMENT"]

        # Panel 1: High market share methods
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

        # Panel 3: Low market share methods
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
            f"{self.output_plots_dir}/payment_amount_boxplot.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

        logger.info("✓ Payment amount distribution analysis complete")

    def save_results(self):
        """
        Export analysis results to CSV files.

        Saves: monthly shares, detected anomalies, and summary statistics by payment method.
        """
        logger.info(" -> Saving results to CSV files...")

        # Save monthly_shares DataFrame
        self.monthly_shares.to_csv(
            f"{self.output_report_dir}/monthly_payment_shares.csv", index=False
        )

        # Save only anomalies
        anomalies = self.monthly_shares[self.monthly_shares["is_anomaly"]]
        anomalies.to_csv(f"{self.output_report_dir}/payment_anomalies.csv", index=False)

        # Create summary DataFrame
        summary = (
            self.monthly_shares.groupby("payment_method")
            .agg(
                total_transactions=("total_transactions", "sum"),
                avg_market_share=("share_pct", "mean"),
                anomaly_count=("is_anomaly", "sum"),
            )
            .reset_index()
            .round(3)
        )
        summary.to_csv(
            f"{self.output_report_dir}/payment_method_summary.csv", index=False
        )

        logger.info("✓ Results saved successfully")

    def print_business_insights(self):
        """
        Generate executive summary with key business insights.

        Includes: analysis period, top payment methods, anomaly count, and recommendations.
        Saves formatted report to payment_trends_summary.txt.
        """
        logger.info("-> Printing business insights summary...")

        analysis_start = self.monthly_shares["year_month"].min()
        analysis_end = self.monthly_shares["year_month"].max()
        total_transactions = self.monthly_shares["total_transactions"].sum()

        # Top 3 payment methods
        top_n = 3
        top_methods = (
            self.monthly_shares.groupby("payment_method")["total_transactions"]
            .sum()
            .nlargest(top_n)
        )

        # Total anomalies detected
        total_anomalies = self.monthly_shares["is_anomaly"].sum()
        anomalies_sample = (
            self.monthly_shares[self.monthly_shares["is_anomaly"]]
            .sort_values("zscore", key=abs, ascending=False)[
                ["payment_method", "year_month", "share_pct", "zscore"]
            ]
            .head(2)
        )

        # Write into the file
        with open(f"{self.output_report_dir}/payment_trends_summary.txt", "w") as f:
            # Format the report
            report = "=" * 69 + "\n"
            report += "PAYMENT TRENDS ANALYSIS SUMMARY\n"
            report += "=" * 69 + "\n"
            report += f"Analysis Period: {analysis_start} to {analysis_end}\n"
            report += f"Total Transactions: {total_transactions:,}\n"
            report += f"\nTop {top_n} Payment Methods:\n"
            for i, (method, count) in enumerate(top_methods.items(), 1):
                pct = count / total_transactions * 100
                report += f"  {i}. {method}: {count:,} ({pct:.1f}%)\n"
            report += f"\nAnomalies Detected: {total_anomalies}\n"
            if not anomalies_sample.empty:
                for anomaly in anomalies_sample.itertuples(index=False):
                    report += f"  • {anomaly.payment_method} in {anomaly.year_month}: {anomaly.share_pct:.1f}% (z-score: {anomaly.zscore:.1f})\n"
                report += f"\nRecommendation: Investigate {anomalies_sample.iloc[0].year_month} {anomalies_sample.iloc[0].payment_method} anomaly - possible promotion?\n"
            else:
                report += "  • No significant anomalies detected.\n"
            report += "=" * 69 + "\n"
            f.write(report)

        logger.info("✓ Business insights summary printed")

    def seasonal_analysis(self):
        """
        Analyze seasonal patterns in payment method usage.

        Groups by calendar month (ignoring year) to identify recurring patterns.
        Creates heatmap: payment_method × month with average market share.
        """
        logger.info(" -> Performing seasonal analysis of payment methods...")

        # Extract month from year_month
        self.monthly_shares["month"] = (
            self.monthly_shares["year_month"].dt.to_timestamp().dt.month_name().str[:3]
        )

        # Group by month and payment_method to calculate average share_pct
        seasonal_data = (
            self.monthly_shares.groupby(["month", "payment_method"])["share_pct"]
            .mean()
            .reset_index()
        )

        # Pivot for heatmap
        heatmap_data = seasonal_data.pivot(
            index="payment_method", columns="month", values="share_pct"
        ).fillna(0)

        # Create heatmap
        plt.figure(figsize=(12, 8))
        sns.heatmap(
            heatmap_data,
            annot=True,
            fmt=".1f",
            cmap="YlGnBu",
            linewidths=0.5,
            linecolor="gray",
            cbar_kws={"label": "Average Market Share (%)"},
        )
        plt.title("Seasonal Payment Method Patterns")
        plt.xlabel("Month")
        plt.ylabel("Payment Method")
        plt.tight_layout()

        # Save plot
        plt.savefig(
            f"{self.output_plots_dir}/seasonal_payment_patterns.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

        logger.info("✓ Seasonal analysis complete")

    def correlation_analysis(self):
        """
        Calculate Pearson correlations between payment methods.

        Negative correlation indicates substitution effect (users switch between methods).
        Positive correlation suggests methods used by similar customer segments.
        """
        logger.info(" -> Performing correlation analysis between payment methods...")

        # Pivot to create a wide format DataFrame
        corr_data = self.monthly_shares.pivot(
            index="year_month", columns="payment_method", values="share_pct"
        ).fillna(0)

        # Calculate correlation matrix
        corr_matrix = corr_data.corr()

        # Extract ONLINE vs CREDIT CORRELATION
        online_credit_corr = corr_matrix.loc["ONLINE", "CREDIT"]
        logger.info(
            f"   • Correlation between ONLINE and CREDIT: {online_credit_corr:.3f}"
        )

        # Interpret correlation
        if online_credit_corr < -0.5:
            logger.info(
                "     - Strong negative correlation: substitution effect likely."
            )
        elif online_credit_corr > 0.5:
            logger.info("     - Strong positive correlation: methods used together.")
        else:
            logger.info("     - Weak correlation: little relationship between methods.")

        # Filter to top 5 payment methods for better visualization
        top_n = 5
        top_methods = (
            self.monthly_shares.groupby("payment_method")["total_transactions"]
            .sum()
            .nlargest(top_n)
            .index
        )
        filtered_corr_matrix = corr_matrix.loc[top_methods, top_methods]

        # Create heatmap
        plt.figure(figsize=(10, 8))
        sns.heatmap(
            filtered_corr_matrix,
            annot=True,
            fmt=".2f",
            cmap="coolwarm",
            vmin=-1,
            vmax=1,
            linewidths=0.5,
            linecolor="gray",
            cbar_kws={"label": "Correlation Coefficient"},
        )
        plt.title("Payment Method Correlation Analysis")
        plt.xlabel("Payment Method")
        plt.ylabel("Payment Method")
        plt.xticks(rotation=45, ha="right")
        plt.yticks(rotation=0)
        plt.tight_layout()

        # Save plot
        plt.savefig(
            f"{self.output_plots_dir}/payment_method_correlations.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.show()

        logger.info("✓ Correlation analysis complete")

    def run(self):
        """
        Execute complete payment trends analysis pipeline.

        Pipeline stages:
        1. Load and prepare data
        2. Calculate monthly market shares
        3. Detect statistical anomalies
        4. Generate 5 visualizations
        5. Export results and insights
        """
        self.load_data()
        self.prepare_data()
        self.merge_orders_payments()
        self.extract_time_components()
        self.calculate_monthly_payment_shares()
        self.calculate_anomalies()
        self.visualize_payment_trends()
        self.visualize_stacked_area_chart()
        self.analyze_payment_amount_distribution()
        self.save_results()
        self.print_business_insights()
        self.seasonal_analysis()
        self.correlation_analysis()


# Entry point for payment trends analysis
if __name__ == "__main__":
    analyzer = PaymentTrendsAnalyzer()
    analyzer.run()

    print("✅ Task E: Payment Trends Analysis Complete!")
    print("📁 Check results/task-e/ for CSV files and visualizations")
