"""
Task C: Hub Coverage & Store Network Analysis

This script analyzes the geographical distribution of hubs and stores to:
1. Calculate hub coverage metrics (stores per hub, revenue per hub)
2. Identify optimization opportunities (high-potential hubs, consolidation targets)
3. Generate geographical visualizations for strategic planning

Author: Ivan Zamurenko
Date: October 30, 2025
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path


class HubCoverageAnalyzer:
    """Analyze hub coverage and store network distribution."""

    def __init__(
        self, data_dir: str = "data/data-cleaned", output_dir: str = "results/task-c"
    ):
        """
        Initialize the hub coverage analyzer.

        Args:
            data_dir: Directory containing input CSV files
            output_dir: Directory for saving analysis results
        """

        # Get project root directory
        project_root = Path(__file__).parent.parent

        self.data_dir = project_root / data_dir
        self.output_dir = project_root / output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.output_reports_dir = self.output_dir / "reports"
        self.output_reports_dir.mkdir(parents=True, exist_ok=True)

        self.output_plots_dir = self.output_dir / "plots"
        self.output_plots_dir.mkdir(parents=True, exist_ok=True)

        self.hubs = None
        self.stores = None
        self.orders = None
        self.payments = None
        self.hub_metrics = None

    def load_data(self):
        """Load required datasets from CSV files."""
        print("Loading data files...")

        self.hubs = pd.read_csv(self.data_dir / "hubs_cleaned.csv", encoding="latin1")

        self.stores = pd.read_csv(
            self.data_dir / "stores_cleaned.csv", encoding="latin1"
        )

        self.orders = pd.read_csv(
            self.data_dir / "orders_cleaned.csv", encoding="latin1"
        )

        self.payments = pd.read_csv(
            self.data_dir / "payments_cleaned.csv", encoding="latin1"
        )

        print(
            f"Loaded: {len(self.hubs)} hubs, {len(self.stores)} stores, "
            f"{len(self.orders)} orders, {len(self.payments)} payments"
        )

    def calculate_hub_metrics(self):
        """
        Calculate key metrics for each hub:
        - Number of stores
        - Total revenue
        - Average revenue per store
        - Order volume
        """
        print("\nCalculating hub metrics...")

        # Count stores per hub
        stores_per_hub = (
            self.stores.groupby("hub_id").size().reset_index(name="store_count")
        )

        # Calculate revenue per hub via stores
        # Join orders -> stores to get hub_id
        orders_with_hub = self.orders.merge(
            self.stores[["store_id", "hub_id"]], on="store_id", how="left"
        )

        # Join with payments to get revenue
        orders_revenue = orders_with_hub.merge(
            self.payments[["payment_order_id", "payment_amount"]],
            left_on="order_id",
            right_on="payment_order_id",
            how="left",
        )

        # Aggregate by hub
        hub_revenue = (
            orders_revenue.groupby("hub_id")
            .agg({"order_id": "count", "payment_amount": "sum"})
            .reset_index()
        )
        hub_revenue.columns = ["hub_id", "order_count", "total_revenue"]

        # Combine all metrics
        self.hub_metrics = stores_per_hub.merge(hub_revenue, on="hub_id", how="left")

        # Add hub information (name, city, state)
        self.hub_metrics = self.hub_metrics.merge(
            self.hubs[["hub_id", "hub_name", "hub_city", "hub_state"]],
            on="hub_id",
            how="left",
        )

        # Fill NaN values (hubs with no orders)
        self.hub_metrics["order_count"] = (
            self.hub_metrics["order_count"].fillna(0).astype(int)
        )
        self.hub_metrics["total_revenue"] = self.hub_metrics["total_revenue"].fillna(0)

        # Calculate revenue per store
        self.hub_metrics["revenue_per_store"] = (
            self.hub_metrics["total_revenue"] / self.hub_metrics["store_count"]
        ).round(2)

        # Sort by total revenue
        self.hub_metrics = self.hub_metrics.sort_values(
            "total_revenue", ascending=False
        )

        print(f"Calculated metrics for {len(self.hub_metrics)} hubs")

    def identify_opportunities(self):
        """
        Identify strategic opportunities:
        1. High-potential hubs (many stores, low revenue)
        2. Consolidation targets (few stores, low revenue)
        3. Top performers (high revenue per store)
        """
        print("\nIdentifying strategic opportunities...")

        # Calculate quartiles for segmentation
        store_q75 = self.hub_metrics["store_count"].quantile(0.75)
        revenue_q25 = self.hub_metrics["total_revenue"].quantile(0.25)
        revenue_per_store_q75 = self.hub_metrics["revenue_per_store"].quantile(0.75)

        # High-potential: many stores, low revenue (untapped potential)
        high_potential = self.hub_metrics[
            (self.hub_metrics["store_count"] >= store_q75)
            & (self.hub_metrics["total_revenue"] <= revenue_q25)
        ].copy()

        # Consolidation targets: few stores, low revenue
        consolidation_targets = self.hub_metrics[
            (self.hub_metrics["store_count"] < store_q75)
            & (self.hub_metrics["total_revenue"] <= revenue_q25)
        ].copy()

        # Top performers: high revenue per store
        top_performers = self.hub_metrics[
            self.hub_metrics["revenue_per_store"] >= revenue_per_store_q75
        ].copy()

        # Save opportunity reports
        high_potential.to_csv(
            self.output_reports_dir / "high_potential_hubs.csv", index=False
        )
        consolidation_targets.to_csv(
            self.output_reports_dir / "consolidation_targets.csv", index=False
        )
        top_performers.to_csv(
            self.output_reports_dir / "top_performing_hubs.csv", index=False
        )

        print(f"  High-potential hubs: {len(high_potential)}")
        print(f"  Consolidation targets: {len(consolidation_targets)}")
        print(f"  Top performers: {len(top_performers)}")

        return high_potential, consolidation_targets, top_performers

    def visualize_hub_coverage(self):
        """Create comprehensive visualizations of hub coverage."""
        print("\nGenerating visualizations...")

        # 1. Scatter plot: Store count vs Total revenue
        fig, ax = plt.subplots(figsize=(12, 8))

        scatter = ax.scatter(
            self.hub_metrics["store_count"],
            self.hub_metrics["total_revenue"],
            s=self.hub_metrics["revenue_per_store"] / 100,  # Size by efficiency
            alpha=0.6,
            c=self.hub_metrics["total_revenue"],
            cmap="viridis",
        )

        ax.set_xlabel("Number of Stores", fontsize=12)
        ax.set_ylabel("Total Revenue (R$)", fontsize=12)
        ax.set_title(
            "Hub Coverage Analysis: Stores vs Revenue", fontsize=14, fontweight="bold"
        )
        ax.grid(alpha=0.3)

        # Add colorbar
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label("Total Revenue (R$)", fontsize=10)

        # Annotate top 5 hubs
        top_5 = self.hub_metrics.head(5)
        for _, row in top_5.iterrows():
            ax.annotate(
                row["hub_name"],
                (row["store_count"], row["total_revenue"]),
                xytext=(10, 10),
                textcoords="offset points",
                fontsize=8,
                bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.3),
            )

        plt.tight_layout()
        plt.savefig(
            self.output_plots_dir / "hub_coverage_scatter.png",
            dpi=300,
            bbox_inches="tight",
        )
        print("  Saved: hub_coverage_scatter.png")
        plt.close()

        # 2. Bar chart: Top 15 hubs by revenue
        fig, ax = plt.subplots(figsize=(14, 8))

        top_15 = self.hub_metrics.head(15)
        bars = ax.barh(
            range(len(top_15)),
            top_15["total_revenue"],
            color=sns.color_palette("rocket", len(top_15)),
        )

        ax.set_yticks(range(len(top_15)))
        ax.set_yticklabels(top_15["hub_name"], fontsize=10)
        ax.set_xlabel("Total Revenue (R$)", fontsize=12)
        ax.set_title("Top 15 Hubs by Total Revenue", fontsize=14, fontweight="bold")
        ax.grid(axis="x", alpha=0.3)

        # Add value labels
        for i, (bar, row) in enumerate(zip(bars, top_15.itertuples())):
            ax.text(
                bar.get_width(),
                bar.get_y() + bar.get_height() / 2,
                f" R${row.total_revenue:,.0f} ({row.store_count} stores)",
                va="center",
                fontsize=8,
            )

        plt.tight_layout()
        plt.savefig(
            self.output_plots_dir / "top_hubs_revenue.png", dpi=300, bbox_inches="tight"
        )
        print("  Saved: top_hubs_revenue.png")
        plt.close()

        # 3. Dual bar chart: Store count vs Revenue comparison
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

        top_10 = self.hub_metrics.head(10)

        # Store count
        ax1.barh(range(len(top_10)), top_10["store_count"], color="steelblue")
        ax1.set_yticks(range(len(top_10)))
        ax1.set_yticklabels(top_10["hub_name"], fontsize=10)
        ax1.set_xlabel("Number of Stores", fontsize=12)
        ax1.set_title("Top 10 Hubs by Store Count", fontsize=12, fontweight="bold")
        ax1.grid(axis="x", alpha=0.3)

        # Revenue
        ax2.barh(range(len(top_10)), top_10["total_revenue"], color="coral")
        ax2.set_yticks(range(len(top_10)))
        ax2.set_yticklabels(top_10["hub_name"], fontsize=10)
        ax2.set_xlabel("Total Revenue (R$)", fontsize=12)
        ax2.set_title("Top 10 Hubs by Revenue", fontsize=12, fontweight="bold")
        ax2.grid(axis="x", alpha=0.3)

        plt.tight_layout()
        plt.savefig(
            self.output_plots_dir / "hub_comparison.png", dpi=300, bbox_inches="tight"
        )
        print("  Saved: hub_comparison.png")
        plt.close()

        # 4. Distribution plots
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))

        # Store count distribution
        axes[0, 0].hist(
            self.hub_metrics["store_count"], bins=20, color="skyblue", edgecolor="black"
        )
        axes[0, 0].set_xlabel("Stores per Hub", fontsize=10)
        axes[0, 0].set_ylabel("Frequency", fontsize=10)
        axes[0, 0].set_title(
            "Distribution of Store Count", fontsize=11, fontweight="bold"
        )
        axes[0, 0].grid(alpha=0.3)

        # Revenue distribution
        axes[0, 1].hist(
            self.hub_metrics["total_revenue"],
            bins=20,
            color="lightcoral",
            edgecolor="black",
        )
        axes[0, 1].set_xlabel("Total Revenue (R$)", fontsize=10)
        axes[0, 1].set_ylabel("Frequency", fontsize=10)
        axes[0, 1].set_title(
            "Distribution of Hub Revenue", fontsize=11, fontweight="bold"
        )
        axes[0, 1].grid(alpha=0.3)

        # Revenue per store distribution
        axes[1, 0].hist(
            self.hub_metrics["revenue_per_store"].dropna(),
            bins=20,
            color="lightgreen",
            edgecolor="black",
        )
        axes[1, 0].set_xlabel("Revenue per Store (R$)", fontsize=10)
        axes[1, 0].set_ylabel("Frequency", fontsize=10)
        axes[1, 0].set_title(
            "Distribution of Revenue Efficiency", fontsize=11, fontweight="bold"
        )
        axes[1, 0].grid(alpha=0.3)

        # Order count distribution
        axes[1, 1].hist(
            self.hub_metrics["order_count"], bins=20, color="plum", edgecolor="black"
        )
        axes[1, 1].set_xlabel("Orders per Hub", fontsize=10)
        axes[1, 1].set_ylabel("Frequency", fontsize=10)
        axes[1, 1].set_title(
            "Distribution of Order Volume", fontsize=11, fontweight="bold"
        )
        axes[1, 1].grid(alpha=0.3)

        plt.tight_layout()
        plt.savefig(
            self.output_plots_dir / "hub_distributions.png",
            dpi=300,
            bbox_inches="tight",
        )
        print("  Saved: hub_distributions.png")
        plt.close()

    def generate_summary_report(self):
        """Generate executive summary report."""
        print("\nGenerating summary report...")

        total_hubs = len(self.hub_metrics)
        total_stores = self.hub_metrics["store_count"].sum()
        total_revenue = self.hub_metrics["total_revenue"].sum()
        total_orders = self.hub_metrics["order_count"].sum()

        avg_stores_per_hub = self.hub_metrics["store_count"].mean()
        avg_revenue_per_hub = self.hub_metrics["total_revenue"].mean()
        avg_revenue_per_store = self.hub_metrics["revenue_per_store"].mean()

        # Top hubs
        top_3_hubs = self.hub_metrics.head(3)

        report = []
        report.append("=" * 80)
        report.append("HUB COVERAGE & STORE NETWORK ANALYSIS - EXECUTIVE SUMMARY")
        report.append("=" * 80)
        report.append("")
        report.append("OVERVIEW")
        report.append("-" * 80)
        report.append(f"Total Hubs:                     {total_hubs:>12}")
        report.append(f"Total Stores:                   {total_stores:>12,}")
        report.append(f"Total Orders:                   {total_orders:>12,}")
        report.append(f"Total Revenue:                  R$ {total_revenue:>12,.2f}")
        report.append("")
        report.append("AVERAGES")
        report.append("-" * 80)
        report.append(f"Stores per Hub:                 {avg_stores_per_hub:>12.1f}")
        report.append(
            f"Revenue per Hub:                R$ {avg_revenue_per_hub:>12,.2f}"
        )
        report.append(
            f"Revenue per Store:              R$ {avg_revenue_per_store:>12,.2f}"
        )
        report.append("")
        report.append("TOP 3 PERFORMING HUBS")
        report.append("-" * 80)

        for i, row in enumerate(top_3_hubs.itertuples(), 1):
            report.append(f"{i}. {row.hub_name} ({row.hub_city}, {row.hub_state})")
            report.append(
                f"   Stores: {row.store_count} | Revenue: R${row.total_revenue:,.2f} | "
                f"Orders: {row.order_count:,}"
            )
            report.append("")

        report.append("STRATEGIC INSIGHTS")
        report.append("-" * 80)
        report.append(
            "• High-potential hubs identified (many stores, underperforming revenue)"
        )
        report.append("• Consolidation opportunities for low-performing hubs")
        report.append("• Top performers can serve as operational benchmarks")
        report.append("")
        report.append("=" * 80)

        summary_text = "\n".join(report)

        # Save to file
        with open(
            self.output_reports_dir / "hub_coverage_summary.txt", "w", encoding="utf-8"
        ) as f:
            f.write(summary_text)

        print(summary_text)
        print(
            f"\nSaved summary to: {self.output_reports_dir / 'hub_coverage_summary.txt'}"
        )

    def save_results(self):
        """Save all hub metrics to CSV."""
        output_file = self.output_reports_dir / "hub_metrics.csv"
        self.hub_metrics.to_csv(output_file, index=False)
        print(f"\nSaved hub metrics to: {output_file}")

    def run_analysis(self):
        """Execute complete hub coverage analysis pipeline."""
        print("=" * 80)
        print("TASK C: HUB COVERAGE & STORE NETWORK ANALYSIS")
        print("=" * 80)

        self.load_data()
        self.calculate_hub_metrics()
        self.identify_opportunities()
        self.visualize_hub_coverage()
        self.generate_summary_report()
        self.save_results()

        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE!")
        print("=" * 80)
        print(f"\nAll results saved to: {self.output_dir}")


def main():
    """Main entry point for hub coverage analysis."""
    analyzer = HubCoverageAnalyzer()
    analyzer.run_analysis()


if __name__ == "__main__":
    main()
