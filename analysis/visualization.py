import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# ----------------------------------------
# Visualization Utilities for Analysis
# ----------------------------------------
# This module provides functions to visualize key business metrics and delivery performance.
# Plots include heatmaps for profitability, boxplots/violin plots for delivery time distributions,
# and bar charts for average delivery times by driver modal/type.


# This function creates a heatmap showing the profitability (total revenue) for each channel and payment method combination.
# It helps business stakeholders quickly identify which channels and payment types are most profitable, using color intensity and log scaling for clarity.
# The chart is saved for reporting and displayed for interactive analysis.
def channel_profitability_heatmap(profitability_df):
    """
    Generate a heatmap to visualize profitability by channel and payment method.
    Each cell shows the (log-scaled) total revenue for a channel/payment combination.
    Steps:
    1. Pivot the DataFrame so rows are channels and columns are payment methods.
    2. Fill missing values with 0 for completeness.
    3. Apply log scaling to compress large outliers and handle zeros.
    4. Plot the heatmap with annotations and color intensity for revenue.
    5. Save the figure and show it.
    """
    # Pivot and sort for better readability
    heatmap_df = (
        profitability_df.pivot(
            index="channel_id", columns="payment_method", values="total_revenue"
        )
        .sort_index()
        .sort_index(axis=1)
        .fillna(0)
    )

    # Apply log scaling to compress large outliers and handle zeros
    log_heatmap_df = np.log1p(heatmap_df)  # log(1 + x) to handle zeros

    # Set figure size dynamically based on number of columns and rows
    plt.figure(
        figsize=(
            max(10, 1.5 * len(log_heatmap_df.columns)),
            max(6, 0.5 * len(log_heatmap_df.index)),
        )
    )
    # Draw the heatmap with annotations, color map, and grid lines
    sns.heatmap(
        log_heatmap_df,
        annot=True,
        fmt=".2f",
        cmap="YlGnBu",
        linewidths=0.5,
        linecolor="gray",
        vmin=0,
        vmax=log_heatmap_df.max().max(),
    )
    # Add titles and axis labels for clarity
    plt.title("Channel Profitability Heatmap (Log Scale)")
    plt.xlabel("Payment Method")
    plt.ylabel("Channel ID")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    # Save the heatmap to file for documentation and reproducibility
    plt.savefig(
        "../results/task-a/plots/channel_profitability_heatmap.png",
        dpi=300,
        bbox_inches="tight",
    )

    # Show the plot in the notebook or script
    plt.show()


# Delivery Time Analysis Visualization
# -----------------------------------
# This module provides functions to visualize delivery time distributions by driver modal/type.
# Recommended plots: boxplot (for spread/outliers), violin plot (for distribution shape), and bar chart (for means/medians).
def plot_delivery_time_by_driver_modal(delivery_time_df):
    """
    Visualize delivery time distribution by driver modal/type using boxplot and violin plot.
    Shows both spread/outliers and distribution shape, with mean overlay.
    Steps:
    1. Set outlier threshold for y-axis (business logic).
    2. Draw boxplot to show spread, median, and outliers for each driver modal.
    3. Draw violin plot to show distribution shape and quartiles.
    4. Overlay mean delivery time as a red diamond on violin plot.
    5. Save figures and show them.
    """
    outlier_threshold = 120  # minutes

    # --- Boxplot ---
    plt.figure(figsize=(8, 6))
    sns.boxplot(
        x="driver_modal",
        y="delivery_time_minutes",
        data=delivery_time_df,
        showfliers=True,
    )

    plt.ylim(
        0, outlier_threshold
    )  # Set y-axis limits to focus on relevant delivery times
    plt.title("Delivery Time Distribution by Driver Modal")
    plt.xlabel("Driver Modal")
    plt.ylabel("Delivery Time (Minutes)")
    plt.tight_layout()
    # Save boxplot to file
    plt.savefig(
        "../results/task-b/plots/delivery_time_distribution_boxplot.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.show()

    # --- Violin Plot ---
    plt.figure(figsize=(8, 6))
    ax = sns.violinplot(
        x="driver_modal",
        y="delivery_time_minutes",
        data=delivery_time_df,
        inner="quartile",
    )

    # Calculate mean delivery time for each driver modal
    means = delivery_time_df.groupby("driver_modal")["delivery_time_minutes"].mean()
    # Overlay mean as a red diamond on the violin plot
    for i, modal in enumerate(means.index):
        ax.scatter(
            i,
            means[modal],
            color="red",
            marker="D",
            s=100,
            label="Mean" if i == 0 else "",
        )
    handles, labels = ax.get_legend_handles_labels()
    if "Mean" in labels:
        ax.legend(["Mean"], loc="upper right")

    plt.ylim(
        0, outlier_threshold
    )  # Set y-axis limits to focus on relevant delivery times
    plt.title("Delivery Time Distribution (Violin) by Driver Modal")
    plt.xlabel("Driver Modal")
    plt.ylabel("Delivery Time (Minutes)")
    plt.tight_layout()
    # Save violin plot to file
    plt.savefig(
        "../results/task-b/plots/delivery_time_distribution_violin.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.show()


# ------------------------------------------
# This function creates a bar chart showing the average delivery time for each driver modal (e.g., Motoboy, Biker, Unknown).
# It provides a business summary of delivery performance by driver type, helping stakeholders quickly compare efficiency across categories.
# The chart uses custom colors for clarity, saves the figure for reporting, and displays it for interactive analysis.
def plot_average_delivery_time_by_driver_modal(avg_delivery_time_df):
    """
    Bar chart to visualize average delivery time by driver modal/type.
    Shows business summary for each driver type.
    Steps:
    1. Define a custom color palette for each driver modal for clarity.
    2. Draw bar chart to show average delivery time for each driver modal.
    3. Add titles and axis labels for clarity.
    4. Save figure and show it.
    """
    # Step 1: Define a custom color palette for each driver modal for clarity in the plot
    palette = {
        "Unknown": "#1f77b4",  # Blue for Unknown modal
        "BIKER": "#ff7f0e",  # Orange for Biker modal
        "MOTOBOY": "#2ca02c",  # Green for Motoboy modal
    }

    # Step 2: Create a new figure for the bar chart with a fixed size
    plt.figure(figsize=(8, 6))

    # Step 3: Draw the bar chart using seaborn
    # - x: driver_modal (category on x-axis)
    # - y: average_delivery_time_minutes (height of each bar)
    # - hue: driver_modal (ensures each bar gets its palette color)
    # - palette: use the custom palette defined above
    sns.barplot(
        x="driver_modal",
        y="average_delivery_time_minutes",
        data=avg_delivery_time_df.reset_index(),
        hue="driver_modal",
        palette=palette,
    )

    # Step 4: Add a descriptive title and axis labels for clarity
    plt.title("Average Delivery Time by Driver Modal")
    plt.xlabel("Driver Modal")
    plt.ylabel("Average Delivery Time (Minutes)")
    plt.tight_layout()

    # Step 5: Save the bar chart to file for documentation and reproducibility
    plt.savefig(
        "../results/task-b/plots/average_delivery_time_by_driver_modal.png",
        dpi=300,
        bbox_inches="tight",
    )

    # Step 6: Show the plot in the notebook or script
    plt.show()
