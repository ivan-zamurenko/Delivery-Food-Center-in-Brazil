import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np


def channel_profitability_heatmap(profitability_df):
    """Generate a heatmap to visualize profitability by channel and payment method."""
    # Pivot and sort for better readability
    heatmap_df = (
        profitability_df.pivot(
            index="channel_id", columns="payment_method", values="total_revenue"
        )
        .sort_index()
        .sort_index(axis=1)
        .fillna(0)
    )

    #! Apply log scaling to compress large outliers
    log_heatmap_df = np.log1p(heatmap_df)  # log(1 + x) to handle zeros

    plt.figure(
        figsize=(
            max(10, 1.5 * len(log_heatmap_df.columns)),
            max(6, 0.5 * len(log_heatmap_df.index)),
        )
    )
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
    plt.title("Channel Profitability Heatmap (Log Scale)")
    plt.xlabel("Payment Method")
    plt.ylabel("Channel ID")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    # ? Save heatmap to file
    plt.savefig(
        "../notebook/task-a/channel_profitability_heatmap.png",
        dpi=300,
        bbox_inches="tight",
    )

    plt.show()
