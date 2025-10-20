import matplotlib.pyplot as plt


def plot_orders(df):
    """Plot order creation and completion times."""
    plt.plot(df["order_moment_created"], df["order_moment_finished"])
    plt.show()


def horizontal_bar_chart_order_channels_distribution(df):
    """Create a horizontal bar chart for order channel distribution."""
    df_sorted = df.sort_values("total_orders", ascending=True)

    plt.figure(figsize=(10, 6))
    plt.barh(
        df_sorted["channel_name"],
        df_sorted["percentage_orders"],
        height=0.5,
        color="skyblue",
    )
    plt.xlabel("Percentage of Orders (%)")
    plt.title("Order Distribution by Channel")
    plt.tight_layout()
    plt.show()
