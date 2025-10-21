import matplotlib.pyplot as plt


def plot_orders(df):
    """Plot order creation and completion times."""
    plt.plot(df["order_moment_created"], df["order_moment_finished"])
    plt.show()
