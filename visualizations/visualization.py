import matplotlib.pyplot as plt
import seaborn as sns


def plot_orders(data):
    """Plot order creation and completion times."""
    plt.plot(data["order_moment_created"], data["order_moment_finished"])
    plt.show()
