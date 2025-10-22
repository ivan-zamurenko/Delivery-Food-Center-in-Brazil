import analysis.queries as queries
from analysis.db_connection import get_engine


def calculate_profitability(merged_df):
    """Aggregate profitability by channel and payment method."""
    return (
        merged_df.groupby(["channel_id", "payment_method"])
        .agg(
            payments_count=("payment_id", "count"),
            total_revenue=(
                "payment_amount",
                lambda x: (x - merged_df.loc[x.index, "payment_fee"]).sum(),
            ),
        )
        .reset_index()
        .sort_values(by="total_revenue", ascending=False)
    )


def get_channel_profitability_data():
    """Fetch data, merge and calculate profitability."""
    engine = get_engine()

    orders_df = queries.fetch_orders(engine)
    payments_df = queries.fetch_payments(engine)
    merged_df = orders_df.merge(
        payments_df,
        left_on="order_id",
        right_on="payment_order_id",
    )
    profitability_df = calculate_profitability(merged_df)
    profitability_df.to_csv(
        "../results/task-a/channel_payment_profitability.csv", index=True
    )
    return profitability_df


if __name__ == "__main__":
    get_channel_profitability_data()
