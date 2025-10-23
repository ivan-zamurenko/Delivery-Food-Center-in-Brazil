# Channel & Payment Mix Profitability Script
# ------------------------------------------
# Calculates profitability metrics for each channel and payment method.
# Loads data from CSVs, merges relevant tables, computes revenue, and outputs results to CSV.

import analysis.queries as queries
from analysis.db_connection import get_engine


def get_channel_profitability_data():
    """
    Main function to fetch, merge, and analyze channel/payment profitability.
    Steps:
    1. Read data from Postgres database.
    2. Merge tables to create a unified DataFrame with all relevant columns.
    3. Calculate total revenue and payment count per channel/payment method.
    4. Save results to CSV and return the DataFrame.
    """
    engine = get_engine()

    orders_df = queries.fetch_orders(engine)
    payments_df = queries.fetch_payments(engine)
    merged_df = orders_df.merge(
        payments_df,
        left_on="order_id",
        right_on="payment_order_id",
    )
    # Calculate profitability metrics and save results
    profitability_df = calculate_profitability(merged_df)
    profitability_df.to_csv(
        "../results/task-a/channel_payment_profitability.csv", index=True
    )
    return profitability_df


def calculate_profitability(merged_df):
    """
    Calculate total revenue and payment count for each channel/payment method.
    - Groups by 'channel_id' and 'payment_method' and computes:
        * payments_count: number of payments
        * total_revenue: sum of payment_amount minus payment_fee
    - Returns a sorted DataFrame with results.
    """
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


# Entry point for script execution
if __name__ == "__main__":
    # Run the main analysis and save results
    get_channel_profitability_data()
