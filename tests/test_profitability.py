import pandas as pd
from scripts.run_profitability import calculate_profitability


def test_calculate_profitability():
    # Create mock merged DataFrame
    data = {
        "channel_id": [1, 1, 2, 2],
        "payment_method": ["CREDIT", "DEBIT", "CREDIT", "DEBIT"],
        "payment_id": [101, 102, 201, 202],
        "payment_amount": [1000, 500, 2000, 1500],
        "payment_fee": [100, 50, 200, 150],
    }
    merged_df = pd.DataFrame(data)
    result_df = calculate_profitability(merged_df)

    # Check results
    expected = {
        (1, "CREDIT"): {"payments_count": 1, "total_revenue": 900},
        (1, "DEBIT"): {"payments_count": 1, "total_revenue": 450},
        (2, "CREDIT"): {"payments_count": 1, "total_revenue": 1800},
        (2, "DEBIT"): {"payments_count": 1, "total_revenue": 1350},
    }

    for _, row in result_df.iterrows():
        key = (row["channel_id"], row["payment_method"])
        assert row["payments_count"] == expected[key]["payments_count"]
        assert row["total_revenue"] == expected[key]["total_revenue"]
