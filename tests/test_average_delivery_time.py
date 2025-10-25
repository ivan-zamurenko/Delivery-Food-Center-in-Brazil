import pandas as pd
import scripts.run_delivery_time_optimization as delivery_time_optimization


def test_calculate_delivery_time_statistics():
    # Create mock filtered DataFrame
    data = {
        "order_id": [1, 2, 3, 4, 5],
        "order_moment_delivering": pd.to_datetime(
            [
                "2023-01-01 10:00:00",
                "2023-01-01 11:00:00",
                "2023-01-01 12:00:00",
                "2023-01-01 13:00:00",
                "2023-01-01 14:00:00",
            ]
        ),
        "order_moment_delivered": pd.to_datetime(
            [
                "2023-01-01 10:30:00",
                "2023-01-01 11:45:00",
                "2023-01-01 12:20:00",
                "2023-01-01 13:50:00",
                "2023-01-01 14:10:00",
            ]
        ),
        "driver_id": [101, 102, 101, 103, 102],
        "delivery_status": ["DELIVERED"] * 5,
        "delivery_order_id": [1, 2, 3, 4, 5],
        "driver_modal": ["MOTOBOY", "BIKER", "MOTOBOY", "UNKNOWN", "BIKER"],
    }
    filtered_df = pd.DataFrame(data)

    result_df = delivery_time_optimization.calculate_delivery_time_statistics(
        filtered_df, save_fig=False
    )

    # Check results
    expected_avg_times = {
        "MOTOBOY": (30 + 20) / 2,  # Average of delivery times for MOTOBOY
        "BIKER": (45 + 10) / 2,  # Average of delivery times for BIKER
        "UNKNOWN": 50,  # Only one entry for UNKNOWN
    }

    for _, row in result_df.iterrows():
        driver_modal = row["driver_modal"]
        assert row["average_delivery_time_minutes"] == expected_avg_times[driver_modal]
        if driver_modal == "MOTOBOY":
            assert row["delivery_orders_count"] == 2
        elif driver_modal == "BIKER":
            assert row["delivery_orders_count"] == 2
        elif driver_modal == "UNKNOWN":
            assert row["delivery_orders_count"] == 1
