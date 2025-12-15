import pandas as pd
import pytest

@pytest.fixture
def extract_data_mock():
    return {
        "review": pd.DataFrame({
            "review_id": [1, 2, 3, 4],
            "buyer_id": ["A", "B", "C", "D"],
            "r_desc": ["Good product", "", "<b>Bad</b>", "       "],
            "rating": [5, 4, 6, 1]
        }),
        "review_images": pd.DataFrame({
            "review_id": [1]
        }),
        "orders": pd.DataFrame({
            "buyer_id": ["A"]
        }),
        "subscription": pd.DataFrame({
            "c_id": ["A"],
            "end_date": ["2099-01-01"]
        }),
        "product_reviews": pd.DataFrame({
            "review_id": [1, 2],
            "p_id": ["P1", "P2"]
        })
    }