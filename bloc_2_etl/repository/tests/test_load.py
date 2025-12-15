import pandas as pd
from src.etl.load import load_all
import pytest

def test_load_accepts_dataframe(monkeypatch, tmp_path):
    df = pd.DataFrame({
        "review_id": [1],
        "rating": [5],
        "has_image": [False],
        "verified_buyer": [True]
    })

    monkeypatch.setattr(
        "src.etl.load.CLEANED_DIR",
        tmp_path
    )

    parquet_path = load_all(df_reviews=df)

    assert parquet_path.exists()
    assert parquet_path.suffix == ".parquet"


def test_load_fails_on_empty_dataframe():
    df = pd.DataFrame()

    with pytest.raises(SystemExit):
        load_all(df_reviews=df)


def test_load_without_rejects(monkeypatch, tmp_path):
    df = pd.DataFrame({
        "review_id": [1],
        "rating": [4],
        "has_image": [False],
        "verified_buyer": [False]
    })

    monkeypatch.setattr(
        "src.etl.load.CLEANED_DIR",
        tmp_path
    )

    parquet_path = load_all(df_reviews=df, df_rejects=None)

    assert parquet_path.exists()