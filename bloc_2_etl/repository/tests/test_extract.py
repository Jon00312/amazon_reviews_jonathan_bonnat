from src.etl.extract import extract_all
import pandas as pd
import pytest


def test_extract_returns_dict(monkeypatch):
    def fake_read_sql(query, conn):
        return pd.DataFrame({"id": [1, 2]})

    monkeypatch.setattr("pandas.read_sql", fake_read_sql)

    data = extract_all()

    assert isinstance(data, dict)
    assert "review" in data
    assert isinstance(data["review"], pd.DataFrame)

def test_extract_handles_empty_table(monkeypatch):
    import pandas as pd

    monkeypatch.setattr(
        "pandas.read_sql",
        lambda q, c: pd.DataFrame()
    )

    data = extract_all()

    assert data["review"].empty

def test_extract_fails_cleanly_if_db_down(monkeypatch):
    class FakeEngine:
        def connect(self):
            raise Exception("DB DOWN")

    # Patch la référence réellement utilisée dans src/etl/extract.py
    monkeypatch.setattr(
        "src.etl.extract.create_engine",
        lambda *_: FakeEngine()
    )

    with pytest.raises(SystemExit):
        extract_all()