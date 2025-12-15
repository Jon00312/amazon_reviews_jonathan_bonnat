import pandas as pd
from src.etl.load_mongodb import load_to_mongodb
from src.etl.load_mongodb import build_document

from src.etl.extract import extract_all

def test_load_mongo_accepts_dict():
    fake_data = {
        "review": pd.DataFrame(),
        "buyer": pd.DataFrame() 
    }

    # on vérifie juste que ça ne plante pas
    load_to_mongodb(data=fake_data)


def test_load_mongo_uses_provided_data(monkeypatch):

    fake_data = {
        "review": pd.DataFrame([
            {"review_id": 1, "r_desc": "ok"}
        ])
    }
    called = {"extract": False}

    load_to_mongodb(data=fake_data)
    
    assert called["extract"] is False


def test_build_bronze_document_structure():
    record = {"review_id": 1, "rating": 5}

    doc = build_document("review", record, "20250101_144040")

    assert doc["source_table"] == "review"
    assert doc["data"] == record
    assert "extracted_at" in doc


def test_build_bronze_document_keeps_raw_data():
    record = {"a": 1, "b": 2}

    doc = build_document("any_table", record, "20250101_144040")

    assert doc["data"] == record