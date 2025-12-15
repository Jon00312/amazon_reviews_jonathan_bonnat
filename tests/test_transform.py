from src.etl.transform import transform_all

def test_transform_basic_flow(extract_data_mock):
    clean_df, reject_df = transform_all(extract_data_mock)

    # Dataset clean
    assert not clean_df.empty
    assert "review_text" in clean_df.columns
    assert clean_df["rating"].between(1, 5).all()

    # Rejets
    assert not reject_df.empty
    assert "reject_reason" in reject_df.columns


def test_transform_rejects_invalid_rating(extract_data_mock):
    _, rejects = transform_all(extract_data_mock)
    assert "invalid_rating" in rejects["reject_reason"].values

def test_transform_output_schema(extract_data_mock):

    df_clean, _ = transform_all(extract_data_mock)

    expected_cols = {
        "review_id",
        "buyer_id",
        "product_id",
        "review_text",
        "rating",
        "has_image",
        "has_subscription",
        "verified_buyer",
    }

    assert set(df_clean.columns) == expected_cols

def test_transform_rejects_empty_reviews(extract_data_mock):

    extract_data_mock["review"].loc[0, "r_desc"] = ""

    _, rejects = transform_all(extract_data_mock)

    assert not rejects.empty
    assert "empty_review_raw" in rejects["reject_reason"].values


def test_transform_rejects_out_of_range_rating(extract_data_mock):

    extract_data_mock["review"].loc[0, "rating"] = 12

    _, rejects = transform_all(extract_data_mock)

    assert "invalid_rating" in rejects["reject_reason"].values


def test_transform_boolean_flags(extract_data_mock):

    df_clean, _ = transform_all(extract_data_mock)

    assert df_clean["has_image"].dtype == bool
    assert df_clean["has_subscription"].dtype == bool
    assert df_clean["verified_buyer"].dtype == bool
