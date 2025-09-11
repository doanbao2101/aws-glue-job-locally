def test_output_path():
    silver_bucket = "my-bucket"
    file_name = "orders"

    # Case 1: category is None
    category = None
    output_path = f"s3://{silver_bucket}/galaxy/{file_name}/{category or ''}/"
    assert output_path == "s3://my-bucket/galaxy/orders/"

    # Case 2: category is empty string
    category = ""
    output_path = f"s3://{silver_bucket}/galaxy/{file_name}/{category or ''}/"
    assert output_path == "s3://my-bucket/galaxy/orders/"

    # Case 3: category is normal string
    category = "transactions"
    output_path = f"s3://{silver_bucket}/galaxy/{file_name}/{category or ''}/"
    assert output_path == "s3://my-bucket/galaxy/orders/transactions/"

    # Case 4: category with spaces (should be preserved)
    category = "2025 reports"
    output_path = f"s3://{silver_bucket}/galaxy/{file_name}/{category or ''}/"
    assert output_path == "s3://my-bucket/galaxy/orders/2025 reports/"

test_output_path()