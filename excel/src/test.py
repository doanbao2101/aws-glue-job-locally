import re

SILVER_BUCKET = "lanhm-lake-silver"


def to_snake_case(name: str) -> str:
    """
    Convert a string into snake_case format.

    - Replace spaces & special characters with underscores.
    - Collapse multiple underscores into one.
    - Strip leading/trailing underscores.
    - Lowercase the result.

    Example:
        "Hello World!!  Test" -> "hello_world_test"
    """
    # Replace non-alphanumeric characters with underscore
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    # Collapse multiple underscores
    name = re.sub(r"_+", "_", name)
    # Strip leading/trailing underscores & lowercase
    return name.strip("_").lower()


def build_code_checkpoint(output_path: str) -> str:
    """
    Build a code checkpoint in the format
    """

    raw = output_path.replace(
        f"s3://{SILVER_BUCKET}/", "").replace("/", ".").replace("-", "_").strip('.')
    return raw


print(build_code_checkpoint(
    "s3://lanhm-lake-silver/galaxy/corporate-members/members-list/"))
