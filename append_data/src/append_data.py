import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, lit


def _to_bool(x: str) -> bool:
    return str(x).strip().lower() in {"1", "true", "t", "yes", "y"}


# ----------------------------
# Parse required job arguments
# ----------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "jdbc_url",
        "db_user",
        "db_password",
        "source_schema",
        "target_schema",
        "is_upsert"
    ]
)


jdbc_url = args["jdbc_url"]
db_user = args["db_user"]
db_password = args["db_password"]
source_schema = args["source_schema"]
target_schema = args["target_schema"]

include_tables = set()
exclude_tables = set()
is_upsert = _to_bool(args["is_upsert"])
HASH_COL = "hash_value"  # đổi nếu cần

# ----------------------------
# Glue/Spark
# ----------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ----------------------------
# Logging with icons
# ----------------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("append-tables")

ICON_INFO = "ℹ️"
ICON_WARN = "⚠️"
ICON_ERROR = "❌"
ICON_SUCCESS = "✅"
ICON_SKIP = "⏭️"
ICON_PROC = "🔄"
ICON_DONE = "🏁"

# ----------------------------
# Helpers
# ----------------------------

# NEW: parse bool (chấp nhận: true/false/1/0/yes/no)


def jdbc_read_query(query_sql: str):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"({query_sql}) AS t")
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def list_tables_with_columns(schema_name: str):
    sql = f"""
        SELECT table_name, column_name, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}'
        ORDER BY table_name, ordinal_position
    """
    df = jdbc_read_query(sql)
    grouped = (
        df.groupBy("table_name")
        .agg({"column_name": "collect_list"})
        .rdd.map(lambda r: (r["table_name"], r["collect_list(column_name)"]))
        .collect()
    )
    return {tbl: cols for tbl, cols in grouped}


def get_target_columns(table_name: str):
    sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{target_schema}' AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """
    return [r["column_name"] for r in jdbc_read_query(sql).collect()]


def count_rows(schema_name: str, table_name: str) -> int:
    sql = f'SELECT count(*) AS c FROM "{schema_name}"."{table_name}"'
    return int(jdbc_read_query(sql).collect()[0]["c"])


def with_row_hash(df):
    df = df.withColumn(HASH_COL, sha2(concat_ws("||", *df.columns), 256))
    return df


# ----------------------------
# Job
# ----------------------------
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

log.info(f'{ICON_INFO} JDBC: {jdbc_url}')
log.info(f'{ICON_INFO} Source → Target: {source_schema} → {target_schema}')
log.info(
    f'{ICON_INFO} include_tables={sorted(include_tables) if include_tables else "ALL"}')
log.info(
    f'{ICON_INFO} exclude_tables={sorted(exclude_tables) if exclude_tables else "[]"}')

tables_columns = list_tables_with_columns(source_schema)
log.info(f"{ICON_INFO} Found {len(tables_columns)} tables in '{source_schema}'")

# Apply include/exclude filter
candidate_tables = set(tables_columns.keys())
if include_tables:
    candidate_tables &= include_tables
candidate_tables -= exclude_tables
candidate_tables = sorted(candidate_tables)

log.info(f"{ICON_INFO} Will process {len(candidate_tables)} tables after include/exclude filtering")

total_appended = 0
processed = 0
skipped = 0
failed = 0

for table_name in candidate_tables:
    src_cols = tables_columns.get(table_name, [])
    try:
        log.info(f"{ICON_PROC} ---- {table_name} ----")

        tgt_cols = get_target_columns(table_name)
        if not tgt_cols:
            log.warning(
                f"{ICON_WARN} Target table {target_schema}.{table_name} not found → skip")
            skipped += 1
            continue

        overlap_cols = [c for c in tgt_cols if c in src_cols]
        if not overlap_cols:
            log.warning(
                f"{ICON_WARN} No overlapping columns → skip {table_name}")
            skipped += 1
            continue

        # Count target BEFORE
        try:
            tgt_before = count_rows(target_schema, table_name)
        except Exception:
            tgt_before = 0
            log.warning(
                f"{ICON_WARN} Could not count BEFORE for {target_schema}.{table_name} (treat as 0)")

        # Read from source with only overlap columns in target order
        src_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f'(SELECT * FROM "{source_schema}"."{table_name}") AS src')
            .option("user", db_user)
            .option("password", db_password)
            .option("driver", "org.postgresql.Driver")
            .load()
            .select(*overlap_cols)
        )

        n_src = src_df.count()
        if n_src == 0:
            log.info(f"{ICON_SKIP} No rows in source → skip {table_name}")
            skipped += 1
            continue

        if is_upsert:
            # # Yêu cầu phải có cột hash
            # if HASH_COL not in overlap_cols:
            #     log.warning(
            #         f"{ICON_WARN} {table_name}: thiếu cột {HASH_COL} → skip upsert")
            #     skipped += 1
            #     continue

            # Đếm BEFORE (để log)
            try:
                tgt_before = count_rows(target_schema, table_name)
            except Exception:
                tgt_before = 0
                log.warning(
                    f"{ICON_WARN} Could not count BEFORE for {target_schema}.{table_name} (treat as 0)")

            # Đọc FULL nguồn & đích (không LIMIT) với thứ tự cột theo target
            src_df = with_row_hash(src_df)  # ADDED

            tgt_df = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", f'"{target_schema}"."{table_name}"')
                .option("user", db_user)
                .option("password", db_password)
                .option("driver", "org.postgresql.Driver")
                .load()
                .select(*overlap_cols)
            )
            tgt_df = with_row_hash(tgt_df)  # ADDED

            # Tính insert / delete theo HASH_COL (giống kỹ thuật S3 trước đây)
            insert_data = src_df.join(tgt_df.select(
                HASH_COL), on=HASH_COL, how="left_anti")
            delete_data = tgt_df.join(src_df.select(
                HASH_COL), on=HASH_COL, how="left_anti")

            log.info(
                f"{ICON_PROC} {table_name} | insert={insert_data.count():,} | delete={delete_data.count():,}")

            # Kết quả cuối cùng thực chất = snapshot latest (src_df) đã dedup theo HASH_COL
            updated_df = src_df.select(*overlap_cols)

            log.info(f"{ICON_PROC} Overwrite → {target_schema}.{table_name}")
            (
                updated_df.write.format("jdbc")
                .mode("overwrite")
                .option("url", jdbc_url)
                .option("dbtable", f'"{target_schema}"."{table_name}"')
                .option("user", db_user)
                .option("password", db_password)
                .option("driver", "org.postgresql.Driver")
                # cố gắng TRUNCATE thay vì DROP/CREATE để an toàn quyền hạn
                .option("truncate", "true")
                .save()
            )

            # Count AFTER & log
            try:
                tgt_after = count_rows(target_schema, table_name)
            except Exception:
                tgt_after = None
                log.warning(
                    f"{ICON_WARN} Could not count AFTER for {target_schema}.{table_name}")

            delta = (
                tgt_after - tgt_before) if (tgt_after is not None) else updated_df.count()
            total_appended += max(0, delta)
            processed += 1

            log.info(
                f"{ICON_SUCCESS} UPSERT DONE {table_name} | "
                f"target_before={tgt_before:,} | target_after={(tgt_after if tgt_after is not None else 'N/A')} | "
                f"insert={insert_data.count():,} | delete={delete_data.count():,}"
            )

        else:

            log.info(
                f"{ICON_PROC} Appending {n_src} rows → {target_schema}.{table_name}")
            src_df.write.format("jdbc") \
                .mode("append") \
                .option("url", jdbc_url) \
                .option("dbtable", f'"{target_schema}"."{table_name}"') \
                .option("user", db_user) \
                .option("password", db_password) \
                .option("driver", "org.postgresql.Driver") \
                .option("batchsize", "5000") \
                .save()
    except Exception as e:
        failed += 1
        log.exception(f"{ICON_ERROR} FAILED {table_name} | {e}")

log.info(
    f"{ICON_DONE} Job complete | "
    f"{ICON_SUCCESS} processed={processed} | "
    f"{ICON_SKIP} skipped={skipped} | "
    f"{ICON_ERROR} failed={failed} | "
    f"📊 total_rows_appended={total_appended:,}"
)

job.commit()
