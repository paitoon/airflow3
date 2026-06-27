import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--csv-dir", default="/data/chinook_csv")
    parser.add_argument("--cleansed-dir", default="/data/chinook_cleansed")
    parser.add_argument("--metadata-dir", default="/data/chinook_metadata")
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"chinook-dq-cleansing-{args.table}").getOrCreate()

    os.makedirs(args.cleansed_dir, exist_ok=True)

    source_path = f"{args.csv_dir}/{args.table}.csv"
    target_path = f"{args.cleansed_dir}/{args.table}"
    metadata_path = f"{args.metadata_dir}/{args.table}.json"

    metadata = {}
    if os.path.exists(metadata_path):
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

    print(f"[DQ_START] table={args.table} source={source_path} target={target_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(source_path)
    )

    source_count = df.count()
    print(f"[DQ_SOURCE_COUNT] table={args.table} rows={source_count}")

    for col_name in df.columns:
        clean_name = col_name.strip()
        if clean_name != col_name:
            df = df.withColumnRenamed(col_name, clean_name)

    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            c = field.name
            df = df.withColumn(
                c,
                F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.trim(F.col(c)))
            )

    before_dedup = df.count()
    df = df.dropDuplicates()
    cleansed_count = df.count()
    duplicate_rows_removed = before_dedup - cleansed_count
    print(f"[DQ_DUPLICATES] table={args.table} removed={duplicate_rows_removed}")

    primary_keys = metadata.get("primary_keys", [])
    if primary_keys:
        for pk in primary_keys:
            if pk not in df.columns:
                raise ValueError(f"[DQ_FAIL] PK column missing: table={args.table} pk={pk}")

            null_count = df.filter(F.col(pk).isNull()).count()
            print(f"[DQ_PK_NULLS] table={args.table} pk={pk} nulls={null_count}")
            if null_count > 0:
                raise ValueError(f"[DQ_FAIL] PK contains nulls: table={args.table} pk={pk}")

        duplicate_pk_count = (
            df.groupBy(*primary_keys)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        print(f"[DQ_PK_DUPLICATES] table={args.table} duplicate_keys={duplicate_pk_count}")
        if duplicate_pk_count > 0:
            raise ValueError(f"[DQ_FAIL] PK duplicate keys found: table={args.table}")

    if cleansed_count > source_count:
        raise ValueError(
            f"[DQ_FAIL] cleansed count increased: table={args.table} source={source_count} cleansed={cleansed_count}"
        )

    dq_report = {
        "table": args.table,
        "source_rows": source_count,
        "cleansed_rows": cleansed_count,
        "duplicate_rows_removed": duplicate_rows_removed,
        "primary_keys": primary_keys,
        "source_path": source_path,
        "target_path": target_path,
    }

    dq_report_path = f"{args.metadata_dir}/{args.table}_dq_report.json"
    with open(dq_report_path, "w", encoding="utf-8") as f:
        json.dump(dq_report, f, indent=2, ensure_ascii=False)

    df.write.mode("overwrite").parquet(target_path)

    print(f"[DQ_OK] table={args.table} source_rows={source_count} cleansed_rows={cleansed_count}")
    print(f"[DQ_REPORT] {dq_report_path}")

    spark.stop()


if __name__ == "__main__":
    main()
