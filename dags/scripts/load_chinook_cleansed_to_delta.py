import argparse

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--cleansed-dir", default="/data/chinook_cleansed")
    parser.add_argument("--target-base", default="s3a://lakehouse/bronze/chinook")
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"chinook-cleansed-to-delta-{args.table}").getOrCreate()

    source_path = f"{args.cleansed_dir}/{args.table}"
    target_path = f"{args.target_base}/{args.table.lower()}"

    print(f"[LOAD_START] table={args.table} source={source_path} target={target_path}")

    df = spark.read.parquet(source_path)
    source_count = df.count()

    df.write.format("delta").mode("overwrite").save(target_path)

    delta_count = spark.read.format("delta").load(target_path).count()

    if source_count != delta_count:
        raise ValueError(f"Count mismatch for {args.table}: source={source_count}, delta={delta_count}")

    print(f"[LOAD_OK] table={args.table} target={target_path} rows={delta_count}")

    spark.stop()


if __name__ == "__main__":
    main()
