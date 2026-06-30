import argparse

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--target-base", default="s3a://lakehouse/bronze/chinook")
    parser.add_argument("--min-rows", type=int, default=0)
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"validate-chinook-delta-{args.table}").getOrCreate()

    target_path = f"{args.target_base}/{args.table.lower()}"
    df = spark.read.format("delta").load(target_path)
    row_count = df.count()

    if row_count < args.min_rows:
        raise ValueError(f"Delta validation failed for {args.table}: rows={row_count}, min_rows={args.min_rows}")

    print(f"[VALIDATE_OK] table={args.table} rows={row_count}")
    df.printSchema()
    df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
