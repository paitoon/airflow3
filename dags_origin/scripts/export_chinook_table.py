import argparse
import csv
import json
import os
import sqlite3


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--sqlite-path", default="/data/chinook.db")
    parser.add_argument("--csv-dir", default="/data/chinook_csv")
    parser.add_argument("--metadata-dir", default="/data/chinook_metadata")
    args = parser.parse_args()

    os.makedirs(args.csv_dir, exist_ok=True)
    os.makedirs(args.metadata_dir, exist_ok=True)

    if not os.path.exists(args.sqlite_path):
        raise FileNotFoundError(f"SQLite file not found: {args.sqlite_path}")

    conn = sqlite3.connect(args.sqlite_path)
    conn.row_factory = sqlite3.Row
    table_q = quote_identifier(args.table)

    columns_info = [dict(r) for r in conn.execute(f"PRAGMA table_info({table_q})")]
    if not columns_info:
        raise ValueError(f"Table not found or has no columns: {args.table}")

    columns = [c["name"] for c in columns_info]
    csv_path = os.path.join(args.csv_dir, f"{args.table}.csv")

    row_count = 0
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for row in conn.execute(f"SELECT * FROM {table_q}"):
            writer.writerow([row[col] for col in columns])
            row_count += 1

    metadata = {
        "table": args.table,
        "columns": columns_info,
        "primary_keys": [c["name"] for c in sorted(columns_info, key=lambda x: x["pk"]) if c["pk"]],
        "foreign_keys": [dict(r) for r in conn.execute(f"PRAGMA foreign_key_list({table_q})")],
        "row_count": row_count,
        "csv_path": csv_path,
    }

    metadata_path = os.path.join(args.metadata_dir, f"{args.table}.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    conn.close()
    print(f"[EXPORT_OK] table={args.table} rows={row_count} csv={csv_path} metadata={metadata_path}")


if __name__ == "__main__":
    main()
