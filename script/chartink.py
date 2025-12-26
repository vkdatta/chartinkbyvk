#!/usr/bin/env python3

import requests
import pandas as pd
from bs4 import BeautifulSoup
from collections import Counter
import sys
import os
import time


CHARTINK_PROCESS = "https://chartink.com/screener/process"
CHARTINK_HOME = "https://chartink.com/screener"


def get_session():
    s = requests.Session()
    r = s.get(CHARTINK_HOME, timeout=10)
    soup = BeautifulSoup(r.text, "lxml")
    token = soup.find("meta", {"name": "csrf-token"})["content"]
    s.headers.update({"x-csrf-token": token})
    return s


def fetch_condition_result(session, condition):
    payload = {"scan_clause": condition}
    r = session.post(CHARTINK_PROCESS, data=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    return pd.DataFrame(data.get("data", []))


def read_conditions(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        raw = f.read()

    print("\nSelect condition separator:")
    print("1) New line (default)")
    print("2) Comma (,)")
    print("3) Semicolon (;)")
    print("4) Tab")
    print("5) Custom")

    choice = input("Enter choice [1-5]: ").strip() or "1"

    if choice == "2":
        parts = raw.split(",")
    elif choice == "3":
        parts = raw.split(";")
    elif choice == "4":
        parts = raw.split("\t")
    elif choice == "5":
        sep = input("Enter custom separator: ")
        parts = raw.split(sep)
    else:
        parts = raw.splitlines()

    return [p.strip() for p in parts if p.strip()]


def intersect_results(all_frames, min_count):
    counter = Counter()

    for df in all_frames:
        if "nsecode" not in df.columns:
            continue
        counter.update(df["nsecode"].unique())

    result = [
        {"nsecode": k, "appearances": v}
        for k, v in counter.items()
        if v >= min_count
    ]

    # Always return a dataframe with expected columns
    df_result = pd.DataFrame(result, columns=["nsecode", "appearances"])

    if df_result.empty:
        return df_result

    return df_result.sort_values("appearances", ascending=False)


def union_results(all_frames):
    valid_frames = [df for df in all_frames if "nsecode" in df.columns]

    if not valid_frames:
        return pd.DataFrame(columns=["nsecode"])

    combined = pd.concat(valid_frames, ignore_index=True)
    return combined.drop_duplicates(subset=["nsecode"])


def main():
    if len(sys.argv) < 3:
        print("\nUsage:")
        print("  chartink intersect conditions.txt")
        print("  chartink union conditions.txt\n")
        sys.exit(1)

    mode = sys.argv[1].lower()
    file_path = sys.argv[2]

    if mode not in ("intersect", "union"):
        print("Mode must be: intersect | union")
        sys.exit(1)

    if not os.path.exists(file_path):
        print("File not found:", file_path)
        sys.exit(1)

    conditions = read_conditions(file_path)
    print(f"\nLoaded {len(conditions)} conditions")

    session = get_session()
    all_frames = []

    for i, cond in enumerate(conditions, 1):
        try:
            print(f"Fetching condition {i}/{len(conditions)}...")
            df = fetch_condition_result(session, cond)
            if not df.empty:
                all_frames.append(df)
            else:
                print(f"  → No results")
            time.sleep(1)
        except Exception as e:
            print(f"Skipped condition {i}: {e}")

    if not all_frames:
        print("No data fetched.")
        sys.exit(1)

    base_name = os.path.splitext(file_path)[0]

    if mode == "intersect":
        repeat = input("\nMinimum appearances? [default 2]: ").strip()
        repeat = int(repeat) if repeat.isdigit() else 2

        final = intersect_results(all_frames, repeat)
        out = f"{base_name}_intersect.csv"

        if final.empty:
            print("\n⚠️ No symbols met the minimum appearance threshold.")

    else:
        final = union_results(all_frames)
        out = f"{base_name}_union.csv"

    final.to_csv(out, index=False)
    print(f"\n✅ Output saved: {out}")
    print(f"Rows: {len(final)}")


if __name__ == "__main__":
    main()
