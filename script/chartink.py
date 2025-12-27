#!/usr/bin/env python3

import requests
import pandas as pd
from bs4 import BeautifulSoup
from collections import defaultdict
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


def parse_conditions(raw):
    conditions = []
    i = 0
    n = len(raw)
    cond_id = 1
    while i < n:
        while i < n and raw[i].isspace():
            i += 1
        if i >= n:
            break
        name_start = i
        while i < n and raw[i] not in ':=':
            i += 1
        name_end = i
        name = raw[name_start:name_end].strip()
        if i < n and raw[i] in ':=':
            i += 1
            while i < n and raw[i].isspace():
                i += 1
            if i < n and raw[i] == '(':
                clause_start = i
                i += 1
                open_count = 1
                while i < n and open_count > 0:
                    if raw[i] == '(':
                        open_count += 1
                    elif raw[i] == ')':
                        open_count -= 1
                    i += 1
                clause = raw[clause_start:i].strip()
                if clause and clause.count('(') == clause.count(')'):
                    conditions.append((name, clause))
                continue
        i = name_start
        while i < n and raw[i].isspace():
            i += 1
        if i >= n or raw[i] != '(':
            i += 1
            continue
        clause_start = i
        i += 1
        open_count = 1
        while i < n and open_count > 0:
            if raw[i] == '(':
                open_count += 1
            elif raw[i] == ')':
                open_count -= 1
            i += 1
        clause = raw[clause_start:i].strip()
        if clause and clause.count('(') == clause.count(')'):
            conditions.append((f"condition_{cond_id}", clause))
            cond_id += 1
    return conditions


def read_conditions(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        raw = f.read()
    conditions = parse_conditions(raw)
    print(f"\nLoaded {len(conditions)} conditions")
    return conditions


def intersect_results(all_results, min_count):
    stock_data = defaultdict(list)
    for name, df in all_results:
        if 'nsecode' not in df.columns:
            continue
        for _, row in df.iterrows():
            nsecode = row['nsecode']
            stock_data[nsecode].append((name, row.to_dict()))
    final_rows = []
    for nsecode, matches in stock_data.items():
        if len(matches) >= min_count:
            _, first_row_dict = matches[0]
            row_dict = first_row_dict.copy()
            row_dict['appearances'] = len(matches)
            cond_names = [nm for nm, _ in matches]
            row_dict['conditions'] = ', '.join(cond_names)
            final_rows.append(row_dict)
    if not final_rows:
        return pd.DataFrame()
    df_result = pd.DataFrame(final_rows)
    return df_result.sort_values("appearances", ascending=False)


def union_results(all_results):
    valid = [df for name, df in all_results if "nsecode" in df.columns]
    if not valid:
        return pd.DataFrame(columns=["nsecode"])
    combined = pd.concat(valid, ignore_index=True)
    return combined.drop_duplicates(subset=["nsecode"])


def all_results_func(all_results):
    valid = [df for name, df in all_results if "nsecode" in df.columns]
    if not valid:
        return pd.DataFrame(columns=["nsecode"])
    combined = pd.concat(valid, ignore_index=True)
    return combined


def main():
    if len(sys.argv) < 3:
        print("\nUsage:")
        print("  chartink intersect|union|all conditions.txt")
        sys.exit(1)

    mode = sys.argv[1].lower()
    file_path = sys.argv[2]

    if mode not in ("intersect", "union", "all"):
        print("Mode must be: intersect | union | all")
        sys.exit(1)

    if not os.path.exists(file_path):
        print("File not found:", file_path)
        sys.exit(1)

    conditions = read_conditions(file_path)

    session = get_session()
    all_results = []

    for i, (name, cond) in enumerate(conditions, 1):
        try:
            print(f"Fetching '{name}' ({i}/{len(conditions)})...")
            df = fetch_condition_result(session, cond)
            if not df.empty:
                all_results.append((name, df))
            else:
                print(f"  → No results")
            time.sleep(1)
        except Exception as e:
            print(f"Skipped '{name}': {e}")

    if not all_results:
        print("No data fetched.")
        sys.exit(1)

    base_name = os.path.splitext(file_path)[0]

    if mode == "intersect":
        repeat = input("\nMinimum appearances? [default 2]: ").strip()
        repeat = int(repeat) if repeat.isdigit() else 2
        final = intersect_results(all_results, repeat)
        out = f"{base_name}_intersect.csv"
        if final.empty:
            print("\n⚠️ No symbols met the minimum appearance threshold.")
    else:
        if mode == "union":
            final = union_results(all_results)
            out = f"{base_name}_union.csv"
        else:
            final = all_results_func(all_results)
            out = f"{base_name}_all.csv"

    if not final.empty:
        final.insert(0, 'serial', range(1, len(final) + 1))
        if 'nsecode' in final.columns:
            final = final.rename(columns={'nsecode': 'symbol'})

    final.to_csv(out, index=False)
    print(f"\n✅ Output saved: {out}")
    print(f"Rows: {len(final)}")


if __name__ == "__main__":
    main()
