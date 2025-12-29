#!/usr/bin/env python3
import requests
import pandas as pd
from bs4 import BeautifulSoup
from collections import Counter, defaultdict
import sys
import os
import time
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

CHARTINK_PROCESS = "https://chartink.com/screener/process"
CHARTINK_HOME = "https://chartink.com/screener"
HISTORY_PATH = os.path.expanduser("~/.chartink_history.json")
IST = ZoneInfo("Asia/Kolkata")

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
    parts = [p.strip() for p in raw.splitlines() if p.strip()]
    parsed = []
    for i, p in enumerate(parts):
        if "=" in p:
            name, cond = p.split("=", 1)
            parsed.append((name.strip(), cond.strip()))
        else:
            parsed.append((f"condition {i+1}", p))
    return parsed

def intersect_results(all_frames, cond_name_map, min_count):
    counter = Counter()
    for df in all_frames:
        if "nsecode" not in df.columns:
            continue
        counter.update(df["nsecode"].unique())
    combined = pd.concat([df for df in all_frames if "nsecode" in df.columns], ignore_index=True) if any("nsecode" in df.columns for df in all_frames) else pd.DataFrame()
    combined_unique = combined.drop_duplicates(subset=["nsecode"]).reset_index(drop=True) if not combined.empty else pd.DataFrame()
    rows = []
    for nse, count in counter.items():
        if count >= min_count:
            row = combined_unique[combined_unique["nsecode"] == nse]
            base = row.iloc[0].to_dict() if not row.empty else {"nsecode": nse}
            base["appearances"] = count
            base["conditions met"] = "; ".join(cond_name_map.get(nse, []))
            rows.append(base)
    if not rows:
        return pd.DataFrame(columns=["nsecode", "appearances", "conditions met"])
    df_result = pd.DataFrame(rows)
    df_result = df_result.sort_values("appearances", ascending=False).reset_index(drop=True)
    return df_result

def union_results(all_frames, cond_name_map):
    valid_frames = [df for df in all_frames if "nsecode" in df.columns]
    if not valid_frames:
        return pd.DataFrame(columns=["nsecode"])
    combined = pd.concat(valid_frames, ignore_index=True)
    combined_unique = combined.drop_duplicates(subset=["nsecode"]).reset_index(drop=True)
    if "conditions met" not in combined_unique.columns:
        conditions_met = []
        for _, row in combined_unique.iterrows():
            nse = row["nsecode"]
            names = cond_name_map.get(nse, [])
            conditions_met.append("; ".join(names))
        combined_unique["conditions met"] = conditions_met
    return combined_unique

def all_results(all_frames, cond_name_map):
    return union_results(all_frames, cond_name_map)

def load_history():
    if not os.path.exists(HISTORY_PATH):
        return []
    try:
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    pruned = []
    today = datetime.now(IST).date()
    for entry in data:
        entry_date = datetime.fromisoformat(entry["date"]).date()
        if (today - entry_date).days <= 7:
            pruned.append(entry)
    return pruned

def save_history(entries):
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)

def append_all_history(base_name, records):
    entries = load_history()
    now = datetime.now(IST)
    entry = {
        "date": now.date().isoformat(),
        "timestamp": now.isoformat(),
        "filename": base_name,
        "operation": "all",
        "records": records
    }
    entries.append(entry)
    save_history(entries)

def build_cond_name_map(conditions_parsed, fetched_frames):
    cond_name_map = defaultdict(list)
    for (name, cond), df in zip(conditions_parsed, fetched_frames):
        if df is None or df.empty or "nsecode" not in df.columns:
            continue
        for n in df["nsecode"].unique():
            cond_name_map[n].append(name)
    return cond_name_map

def build_all_records(all_df, cond_name_map):
    records = []
    for _, row in all_df.iterrows():
        nse = row["nsecode"]
        rec = {"nsecode": nse, "conditions_met": cond_name_map.get(nse, [])}
        records.append(rec)
    return records

def intersect_last_7_days(base_name, min_count, combined_unique):
    entries = load_history()
    relevant = [e for e in entries if e["filename"] == base_name]
    symbol_days = defaultdict(set)
    symbol_cond_dates = defaultdict(list)
    for e in relevant:
        entry_date_str = e.get("date")
        for r in e.get("records", []):
            nse = r.get("nsecode")
            for cond in r.get("conditions_met", []):
                symbol_cond_dates[nse].append((cond, entry_date_str))
            symbol_days[nse].add(entry_date_str)
    rows = []
    for nse, days in symbol_days.items():
        if len(days) >= min_count:
            if combined_unique is not None:
                row = combined_unique[combined_unique["nsecode"] == nse]
                base = row.iloc[0].to_dict() if not row.empty else {"nsecode": nse}
            else:
                base = {"nsecode": nse}
            base["appearances"] = len(days)
            conds = []
            seen = set()
            for cond, d in symbol_cond_dates.get(nse, []):
                label = f"{cond} ({datetime.fromisoformat(d).strftime('%b %d')})"
                if label not in seen:
                    conds.append(label)
                    seen.add(label)
            base["conditions met"] = "; ".join(conds)
            rows.append(base)
    if not rows:
        return pd.DataFrame(columns=["nsecode", "appearances", "conditions met"])
    return pd.DataFrame(rows).sort_values("appearances", ascending=False).reset_index(drop=True)

def main():
    if len(sys.argv) < 3:
        print("\nUsage:")
        print("  chartink intersect conditions.txt")
        print("  chartink union conditions.txt")
        print("  chartink all conditions.txt\n")
        sys.exit(1)
    mode = sys.argv[1].lower()
    file_path = sys.argv[2]
    if mode not in ("intersect", "union", "all"):
        print("Mode must be: intersect | union | all")
        sys.exit(1)
    if not os.path.exists(file_path):
        print("File not found:", file_path)
        sys.exit(1)
    conditions_parsed = read_conditions(file_path)
    print(f"\nLoaded {len(conditions_parsed)} conditions")
    session = get_session()
    fetched_frames = []
    for i, (_, cond) in enumerate(conditions_parsed, 1):
        try:
            print(f"Fetching condition {i}/{len(conditions_parsed)}...")
            df = fetch_condition_result(session, cond)
            if df.empty:
                fetched_frames.append(pd.DataFrame())
                print("  → No results")
            else:
                fetched_frames.append(df)
            time.sleep(1)
        except Exception as e:
            print(f"Skipped condition {i}: {e}")
            fetched_frames.append(pd.DataFrame())
    valid_frames = [df for df in fetched_frames if isinstance(df, pd.DataFrame)]
    cond_name_map = build_cond_name_map(conditions_parsed, fetched_frames)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    all_df = all_results(valid_frames, cond_name_map)
    records = build_all_records(all_df, cond_name_map)
    append_all_history(base_name, records)
    if mode == "all":
        out = f"{base_name}_all.csv"
        if all_df.empty:
            print("\n⚠️ No symbols found for any condition.")
        all_df.to_csv(out, index=False)
        print(f"\n✅ Output saved: {out}")
        print(f"Rows: {len(all_df)}")
        return
    if mode == "union":
        final = union_results(valid_frames, cond_name_map)
        out = f"{base_name}_union.csv"
        final.to_csv(out, index=False)
        print(f"\n✅ Output saved: {out}")
        print(f"Rows: {len(final)}")
        return
    if mode == "intersect":
        choice = input("\nChoose intersect mode:\n1) intersect only within the file (default)\n2) intersect for the last 7 days\nEnter choice [1-2]: ").strip() or "1"
        repeat = input("\nMinimum appearances? [default 2]: ").strip()
        repeat = int(repeat) if repeat.isdigit() else 2
        combined = pd.concat([df for df in valid_frames if "nsecode" in df.columns], ignore_index=True) if valid_frames else pd.DataFrame()
        combined_unique = combined.drop_duplicates(subset=["nsecode"]).reset_index(drop=True) if not combined.empty else None
        if choice == "2":
            final = intersect_last_7_days(base_name, repeat, combined_unique)
        else:
            final = intersect_results(valid_frames, cond_name_map, repeat)
        out = f"{base_name}_intersect.csv"
        final.to_csv(out, index=False)
        if final.empty:
            print("\n⚠️ No symbols met the minimum appearance threshold.")
        print(f"\n✅ Output saved: {out}")
        print(f"Rows: {len(final)}")

if __name__ == "__main__":
    main()