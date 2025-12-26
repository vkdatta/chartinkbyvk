import requests
import pandas as pd
from bs4 import BeautifulSoup
from collections import Counter
import time

def fetch_chartink(url):
    session = requests.Session()
    headers_base = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    
    r = session.get(url, headers=headers_base)
    soup = BeautifulSoup(r.content, "lxml")
    csrf_token = soup.find("meta", {"name": "csrf-token"})["content"]

    process_url = "https://chartink.com/screener/process"
    headers = {**headers_base, "x-csrf-token": csrf_token}

    textarea = soup.find("textarea", {"id": "scan_clause"})
    if not textarea:
        raise Exception(f"Could not extract condition from {url}")
    clause = textarea.text.strip()

    resp = session.post(process_url, headers=headers, data={"scan_clause": clause})
    resp.raise_for_status()
    data = resp.json().get("data", [])
    df = pd.DataFrame(data)
    if "nsecode" in df.columns:
        return df["nsecode"].dropna().tolist()
    return []

def read_urls(filename, sep):
    with open(filename, "r") as f:
        content = f.read().strip()
    if sep == "space":
        return content.split()
    elif sep == "newline":
        return content.splitlines()
    elif sep == "comma":
        return [u.strip() for u in content.split(",")]
    elif sep == "tab":
        return [u.strip() for u in content.split("\t")]
    else:
        return content.split(sep)

def intersect(urls, min_occurrences=2, output_file=None):
    all_symbols = []
    for url in urls:
        try:
            symbols = fetch_chartink(url)
            all_symbols.append(symbols)
            print(f"{len(symbols)} stocks fetched from {url}")
            time.sleep(1.5)
        except Exception as e:
            print(f"Error fetching {url}: {e}")

    flat_list = [item for sublist in all_symbols for item in sublist]
    counter = Counter(flat_list)
    final_df = (
        pd.DataFrame(counter.items(), columns=["nsecode", "appearances"])
        .query(f"appearances >= {min_occurrences}")
        .sort_values("appearances", ascending=False)
        .reset_index(drop=True)
    )
    if not output_file:
        output_file = "intersect_output.csv"
    final_df.to_csv(output_file, index=False)
    print(f"\nIntersect saved to {output_file}")
    return final_df

def union(urls, output_file=None):
    all_symbols = []
    for url in urls:
        try:
            symbols = fetch_chartink(url)
            all_symbols.extend(symbols)
            print(f"{len(symbols)} stocks fetched from {url}")
            time.sleep(1.5)
        except Exception as e:
            print(f"Error fetching {url}: {e}")

    unique_symbols = sorted(set(all_symbols))
    df = pd.DataFrame(unique_symbols, columns=["nsecode"])
    if not output_file:
        output_file = "union_output.csv"
    df.to_csv(output_file, index=False)
    print(f"\nUnion saved to {output_file}")
    return df

def choose_separator():
    print("Choose the separator used in your file:")
    print("1) space")
    print("2) newline")
    print("3) comma")
    print("4) tab")
    print("5) custom")
    choice = input("Enter choice [1-5]: ").strip()
    sep_map = {"1":"space","2":"newline","3":"comma","4":"tab","5":"custom"}
    sep = sep_map.get(choice,"newline")
    if sep=="custom":
        sep = input("Enter your custom separator: ")
    return sep

def main():
    print("Welcome to Chartink CLI Interactive")
    cmd = input("Enter command (intersect / union): ").strip().lower()
    filename = input("Enter filename containing Chartink URLs: ").strip()
    sep = choose_separator()
    urls = read_urls(filename, sep)

    if cmd == "intersect":
        min_occurrences = input("Enter minimum occurrences (default 2): ").strip()
        min_occurrences = int(min_occurrences) if min_occurrences.isdigit() else 2
        output_file = filename.replace(".txt","_intersect.csv")
        intersect(urls, min_occurrences=min_occurrences, output_file=output_file)
    elif cmd == "union":
        output_file = filename.replace(".txt","_union.csv")
        union(urls, output_file=output_file)
    else:
        print("Invalid command! Use 'intersect' or 'union'.")

if __name__=="__main__":
    main()
