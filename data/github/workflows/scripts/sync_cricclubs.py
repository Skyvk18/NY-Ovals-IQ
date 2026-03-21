import csv
import json
import os
from datetime import datetime, timezone
from io import StringIO

import requests


OUTPUT_PLAYERS = "data/players.json"
OUTPUT_META = "data/last_sync.json"


def to_number(value, default=0):
    if value is None:
        return default
    s = str(value).strip().replace(",", "")
    if s == "":
        return default
    try:
        if "." in s:
            return float(s)
        return int(s)
    except ValueError:
        return default


def pick(row, *keys, default=""):
    normalized = {normalize_key(k): v for k, v in row.items()}
    for key in keys:
        nk = normalize_key(key)
        if nk in normalized and str(normalized[nk]).strip() != "":
            return str(normalized[nk]).strip()
    return default


def normalize_key(key):
    return str(key or "").strip().lower().replace(" ", "").replace("_", "")


def fetch_csv(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; NYOvalsIQ/1.0)",
        "Accept": "text/csv,text/plain;q=0.9,*/*;q=0.8",
        "Referer": "https://www.cricclubs.com/",
    }
    response = requests.get(url, headers=headers, timeout=45)
    response.raise_for_status()
    return response.text


def parse_rows(csv_text):
    reader = csv.DictReader(StringIO(csv_text))
    rows = list(reader)
    if not rows:
        raise ValueError("CSV had no rows")
    return rows


def convert_row(row):
    name = pick(row, "Player Name", "Player", "Name", "Full Name")
    team = pick(row, "Team", "Team Name", "Club", default="Unassigned")
    role = pick(row, "Role", "Type", "Position", default="Player")

    runs = to_number(pick(row, "Runs", "R"))
    wickets = to_number(pick(row, "Wkts", "Wickets"))
    strike_rate = to_number(pick(row, "SR", "Strike Rate", "StrikeRate"))
    average = to_number(pick(row, "Avg", "Average"))
    economy = to_number(pick(row, "Econ", "Economy"))
    catches = to_number(pick(row, "Catches", "Ct", "Catch"))
    matches_played = to_number(pick(row, "Mat", "Matches", "Matches Played", "Inns"), default=1)

    return {
        "name": name or "Unnamed Player",
        "team": team or "Unassigned",
        "role": role or "Player",
        "runs": runs,
        "wickets": wickets,
        "strikeRate": strike_rate,
        "average": average,
        "economy": economy,
        "catches": catches,
        "matchesPlayed": max(1, matches_played),
    }


def main():
    csv_url = os.environ.get("CRICCLUBS_CSV_URL", "").strip()
    if not csv_url:
        raise RuntimeError("Missing CRICCLUBS_CSV_URL secret")

    csv_text = fetch_csv(csv_url)
    rows = parse_rows(csv_text)

    players = []
    seen = set()

    for row in rows:
        player = convert_row(row)
        key = (player["name"].lower(), player["team"].lower())
        if key in seen:
            continue
        seen.add(key)
        players.append(player)

    players.sort(key=lambda p: (p["team"].lower(), p["name"].lower()))

    os.makedirs("data", exist_ok=True)

    with open(OUTPUT_PLAYERS, "w", encoding="utf-8") as f:
        json.dump(players, f, indent=2)

    with open(OUTPUT_META, "w", encoding="utf-8") as f:
        json.dump(
            {
                "syncedAt": datetime.now(timezone.utc).isoformat(),
                "source": csv_url,
                "playerCount": len(players),
            },
            f,
            indent=2,
        )

    print(f"Synced {len(players)} players")


if __name__ == "__main__":
    main()
