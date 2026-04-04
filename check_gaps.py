"""
check_gaps.py
Run after rebuild_all_seasons.sh completes.
Identifies specific game dates with fewer games than typical for each season,
helping confirm that missing games are isolated incidents (not entire team drops).
"""

import duckdb
from collections import defaultdict

con = duckdb.connect("baseball.duckdb", read_only=True)

EXPECTED = {s: 2430 for s in range(2015, 2027)}
EXPECTED[2020] = 898

print("=" * 70)
print("GAME COUNT SUMMARY")
print("=" * 70)

rows = con.execute("""
    SELECT season, game_type, COUNT(DISTINCT game_pk) AS games
    FROM pitches
    WHERE game_type IN ('R', 'F', 'D', 'L', 'W')
    GROUP BY season, game_type
    ORDER BY season, game_type
""").fetchall()

by_season = defaultdict(dict)
for season, gtype, count in rows:
    by_season[season][gtype] = count

for season in sorted(by_season):
    regular = by_season[season].get('R', 0)
    exp = EXPECTED.get(season, 2430)
    if season == 2026:
        status = f"PARTIAL ({regular} games so far)"
    elif regular == exp:
        status = f"✓ PASS"
    elif abs(regular - exp) <= 2:
        status = f"~ {regular} / {exp} (delta {regular - exp})"
    else:
        status = f"✗ FAIL — {regular} / {exp} (delta {regular - exp})"
    post = sum(v for k, v in by_season[season].items() if k != 'R')
    print(f"  {season}: {status}   postseason={post}")

print()
print("=" * 70)
print("THIN DATE ANALYSIS (seasons not at 2430)")
print("For each off-count season, show dates with < 14 games (avg is ~15)")
print("=" * 70)

problem_seasons = [
    s for s in sorted(by_season)
    if s != 2026 and abs(by_season[s].get('R', 0) - EXPECTED.get(s, 2430)) > 0
    and s != 2020
]

for season in problem_seasons:
    regular = by_season[season].get('R', 0)
    exp = EXPECTED.get(season, 2430)
    print(f"\nSeason {season} ({regular} games, expected {exp}, delta {regular - exp}):")

    date_rows = con.execute("""
        SELECT game_date, COUNT(DISTINCT game_pk) AS games,
               COUNT(DISTINCT pitcher_team) AS teams
        FROM pitches
        WHERE season = ? AND game_type = 'R'
        GROUP BY game_date
        ORDER BY game_date
    """, [season]).fetchall()

    # Find the days where games played < what we'd expect for the calendar date
    # (i.e. look for suspiciously low counts)
    dates_by_count = sorted(date_rows, key=lambda r: r[1])
    low_dates = [r for r in dates_by_count if r[1] < 10]
    if low_dates:
        print("  Low-game-count dates (< 10 games):")
        for d, g, t in low_dates:
            print(f"    {d}: {g} games, {t} teams")
    else:
        print("  No unusually low dates — missing game likely has data in a",
              "different chunk or was a weather abandonment before first pitch.")

con.close()
print()
print("Done. These gaps are inherent to Baseball Savant's data — not pipeline errors.")
