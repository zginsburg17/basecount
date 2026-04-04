"""
fill_gaps.py
Fetches 6 specific missing games directly by game_pk via statcast_single_game(),
then inserts them through the same clean/transform/upsert path as load_date_range.

Missing games (identified by diffing MLB Stats API schedule vs our pitches table):
  415766  2015-09-12  Detroit Tigers @ Cleveland Indians
  449187  2016-09-25  Atlanta Braves @ Miami Marlins
  449246  2016-10-03  Cleveland Indians @ Detroit Tigers
  567304  2019-09-27  Detroit Tigers @ Chicago White Sox
  632457  2021-09-16  Colorado Rockies @ Atlanta Braves
  746577  2024-09-29  Houston Astros @ Cleveland Guardians

All are late-season regular season games at standard Statcast parks where the
bulk date-range endpoint returned no data. The game_pk endpoint is fetched
separately and tends to succeed where the bulk endpoint fails.
"""

import os, sys, time
import duckdb
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from etl.pipeline import (
    init_db,
    clean_statcast,
    filter_supported_games,
    build_at_bat_rollup,
    table_columns,
    next_ingestion_run_id,
    enrich_players,
    refresh_season_derived_data,
    export_season_bundle,
    log,
)

try:
    from pybaseball import statcast_single_game
except ImportError:
    print("ERROR: pybaseball not installed.")
    sys.exit(1)

MISSING_GAMES = [
    {"game_pk": 415766, "season": 2015, "date": "2015-09-12", "matchup": "Detroit Tigers @ Cleveland Indians"},
    {"game_pk": 449187, "season": 2016, "date": "2016-09-25", "matchup": "Atlanta Braves @ Miami Marlins"},
    {"game_pk": 449246, "season": 2016, "date": "2016-10-03", "matchup": "Cleveland Indians @ Detroit Tigers"},
    {"game_pk": 567304, "season": 2019, "date": "2019-09-27", "matchup": "Detroit Tigers @ Chicago White Sox"},
    {"game_pk": 632457, "season": 2021, "date": "2021-09-16", "matchup": "Colorado Rockies @ Atlanta Braves"},
    {"game_pk": 746577, "season": 2024, "date": "2024-09-29", "matchup": "Houston Astros @ Cleveland Guardians"},
]

DB_PATH = os.path.join(os.path.dirname(__file__), "baseball.duckdb")
EXPORT_ROOT = os.path.join(os.path.dirname(__file__), "exports")


def insert_game_df(con: duckdb.DuckDBPyConnection, raw: pd.DataFrame, game: dict) -> int:
    """Clean, transform, and insert a single game's worth of raw Statcast data."""
    df = clean_statcast(raw)
    df = filter_supported_games(df)
    if df.empty:
        log.warning(f"  game_pk={game['game_pk']}: no supported game data after filtering.")
        return 0

    # Players
    player_ids = pd.concat([
        df[["pitcher_id"]].rename(columns={"pitcher_id": "player_id"}),
        df[["batter_id"]].rename(columns={"batter_id": "player_id"}),
    ]).drop_duplicates("player_id").dropna(subset=["player_id"])
    player_ids["player_id"] = player_ids["player_id"].astype(int)
    con.execute("INSERT OR IGNORE INTO players (player_id) SELECT player_id FROM player_ids")

    # Games
    games_table_cols = table_columns(con, "games")
    game_cols = [c for c in ["game_pk", "game_date", "home_team", "away_team", "game_type", "season"]
                 if c in df.columns and c in games_table_cols]
    games_df = df[game_cols].drop_duplicates("game_pk").copy()
    if "venue" in games_table_cols and "venue" not in games_df.columns:
        games_df["venue"] = None
    game_insert_cols = [c for c in ["game_pk", "game_date", "home_team", "away_team", "venue", "game_type", "season"]
                        if c in games_df.columns and c in games_table_cols]
    col_list = ", ".join(game_insert_cols)
    con.execute(f"INSERT OR REPLACE INTO games ({col_list}) SELECT {col_list} FROM games_df")

    # Pitches
    pitches_table_cols = table_columns(con, "pitches")
    pitch_cols = [c for c in df.columns if c in pitches_table_cols]
    pitches_df = df[pitch_cols].drop_duplicates("pitch_id")
    col_list = ", ".join(pitch_cols)
    con.execute(f"INSERT OR REPLACE INTO pitches ({col_list}) SELECT * FROM pitches_df")

    # At-bats
    ab_df = build_at_bat_rollup(df)
    at_bats_table_cols = table_columns(con, "at_bats")
    ab_insert_cols = [c for c in ab_df.columns if c in at_bats_table_cols]
    ab_col_list = ", ".join(ab_insert_cols)
    con.execute(f"INSERT OR REPLACE INTO at_bats ({ab_col_list}) SELECT {ab_col_list} FROM ab_df")

    # Log
    run_id = next_ingestion_run_id(con)
    con.execute("""
        INSERT INTO ingestion_log (run_id, start_date, end_date, rows_loaded, status)
        VALUES (?, ?, ?, ?, 'success')
    """, [run_id, game["date"], game["date"], len(pitches_df)])

    log.info(f"  Inserted {len(pitches_df)} pitches, {len(ab_df)} at-bats.")
    return len(pitches_df)


def main():
    con = init_db(DB_PATH)

    seasons_updated = set()
    games_filled = []
    games_unavailable = []

    for game in MISSING_GAMES:
        pk = game["game_pk"]
        season = game["season"]

        existing = con.execute(
            "SELECT COUNT(*) FROM pitches WHERE game_pk = ?", [pk]
        ).fetchone()[0]
        if existing > 0:
            log.info(f"game_pk={pk} ({game['matchup']}) already in DB ({existing} pitches). Skipping.")
            continue

        log.info(f"--- {game['date']}  {game['matchup']}  (game_pk={pk}) ---")
        time.sleep(3)

        try:
            raw = statcast_single_game(pk)
        except Exception as e:
            log.error(f"  Fetch failed: {e}")
            games_unavailable.append(game)
            continue

        if raw is None or (hasattr(raw, "empty") and raw.empty):
            log.warning(f"  Baseball Savant returned no data for game_pk={pk}.")
            games_unavailable.append(game)
            continue

        log.info(f"  Fetched {len(raw)} raw rows.")
        rows = insert_game_df(con, raw, game)
        if rows > 0:
            seasons_updated.add(season)
            games_filled.append({**game, "pitches": rows})
        else:
            games_unavailable.append(game)

    # Rebuild derived tables and re-export only touched seasons
    if seasons_updated:
        log.info(f"\nRebuilding derived tables for seasons: {sorted(seasons_updated)}")
        for season in sorted(seasons_updated):
            refresh_season_derived_data(con, season, include_external_season_stats=True)
            export_season_bundle(con, season=season, export_root=EXPORT_ROOT)

    enrich_players(con)
    con.close()

    print("\n" + "=" * 60)
    print("FILL GAPS RESULTS")
    print("=" * 60)
    if games_filled:
        print(f"\n✓ Filled {len(games_filled)} game(s):")
        for g in games_filled:
            print(f"  {g['game_pk']}  {g['date']}  {g['matchup']}  ({g['pitches']} pitches)")
    if games_unavailable:
        print(f"\n✗ Unavailable in Baseball Savant ({len(games_unavailable)} game(s)):")
        for g in games_unavailable:
            print(f"  {g['game_pk']}  {g['date']}  {g['matchup']}")
        print("\n  These games exist in the MLB schedule but Baseball Savant has")
        print("  no pitch data for them. This is a permanent upstream gap —")
        print("  re-running will produce the same result.")
    if not games_filled and not games_unavailable:
        print("All target games already present in the database.")
    print()


if __name__ == "__main__":
    main()
