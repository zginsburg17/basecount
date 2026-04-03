"""
Baseball Analytics Platform - ETL Pipeline
Ingests pitch-level Statcast data via pybaseball and loads into DuckDB.
"""

import argparse
import duckdb
import pandas as pd
from pybaseball import batting_stats, pitching_stats, playerid_reverse_lookup, statcast
from pandas.errors import ParserError
from datetime import date, datetime, timedelta
import json
import logging
from pathlib import Path
import time
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

STATCAST_START_SEASON = 2015
REGULAR_SEASON_GAME_TYPES = ("R",)
POSTSEASON_GAME_TYPES = ("F", "D", "L", "W")
SUPPORTED_GAME_TYPES = REGULAR_SEASON_GAME_TYPES + POSTSEASON_GAME_TYPES
FANGRAPHS_STANDARD_BATTING_COLUMNS = [
    "Name", "Team", "Season", "Age", "G", "AB", "PA", "H", "1B", "2B", "3B",
    "HR", "R", "RBI", "BB", "IBB", "SO", "HBP", "SF", "SH", "GDP", "SB", "CS",
    "AVG", "OBP", "SLG", "OPS", "ISO", "BABIP", "BB%", "K%",
]
FANGRAPHS_VALUE_BATTING_COLUMNS = [
    "Name", "Team", "Season", "Age", "wOBA", "wRAA", "wRC", "wRC+", "BsR",
    "Off", "Def", "WAR", "WPA", "RE24", "RAR", "Dol",
]
FANGRAPHS_STANDARD_PITCHING_COLUMNS = [
    "Name", "Team", "Season", "Age", "W", "L", "ERA", "G", "GS", "CG", "ShO",
    "SV", "BS", "IP", "TBF", "H", "R", "ER", "HR", "BB", "IBB", "HBP", "WP",
    "BK", "SO", "K/9", "BB/9", "K/BB", "AVG", "WHIP", "BABIP", "LOB%", "FIP",
]
FANGRAPHS_VALUE_PITCHING_COLUMNS = [
    "Name", "Team", "Season", "Age", "WAR", "RAR", "Dol", "xFIP", "SIERA",
    "WPA", "RE24", "ERA-", "FIP-", "K%", "BB%", "LOB%",
]
SEASON_VALIDATION_EXPECTATIONS = {
    2020: {"regular_games": 898},
    2025: {
        "regular_games": 2430,
        "postseason_total": 47,
        "postseason_breakdown": {"F": 11, "D": 18, "L": 11, "W": 7},
    },
}


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
-- Core dimension tables
CREATE TABLE IF NOT EXISTS players (
    player_id     INTEGER PRIMARY KEY,
    full_name     VARCHAR,
    bats          VARCHAR(1),   -- L / R / S
    throws        VARCHAR(1),   -- L / R
    position      VARCHAR(5),
    team          VARCHAR(5),
    updated_at    TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS games (
    game_pk       INTEGER PRIMARY KEY,
    game_date     DATE,
    home_team     VARCHAR(5),
    away_team     VARCHAR(5),
    venue         VARCHAR(100),
    game_type     VARCHAR(2),
    season        INTEGER
);

-- Atomic unit: individual pitch
CREATE TABLE IF NOT EXISTS pitches (
    pitch_id              VARCHAR PRIMARY KEY,   -- {game_pk}_{at_bat_number}_{pitch_number}
    game_pk               INTEGER REFERENCES games(game_pk),
    at_bat_number         INTEGER,
    pitch_number          INTEGER,               -- pitch number within at-bat (sequence)

    -- Participants
    pitcher_id            INTEGER REFERENCES players(player_id),
    batter_id             INTEGER REFERENCES players(player_id),
    pitcher_team          VARCHAR(5),
    batter_team           VARCHAR(5),
    stand                 VARCHAR(1),            -- batter handedness this AB
    p_throws              VARCHAR(1),            -- pitcher handedness

    -- Game state at time of pitch
    inning                INTEGER,
    inning_half           VARCHAR(3),            -- "top" / "bot"
    outs_when_up          INTEGER,               -- 0, 1, 2
    balls                 INTEGER,               -- balls BEFORE this pitch
    strikes               INTEGER,               -- strikes BEFORE this pitch
    on_1b                 INTEGER,               -- player_id or NULL
    on_2b                 INTEGER,
    on_3b                 INTEGER,
    base_state            VARCHAR(3),            -- bitmask string e.g. "010"

    -- Pitch physical characteristics
    pitch_type            VARCHAR(5),            -- FF, SL, CH, CU, SI, FC, etc.
    pitch_name            VARCHAR(30),
    game_type             VARCHAR(2),
    release_speed         FLOAT,
    release_spin_rate     FLOAT,
    release_extension     FLOAT,
    release_pos_x         FLOAT,
    release_pos_z         FLOAT,
    pfx_x                 FLOAT,                -- horizontal movement (inches)
    pfx_z                 FLOAT,                -- vertical movement (inches)
    plate_x               FLOAT,                -- horizontal location at plate
    plate_z               FLOAT,                -- vertical location at plate
    sz_top                FLOAT,                -- batter's strike zone top
    sz_bot                FLOAT,                -- batter's strike zone bottom

    -- Pitch outcome
    description           VARCHAR(50),          -- called_strike, swinging_strike, ball, foul, hit_into_play, etc.
    zone                  INTEGER,              -- 1-14 Statcast zone
    type                  VARCHAR(1),           -- B / S / X

    -- Batted ball (non-null only on contact)
    launch_speed          FLOAT,
    launch_angle          FLOAT,
    hit_distance_sc       FLOAT,
    hc_x                  FLOAT,                -- hit coordinate x (spray chart)
    hc_y                  FLOAT,
    bb_type               VARCHAR(20),          -- ground_ball, fly_ball, line_drive, popup

    -- At-bat ending event (non-null only on final pitch of AB)
    events                VARCHAR(50),          -- strikeout, home_run, single, walk, field_out, etc.

    -- Advanced metrics (Statcast)
    estimated_ba_using_speedangle   FLOAT,      -- xBA
    estimated_woba_using_speedangle FLOAT,      -- xwOBA
    woba_value                      FLOAT,
    woba_denom                      INTEGER,
    launch_speed_angle              INTEGER,    -- Statcast sweet spot category

    game_date             DATE,
    season                INTEGER
);

-- At-bat rollup (derived from pitches, pre-aggregated for query speed)
CREATE TABLE IF NOT EXISTS at_bats (
    at_bat_id       VARCHAR PRIMARY KEY,        -- {game_pk}_{at_bat_number}
    game_pk         INTEGER REFERENCES games(game_pk),
    at_bat_number   INTEGER,

    pitcher_id      INTEGER REFERENCES players(player_id),
    batter_id       INTEGER REFERENCES players(player_id),
    stand           VARCHAR(1),
    p_throws        VARCHAR(1),

    inning          INTEGER,
    inning_half     VARCHAR(3),
    outs_at_start   INTEGER,
    on_1b           INTEGER,
    on_2b           INTEGER,
    on_3b           INTEGER,
    base_state      VARCHAR(3),

    pitch_count     INTEGER,
    final_balls     INTEGER,
    final_strikes   INTEGER,
    final_count     VARCHAR(3),                 -- e.g. "3-2"

    -- Did it reach each notable count?
    reached_2strike  BOOLEAN DEFAULT FALSE,
    reached_3ball    BOOLEAN DEFAULT FALSE,
    reached_full     BOOLEAN DEFAULT FALSE,     -- 3-2

    final_event     VARCHAR(50),               -- outcome of AB
    final_pitch_type VARCHAR(5),
    game_type       VARCHAR(2),
    xwoba           FLOAT,
    woba_value      FLOAT,

    game_date       DATE,
    season          INTEGER
);

-- Metadata table to track ingestion runs
CREATE TABLE IF NOT EXISTS ingestion_log (
    run_id      INTEGER PRIMARY KEY,
    start_date  DATE,
    end_date    DATE,
    rows_loaded INTEGER,
    status      VARCHAR(20),
    run_at      TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS season_backfill_status (
    season              INTEGER PRIMARY KEY,
    expected_start_date DATE NOT NULL,
    expected_end_date   DATE NOT NULL,
    loaded_rows         BIGINT DEFAULT 0,
    completed           BOOLEAN DEFAULT FALSE,
    completed_at        TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS batting_standard_stats (
    row_key             VARCHAR PRIMARY KEY,
    season              INTEGER NOT NULL,
    player_id_fangraphs INTEGER,
    player_name         VARCHAR,
    team                VARCHAR,
    age                 INTEGER,
    g                   INTEGER,
    ab                  INTEGER,
    pa                  INTEGER,
    h                   INTEGER,
    singles             INTEGER,
    doubles             INTEGER,
    triples             INTEGER,
    hr                  INTEGER,
    r                   INTEGER,
    rbi                 INTEGER,
    bb                  INTEGER,
    ibb                 INTEGER,
    so                  INTEGER,
    hbp                 INTEGER,
    sf                  INTEGER,
    sh                  INTEGER,
    gdp                 INTEGER,
    sb                  INTEGER,
    cs                  INTEGER,
    avg                 FLOAT,
    obp                 FLOAT,
    slg                 FLOAT,
    ops                 FLOAT,
    iso                 FLOAT,
    babip               FLOAT,
    bb_pct              FLOAT,
    k_pct               FLOAT,
    stats_json          VARCHAR,
    updated_at          TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS batting_value_stats (
    row_key             VARCHAR PRIMARY KEY,
    season              INTEGER NOT NULL,
    player_id_fangraphs INTEGER,
    player_name         VARCHAR,
    team                VARCHAR,
    age                 INTEGER,
    woba                FLOAT,
    wraa                FLOAT,
    wrc                 FLOAT,
    wrc_plus            FLOAT,
    bsr                 FLOAT,
    off_value           FLOAT,
    def_value           FLOAT,
    war                 FLOAT,
    wpa                 FLOAT,
    re24                FLOAT,
    rar                 FLOAT,
    dollars             FLOAT,
    stats_json          VARCHAR,
    updated_at          TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS pitching_standard_stats (
    row_key             VARCHAR PRIMARY KEY,
    season              INTEGER NOT NULL,
    player_id_fangraphs INTEGER,
    player_name         VARCHAR,
    team                VARCHAR,
    age                 INTEGER,
    w                   INTEGER,
    l                   INTEGER,
    era                 FLOAT,
    g                   INTEGER,
    gs                  INTEGER,
    cg                  INTEGER,
    sho                 INTEGER,
    sv                  INTEGER,
    bs                  INTEGER,
    ip                  FLOAT,
    tbf                 INTEGER,
    h                   INTEGER,
    r                   INTEGER,
    er                  INTEGER,
    hr                  INTEGER,
    bb                  INTEGER,
    ibb                 INTEGER,
    hbp                 INTEGER,
    wp                  INTEGER,
    bk                  INTEGER,
    so                  INTEGER,
    k_per_9             FLOAT,
    bb_per_9            FLOAT,
    k_per_bb            FLOAT,
    avg                 FLOAT,
    whip                FLOAT,
    babip               FLOAT,
    lob_pct             FLOAT,
    fip                 FLOAT,
    stats_json          VARCHAR,
    updated_at          TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS pitching_value_stats (
    row_key             VARCHAR PRIMARY KEY,
    season              INTEGER NOT NULL,
    player_id_fangraphs INTEGER,
    player_name         VARCHAR,
    team                VARCHAR,
    age                 INTEGER,
    war                 FLOAT,
    rar                 FLOAT,
    dollars             FLOAT,
    xfip                FLOAT,
    siera               FLOAT,
    wpa                 FLOAT,
    re24                FLOAT,
    era_minus           FLOAT,
    fip_minus           FLOAT,
    k_pct               FLOAT,
    bb_pct              FLOAT,
    lob_pct             FLOAT,
    stats_json          VARCHAR,
    updated_at          TIMESTAMP DEFAULT current_timestamp
);
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_base_state(row) -> str:
    """Encode runner presence as a 3-char bitmask string: '1B 2B 3B' -> '101' etc."""
    return "".join([
        "1" if pd.notna(row.get("on_1b")) else "0",
        "1" if pd.notna(row.get("on_2b")) else "0",
        "1" if pd.notna(row.get("on_3b")) else "0",
    ])


def clean_statcast(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw pybaseball Statcast DataFrame into our schema shape."""
    df = df.copy()

    # Synthetic keys
    df["pitch_id"] = (
        df["game_pk"].astype(str) + "_"
        + df["at_bat_number"].astype(str) + "_"
        + df["pitch_number"].astype(str)
    )
    df["at_bat_id"] = (
        df["game_pk"].astype(str) + "_"
        + df["at_bat_number"].astype(str)
    )

    # Base state bitmask
    df["base_state"] = df.apply(build_base_state, axis=1)

    # Season from game_date
    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date
    df["season"] = pd.to_datetime(df["game_date"]).dt.year

    # Rename to match schema
    renames = {
        "pitcher":              "pitcher_id",
        "batter":               "batter_id",
    }
    df = df.rename(columns=renames)

    return df


def filter_supported_games(df: pd.DataFrame) -> pd.DataFrame:
    if "game_type" not in df.columns:
        return df

    filtered = df[df["game_type"].isin(SUPPORTED_GAME_TYPES)].copy()
    removed = len(df) - len(filtered)
    if removed:
        log.info(f"  Excluded {removed:,} spring-training or unsupported rows by game_type")
    return filtered


def build_at_bat_rollup(pitches_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate pitch-level data into at_bats rows."""
    groups = []
    for ab_id, ab in pitches_df.groupby("at_bat_id"):
        ab_sorted = ab.sort_values("pitch_number")
        first = ab_sorted.iloc[0]
        last  = ab_sorted.iloc[-1]

        row = {
            "at_bat_id":       ab_id,
            "game_pk":         first["game_pk"],
            "at_bat_number":   first["at_bat_number"],
            "pitcher_id":      first["pitcher_id"],
            "batter_id":       first["batter_id"],
            "stand":           first.get("stand"),
            "p_throws":        first.get("p_throws"),
            "inning":          first["inning"],
            "inning_half":     first.get("inning_half", first.get("inning_topbot", "")).lower()[:3],
            "outs_at_start":   first["outs_when_up"],
            "on_1b":           first.get("on_1b"),
            "on_2b":           first.get("on_2b"),
            "on_3b":           first.get("on_3b"),
            "base_state":      first["base_state"],
            "pitch_count":     len(ab_sorted),
            "final_balls":     last["balls"],
            "final_strikes":   last["strikes"],
            "final_count":     f"{last['balls']}-{last['strikes']}",
            "reached_2strike": (ab_sorted["strikes"] >= 2).any(),
            "reached_3ball":   (ab_sorted["balls"] >= 3).any(),
            "reached_full":    ((ab_sorted["balls"] >= 3) & (ab_sorted["strikes"] >= 2)).any(),
            "final_event":     last.get("events"),
            "final_pitch_type":last.get("pitch_type"),
            "game_type":       first.get("game_type"),
            "xwoba":           last.get("estimated_woba_using_speedangle"),
            "woba_value":      last.get("woba_value"),
            "game_date":       first["game_date"],
            "season":          first["season"],
        }
        groups.append(row)

    return pd.DataFrame(groups)


# ---------------------------------------------------------------------------
# Player enrichment
# ---------------------------------------------------------------------------

def resolve_player_names(
    player_ids: list[int],
    batch_size: int = 500,
    retry_delay: float = 1.0,
) -> dict[int, str]:
    """
    Resolve MLBAM player IDs to names.

    Strategy:
      1. Try the requested batch size.
      2. If a batch errors, split it into smaller batches.
      3. Continue splitting until individual IDs.

    This is much more resilient than losing an entire large batch on a single
    transient lookup failure.
    """
    resolved: dict[int, str] = {}

    def _lookup(ids: list[int], size: int) -> None:
        if not ids:
            return

        for start in range(0, len(ids), size):
            chunk = ids[start : start + size]
            try:
                result = playerid_reverse_lookup(chunk, key_type="mlbam")
                if result is not None and not result.empty:
                    for _, row in result.iterrows():
                        pid = int(row["key_mlbam"])
                        full = f"{row['name_first'].strip().title()} {row['name_last'].strip().title()}"
                        resolved[pid] = full
                time.sleep(retry_delay)
            except Exception as exc:
                if len(chunk) == 1:
                    log.warning(f"playerid_reverse_lookup failed for player {chunk[0]}: {exc}")
                else:
                    next_size = max(1, len(chunk) // 2)
                    log.warning(
                        f"playerid_reverse_lookup failed for batch starting at {start} "
                        f"(size {len(chunk)}). Retrying in smaller chunks of {next_size}. Error: {exc}"
                    )
                    _lookup(chunk, next_size)

    _lookup(player_ids, max(1, batch_size))
    return resolved


def lookup_player_profile_via_mlb_api(player_id: int) -> Optional[dict]:
    url = f"https://statsapi.mlb.com/api/v1/people/{player_id}"
    request = Request(url, headers={"User-Agent": "basecount/1.0"})
    try:
        with urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, ValueError) as exc:
        log.warning(f"MLB Stats API lookup failed for player {player_id}: {exc}")
        return None

    people = payload.get("people") or []
    if not people:
        return None

    person = people[0]
    return {
        "full_name": person.get("fullName"),
        "bats": (person.get("batSide") or {}).get("code"),
        "throws": (person.get("pitchHand") or {}).get("code"),
        "position": (person.get("primaryPosition") or {}).get("abbreviation"),
    }


def resolve_player_profiles_via_mlb_api(
    player_ids: list[int],
    delay: float = 0.1,
) -> dict[int, dict]:
    resolved: dict[int, dict] = {}
    for player_id in player_ids:
        profile = lookup_player_profile_via_mlb_api(player_id)
        if profile:
            resolved[player_id] = profile
        time.sleep(delay)
    return resolved


def enrich_players(con: duckdb.DuckDBPyConnection, batch_size: int = 500) -> int:
    """
    Fill in full_name, bats, throws, position, and team for player stubs
    (rows where full_name IS NULL).

    Sources:
      - full_name  : pybaseball playerid_reverse_lookup (MLBAM key)
      - bats       : most common `stand` value from pitches (batters)
      - throws     : most common `p_throws` value from pitches (pitchers)
      - position   : 'P' if they appear as pitcher_id, else 'B' (heuristic)
      - team       : most recent pitcher_team / batter_team from pitches
    """
    stub_ids = con.execute(
        "SELECT player_id FROM players WHERE full_name IS NULL"
    ).fetchdf()["player_id"].tolist()

    if not stub_ids:
        log.info("No player stubs to enrich.")
        return 0

    log.info(f"Enriching {len(stub_ids)} player stubs...")

    name_map = resolve_player_names(stub_ids, batch_size=batch_size)
    unresolved_ids = [pid for pid in stub_ids if pid not in name_map]
    fallback_profiles = resolve_player_profiles_via_mlb_api(unresolved_ids) if unresolved_ids else {}

    # --- Derive bats, throws, position, team from pitch data ---
    enrichment = con.execute("""
        WITH pitcher_stats AS (
            SELECT
                pitcher_id                          AS player_id,
                'P'                                 AS position,
                first(p_throws ORDER BY game_date DESC) FILTER (WHERE p_throws IS NOT NULL)  AS throws,
                NULL                                AS bats,
                first(pitcher_team ORDER BY game_date DESC) FILTER (WHERE pitcher_team IS NOT NULL) AS team
            FROM pitches
            WHERE pitcher_id IS NOT NULL
            GROUP BY pitcher_id
        ),
        batter_stats AS (
            SELECT
                batter_id                           AS player_id,
                'B'                                 AS position,
                NULL                                AS throws,
                first(stand ORDER BY game_date DESC) FILTER (WHERE stand IS NOT NULL) AS bats,
                first(batter_team ORDER BY game_date DESC) FILTER (WHERE batter_team IS NOT NULL) AS team
            FROM pitches
            WHERE batter_id IS NOT NULL
            GROUP BY batter_id
        ),
        combined AS (
            SELECT player_id, position, throws, bats, team FROM pitcher_stats
            UNION ALL
            SELECT player_id, position, throws, bats, team FROM batter_stats
        )
        SELECT
            player_id,
            -- prefer 'P' if they pitched at all
            CASE WHEN bool_or(position = 'P') THEN 'P' ELSE 'B' END AS position,
            first(throws) FILTER (WHERE throws IS NOT NULL)  AS throws,
            first(bats)   FILTER (WHERE bats   IS NOT NULL)  AS bats,
            first(team)   FILTER (WHERE team   IS NOT NULL)  AS team
        FROM combined
        GROUP BY player_id
    """).fetchdf()

    updated = 0
    for _, row in enrichment.iterrows():
        pid = int(row["player_id"])
        if pid not in stub_ids:
            continue
        fallback_profile = fallback_profiles.get(pid, {})
        full_name = name_map.get(pid) or fallback_profile.get("full_name")
        con.execute("""
            UPDATE players SET
                full_name = COALESCE(?, full_name),
                bats      = COALESCE(?, ?, bats),
                throws    = COALESCE(?, ?, throws),
                position  = COALESCE(?, ?, position),
                team      = COALESCE(?, team),
                updated_at = current_timestamp
            WHERE player_id = ?
        """, [
            full_name,
            row.get("bats"), fallback_profile.get("bats"),
            row.get("throws"), fallback_profile.get("throws"),
            row.get("position"), fallback_profile.get("position"),
            row.get("team"),
            pid,
        ])
        updated += 1

    unresolved = max(len(stub_ids) - len(name_map), 0)
    fallback_resolved = sum(1 for profile in fallback_profiles.values() if profile.get("full_name"))
    total_resolved = len(name_map) + fallback_resolved
    unresolved = max(len(stub_ids) - total_resolved, 0)
    log.info(
        f"  Players enriched: {updated} "
        f"(names resolved via pybaseball: {len(name_map)}, "
        f"via MLB Stats API: {fallback_resolved}, unresolved: {unresolved})"
    )
    return updated


# ---------------------------------------------------------------------------
# Core ETL
# ---------------------------------------------------------------------------

def init_db(db_path: str = "baseball.duckdb") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path)
    con.execute(SCHEMA_SQL)
    ensure_optional_column(con, "games", "game_type", "VARCHAR")
    ensure_optional_column(con, "pitches", "game_type", "VARCHAR")
    ensure_optional_column(con, "at_bats", "game_type", "VARCHAR")
    log.info(f"Database initialized at {db_path}")
    return con


def table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return {row[1] for row in rows}


def has_column(con: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
    return column_name in table_columns(con, table_name)


def ensure_optional_column(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
    column_type: str,
) -> None:
    if has_column(con, table_name, column_name):
        return
    try:
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
    except Exception as exc:
        log.warning(
            f"Could not add optional column {table_name}.{column_name}. "
            f"Continuing with existing schema. Error: {exc}"
        )


def next_ingestion_run_id(con: duckdb.DuckDBPyConnection) -> int:
    run_id = con.execute(
        "SELECT COALESCE(MAX(run_id), 0) + 1 FROM ingestion_log"
    ).fetchone()[0]
    return int(run_id)


def delete_season_data(con: duckdb.DuckDBPyConnection, season: int) -> None:
    log.info(f"Deleting existing data for season {season}...")
    for table_name in [
        "pitching_value_stats",
        "pitching_standard_stats",
        "batting_value_stats",
        "batting_standard_stats",
        "at_bats",
        "pitches",
        "games",
        "season_backfill_status",
    ]:
        con.execute(f"DELETE FROM {table_name} WHERE season = ?", [season])


def _serialize_stat_value(value):
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    return value


def _stat_row_key(season: int, row: pd.Series) -> str:
    player_id = row.get("IDfg")
    player_name = (row.get("Name") or "").strip()
    team = (row.get("Team") or "").strip()
    player_token = str(int(player_id)) if pd.notna(player_id) else player_name.replace(" ", "_")
    return f"{season}:{player_token}:{team or 'NA'}"


def _safe_value(row: pd.Series, column: str):
    if column not in row.index:
        return None
    return _serialize_stat_value(row[column])


def _build_processed_stats_frame(
    raw_df: pd.DataFrame,
    season: int,
    *,
    metric_columns: dict[str, str],
) -> pd.DataFrame:
    records = []
    for _, row in raw_df.iterrows():
        records.append({
            "row_key": _stat_row_key(season, row),
            "season": int(_safe_value(row, "Season") or season),
            "player_id_fangraphs": _safe_value(row, "IDfg"),
            "player_name": _safe_value(row, "Name"),
            "team": _safe_value(row, "Team"),
            "age": _safe_value(row, "Age"),
            **{target: _safe_value(row, source) for target, source in metric_columns.items()},
            "stats_json": json.dumps(
                {str(col): _serialize_stat_value(row[col]) for col in raw_df.columns},
                default=str,
            ),
        })
    return pd.DataFrame(records)


def _replace_stat_rows(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    rows_df: pd.DataFrame,
    season: int,
) -> int:
    con.execute(f"DELETE FROM {table_name} WHERE season = ?", [season])
    if rows_df.empty:
        log.warning(f"No rows returned for {table_name} season {season}.")
        return 0

    con.register("_stats_rows_df", rows_df)
    column_list = ", ".join(rows_df.columns.tolist())
    con.execute(f"INSERT OR REPLACE INTO {table_name} ({column_list}) SELECT {column_list} FROM _stats_rows_df")
    con.unregister("_stats_rows_df")
    return len(rows_df)


def ingest_fangraphs_season_stats(
    con: duckdb.DuckDBPyConnection,
    season: int,
    *,
    qual: int = 0,
) -> dict:
    log.info(f"Fetching Fangraphs batting/pitching stats for season {season}...")
    raw_batting = batting_stats(season, qual=qual, split_seasons=True)
    raw_pitching = pitching_stats(season, qual=qual, split_seasons=True)

    if raw_batting is None:
        raw_batting = pd.DataFrame()
    if raw_pitching is None:
        raw_pitching = pd.DataFrame()

    batting_standard_df = _build_processed_stats_frame(
        raw_batting[[col for col in FANGRAPHS_STANDARD_BATTING_COLUMNS if col in raw_batting.columns]].copy(),
        season,
        metric_columns={
            "g": "G", "ab": "AB", "pa": "PA", "h": "H", "singles": "1B", "doubles": "2B",
            "triples": "3B", "hr": "HR", "r": "R", "rbi": "RBI", "bb": "BB", "ibb": "IBB",
            "so": "SO", "hbp": "HBP", "sf": "SF", "sh": "SH", "gdp": "GDP", "sb": "SB",
            "cs": "CS", "avg": "AVG", "obp": "OBP", "slg": "SLG", "ops": "OPS", "iso": "ISO",
            "babip": "BABIP", "bb_pct": "BB%", "k_pct": "K%",
        },
    )
    batting_value_df = _build_processed_stats_frame(
        raw_batting[[col for col in FANGRAPHS_VALUE_BATTING_COLUMNS if col in raw_batting.columns]].copy(),
        season,
        metric_columns={
            "woba": "wOBA", "wraa": "wRAA", "wrc": "wRC", "wrc_plus": "wRC+", "bsr": "BsR",
            "off_value": "Off", "def_value": "Def", "war": "WAR", "wpa": "WPA", "re24": "RE24",
            "rar": "RAR", "dollars": "Dol",
        },
    )
    pitching_standard_df = _build_processed_stats_frame(
        raw_pitching[[col for col in FANGRAPHS_STANDARD_PITCHING_COLUMNS if col in raw_pitching.columns]].copy(),
        season,
        metric_columns={
            "w": "W", "l": "L", "era": "ERA", "g": "G", "gs": "GS", "cg": "CG", "sho": "ShO",
            "sv": "SV", "bs": "BS", "ip": "IP", "tbf": "TBF", "h": "H", "r": "R", "er": "ER",
            "hr": "HR", "bb": "BB", "ibb": "IBB", "hbp": "HBP", "wp": "WP", "bk": "BK",
            "so": "SO", "k_per_9": "K/9", "bb_per_9": "BB/9", "k_per_bb": "K/BB", "avg": "AVG",
            "whip": "WHIP", "babip": "BABIP", "lob_pct": "LOB%", "fip": "FIP",
        },
    )
    pitching_value_df = _build_processed_stats_frame(
        raw_pitching[[col for col in FANGRAPHS_VALUE_PITCHING_COLUMNS if col in raw_pitching.columns]].copy(),
        season,
        metric_columns={
            "war": "WAR", "rar": "RAR", "dollars": "Dol", "xfip": "xFIP", "siera": "SIERA",
            "wpa": "WPA", "re24": "RE24", "era_minus": "ERA-", "fip_minus": "FIP-",
            "k_pct": "K%", "bb_pct": "BB%", "lob_pct": "LOB%",
        },
    )

    results = {
        "batting_standard_rows": _replace_stat_rows(con, "batting_standard_stats", batting_standard_df, season),
        "batting_value_rows": _replace_stat_rows(con, "batting_value_stats", batting_value_df, season),
        "pitching_standard_rows": _replace_stat_rows(con, "pitching_standard_stats", pitching_standard_df, season),
        "pitching_value_rows": _replace_stat_rows(con, "pitching_value_stats", pitching_value_df, season),
    }
    log.info(
        "Fangraphs season stats loaded for %s: batting standard=%s, batting value=%s, "
        "pitching standard=%s, pitching value=%s",
        season,
        results["batting_standard_rows"],
        results["batting_value_rows"],
        results["pitching_standard_rows"],
        results["pitching_value_rows"],
    )
    return results


def season_report(con: duckdb.DuckDBPyConnection, season: int) -> dict:
    breakdown_rows = con.execute(
        """
        SELECT game_type, COUNT(DISTINCT game_pk) AS games
        FROM pitches
        WHERE season = ?
        GROUP BY game_type
        ORDER BY game_type
        """,
        [season],
    ).fetchall()
    breakdown = {row[0]: int(row[1]) for row in breakdown_rows}
    regular_games = breakdown.get("R", 0)
    postseason_breakdown = {k: v for k, v in breakdown.items() if k in POSTSEASON_GAME_TYPES}
    postseason_total = sum(postseason_breakdown.values())
    expectations = SEASON_VALIDATION_EXPECTATIONS.get(season, {})
    return {
        "season": season,
        "regular_games": regular_games,
        "postseason_total": postseason_total,
        "postseason_breakdown": postseason_breakdown,
        "expected": expectations,
        "batting_standard_rows": con.execute("SELECT COUNT(*) FROM batting_standard_stats WHERE season = ?", [season]).fetchone()[0],
        "batting_value_rows": con.execute("SELECT COUNT(*) FROM batting_value_stats WHERE season = ?", [season]).fetchone()[0],
        "pitching_standard_rows": con.execute("SELECT COUNT(*) FROM pitching_standard_stats WHERE season = ?", [season]).fetchone()[0],
        "pitching_value_rows": con.execute("SELECT COUNT(*) FROM pitching_value_stats WHERE season = ?", [season]).fetchone()[0],
    }


def fetch_statcast_with_fallback(
    start: date,
    end: date,
    *,
    delay: float = 2.0,
    retry_count: int = 2,
) -> pd.DataFrame:
    """
    Fetch a Statcast range and automatically split it into smaller windows if
    pybaseball returns malformed CSV or another transient parsing/network error.
    """
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    log.info(f"Fetching Statcast data: {start_str} → {end_str}")
    time.sleep(delay)

    last_error: Optional[Exception] = None
    for attempt in range(1, retry_count + 1):
        try:
            raw = statcast(start_dt=start_str, end_dt=end_str)
            return raw if raw is not None else pd.DataFrame()
        except (ParserError, ValueError) as exc:
            last_error = exc
            log.warning(
                f"Statcast response was malformed for {start_str} → {end_str} "
                f"(attempt {attempt}/{retry_count}): {exc}"
            )
        except Exception as exc:
            last_error = exc
            log.warning(
                f"Statcast request failed for {start_str} → {end_str} "
                f"(attempt {attempt}/{retry_count}): {exc}"
            )
        time.sleep(min(3.0 * attempt, 10.0))

    if start == end:
        raise RuntimeError(
            f"Statcast request failed for single day {start_str} after {retry_count} attempts: {last_error}"
        )

    midpoint = start + timedelta(days=(end - start).days // 2)
    left_end = midpoint
    right_start = midpoint + timedelta(days=1)

    log.warning(
        f"Splitting Statcast range {start_str} → {end_str} into "
        f"{start.strftime('%Y-%m-%d')} → {left_end.strftime('%Y-%m-%d')} and "
        f"{right_start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}"
    )

    frames = []
    left = fetch_statcast_with_fallback(start, left_end, delay=delay, retry_count=retry_count)
    if left is not None and not left.empty:
        frames.append(left)
    right = fetch_statcast_with_fallback(right_start, end, delay=delay, retry_count=retry_count)
    if right is not None and not right.empty:
        frames.append(right)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def mark_season_backfill_started(
    con: duckdb.DuckDBPyConnection,
    season: int,
    expected_start_date: date,
    expected_end_date: date,
) -> None:
    now = datetime.now()
    con.execute(
        """
        INSERT INTO season_backfill_status (
            season,
            expected_start_date,
            expected_end_date,
            loaded_rows,
            completed,
            completed_at,
            updated_at
        )
        VALUES (?, ?, ?, 0, FALSE, NULL, ?)
        ON CONFLICT(season) DO UPDATE SET
            expected_start_date = excluded.expected_start_date,
            expected_end_date = excluded.expected_end_date,
            loaded_rows = 0,
            completed = FALSE,
            completed_at = NULL,
            updated_at = excluded.updated_at
        """,
        [season, expected_start_date, expected_end_date, now],
    )


def mark_season_backfill_complete(
    con: duckdb.DuckDBPyConnection,
    season: int,
    expected_start_date: date,
    expected_end_date: date,
    loaded_rows: int,
) -> None:
    now = datetime.now()
    con.execute(
        """
        INSERT INTO season_backfill_status (
            season,
            expected_start_date,
            expected_end_date,
            loaded_rows,
            completed,
            completed_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, TRUE, ?, ?)
        ON CONFLICT(season) DO UPDATE SET
            expected_start_date = excluded.expected_start_date,
            expected_end_date = excluded.expected_end_date,
            loaded_rows = excluded.loaded_rows,
            completed = TRUE,
            completed_at = excluded.completed_at,
            updated_at = excluded.updated_at
        """,
        [season, expected_start_date, expected_end_date, loaded_rows, now, now],
    )


def load_date_range(
    con: duckdb.DuckDBPyConnection,
    start_date: str,
    end_date: str,
    delay: float = 2.0,
) -> int:
    """
    Pull Statcast data for a date range, transform, and upsert into DuckDB.
    Returns total rows loaded.
    """
    raw = fetch_statcast_with_fallback(
        datetime.strptime(start_date, "%Y-%m-%d").date(),
        datetime.strptime(end_date, "%Y-%m-%d").date(),
        delay=delay,
    )
    if raw is None or raw.empty:
        log.warning("No data returned for this range.")
        return 0

    log.info(f"  Raw rows fetched: {len(raw)}")
    df = clean_statcast(raw)
    df = filter_supported_games(df)
    if df.empty:
        log.warning("No regular-season or postseason data returned for this range.")
        return 0

    # --- Load players (must come before pitches due to FK constraint) ---
    player_ids = pd.concat([
        df[["pitcher_id"]].rename(columns={"pitcher_id": "player_id"}),
        df[["batter_id"]].rename(columns={"batter_id": "player_id"}),
    ]).drop_duplicates("player_id").dropna(subset=["player_id"])
    player_ids["player_id"] = player_ids["player_id"].astype(int)
    con.execute("INSERT OR IGNORE INTO players (player_id) SELECT player_id FROM player_ids")
    log.info(f"  Players upserted: {len(player_ids)}")

    # --- Load games (must come before pitches due to FK constraint) ---
    games_table_cols = table_columns(con, "games")
    game_cols = [c for c in ["game_pk", "game_date", "home_team", "away_team", "game_type"] if c in df.columns and c in games_table_cols]
    games_df = df[game_cols].drop_duplicates("game_pk").copy()
    games_df["venue"] = None
    games_df["season"] = pd.to_datetime(games_df["game_date"]).dt.year
    if "venue" in games_table_cols and "venue" not in games_df.columns:
        games_df["venue"] = None
    game_insert_cols = [col for col in ["game_pk", "game_date", "home_team", "away_team", "venue", "game_type", "season"] if col in games_df.columns and col in games_table_cols]
    game_col_list = ", ".join(game_insert_cols)
    con.execute(f"INSERT OR REPLACE INTO games ({game_col_list}) SELECT {game_col_list} FROM games_df")
    log.info(f"  Games upserted: {len(games_df)}")

    # --- Load pitches ---
    pitches_table_cols = table_columns(con, "pitches")
    pitch_cols = [c for c in df.columns if c in [
        "pitch_id", "game_pk", "at_bat_number", "pitch_number",
        "pitcher_id", "batter_id", "pitcher_team", "batter_team",
        "stand", "p_throws",
        "inning", "inning_half", "outs_when_up", "balls", "strikes",
        "on_1b", "on_2b", "on_3b", "base_state",
        "pitch_type", "pitch_name", "game_type",
        "release_speed", "release_spin_rate", "release_extension",
        "release_pos_x", "release_pos_z",
        "pfx_x", "pfx_z", "plate_x", "plate_z", "sz_top", "sz_bot",
        "description", "zone", "type",
        "launch_speed", "launch_angle", "hit_distance_sc",
        "hc_x", "hc_y", "bb_type",
        "events",
        "estimated_ba_using_speedangle",
        "estimated_woba_using_speedangle",
        "woba_value", "woba_denom", "launch_speed_angle",
        "game_date", "season",
    ] and c in pitches_table_cols]
    pitches_df = df[pitch_cols].drop_duplicates("pitch_id")

    col_list = ", ".join(pitch_cols)
    con.execute(f"INSERT OR REPLACE INTO pitches ({col_list}) SELECT * FROM pitches_df")
    log.info(f"  Pitches upserted: {len(pitches_df)}")

    # --- Load at_bats rollup ---
    ab_df = build_at_bat_rollup(df)
    at_bats_table_cols = table_columns(con, "at_bats")
    ab_insert_cols = [col for col in ab_df.columns if col in at_bats_table_cols]
    ab_col_list = ", ".join(ab_insert_cols)
    con.execute(f"INSERT OR REPLACE INTO at_bats ({ab_col_list}) SELECT {ab_col_list} FROM ab_df")
    log.info(f"  At-bats upserted: {len(ab_df)}")

    # --- Log run ---
    run_id = next_ingestion_run_id(con)
    con.execute("""
        INSERT INTO ingestion_log (run_id, start_date, end_date, rows_loaded, status)
        VALUES (?, ?, ?, ?, 'success')
    """, [run_id, start_date, end_date, len(pitches_df)])

    return len(pitches_df)


def season_bounds(season: int) -> tuple[date, date]:
    """Best-effort Statcast window for a given MLB season."""
    today = date.today()

    if season < STATCAST_START_SEASON:
        raise ValueError(f"Statcast pitch-level coverage starts in {STATCAST_START_SEASON}")

    if season == 2020:
        start = date(2020, 7, 23)
        end = date(2020, 9, 27)
    else:
        start = date(season, 3, 1)
        end = date(season, 11, 15)

    if season == today.year:
        end = min(end, today)

    return start, end


def available_statcast_seasons(current_year: Optional[int] = None) -> list[int]:
    end_year = current_year or date.today().year
    return list(range(STATCAST_START_SEASON, end_year + 1))


def backfill_season(
    con: duckdb.DuckDBPyConnection,
    season: int,
    chunk_days: int = 7,
    delay_between_chunks: float = 3.0,
):
    """
    Backfill a full season in weekly chunks to avoid hammering Baseball Savant.
    Typical MLB season: late March → late September.
    """
    season_start, season_end = season_bounds(season)
    mark_season_backfill_started(con, season, season_start, season_end)
    current = season_start
    total = 0

    while current <= season_end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), season_end)
        rows = load_date_range(
            con,
            start_date=current.strftime("%Y-%m-%d"),
            end_date=chunk_end.strftime("%Y-%m-%d"),
        )
        total += rows
        current = chunk_end + timedelta(days=1)
        if current <= season_end:
            time.sleep(delay_between_chunks)

    log.info(f"Season {season} backfill complete. Total pitches: {total:,}")
    mark_season_backfill_complete(con, season, season_start, season_end, total)
    return total


def backfill_season_range(
    con: duckdb.DuckDBPyConnection,
    season_start: int,
    season_end: int,
    chunk_days: int = 7,
    delay_between_seasons: float = 5.0,
    include_season_stats: bool = True,
) -> int:
    """Backfill an inclusive season range."""
    start, end = sorted((season_start, season_end))
    total = 0

    for idx, season in enumerate(range(start, end + 1)):
        total += backfill_season(con, season=season, chunk_days=chunk_days)
        if include_season_stats:
            ingest_fangraphs_season_stats(con, season)
        if idx < (end - start):
            time.sleep(delay_between_seasons)

    log.info(f"Historical backfill {start}-{end} complete. Total pitches: {total:,}")
    return total


def backfill_all_history(
    con: duckdb.DuckDBPyConnection,
    chunk_days: int = 7,
    include_season_stats: bool = True,
) -> int:
    seasons = available_statcast_seasons()
    return backfill_season_range(
        con,
        seasons[0],
        seasons[-1],
        chunk_days=chunk_days,
        include_season_stats=include_season_stats,
    )


def loaded_seasons(con: duckdb.DuckDBPyConnection) -> list[int]:
    return [
        row[0]
        for row in con.execute(
            "SELECT DISTINCT season FROM pitches WHERE season IS NOT NULL ORDER BY season"
        ).fetchall()
    ]


def completed_seasons(con: duckdb.DuckDBPyConnection) -> list[int]:
    return [
        row[0]
        for row in con.execute(
            """
            SELECT season
            FROM season_backfill_status
            WHERE completed = TRUE
            ORDER BY season
            """
        ).fetchall()
    ]


def expected_statcast_seasons(current_year: Optional[int] = None) -> list[int]:
    return available_statcast_seasons(current_year=current_year)


def missing_statcast_seasons(
    con: duckdb.DuckDBPyConnection,
    current_year: Optional[int] = None,
) -> list[int]:
    loaded = set(loaded_seasons(con))
    expected = expected_statcast_seasons(current_year=current_year)
    return [season for season in expected if season not in loaded]


def player_name_coverage(con: duckdb.DuckDBPyConnection) -> dict:
    total_players, named_players, unnamed_players, team_players = con.execute(
        """
        SELECT
            COUNT(*) AS total_players,
            COUNT(*) FILTER (WHERE full_name IS NOT NULL) AS named_players,
            COUNT(*) FILTER (WHERE full_name IS NULL) AS unnamed_players,
            COUNT(*) FILTER (WHERE team IS NOT NULL) AS team_players
        FROM players
        """
    ).fetchone()

    return {
        "total_players": total_players or 0,
        "named_players": named_players or 0,
        "unnamed_players": unnamed_players or 0,
        "team_players": team_players or 0,
    }


def verify_history_coverage(
    con: duckdb.DuckDBPyConnection,
    current_year: Optional[int] = None,
) -> dict:
    expected = expected_statcast_seasons(current_year=current_year)
    loaded = loaded_seasons(con)
    completed = completed_seasons(con)
    missing = [season for season in expected if season not in set(completed)]
    incomplete = [season for season in loaded if season not in set(completed)]

    return {
        "expected_seasons": expected,
        "loaded_seasons": loaded,
        "completed_seasons": completed,
        "missing_seasons": missing,
        "incomplete_seasons": incomplete,
        "history_complete": len(missing) == 0,
    }


def ensure_full_history(
    con: duckdb.DuckDBPyConnection,
    chunk_days: int = 7,
    include_season_stats: bool = True,
) -> dict:
    coverage = verify_history_coverage(con)
    missing = coverage["missing_seasons"]

    if missing:
        log.info(f"Missing historical seasons detected: {missing}")
        for season in missing:
            backfill_season(con, season=season, chunk_days=chunk_days)
            if include_season_stats:
                ingest_fangraphs_season_stats(con, season)
    else:
        log.info("Full historical season coverage already present.")

    enrich_players(con)
    coverage = verify_history_coverage(con)
    coverage["players_status"] = player_name_coverage(con)
    return coverage


def current_season_update(
    con: duckdb.DuckDBPyConnection,
    days: int = 2,
    include_season_stats: bool = True,
) -> None:
    today = date.today()
    start = today - timedelta(days=max(days - 1, 0))
    load_date_range(
        con,
        start_date=start.strftime("%Y-%m-%d"),
        end_date=today.strftime("%Y-%m-%d"),
    )
    enrich_players(con)
    if include_season_stats:
        ingest_fangraphs_season_stats(con, today.year)


def export_season_bundle(
    con: duckdb.DuckDBPyConnection,
    season: int,
    export_root: str = "exports",
) -> Path:
    export_dir = Path(export_root) / f"season={season}"
    export_dir.mkdir(parents=True, exist_ok=True)

    pitches_path = export_dir / "pitches.parquet"
    at_bats_path = export_dir / "at_bats.parquet"
    games_path = export_dir / "games.parquet"
    players_path = export_dir / "players.parquet"
    batting_standard_path = export_dir / "batting_standard_stats.parquet"
    batting_value_path = export_dir / "batting_value_stats.parquet"
    pitching_standard_path = export_dir / "pitching_standard_stats.parquet"
    pitching_value_path = export_dir / "pitching_value_stats.parquet"
    metadata_path = export_dir / "metadata.json"

    con.execute(
        f"COPY (SELECT * FROM pitches WHERE season = {season}) TO '{pitches_path.as_posix()}' (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY (SELECT * FROM at_bats WHERE season = {season}) TO '{at_bats_path.as_posix()}' (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY (SELECT * FROM games WHERE season = {season}) TO '{games_path.as_posix()}' (FORMAT PARQUET)"
    )
    con.execute(
        f"""
        COPY (
            SELECT DISTINCT pl.*
            FROM players pl
            WHERE pl.player_id IN (
                SELECT pitcher_id FROM pitches WHERE season = {season}
                UNION
                SELECT batter_id FROM pitches WHERE season = {season}
            )
        ) TO '{players_path.as_posix()}' (FORMAT PARQUET)
        """
    )
    con.execute(
        f"COPY (SELECT * FROM batting_standard_stats WHERE season = {season}) TO '{batting_standard_path.as_posix()}' (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY (SELECT * FROM batting_value_stats WHERE season = {season}) TO '{batting_value_path.as_posix()}' (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY (SELECT * FROM pitching_standard_stats WHERE season = {season}) TO '{pitching_standard_path.as_posix()}' (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY (SELECT * FROM pitching_value_stats WHERE season = {season}) TO '{pitching_value_path.as_posix()}' (FORMAT PARQUET)"
    )

    metadata = {
        "season": season,
        "exported_at": datetime.now().isoformat(),
        "files": {
            "pitches": pitches_path.name,
            "at_bats": at_bats_path.name,
            "games": games_path.name,
            "players": players_path.name,
            "batting_standard_stats": batting_standard_path.name,
            "batting_value_stats": batting_value_path.name,
            "pitching_standard_stats": pitching_standard_path.name,
            "pitching_value_stats": pitching_value_path.name,
        },
    }
    metadata_path.write_text(json.dumps(metadata, indent=2))
    log.info(f"Exported season {season} to {export_dir}")
    return export_dir


def import_season_bundle(
    con: duckdb.DuckDBPyConnection,
    import_dir: str,
) -> None:
    bundle_dir = Path(import_dir)
    if not bundle_dir.exists():
        raise SystemExit(f"Import directory does not exist: {bundle_dir}")

    pitches_path = bundle_dir / "pitches.parquet"
    at_bats_path = bundle_dir / "at_bats.parquet"
    games_path = bundle_dir / "games.parquet"
    players_path = bundle_dir / "players.parquet"
    batting_standard_path = bundle_dir / "batting_standard_stats.parquet"
    batting_value_path = bundle_dir / "batting_value_stats.parquet"
    pitching_standard_path = bundle_dir / "pitching_standard_stats.parquet"
    pitching_value_path = bundle_dir / "pitching_value_stats.parquet"

    required = [
        pitches_path,
        at_bats_path,
        games_path,
        players_path,
        batting_standard_path,
        batting_value_path,
        pitching_standard_path,
        pitching_value_path,
    ]
    missing_files = [path for path in required if not path.exists()]
    if missing_files:
        raise SystemExit(f"Import bundle is incomplete. Missing files: {missing_files}")

    con.execute(f"INSERT OR REPLACE INTO players SELECT * FROM read_parquet('{players_path.as_posix()}')")
    con.execute(f"INSERT OR REPLACE INTO games SELECT * FROM read_parquet('{games_path.as_posix()}')")
    con.execute(f"INSERT OR REPLACE INTO pitches SELECT * FROM read_parquet('{pitches_path.as_posix()}')")
    con.execute(f"INSERT OR REPLACE INTO at_bats SELECT * FROM read_parquet('{at_bats_path.as_posix()}')")
    con.execute(f"INSERT OR REPLACE INTO batting_standard_stats SELECT * FROM read_parquet('{batting_standard_path.as_posix()}')")
    con.execute(f"INSERT OR REPLACE INTO batting_value_stats SELECT * FROM read_parquet('{batting_value_path.as_posix()}')")
    con.execute(f"INSERT OR REPLACE INTO pitching_standard_stats SELECT * FROM read_parquet('{pitching_standard_path.as_posix()}')")
    con.execute(f"INSERT OR REPLACE INTO pitching_value_stats SELECT * FROM read_parquet('{pitching_value_path.as_posix()}')")
    log.info(f"Imported season bundle from {bundle_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Statcast data into DuckDB.")
    parser.add_argument(
        "mode",
        nargs="?",
        default="recent",
        choices=[
            "recent",
            "season",
            "rebuild-season",
            "range",
            "all-history",
            "ensure-history",
            "current-season-update",
            "export-season",
            "import-season",
            "enrich",
            "status",
            "season-report",
        ],
        help="Load recent data, one season, rebuild one season, a season range, all history, ensure full history, update the current season, export/import season bundles, enrich names, inspect DB status, or report one season.",
    )
    parser.add_argument("--db-path", default="baseball.duckdb", help="DuckDB database path.")
    parser.add_argument("--season", type=int, help="Single season to backfill.")
    parser.add_argument("--season-start", type=int, help="First season in a range.")
    parser.add_argument("--season-end", type=int, help="Last season in a range.")
    parser.add_argument("--chunk-days", type=int, default=7, help="Chunk size for backfills.")
    parser.add_argument("--days", type=int, default=7, help="Days of recent data to ingest in recent mode.")
    parser.add_argument("--export-root", default="exports", help="Directory for exported season bundles.")
    parser.add_argument("--import-dir", help="Directory for an exported season bundle to import.")
    parser.add_argument(
        "--skip-enrich",
        action="store_true",
        help="Skip player metadata enrichment after loading new data.",
    )
    parser.add_argument(
        "--skip-season-stats",
        action="store_true",
        help="Skip Fangraphs standard/value batting and pitching stat ingestion.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = parse_args()
    con = init_db(args.db_path)

    if args.mode == "recent":
        today = date.today()
        start = today - timedelta(days=max(args.days - 1, 0))
        load_date_range(
            con,
            start_date=start.strftime("%Y-%m-%d"),
            end_date=today.strftime("%Y-%m-%d"),
        )
        if not args.skip_enrich:
            enrich_players(con)
        if not args.skip_season_stats:
            ingest_fangraphs_season_stats(con, today.year)

    elif args.mode == "season":
        if args.season is None:
            raise SystemExit("--season is required for mode 'season'")
        backfill_season(con, season=args.season, chunk_days=args.chunk_days)
        if not args.skip_season_stats:
            ingest_fangraphs_season_stats(con, args.season)
        if not args.skip_enrich:
            enrich_players(con)

    elif args.mode == "rebuild-season":
        if args.season is None:
            raise SystemExit("--season is required for mode 'rebuild-season'")
        delete_season_data(con, args.season)
        backfill_season(con, season=args.season, chunk_days=args.chunk_days)
        if not args.skip_season_stats:
            ingest_fangraphs_season_stats(con, args.season)
        if not args.skip_enrich:
            enrich_players(con)
        report = season_report(con, args.season)
        log.info(f"Season report: {report}")

    elif args.mode == "range":
        if args.season_start is None or args.season_end is None:
            raise SystemExit("--season-start and --season-end are required for mode 'range'")
        backfill_season_range(
            con,
            season_start=args.season_start,
            season_end=args.season_end,
            chunk_days=args.chunk_days,
            include_season_stats=not args.skip_season_stats,
        )
        if not args.skip_enrich:
            enrich_players(con)

    elif args.mode == "all-history":
        backfill_all_history(
            con,
            chunk_days=args.chunk_days,
            include_season_stats=not args.skip_season_stats,
        )
        if not args.skip_enrich:
            enrich_players(con)

    elif args.mode == "ensure-history":
        coverage = ensure_full_history(
            con,
            chunk_days=args.chunk_days,
            include_season_stats=not args.skip_season_stats,
        )
        if not coverage["history_complete"]:
            raise SystemExit(
                f"Historical coverage is still incomplete. Missing seasons: {coverage['missing_seasons']}"
            )
        if not coverage["players_status"]["named_players"] == coverage["players_status"]["total_players"]:
            raise SystemExit(
                "Player name coverage is incomplete after enrichment. "
                f"Coverage: {coverage['players_status']}"
            )

    elif args.mode == "current-season-update":
        current_season_update(
            con,
            days=args.days,
            include_season_stats=not args.skip_season_stats,
        )

    elif args.mode == "export-season":
        if args.season is None:
            raise SystemExit("--season is required for mode 'export-season'")
        export_season_bundle(con, season=args.season, export_root=args.export_root)

    elif args.mode == "import-season":
        if not args.import_dir:
            raise SystemExit("--import-dir is required for mode 'import-season'")
        import_season_bundle(con, args.import_dir)

    elif args.mode == "enrich":
        enrich_players(con)

    elif args.mode == "status":
        coverage = verify_history_coverage(con)
        seasons = coverage["loaded_seasons"]
        latest = con.execute("SELECT MIN(game_date), MAX(game_date), COUNT(*) FROM pitches").fetchone()
        name_coverage = player_name_coverage(con)
        log.info(f"Loaded seasons: {seasons}")
        log.info(f"Completed seasons: {coverage['completed_seasons']}")
        log.info(f"Expected seasons: {coverage['expected_seasons']}")
        log.info(f"Missing seasons: {coverage['missing_seasons']}")
        log.info(f"Incomplete seasons: {coverage['incomplete_seasons']}")
        log.info(f"History complete: {coverage['history_complete']}")
        log.info(f"Date span / rows: {latest}")
        log.info(f"Player name coverage: {name_coverage}")
        if seasons:
            last_season_report = season_report(con, seasons[-1])
            log.info(f"Latest season report: {last_season_report}")

    elif args.mode == "season-report":
        if args.season is None:
            raise SystemExit("--season is required for mode 'season-report'")
        report = season_report(con, args.season)
        log.info(f"Season report: {report}")
