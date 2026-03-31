"""
Baseball Analytics Platform - ETL Pipeline
Ingests pitch-level Statcast data via pybaseball and loads into DuckDB.
"""

import duckdb
import pandas as pd
from pybaseball import statcast
from datetime import datetime, date
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


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
            "xwoba":           last.get("estimated_woba_using_speedangle"),
            "woba_value":      last.get("woba_value"),
            "game_date":       first["game_date"],
            "season":          first["season"],
        }
        groups.append(row)

    return pd.DataFrame(groups)


# ---------------------------------------------------------------------------
# Core ETL
# ---------------------------------------------------------------------------

def init_db(db_path: str = "baseball.duckdb") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path)
    con.execute(SCHEMA_SQL)
    log.info(f"Database initialized at {db_path}")
    return con


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
    log.info(f"Fetching Statcast data: {start_date} → {end_date}")
    time.sleep(delay)  # be polite to Baseball Savant

    raw = statcast(start_dt=start_date, end_dt=end_date)
    if raw is None or raw.empty:
        log.warning("No data returned for this range.")
        return 0

    log.info(f"  Raw rows fetched: {len(raw)}")
    df = clean_statcast(raw)

    # --- Load pitches ---
    pitch_cols = [c for c in df.columns if c in [
        "pitch_id", "game_pk", "at_bat_number", "pitch_number",
        "pitcher_id", "batter_id", "pitcher_team", "batter_team",
        "stand", "p_throws",
        "inning", "inning_half", "outs_when_up", "balls", "strikes",
        "on_1b", "on_2b", "on_3b", "base_state",
        "pitch_type", "pitch_name",
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
    ]]
    pitches_df = df[pitch_cols].drop_duplicates("pitch_id")

    con.execute("INSERT OR REPLACE INTO pitches SELECT * FROM pitches_df")
    log.info(f"  Pitches upserted: {len(pitches_df)}")

    # --- Load at_bats rollup ---
    ab_df = build_at_bat_rollup(df)
    con.execute("INSERT OR REPLACE INTO at_bats SELECT * FROM ab_df")
    log.info(f"  At-bats upserted: {len(ab_df)}")

    # --- Log run ---
    con.execute("""
        INSERT INTO ingestion_log (start_date, end_date, rows_loaded, status)
        VALUES (?, ?, ?, 'success')
    """, [start_date, end_date, len(pitches_df)])

    return len(pitches_df)


def backfill_season(
    con: duckdb.DuckDBPyConnection,
    season: int,
    chunk_days: int = 7,
):
    """
    Backfill a full season in weekly chunks to avoid hammering Baseball Savant.
    Typical MLB season: late March → late September.
    """
    from datetime import timedelta

    season_start = date(season, 3, 20)
    season_end   = date(season, 10, 1)
    current      = season_start
    total        = 0

    while current < season_end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), season_end)
        rows = load_date_range(
            con,
            start_date=current.strftime("%Y-%m-%d"),
            end_date=chunk_end.strftime("%Y-%m-%d"),
        )
        total += rows
        current = chunk_end + timedelta(days=1)
        time.sleep(3)  # polite delay between chunks

    log.info(f"Season {season} backfill complete. Total pitches: {total:,}")
    return total


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    con = init_db("baseball.duckdb")

    # Example: load the most recent week
    today = date.today()
    week_ago = today.replace(day=today.day - 7)
    load_date_range(
        con,
        start_date=week_ago.strftime("%Y-%m-%d"),
        end_date=today.strftime("%Y-%m-%d"),
    )

    # To backfill a full season, uncomment:
    # backfill_season(con, season=2024)
