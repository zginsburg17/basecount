"""
Baseball Analytics Platform - Analytics Layer
Pre-built query functions for count state, pitch sequencing, and at-bat analysis.
All functions accept a DuckDB connection and return pandas DataFrames.
"""

import duckdb
import pandas as pd
from typing import Optional


# ---------------------------------------------------------------------------
# Count State Analysis
# ---------------------------------------------------------------------------

def count_state_splits(
    con: duckdb.DuckDBPyConnection,
    balls: int,
    strikes: int,
    outs: Optional[int] = None,
    base_state: Optional[str] = None,
    season: Optional[int] = None,
    min_pa: int = 50,
    stand: Optional[str] = None,
    p_throws: Optional[str] = None,
    season_start: Optional[int] = None,
    season_end: Optional[int] = None,
) -> pd.DataFrame:
    """
    For a given count (and optionally outs + base state), return batter
    performance splits — who thrives and who struggles in this exact situation.

    Example: count_state_splits(con, balls=2, strikes=2, outs=1)
    Returns batting metrics for all hitters in 2-2, 1-out situations.
    """
    filters = [f"p.balls = {balls}", f"p.strikes = {strikes}"]
    if outs is not None:
        filters.append(f"p.outs_when_up = {outs}")
    if base_state is not None:
        filters.append(f"p.base_state = '{base_state}'")
    if season is not None:
        filters.append(f"p.season = {season}")
    else:
        if season_start is not None:
            filters.append(f"p.season >= {season_start}")
        if season_end is not None:
            filters.append(f"p.season <= {season_end}")
    if stand is not None:
        filters.append(f"p.stand = '{stand}'")
    if p_throws is not None:
        filters.append(f"p.p_throws = '{p_throws}'")

    where = " AND ".join(filters)

    return con.execute(f"""
        WITH count_pas AS (
            SELECT DISTINCT
                CONCAT(p.game_pk, '_', p.at_bat_number)           AS at_bat_key,
                p.batter_id
            FROM pitches p
            WHERE {where}
        )
        SELECT
            cp.batter_id,
            COALESCE(pl.full_name, 'Batter #' || CAST(cp.batter_id AS VARCHAR)) AS batter_name,
            COUNT(*)                                              AS pa,
            ROUND(AVG(ab.xwoba), 3)                               AS xwoba,
            ROUND(AVG(ab.woba_value), 3)                          AS woba,
            ROUND(SUM(CASE WHEN ab.final_event IN ('single', 'double', 'triple', 'home_run')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS avg,
            ROUND(SUM(CASE WHEN ab.final_event IN ('strikeout', 'strikeout_double_play')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct,
            ROUND(SUM(CASE WHEN ab.final_event IN ('walk', 'intent_walk')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS bb_pct,
            ROUND(AVG(ps.swing_pct), 3)                           AS swing_pct,
            ROUND(AVG(ps.whiff_pct), 3)                           AS whiff_pct,
            ROUND(AVG(ps.called_strike_pct), 3)                   AS called_strike_pct,
            ROUND(AVG(ps.contact_pct), 3)                         AS contact_pct
        FROM count_pas cp
        INNER JOIN at_bats ab ON ab.at_bat_id = cp.at_bat_key
        LEFT JOIN players pl ON pl.player_id = cp.batter_id
        LEFT JOIN (
            SELECT
                CONCAT(game_pk, '_', at_bat_number)               AS at_bat_key,
                ROUND(SUM(CASE WHEN description LIKE '%swing%' OR description = 'foul'
                              THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) AS swing_pct,
                ROUND(SUM(CASE WHEN description IN ('swinging_strike', 'swinging_strike_blocked')
                              THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) AS whiff_pct,
                ROUND(SUM(CASE WHEN description = 'called_strike'
                              THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) AS called_strike_pct,
                ROUND(SUM(CASE WHEN bb_type IS NOT NULL
                              THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) AS contact_pct
            FROM pitches
            GROUP BY game_pk, at_bat_number
        ) ps ON ps.at_bat_key = cp.at_bat_key
        GROUP BY cp.batter_id, COALESCE(pl.full_name, 'Batter #' || CAST(cp.batter_id AS VARCHAR))
        HAVING COUNT(*) >= {min_pa}
        ORDER BY xwoba DESC
    """).df()


def pitcher_count_profile(
    con: duckdb.DuckDBPyConnection,
    pitcher_id: int,
    season: Optional[int] = None,
    season_start: Optional[int] = None,
    season_end: Optional[int] = None,
) -> pd.DataFrame:
    """
    For a given pitcher, show pitch mix and location tendencies broken out
    by every count (balls x strikes grid). Reveals how they attack differently
    when ahead vs. behind.
    """
    if season is not None:
        season_filter = f"AND season = {season}"
    else:
        parts = []
        if season_start is not None:
            parts.append(f"season >= {season_start}")
        if season_end is not None:
            parts.append(f"season <= {season_end}")
        season_filter = "".join(f" AND {part}" for part in parts)

    return con.execute(f"""
        SELECT
            balls,
            strikes,
            CONCAT(balls, '-', strikes)                         AS count,
            CASE
                WHEN strikes > balls THEN 'pitcher_count'
                WHEN balls > strikes THEN 'hitter_count'
                ELSE 'even'
            END                                                  AS count_type,
            COUNT(*)                                             AS pitches,
            pitch_type,
            ROUND(COUNT(*)::FLOAT / SUM(COUNT(*)) OVER (PARTITION BY balls, strikes), 3) AS usage_pct,
            ROUND(AVG(release_speed), 1)                        AS avg_velo,
            ROUND(AVG(release_spin_rate), 0)                    AS avg_spin,
            ROUND(SUM(CASE WHEN description IN ('swinging_strike','swinging_strike_blocked')
                          THEN 1 ELSE 0 END)::FLOAT
                  / NULLIF(COUNT(*), 0), 3)                     AS whiff_pct
        FROM pitches
        WHERE pitcher_id = {pitcher_id}
          AND pitch_type IS NOT NULL
          {season_filter}
        GROUP BY balls, strikes, pitch_type
        ORDER BY balls, strikes, pitches DESC
    """).df()


def at_bat_outcome_by_count(
    con: duckdb.DuckDBPyConnection,
    season: Optional[int] = None,
    season_start: Optional[int] = None,
    season_end: Optional[int] = None,
) -> pd.DataFrame:
    """
    Return a full count-state matrix showing outcome distributions.
    Useful for visualizing how outcome probabilities shift as the count changes.
    """
    if season is not None:
        season_filter = f"WHERE season = {season}"
    else:
        filters = []
        if season_start is not None:
            filters.append(f"season >= {season_start}")
        if season_end is not None:
            filters.append(f"season <= {season_end}")
        season_filter = f"WHERE {' AND '.join(filters)}" if filters else ""

    return con.execute(f"""
        SELECT
            final_balls                                          AS balls,
            final_strikes                                        AS strikes,
            CONCAT(final_balls, '-', final_strikes)             AS count,
            COUNT(*)                                             AS at_bats,
            ROUND(SUM(CASE WHEN final_event IN ('strikeout','strikeout_double_play')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct,
            ROUND(SUM(CASE WHEN final_event IN ('walk','intent_walk')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS bb_pct,
            ROUND(SUM(CASE WHEN final_event = 'home_run'
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS hr_pct,
            ROUND(SUM(CASE WHEN final_event IN ('single','double','triple','home_run')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS hit_pct,
            ROUND(AVG(xwoba), 3)                                AS avg_xwoba
        FROM at_bats
        {season_filter}
        GROUP BY final_balls, final_strikes
        ORDER BY balls, strikes
    """).df()


# ---------------------------------------------------------------------------
# Pitch Sequencing
# ---------------------------------------------------------------------------

def pitch_sequence_patterns(
    con: duckdb.DuckDBPyConnection,
    pitcher_id: Optional[int] = None,
    min_occurrences: int = 20,
    season: Optional[int] = None,
    season_start: Optional[int] = None,
    season_end: Optional[int] = None,
) -> pd.DataFrame:
    """
    Identify common 2-pitch and 3-pitch sequences and their outcomes.
    Reveals pitcher tendencies (e.g. always follows a fastball with a slider
    in two-strike counts).
    """
    pid_filter  = f"AND pitcher_id = {pitcher_id}" if pitcher_id else ""
    if season is not None:
        seas_filter = f"AND season = {season}"
    else:
        parts = []
        if season_start is not None:
            parts.append(f"season >= {season_start}")
        if season_end is not None:
            parts.append(f"season <= {season_end}")
        seas_filter = "".join(f" AND {part}" for part in parts)

    return con.execute(f"""
        WITH sequenced AS (
            SELECT
                CONCAT(game_pk, '_', at_bat_number) AS at_bat_id,
                pitcher_id,
                pitch_type,
                pitch_number,
                balls,
                strikes,
                description,
                events,
                LEAD(pitch_type) OVER (PARTITION BY at_bat_id ORDER BY pitch_number) AS next_pitch,
                LAG(pitch_type)  OVER (PARTITION BY at_bat_id ORDER BY pitch_number) AS prev_pitch
            FROM pitches
            WHERE pitch_type IS NOT NULL
              {pid_filter}
              {seas_filter}
        )
        SELECT
            CONCAT(pitch_type, ' → ', next_pitch)   AS sequence,
            pitch_type                               AS first_pitch,
            next_pitch                               AS second_pitch,
            COUNT(*)                                 AS occurrences,
            ROUND(SUM(CASE WHEN description IN ('swinging_strike','swinging_strike_blocked')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS whiff_pct,
            ROUND(SUM(CASE WHEN events IN ('strikeout','strikeout_double_play')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct
        FROM sequenced
        WHERE next_pitch IS NOT NULL
        GROUP BY pitch_type, next_pitch
        HAVING COUNT(*) >= {min_occurrences}
        ORDER BY occurrences DESC
    """).df()


# ---------------------------------------------------------------------------
# Situational / At-Bat Level
# ---------------------------------------------------------------------------

def situational_splits(
    con: duckdb.DuckDBPyConnection,
    player_id: int,
    role: str = "batter",            # "batter" or "pitcher"
    split_by: str = "base_state",    # "base_state" | "outs_at_start" | "final_count" | "p_throws" | "stand"
    season: Optional[int] = None,
    season_start: Optional[int] = None,
    season_end: Optional[int] = None,
) -> pd.DataFrame:
    """
    Generic situational split builder. Returns xwOBA and outcome rates
    broken down by whatever dimension you pass as split_by.
    """
    id_col      = "batter_id" if role == "batter" else "pitcher_id"
    if season is not None:
        seas_filter = f"AND season = {season}"
    else:
        parts = []
        if season_start is not None:
            parts.append(f"season >= {season_start}")
        if season_end is not None:
            parts.append(f"season <= {season_end}")
        seas_filter = "".join(f" AND {part}" for part in parts)

    return con.execute(f"""
        SELECT
            {split_by}                                          AS split_value,
            COUNT(*)                                            AS pa,
            ROUND(AVG(xwoba), 3)                               AS xwoba,
            ROUND(AVG(woba_value), 3)                          AS woba,
            ROUND(SUM(CASE WHEN final_event IN ('strikeout','strikeout_double_play')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct,
            ROUND(SUM(CASE WHEN final_event IN ('walk','intent_walk')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS bb_pct,
            ROUND(SUM(CASE WHEN final_event = 'home_run'
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS hr_pct
        FROM at_bats
        WHERE {id_col} = {player_id}
          {seas_filter}
        GROUP BY {split_by}
        ORDER BY pa DESC
    """).df()


def full_at_bat_timeline(
    con: duckdb.DuckDBPyConnection,
    game_pk: int,
    at_bat_number: int,
) -> pd.DataFrame:
    """
    Return every pitch in a specific at-bat in sequence — the full story
    of how that AB played out. Perfect for the at-bat detail view in the UI.
    """
    return con.execute(f"""
        SELECT
            pitch_number,
            CONCAT(balls, '-', strikes)      AS count_before,
            pitch_type,
            pitch_name,
            ROUND(release_speed, 1)          AS velo,
            ROUND(plate_x, 2)               AS plate_x,
            ROUND(plate_z, 2)               AS plate_z,
            ROUND(pfx_x, 1)                 AS h_break,
            ROUND(pfx_z, 1)                 AS v_break,
            description,
            events
        FROM pitches
        WHERE game_pk = {game_pk}
          AND at_bat_number = {at_bat_number}
        ORDER BY pitch_number
    """).df()


# ---------------------------------------------------------------------------
# Leaderboards
# ---------------------------------------------------------------------------

def batting_leaderboard(
    con: duckdb.DuckDBPyConnection,
    season: int,
    limit: int = 10,
    min_pa: int = 100,
) -> pd.DataFrame:
    """
    Return top batters leaderboard ranked by xwOBA for a given season.
    """
    return con.execute(f"""
        SELECT
            ab.batter_id,
            pl.full_name                        AS batter_name,
            COUNT(DISTINCT ab.at_bat_id)        AS pa,
            ROUND(AVG(ab.xwoba), 3)             AS xwoba,
            ROUND(AVG(ab.woba_value), 3)        AS woba,
            ROUND(SUM(CASE WHEN ab.final_event IN ('strikeout','strikeout_double_play')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct,
            ROUND(SUM(CASE WHEN ab.final_event IN ('walk','intent_walk')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS bb_pct,
            ROUND(SUM(CASE WHEN ab.final_event = 'home_run'
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS hr_pct,
            ROUND(SUM(CASE WHEN ab.final_event IN ('single','double','triple','home_run')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS avg
        FROM at_bats ab
        LEFT JOIN players pl ON pl.player_id = ab.batter_id
        WHERE ab.season = {season}
        GROUP BY ab.batter_id, pl.full_name
        HAVING COUNT(DISTINCT ab.at_bat_id) >= {min_pa}
        ORDER BY xwoba DESC
        LIMIT {limit}
    """).df()


def stuff_plus_proxy(
    con: duckdb.DuckDBPyConnection,
    season: int,
    pitch_type: Optional[str] = None,
    min_pitches: int = 100,
) -> pd.DataFrame:
    """
    Proxy for Stuff+ using whiff rate, velocity, and movement.
    Not a true Stuff+ model but a useful relative ranking signal.
    """
    pt_filter = f"AND pitch_type = '{pitch_type}'" if pitch_type else ""

    return con.execute(f"""
        SELECT
            p.pitcher_id,
            pl.full_name                        AS pitcher_name,
            p.pitch_type,
            COUNT(*)                            AS pitches,
            ROUND(AVG(release_speed), 1)        AS avg_velo,
            ROUND(AVG(release_spin_rate), 0)    AS avg_spin,
            ROUND(AVG(ABS(pfx_x)), 2)           AS avg_h_break,
            ROUND(AVG(pfx_z), 2)                AS avg_v_break,
            ROUND(SUM(CASE WHEN description IN ('swinging_strike','swinging_strike_blocked')
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS whiff_pct,
            ROUND(SUM(CASE WHEN description = 'called_strike'
                          THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS csw_pct
        FROM pitches p
        LEFT JOIN players pl ON pl.player_id = p.pitcher_id
        WHERE p.season = {season}
          AND p.pitch_type IS NOT NULL
          {pt_filter}
        GROUP BY p.pitcher_id, pl.full_name, p.pitch_type
        HAVING COUNT(*) >= {min_pitches}
        ORDER BY whiff_pct DESC
    """).df()
