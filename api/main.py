"""
Baseball Analytics Platform - FastAPI Backend
Serves analytics data as JSON for the frontend dashboard.
"""

from datetime import date, timedelta
import json
import os
import sys
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from analytics.queries import (  # noqa: E402
    at_bat_outcome_by_count,
    batting_leaderboard,
    count_state_splits,
    full_at_bat_timeline,
    pitch_sequence_patterns,
    pitcher_count_profile,
    situational_splits,
    stuff_plus_proxy,
)

app = FastAPI(title="Baseball Analytics API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = os.getenv("DB_PATH", "baseball.duckdb")

HIT_EVENTS = ("single", "double", "triple", "home_run")
WALK_EVENTS = ("walk", "intent_walk")
STRIKEOUT_EVENTS = ("strikeout", "strikeout_double_play")
AB_EXCLUDED_EVENTS = ("walk", "intent_walk", "hit_by_pitch", "sac_fly", "sac_bunt", "catcher_interf")
PLAYER_NAME_CACHE: dict[int, Optional[str]] = {}


def get_con():
    return duckdb.connect(DB_PATH, read_only=True)


def df_to_json(df: pd.DataFrame) -> list[dict]:
    normalized = df.replace([float("inf"), float("-inf")], pd.NA)
    normalized = normalized.astype(object).where(pd.notna(normalized), None)
    return normalized.to_dict(orient="records")


def latest_data_context(con: duckdb.DuckDBPyConnection) -> dict:
    earliest_game_date, latest_game_date, earliest_season, latest_season = con.execute(
        """
        SELECT
            MIN(game_date) AS earliest_game_date,
            MAX(game_date) AS latest_game_date,
            MIN(season) AS earliest_season,
            MAX(season) AS latest_season
        FROM pitches
        """
    ).fetchone()
    seasons = [
        row[0]
        for row in con.execute(
            "SELECT DISTINCT season FROM pitches WHERE season IS NOT NULL ORDER BY season DESC"
        ).fetchall()
    ]
    return {
        "earliest_game_date": earliest_game_date,
        "latest_game_date": latest_game_date,
        "earliest_season": earliest_season,
        "latest_season": latest_season,
        "seasons": seasons,
    }


def expected_statcast_seasons() -> list[int]:
    current_year = date.today().year
    return list(range(2015, current_year + 1))


def history_completeness(con: duckdb.DuckDBPyConnection) -> dict:
    context = latest_data_context(con)
    loaded = context["seasons"]
    expected = expected_statcast_seasons()
    completed = [
        row[0]
        for row in con.execute(
            """
            SELECT season
            FROM season_backfill_status
            WHERE completed = TRUE
            ORDER BY season DESC
            """
        ).fetchall()
    ]
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


def player_name_completeness(con: duckdb.DuckDBPyConnection) -> dict:
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
        "name_complete": (unnamed_players or 0) == 0,
    }


def resolve_window(
    con: duckdb.DuckDBPyConnection,
    window: str = "season",
    season: Optional[int] = None,
    season_start: Optional[int] = None,
    season_end: Optional[int] = None,
):
    context = latest_data_context(con)
    latest_game_date = context["latest_game_date"]
    latest_season = context["latest_season"]
    earliest_season = context["earliest_season"]

    if latest_game_date is None:
        return {
            "season": season,
            "season_start": season_start,
            "season_end": season_end,
            "start_date": None,
            "end_date": None,
            **context,
        }

    if window == "career":
        return {
            "season": None,
            "season_start": earliest_season,
            "season_end": latest_season,
            "start_date": None,
            "end_date": None,
            **context,
        }

    if window == "last7":
        end_date = latest_game_date
        start_date = latest_game_date - timedelta(days=6)
        return {
            "season": None,
            "season_start": None,
            "season_end": None,
            "start_date": start_date,
            "end_date": end_date,
            **context,
        }

    if season_start is not None or season_end is not None:
        start = season_start if season_start is not None else (season if season is not None else earliest_season)
        end = season_end if season_end is not None else (season if season is not None else latest_season)
        if start > end:
            start, end = end, start
        return {
            "season": None,
            "season_start": start,
            "season_end": end,
            "start_date": None,
            "end_date": None,
            **context,
        }

    return {
        "season": season or latest_season,
        "season_start": season or latest_season,
        "season_end": season or latest_season,
        "start_date": None,
        "end_date": None,
        **context,
    }


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_filters(
    alias: str,
    *,
    season: Optional[int] = None,
    season_start: Optional[int] = None,
    season_end: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    balls: Optional[int] = None,
    strikes: Optional[int] = None,
    outs_col: Optional[str] = None,
    outs: Optional[int] = None,
    base_state: Optional[str] = None,
    stand: Optional[str] = None,
    p_throws: Optional[str] = None,
    player_col: Optional[str] = None,
    player_id: Optional[int] = None,
    pitch_type: Optional[str] = None,
) -> str:
    filters = ["1=1"]

    if season is not None:
        filters.append(f"{alias}.season = {season}")
    else:
        if season_start is not None:
            filters.append(f"{alias}.season >= {season_start}")
        if season_end is not None:
            filters.append(f"{alias}.season <= {season_end}")
    if start_date is not None:
        filters.append(f"{alias}.game_date >= DATE {sql_quote(start_date.isoformat())}")
    if end_date is not None:
        filters.append(f"{alias}.game_date <= DATE {sql_quote(end_date.isoformat())}")
    if balls is not None:
        filters.append(f"{alias}.balls = {balls}")
    if strikes is not None:
        filters.append(f"{alias}.strikes = {strikes}")
    if outs is not None and outs_col is not None:
        filters.append(f"{alias}.{outs_col} = {outs}")
    if base_state is not None:
        filters.append(f"{alias}.base_state = {sql_quote(base_state)}")
    if stand is not None:
        filters.append(f"{alias}.stand = {sql_quote(stand)}")
    if p_throws is not None:
        filters.append(f"{alias}.p_throws = {sql_quote(p_throws)}")
    if player_col is not None and player_id is not None:
        filters.append(f"{alias}.{player_col} = {player_id}")
    if pitch_type is not None:
        filters.append(f"{alias}.pitch_type = {sql_quote(pitch_type)}")

    return " AND ".join(filters)


def normalize_hand(value: Optional[str]) -> Optional[str]:
    if value is None or value.lower() == "all":
        return None
    return value.upper()


def player_name_expr(prefix: str, id_col: str) -> str:
    return f"COALESCE(pl.full_name, '{prefix} #' || CAST({id_col} AS VARCHAR))"


def team_expr(prefix: str, id_col: str, team_col: str) -> str:
    return f"""
        COALESCE(
            pl.team,
            (
                SELECT {team_col}
                FROM pitches px
                WHERE px.{id_col} = {prefix}.{id_col}
                  AND {team_col} IS NOT NULL
                ORDER BY px.game_date DESC
                LIMIT 1
            )
        )
    """


def lookup_player_names(player_ids: list[int]) -> dict[int, str]:
    missing_ids = [pid for pid in player_ids if pid not in PLAYER_NAME_CACHE]
    if not missing_ids:
        return {pid: PLAYER_NAME_CACHE[pid] for pid in player_ids if PLAYER_NAME_CACHE.get(pid)}

    try:
        from pybaseball import playerid_reverse_lookup

        lookup_df = playerid_reverse_lookup(missing_ids, key_type="mlbam")
        if lookup_df is not None and not lookup_df.empty:
            for _, row in lookup_df.iterrows():
                pid = int(row["key_mlbam"])
                PLAYER_NAME_CACHE[pid] = f"{row['name_first'].strip().title()} {row['name_last'].strip().title()}"
    except Exception:
        pass

    for pid in missing_ids:
        PLAYER_NAME_CACHE.setdefault(pid, None)

    still_missing = [pid for pid in missing_ids if not PLAYER_NAME_CACHE.get(pid)]
    for pid in still_missing:
        PLAYER_NAME_CACHE[pid] = lookup_player_name_via_mlb_api(pid)

    return {pid: PLAYER_NAME_CACHE[pid] for pid in player_ids if PLAYER_NAME_CACHE.get(pid)}


def lookup_player_name_via_mlb_api(player_id: int) -> Optional[str]:
    url = f"https://statsapi.mlb.com/api/v1/people/{player_id}"
    request = Request(url, headers={"User-Agent": "basecount/1.0"})
    try:
        with urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, ValueError):
        return None

    people = payload.get("people") or []
    if not people:
        return None
    return people[0].get("fullName")


def inject_player_names(records: list[dict], *, id_key: str, name_key: str) -> list[dict]:
    missing_ids = [
        int(record[id_key])
        for record in records
        if record.get(id_key) is not None and str(record.get(name_key, "")).endswith(f"#{record[id_key]}")
    ]
    if not missing_ids:
        return records

    resolved = lookup_player_names(missing_ids)
    for record in records:
        pid = record.get(id_key)
        if pid in resolved:
            record[name_key] = resolved[pid]
    return records


def zone_matrix_from_rows(rows: list[tuple[int, int]]) -> list[list[int]]:
    counts = {zone: count for zone, count in rows if zone is not None}
    return [
        [counts.get(1, 0), counts.get(2, 0), counts.get(3, 0)],
        [counts.get(4, 0), counts.get(5, 0), counts.get(6, 0)],
        [counts.get(7, 0), counts.get(8, 0), counts.get(9, 0)],
    ]


def batter_stat_query(where_sql: str) -> str:
    return f"""
        WITH filtered AS (
            SELECT *
            FROM at_bats ab
            WHERE {where_sql}
        )
        SELECT
            COUNT(*) AS pa,
            ROUND(AVG(xwoba), 3) AS xwoba,
            ROUND(SUM(CASE WHEN final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) AS k_pct,
            ROUND(SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) AS bb_pct,
            SUM(CASE WHEN final_event = 'home_run' THEN 1 ELSE 0 END) AS hr,
            SUM(CASE WHEN final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END) AS hits,
            SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END) AS walks,
            SUM(CASE WHEN final_event NOT IN {AB_EXCLUDED_EVENTS} THEN 1 ELSE 0 END) AS at_bats,
            SUM(
                CASE final_event
                    WHEN 'single' THEN 1
                    WHEN 'double' THEN 2
                    WHEN 'triple' THEN 3
                    WHEN 'home_run' THEN 4
                    ELSE 0
                END
            ) AS total_bases
        FROM filtered
    """


def pitcher_summary_query(where_sql: str) -> str:
    return f"""
        WITH filtered_pitches AS (
            SELECT *
            FROM pitches p
            WHERE {where_sql}
        ),
        filtered_abs AS (
            SELECT DISTINCT
                CONCAT(game_pk, '_', at_bat_number) AS at_bat_key,
                pitcher_id,
                game_date,
                season
            FROM filtered_pitches
        ),
        pitcher_abs AS (
            SELECT ab.*
            FROM at_bats ab
            INNER JOIN filtered_abs fa
                ON fa.at_bat_key = ab.at_bat_id
        )
        SELECT
            (SELECT COUNT(*) FROM filtered_pitches) AS pitches,
            (SELECT COUNT(DISTINCT pitcher_id) FROM filtered_pitches) AS pitchers,
            (SELECT ROUND(AVG(release_speed), 1) FROM filtered_pitches) AS avg_velo,
            (SELECT ROUND(AVG(release_spin_rate), 0) FROM filtered_pitches) AS avg_spin,
            (SELECT ROUND(SUM(CASE WHEN description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM filtered_pitches) AS whiff_pct,
            (SELECT ROUND(SUM(CASE WHEN description IN ('called_strike', 'swinging_strike', 'swinging_strike_blocked', 'foul', 'foul_tip') THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM filtered_pitches) AS csw_pct,
            (SELECT ROUND(AVG(xwoba), 3) FROM pitcher_abs) AS xwoba_allowed,
            (SELECT ROUND(SUM(CASE WHEN final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM pitcher_abs) AS k_pct,
            (SELECT ROUND(SUM(CASE WHEN final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0), 3) FROM pitcher_abs) AS bb_pct
    """


@app.get("/api/meta/context")
def api_meta_context():
    con = get_con()
    context = latest_data_context(con)
    coverage = history_completeness(con)
    name_coverage = player_name_completeness(con)
    batter_name = player_name_expr("Batter", "ab.batter_id")
    pitcher_name = player_name_expr("Pitcher", "p.pitcher_id")

    batters = con.execute(
        f"""
        SELECT
            ab.batter_id AS id,
            {batter_name} AS name,
            {team_expr('ab', 'batter_id', 'batter_team')} AS team,
            COUNT(*) AS pa
        FROM at_bats ab
        LEFT JOIN players pl ON pl.player_id = ab.batter_id
        GROUP BY ab.batter_id, {batter_name}, {team_expr('ab', 'batter_id', 'batter_team')}
        ORDER BY pa DESC, name
        LIMIT 200
        """
    ).df()

    pitchers = con.execute(
        f"""
        SELECT
            p.pitcher_id AS id,
            {pitcher_name} AS name,
            {team_expr('p', 'pitcher_id', 'pitcher_team')} AS team,
            COUNT(*) AS pitches
        FROM pitches p
        LEFT JOIN players pl ON pl.player_id = p.pitcher_id
        GROUP BY p.pitcher_id, {pitcher_name}, {team_expr('p', 'pitcher_id', 'pitcher_team')}
        ORDER BY pitches DESC, name
        LIMIT 200
        """
    ).df()

    batter_records = inject_player_names(df_to_json(batters), id_key="id", name_key="name")
    pitcher_records = inject_player_names(df_to_json(pitchers), id_key="id", name_key="name")

    return {
        **context,
        "history": coverage,
        "players_status": name_coverage,
        "batters": batter_records,
        "pitchers": pitcher_records,
    }


@app.get("/api/meta/coverage")
def api_meta_coverage():
    con = get_con()
    return {
        **history_completeness(con),
        "players_status": player_name_completeness(con),
    }


# ---------------------------------------------------------------------------
# Count State Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/count-state/batter-splits")
def api_count_state_splits(
    balls: int = Query(..., ge=0, le=3),
    strikes: int = Query(..., ge=0, le=2),
    outs: Optional[int] = Query(None, ge=0, le=2),
    base_state: Optional[str] = None,
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    min_pa: int = Query(10, ge=1),
    stand: Optional[str] = None,
    p_throws: Optional[str] = None,
):
    """Batter performance in a specific count situation."""
    con = get_con()
    resolved = resolve_window(con, "season", season, season_start, season_end)
    df = count_state_splits(
        con,
        balls,
        strikes,
        outs,
        base_state,
        resolved["season"],
        min_pa,
        stand=normalize_hand(stand),
        p_throws=normalize_hand(p_throws),
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
    )
    records = inject_player_names(df_to_json(df), id_key="batter_id", name_key="batter_name")
    return {"count": f"{balls}-{strikes}", "results": records}


@app.get("/api/count-state/outcome-matrix")
def api_outcome_matrix(
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    window: str = Query("season", pattern="^(season|career|last7)$"),
    stand: Optional[str] = None,
    p_throws: Optional[str] = None,
):
    """Full count-state outcome probability matrix."""
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)
    stand = normalize_hand(stand)
    p_throws = normalize_hand(p_throws)
    where_sql = build_filters(
        "p",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        stand=stand,
        p_throws=p_throws,
    )
    df = con.execute(
        f"""
        WITH count_states AS (
            SELECT DISTINCT
                CONCAT(p.game_pk, '_', p.at_bat_number) AS at_bat_key,
                p.balls,
                p.strikes
            FROM pitches p
            WHERE {where_sql}
        )
        SELECT
            cs.balls,
            cs.strikes,
            CONCAT(cs.balls, '-', cs.strikes) AS count,
            COUNT(*) AS at_bats,
            ROUND(SUM(CASE WHEN ab.final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct,
            ROUND(SUM(CASE WHEN ab.final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS bb_pct,
            ROUND(SUM(CASE WHEN ab.final_event = 'home_run' THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS hr_pct,
            ROUND(SUM(CASE WHEN ab.final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS hit_pct,
            ROUND(AVG(ab.xwoba), 3) AS avg_xwoba
        FROM count_states cs
        INNER JOIN at_bats ab ON ab.at_bat_id = cs.at_bat_key
        GROUP BY cs.balls, cs.strikes
        ORDER BY cs.balls, cs.strikes
        """
    ).df()
    return df_to_json(df)


@app.get("/api/count-state/zone-map")
def api_count_zone_map(
    balls: Optional[int] = Query(None, ge=0, le=3),
    strikes: Optional[int] = Query(None, ge=0, le=2),
    outs: Optional[int] = Query(None, ge=0, le=2),
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    window: str = Query("season", pattern="^(season|career|last7)$"),
    stand: Optional[str] = None,
    p_throws: Optional[str] = None,
):
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)
    where_sql = build_filters(
        "p",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        balls=balls,
        strikes=strikes,
        outs_col="outs_when_up",
        outs=outs,
        stand=normalize_hand(stand),
        p_throws=normalize_hand(p_throws),
    )
    rows = con.execute(
        f"""
        SELECT zone, COUNT(*) AS pitches
        FROM pitches p
        WHERE {where_sql}
          AND zone BETWEEN 1 AND 9
        GROUP BY zone
        ORDER BY zone
        """
    ).fetchall()
    return {"zones": zone_matrix_from_rows(rows)}


# ---------------------------------------------------------------------------
# Player Profile Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/batter/{batter_id}/overview")
def api_batter_overview(
    batter_id: int,
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    window: str = Query("season", pattern="^(season|career|last7)$"),
):
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)
    where_sql = build_filters(
        "ab",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        player_col="batter_id",
        player_id=batter_id,
    )
    summary = con.execute(batter_stat_query(where_sql)).df()
    if summary.empty or summary.iloc[0]["pa"] in (None, 0):
        raise HTTPException(status_code=404, detail="Batter not found")

    seasons_df = con.execute(
        f"""
        SELECT
            ab.season,
            COUNT(*) AS pa,
            ROUND(AVG(ab.xwoba), 3) AS xwoba,
            ROUND(SUM(CASE WHEN ab.final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct,
            ROUND(SUM(CASE WHEN ab.final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS bb_pct,
            SUM(CASE WHEN ab.final_event = 'home_run' THEN 1 ELSE 0 END) AS hr
        FROM at_bats ab
        WHERE ab.batter_id = {batter_id}
        GROUP BY ab.season
        ORDER BY ab.season DESC
        """
    ).df()

    counts_df = con.execute(
        f"""
        WITH count_pas AS (
            SELECT DISTINCT
                CONCAT(p.game_pk, '_', p.at_bat_number) AS at_bat_key,
                p.balls,
                p.strikes
            FROM pitches p
            WHERE {build_filters('p', season=resolved['season'], season_start=resolved['season_start'], season_end=resolved['season_end'], start_date=resolved['start_date'], end_date=resolved['end_date'], player_col='batter_id', player_id=batter_id)}
        )
        SELECT
            cp.balls,
            cp.strikes,
            COUNT(*) AS pa,
            ROUND(AVG(ab.xwoba), 3) AS xwoba,
            ROUND(SUM(CASE WHEN ab.final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS avg,
            ROUND(SUM(CASE WHEN ab.final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct
        FROM count_pas cp
        INNER JOIN at_bats ab ON ab.at_bat_id = cp.at_bat_key
        GROUP BY cp.balls, cp.strikes
        ORDER BY cp.balls, cp.strikes
        """
    ).df()

    zone_rows = con.execute(
        f"""
        SELECT
            zone,
            ROUND(
                SUM(CASE WHEN bb_type IS NOT NULL OR description IN ('foul', 'foul_tip', 'hit_into_play') THEN 1 ELSE 0 END)::FLOAT
                / NULLIF(COUNT(*), 0) * 100,
                0
            )::INTEGER AS contact_rate
        FROM pitches p
        WHERE {build_filters('p', season=resolved['season'], season_start=resolved['season_start'], season_end=resolved['season_end'], start_date=resolved['start_date'], end_date=resolved['end_date'], player_col='batter_id', player_id=batter_id)}
          AND zone BETWEEN 1 AND 9
        GROUP BY zone
        ORDER BY zone
        """
    ).fetchall()

    return {
        "summary": df_to_json(summary)[0],
        "seasons": df_to_json(seasons_df),
        "counts": df_to_json(counts_df),
        "zones": zone_matrix_from_rows(zone_rows),
    }


@app.get("/api/pitcher/{pitcher_id}/overview")
def api_pitcher_overview(
    pitcher_id: int,
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    window: str = Query("season", pattern="^(season|career|last7)$"),
):
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)
    pitch_where = build_filters(
        "p",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        player_col="pitcher_id",
        player_id=pitcher_id,
    )

    summary = con.execute(pitcher_summary_query(pitch_where)).df()
    if summary.empty or summary.iloc[0]["pitches"] in (None, 0):
        raise HTTPException(status_code=404, detail="Pitcher not found")

    seasons_df = con.execute(
        f"""
        SELECT
            p.season,
            COUNT(*) AS pitches,
            ROUND(AVG(p.release_speed), 1) AS avg_velo,
            ROUND(AVG(p.release_spin_rate), 0) AS avg_spin,
            ROUND(SUM(CASE WHEN p.description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS whiff_pct
        FROM pitches p
        WHERE p.pitcher_id = {pitcher_id}
        GROUP BY p.season
        ORDER BY p.season DESC
        """
    ).df()

    counts_df = con.execute(
        f"""
        WITH pitch_counts AS (
            SELECT
                p.balls,
                p.strikes,
                p.pitch_type,
                COUNT(*) AS pitches,
                ROUND(COUNT(*)::FLOAT / SUM(COUNT(*)) OVER (PARTITION BY p.balls, p.strikes), 3) AS usage_pct,
                ROUND(AVG(p.release_speed), 1) AS avg_velo,
                ROUND(SUM(CASE WHEN p.description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS whiff_pct
            FROM pitches p
            WHERE {pitch_where}
              AND p.pitch_type IS NOT NULL
            GROUP BY p.balls, p.strikes, p.pitch_type
        )
        SELECT *
        FROM pitch_counts
        ORDER BY balls, strikes, usage_pct DESC
        """
    ).df()

    zone_rows = con.execute(
        f"""
        SELECT zone, COUNT(*) AS pitches
        FROM pitches p
        WHERE {pitch_where}
          AND zone BETWEEN 1 AND 9
        GROUP BY zone
        ORDER BY zone
        """
    ).fetchall()

    return {
        "summary": df_to_json(summary)[0],
        "seasons": df_to_json(seasons_df),
        "counts": df_to_json(counts_df),
        "zones": zone_matrix_from_rows(zone_rows),
    }


# ---------------------------------------------------------------------------
# Pitcher / Batter Existing Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/pitcher/{pitcher_id}/count-profile")
def api_pitcher_count_profile(
    pitcher_id: int,
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
):
    """Pitch mix by count for a given pitcher."""
    con = get_con()
    resolved = resolve_window(con, "season", season, season_start, season_end)
    df = pitcher_count_profile(
        con,
        pitcher_id,
        resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
    )
    return df_to_json(df)


@app.get("/api/sequences")
def api_sequences(
    pitcher_id: Optional[int] = None,
    min_occurrences: int = Query(5, ge=1),
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
):
    con = get_con()
    resolved = resolve_window(con, "season", season, season_start, season_end)
    df = pitch_sequence_patterns(
        con,
        pitcher_id,
        min_occurrences,
        resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
    )
    return df_to_json(df)


@app.get("/api/pitcher/{pitcher_id}/sequences")
def api_pitcher_sequences(
    pitcher_id: int,
    min_occurrences: int = Query(5, ge=1),
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
):
    """Most common pitch-to-pitch sequences for a pitcher."""
    con = get_con()
    resolved = resolve_window(con, "season", season, season_start, season_end)
    df = pitch_sequence_patterns(
        con,
        pitcher_id,
        min_occurrences,
        resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
    )
    return df_to_json(df)


@app.get("/api/pitcher/{pitcher_id}/splits")
def api_pitcher_splits(
    pitcher_id: int,
    split_by: str = "base_state",
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
):
    con = get_con()
    resolved = resolve_window(con, "season", season, season_start, season_end)
    df = situational_splits(
        con,
        pitcher_id,
        role="pitcher",
        split_by=split_by,
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
    )
    return df_to_json(df)


@app.get("/api/batter/{batter_id}/splits")
def api_batter_splits(
    batter_id: int,
    split_by: str = "base_state",
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
):
    con = get_con()
    resolved = resolve_window(con, "season", season, season_start, season_end)
    df = situational_splits(
        con,
        batter_id,
        role="batter",
        split_by=split_by,
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
    )
    return df_to_json(df)


# ---------------------------------------------------------------------------
# At-Bat Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/at-bat/{game_pk}/{at_bat_number}")
def api_at_bat_timeline(game_pk: int, at_bat_number: int):
    """Full pitch-by-pitch timeline of a single at-bat."""
    con = get_con()
    df = full_at_bat_timeline(con, game_pk, at_bat_number)
    if df.empty:
        raise HTTPException(status_code=404, detail="At-bat not found")
    return df_to_json(df)


# ---------------------------------------------------------------------------
# Leaderboard Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/leaderboard/batting")
def api_batting_leaderboard(
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    limit: int = Query(10, ge=1, le=100),
    min_pa: int = Query(10, ge=1),
    window: str = Query("season", pattern="^(season|career|last7)$"),
    balls: Optional[int] = Query(None, ge=0, le=3),
    strikes: Optional[int] = Query(None, ge=0, le=2),
    outs: Optional[int] = Query(None, ge=0, le=2),
    stand: Optional[str] = None,
    p_throws: Optional[str] = None,
):
    """Top batters leaderboard for a given season or window."""
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)

    if balls is None or strikes is None:
        if (
            window == "season"
            and resolved["season"] is not None
            and resolved["season_start"] == resolved["season_end"]
            and outs is None and stand is None and p_throws is None
        ):
            df = batting_leaderboard(con, resolved["season"], limit, min_pa)
            return inject_player_names(df_to_json(df), id_key="batter_id", name_key="batter_name")

        where_sql = build_filters(
            "ab",
            season=resolved["season"],
            season_start=resolved["season_start"],
            season_end=resolved["season_end"],
            start_date=resolved["start_date"],
            end_date=resolved["end_date"],
            outs_col="outs_at_start",
            outs=outs,
            stand=normalize_hand(stand),
            p_throws=normalize_hand(p_throws),
        )
        df = con.execute(
            f"""
            SELECT
                ab.batter_id,
                {player_name_expr('Batter', 'ab.batter_id')} AS batter_name,
                COUNT(*) AS pa,
                ROUND(AVG(ab.xwoba), 3) AS xwoba,
                ROUND(SUM(CASE WHEN ab.final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct,
                ROUND(SUM(CASE WHEN ab.final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS bb_pct,
                ROUND(SUM(CASE WHEN ab.final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS avg
            FROM at_bats ab
            LEFT JOIN players pl ON pl.player_id = ab.batter_id
            WHERE {where_sql}
            GROUP BY ab.batter_id, {player_name_expr('Batter', 'ab.batter_id')}
            HAVING COUNT(*) >= {min_pa}
            ORDER BY xwoba DESC
            LIMIT {limit}
            """
        ).df()
        return inject_player_names(df_to_json(df), id_key="batter_id", name_key="batter_name")

    pitch_where = build_filters(
        "p",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        balls=balls,
        strikes=strikes,
        outs_col="outs_when_up",
        outs=outs,
        stand=normalize_hand(stand),
        p_throws=normalize_hand(p_throws),
    )

    df = con.execute(
        f"""
        WITH count_pas AS (
            SELECT DISTINCT
                CONCAT(p.game_pk, '_', p.at_bat_number) AS at_bat_key,
                p.batter_id
            FROM pitches p
            WHERE {pitch_where}
        )
        SELECT
            cp.batter_id,
            {player_name_expr('Batter', 'cp.batter_id')} AS batter_name,
            COUNT(*) AS pa,
            ROUND(AVG(ab.xwoba), 3) AS xwoba,
            ROUND(SUM(CASE WHEN ab.final_event IN {STRIKEOUT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS k_pct,
            ROUND(SUM(CASE WHEN ab.final_event IN {WALK_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS bb_pct,
            ROUND(SUM(CASE WHEN ab.final_event IN {HIT_EVENTS} THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS avg
        FROM count_pas cp
        INNER JOIN at_bats ab ON ab.at_bat_id = cp.at_bat_key
        LEFT JOIN players pl ON pl.player_id = cp.batter_id
        GROUP BY cp.batter_id, {player_name_expr('Batter', 'cp.batter_id')}
        HAVING COUNT(*) >= {min_pa}
        ORDER BY xwoba DESC
        LIMIT {limit}
        """
    ).df()
    return inject_player_names(df_to_json(df), id_key="batter_id", name_key="batter_name")


@app.get("/api/leaderboard/stuff")
def api_stuff_leaderboard(
    season: Optional[int] = None,
    season_start: Optional[int] = Query(None, ge=2015),
    season_end: Optional[int] = Query(None, ge=2015),
    pitch_type: Optional[str] = None,
    min_pitches: int = Query(20, ge=1),
    window: str = Query("season", pattern="^(season|career|last7)$"),
    limit: int = Query(10, ge=1, le=100),
):
    """Pitch quality leaderboard proxied from whiff rate + velo + movement."""
    con = get_con()
    resolved = resolve_window(con, window, season, season_start, season_end)

    if window == "season" and resolved["season"] is not None and resolved["season_start"] == resolved["season_end"]:
        df = stuff_plus_proxy(con, resolved["season"], pitch_type, min_pitches).head(limit)
        return inject_player_names(df_to_json(df), id_key="pitcher_id", name_key="pitcher_name")

    where_sql = build_filters(
        "p",
        season=resolved["season"],
        season_start=resolved["season_start"],
        season_end=resolved["season_end"],
        start_date=resolved["start_date"],
        end_date=resolved["end_date"],
        pitch_type=pitch_type,
    )
    df = con.execute(
        f"""
        SELECT
            p.pitcher_id,
            {player_name_expr('Pitcher', 'p.pitcher_id')} AS pitcher_name,
            p.pitch_type,
            COUNT(*) AS pitches,
            ROUND(AVG(p.release_speed), 1) AS avg_velo,
            ROUND(AVG(p.release_spin_rate), 0) AS avg_spin,
            ROUND(AVG(ABS(p.pfx_x)), 2) AS avg_h_break,
            ROUND(AVG(p.pfx_z), 2) AS avg_v_break,
            ROUND(SUM(CASE WHEN p.description IN ('swinging_strike', 'swinging_strike_blocked') THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS whiff_pct,
            ROUND(SUM(CASE WHEN p.description IN ('called_strike', 'swinging_strike', 'swinging_strike_blocked', 'foul', 'foul_tip') THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS csw_pct
        FROM pitches p
        LEFT JOIN players pl ON pl.player_id = p.pitcher_id
        WHERE {where_sql}
          AND p.pitch_type IS NOT NULL
        GROUP BY p.pitcher_id, {player_name_expr('Pitcher', 'p.pitcher_id')}, p.pitch_type
        HAVING COUNT(*) >= {min_pitches}
        ORDER BY whiff_pct DESC
        LIMIT {limit}
        """
    ).df()
    return inject_player_names(df_to_json(df), id_key="pitcher_id", name_key="pitcher_name")


# ---------------------------------------------------------------------------
# Legacy-compatible endpoint
# ---------------------------------------------------------------------------

@app.get("/api/count-state/outcome-matrix-legacy")
def api_outcome_matrix_legacy(season: Optional[int] = None):
    con = get_con()
    season = season or latest_data_context(con)["latest_season"]
    return df_to_json(at_bat_outcome_by_count(con, season=season))


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
