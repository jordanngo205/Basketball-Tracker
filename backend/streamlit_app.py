from __future__ import annotations

import os
from datetime import date
from uuid import uuid4

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

TEAMS = [
    "Guelph",
    "Queen's",
    "Carleton",
    "Ottawa",
    "Laurentian",
    "Nipissing",
    "Ontario Tech",
    "Windsor",
    "Western",
    "TMU",
    "Brock",
    "York",
    "Toronto",
    "Lakehead",
    "McMaster",
    "Laurier",
    "Algoma",
]

OUTCOMES = [
    {"label": "Shot at Rim - Make", "value": "shot_at_rim_make"},
    {"label": "Shot at Rim - Miss", "value": "shot_at_rim_miss"},
    {"label": "Kick-out 3PT - Make", "value": "kick_out_3_make"},
    {"label": "Kick-out 3PT - Miss", "value": "kick_out_3_miss"},
    {"label": "Foul Drawn", "value": "foul_drawn"},
    {"label": "Turnover", "value": "turnover"},
    {"label": "Putback", "value": "putback"},
    {"label": "Reset (No Advantage)", "value": "reset"},
]

POINT_OPTIONS = [0, 1, 2, 3, 4]
DEFAULT_ROWS = 30

DATABASE_URL = os.getenv("DATABASE_URL", "")


st.set_page_config(
    page_title="Waterloo Warriors Womens basketball",
    layout="wide",
)


if "games" not in st.session_state:
    st.session_state.games = []
if "active_game_id" not in st.session_state:
    st.session_state.active_game_id = None
if "rows_by_quarter" not in st.session_state:
    st.session_state.rows_by_quarter = {}
if "quarter" not in st.session_state:
    st.session_state.quarter = 1
if "db_loaded" not in st.session_state:
    st.session_state.db_loaded = False
if "pending_delete_game_id" not in st.session_state:
    st.session_state.pending_delete_game_id = None


def get_active_game() -> dict | None:
    return next(
        (game for game in st.session_state.games if game["id"] == st.session_state.active_game_id),
        None,
    )


def get_rows_for_quarter(quarter: int) -> int:
    return st.session_state.rows_by_quarter.get(str(quarter), DEFAULT_ROWS)


def update_possession(game: dict, quarter: int, number: int, updates: dict) -> None:
    possessions = game.setdefault("possessions", [])
    existing = next(
        (p for p in possessions if p["quarter"] == quarter and p["number"] == number),
        None,
    )
    if not existing:
        existing = {
            "id": str(uuid4()),
            "quarter": quarter,
            "number": number,
            "paint_touch": False,
            "transition": False,
            "points": None,
            "outcome": "",
            "defense": "",
            "shot_quality": "",
        }
        possessions.append(existing)
    existing.update(updates)


def delete_possession(game: dict, quarter: int, number: int) -> None:
    possessions = [
        p for p in game.get("possessions", []) if not (p["quarter"] == quarter and p["number"] == number)
    ]
    for possession in possessions:
        if possession["quarter"] == quarter and possession["number"] > number:
            possession["number"] -= 1
    game["possessions"] = possessions


@st.cache_resource
def get_engine():
    if not DATABASE_URL:
        return None
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def init_db(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS games (
                    id SERIAL PRIMARY KEY,
                    client_id TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    opponent TEXT,
                    game_date TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS possessions (
                    id SERIAL PRIMARY KEY,
                    client_id TEXT UNIQUE NOT NULL,
                    game_id INTEGER NOT NULL REFERENCES games(id),
                    number INTEGER NOT NULL,
                    quarter INTEGER NOT NULL,
                    paint_touch BOOLEAN NOT NULL,
                    transition BOOLEAN NOT NULL,
                    points INTEGER,
                    outcome TEXT,
                    defense TEXT,
                    shot_quality TEXT,
                    tracker TEXT,
                    timestamp TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE possessions
                ADD COLUMN IF NOT EXISTS defense TEXT
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE possessions
                ADD COLUMN IF NOT EXISTS shot_quality TEXT
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE possessions
                ADD COLUMN IF NOT EXISTS transition BOOLEAN
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE possessions
                ADD COLUMN IF NOT EXISTS tracker TEXT
                """
            )
        )


def sync_game(engine, game: dict) -> int:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO games (client_id, name, opponent, game_date, created_at)
                VALUES (:client_id, :name, :opponent, :game_date, :created_at)
                ON CONFLICT (client_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    opponent = EXCLUDED.opponent,
                    game_date = EXCLUDED.game_date
                """
            ),
            {
                "client_id": game["id"],
                "name": game["name"],
                "opponent": game.get("opponent") or "",
                "game_date": game.get("date"),
                "created_at": date.today().isoformat(),
            },
        )
        game_id = conn.execute(
            text("SELECT id FROM games WHERE client_id = :client_id"),
            {"client_id": game["id"]},
        ).scalar()

        conn.execute(text("DELETE FROM possessions WHERE game_id = :game_id"), {"game_id": game_id})

        synced = 0
        for possession in game.get("possessions", []):
            conn.execute(
                text(
                    """
                    INSERT INTO possessions
                        (client_id, game_id, number, quarter, paint_touch, transition, points, outcome, defense, shot_quality, tracker, timestamp)
                    VALUES
                        (:client_id, :game_id, :number, :quarter, :paint_touch, :transition, :points, :outcome, :defense, :shot_quality, :tracker, :timestamp)
                    """
                ),
                {
                    "client_id": possession.get("id"),
                    "game_id": game_id,
                    "number": possession.get("number"),
                    "quarter": possession.get("quarter"),
                    "paint_touch": possession.get("paint_touch") is True,
                    "transition": possession.get("transition") is True,
                    "points": possession.get("points"),
                    "outcome": possession.get("outcome"),
                    "defense": possession.get("defense") or "",
                    "shot_quality": possession.get("shot_quality") or "",
                    "tracker": "paint",
                    "timestamp": possession.get("timestamp") or date.today().isoformat(),
                },
            )
            synced += 1
        return synced


def load_games(engine) -> list[dict]:
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, client_id, name, opponent, game_date
                FROM games
                ORDER BY created_at DESC, id DESC
                """
            )
        ).mappings().all()
        games: list[dict] = []
        for row in rows:
            possessions = conn.execute(
                text(
                    """
                    SELECT client_id, number, quarter, paint_touch, transition, points, outcome, defense, shot_quality, tracker, timestamp
                    FROM possessions
                    WHERE game_id = :game_id
                      AND (tracker IS NULL OR tracker = '' OR tracker = 'paint')
                    ORDER BY quarter, number
                    """
                ),
                {"game_id": row["id"]},
            ).mappings().all()
            deduped: dict[tuple[int, int], dict] = {}
            for possession in possessions:
                quarter = possession["quarter"]
                number = possession["number"]
                key = (quarter, number)
                existing = deduped.get(key)
                if existing and existing["timestamp"] >= possession["timestamp"]:
                    continue
                deduped[key] = {
                    "id": possession["client_id"],
                    "number": number,
                    "quarter": quarter,
                    "paint_touch": possession["paint_touch"],
                    "transition": possession.get("transition") is True,
                    "points": possession["points"],
                    "outcome": possession["outcome"],
                    "defense": possession.get("defense") or "",
                    "shot_quality": possession.get("shot_quality") or "",
                    "timestamp": possession["timestamp"],
                }
            paint_possessions = sorted(deduped.values(), key=lambda x: (x["quarter"], x["number"]))
            games.append(
                {
                    "id": row["client_id"],
                    "name": row["name"],
                    "opponent": row["opponent"],
                    "date": row["game_date"],
                    "possessions": paint_possessions,
                }
            )
        return games


def delete_game(engine, game_id: str) -> None:
    with engine.begin() as conn:
        db_id = conn.execute(
            text("SELECT id FROM games WHERE client_id = :client_id"),
            {"client_id": game_id},
        ).scalar()
        if db_id is None:
            return
        conn.execute(text("DELETE FROM possessions WHERE game_id = :game_id"), {"game_id": db_id})
        conn.execute(text("DELETE FROM games WHERE id = :game_id"), {"game_id": db_id})

def build_pie_chart(entries: list[tuple[str, int]]) -> go.Figure:
    labels = [label for label, _ in entries]
    values = [count for _, count in entries]
    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                hole=0.55,
                marker=dict(colors=["#EAAB00", "#FED34C", "#2f241b"]),
                textinfo="percent",
            )
        ]
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=230,
        showlegend=False,
    )
    return fig


def render_outcome_legend(labels: list[str]) -> None:
    colors = ["#EAAB00", "#FED34C", "#2f241b"]
    items = []
    for label, color in zip(labels, colors, strict=False):
        items.append(
            f"<span style='display:inline-flex;align-items:center;margin-right:16px;'>"
            f"<span style='display:inline-block;width:12px;height:12px;background:{color};"
            "border-radius:2px;margin-right:6px;'></span>"
            f"{label}</span>"
        )
    st.markdown("".join(items), unsafe_allow_html=True)


def build_bar_chart(quarter_stats: list[dict]) -> go.Figure:
    quarters = [f"Q{stat['quarter']}" for stat in quarter_stats]
    paint_rates = [stat["paint_rate"] for stat in quarter_stats]
    score_rates = [stat["paint_score_rate"] for stat in quarter_stats]

    fig = go.Figure(
        data=[
            go.Bar(name="Paint rate", x=quarters, y=paint_rates, marker_color="#EAAB00"),
            go.Bar(name="Score on paint", x=quarters, y=score_rates, marker_color="#2f241b"),
        ]
    )
    fig.update_layout(
        barmode="group",
        margin=dict(l=0, r=0, t=10, b=0),
        height=230,
        yaxis=dict(range=[0, 100], ticksuffix="%"),
        xaxis=dict(showgrid=False),
        legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="right", x=1),
    )
    return fig


def count_paint_touch_three_make_streaks(possessions: list[dict]) -> int:
    streak = 0
    total = 0
    for possession in possessions:
        made = possession.get("paint_touch") and (possession.get("points") or 0) > 0
        if made:
            streak += 1
            if streak == 3:
                total += 1
            elif streak > 3:
                # Count every run of 3+ as a single streak.
                continue
        else:
            streak = 0
    return total


def defense_breakdown(possessions: list[dict], defense: str) -> dict:
    filtered = [p for p in possessions if (p.get("defense") or "").lower() == defense]
    total = len(filtered)
    paint = sum(1 for p in filtered if p.get("paint_touch"))
    points = sum(p.get("points") or 0 for p in filtered)
    paint_rate = round((paint / total) * 100) if total else 0
    ppp = round(points / total, 2) if total else 0
    return {"total": total, "paint_rate": paint_rate, "ppp": ppp}


def render_analytics(active_game: dict | None, quarter_filter: int | None, half_filter: int | None) -> None:
    st.markdown(
        "<div style='letter-spacing:0.3em;text-transform:uppercase;font-size:11px;color:#5d4936;'>Analytics</div>",
        unsafe_allow_html=True,
    )
    if half_filter in (1, 2):
        st.subheader(f"{'First' if half_filter == 1 else 'Second'} half analysis")
        st.caption("Paint touch possessions for this half.")
    elif quarter_filter is None:
        st.subheader("Full game analysis")
        st.caption("Paint touch possessions across the game.")
    else:
        st.subheader(f"Quarter {quarter_filter} snapshot")
        st.caption("Key outcomes + paint touch performance.")

    if not active_game:
        st.info("Select a game to see analytics.")
        return

    if half_filter == 1:
        analytics_possessions = [
            p for p in active_game.get("possessions", []) if p.get("quarter") in (1, 2)
        ]
    elif half_filter == 2:
        analytics_possessions = [
            p for p in active_game.get("possessions", []) if p.get("quarter") in (3, 4)
        ]
    elif quarter_filter is None:
        analytics_possessions = list(active_game.get("possessions", []))
    else:
        analytics_possessions = [
            p for p in active_game.get("possessions", []) if p.get("quarter") == quarter_filter
        ]
    analytics_possessions = sorted(
        analytics_possessions, key=lambda x: (x.get("quarter") or 0, x.get("number") or 0)
    )
    total = len(analytics_possessions)
    paint_touches = sum(1 for p in analytics_possessions if p.get("paint_touch"))
    transition_possessions = [p for p in analytics_possessions if p.get("transition")]
    transition_total = len(transition_possessions)
    points = sum(p.get("points") or 0 for p in analytics_possessions)
    transition_points = sum(p.get("points") or 0 for p in transition_possessions)
    paint_rate = round((paint_touches / total) * 100) if total else 0
    ppp = round(points / total, 2) if total else 0
    transition_rate = round((transition_total / total) * 100) if total else 0
    transition_ppp = round(transition_points / transition_total, 2) if transition_total else 0
    transition_scores = sum(1 for p in transition_possessions if (p.get("points") or 0) > 0)
    transition_score_rate = round((transition_scores / transition_total) * 100) if transition_total else 0
    paint_touch_possessions = [p for p in analytics_possessions if p.get("paint_touch")]
    non_paint_possessions = [p for p in analytics_possessions if not p.get("paint_touch")]
    paint_scores = sum(1 for p in paint_touch_possessions if (p.get("points") or 0) > 0)
    paint_score_rate = round((paint_scores / paint_touches) * 100) if paint_touches else 0
    non_paint_total = total - paint_touches
    non_paint_scores = sum(1 for p in non_paint_possessions if (p.get("points") or 0) > 0)
    non_paint_score_rate = round((non_paint_scores / non_paint_total) * 100) if non_paint_total else 0
    paint_touch_3_streaks = count_paint_touch_three_make_streaks(analytics_possessions)

    stat_cols = st.columns(4)
    stat_cols[0].metric("Possessions logged", total)
    stat_cols[1].metric("Paint touch rate", f"{paint_rate}%")
    stat_cols[2].metric("Points per possession", f"{ppp:.2f}")
    stat_cols[3].metric("Paint-touch 3 makes in a row", paint_touch_3_streaks)

    st.markdown("---")
    st.markdown("**Outcome share (key results)**")

    key_outcomes = [
        {"label": "Rim Make", "key": "shot_at_rim_make"},
        {"label": "Kick-out 3 Make", "key": "kick_out_3_make"},
        {"label": "Foul Drawn", "key": "foul_drawn"},
    ]
    outcome_entries = [
        (item["label"], sum(1 for p in analytics_possessions if p.get("outcome") == item["key"]))
        for item in key_outcomes
    ]
    st.plotly_chart(build_pie_chart(outcome_entries), use_container_width=True, config={"displayModeBar": False})
    render_outcome_legend([item["label"] for item in key_outcomes])

    st.markdown("---")
    st.markdown("**Paint touch performance**")
    paint_cols = st.columns(3)
    paint_cols[0].metric("Score on paint touches", f"{paint_score_rate}%")
    paint_cols[1].metric("Paint touch scores", f"{paint_scores}/{paint_touches}")
    paint_cols[2].metric("Paint touch possessions", f"{paint_touches}/{total}")

    paint_outcomes = {
        "Rim make": "shot_at_rim_make",
        "Kick-out 3 make": "kick_out_3_make",
        "Foul drawn": "foul_drawn",
    }
    outcome_cols = st.columns(3)
    for col, (label, key) in zip(outcome_cols, paint_outcomes.items(), strict=False):
        count = sum(1 for p in paint_touch_possessions if p.get("outcome") == key)
        pct = round((count / paint_touches) * 100) if paint_touches else 0
        col.metric(f"{label} (paint)", f"{count} ({pct}%)")

    st.markdown("---")
    st.markdown("**Non-paint touch performance**")
    non_paint_cols = st.columns(2)
    non_paint_cols[0].metric("Score on non-paint touches", f"{non_paint_score_rate}%")
    non_paint_cols[1].metric("Non-paint touch scores", f"{non_paint_scores}/{non_paint_total}")

    st.markdown("---")
    st.markdown("**Transition performance**")
    trans_cols = st.columns(3)
    trans_cols[0].metric("Transition possessions", f"{transition_total}/{total}")
    trans_cols[1].metric("Transition rate", f"{transition_rate}%")
    trans_cols[2].metric("Score on transitions", f"{transition_score_rate}%")

    st.markdown("---")
    st.markdown("**Defense split**")
    man_stats = defense_breakdown(analytics_possessions, "man")
    zone_stats = defense_breakdown(analytics_possessions, "zone")
    defense_cols = st.columns(2)
    with defense_cols[0]:
        st.metric("Man possessions", man_stats["total"])
        st.metric("Man paint rate", f"{man_stats['paint_rate']}%")
        st.metric("Man points/poss", f"{man_stats['ppp']:.2f}")
    with defense_cols[1]:
        st.metric("Zone possessions", zone_stats["total"])
        st.metric("Zone paint rate", f"{zone_stats['paint_rate']}%")
        st.metric("Zone points/poss", f"{zone_stats['ppp']:.2f}")

    st.markdown("---")
    st.markdown("**Quarter comparison**")
    quarter_stats = []
    for q in [1, 2, 3, 4]:
        possessions = [p for p in active_game.get("possessions", []) if p.get("quarter") == q]
        total_q = len(possessions)
        paint_q = sum(1 for p in possessions if p.get("paint_touch"))
        paint_scores_q = sum(1 for p in possessions if p.get("paint_touch") and (p.get("points") or 0) > 0)
        paint_rate_q = round((paint_q / total_q) * 100) if total_q else 0
        score_rate_q = round((paint_scores_q / paint_q) * 100) if paint_q else 0
        quarter_stats.append(
            {
                "quarter": q,
                "paint_rate": paint_rate_q,
                "paint_score_rate": score_rate_q,
            }
        )
    st.plotly_chart(build_bar_chart(quarter_stats), use_container_width=True, config={"displayModeBar": False})


if not st.session_state.db_loaded:
    engine = get_engine()
    if engine and not st.session_state.games:
        try:
            init_db(engine)
            st.session_state.games = load_games(engine)
            if st.session_state.games and st.session_state.active_game_id is None:
                st.session_state.active_game_id = st.session_state.games[0]["id"]
        except SQLAlchemyError as exc:
            st.warning(f"Could not load games from database: {exc}")
    st.session_state.db_loaded = True


st.markdown("<div style='letter-spacing:0.3em;text-transform:uppercase;font-size:11px;color:#5d4936;'>Waterloo Warriors Womens basketball</div>", unsafe_allow_html=True)
st.title("In Game Performance Tracker")

st.columns([3, 1])


with st.sidebar:
    st.header("Trackers")
    st.markdown("**Paint touches**  ")
    st.caption("Live paint touch logging")
    st.markdown("---")

    st.subheader("Start a game")
    game_name = st.text_input("Game name", placeholder="vs ....")
    opponent = st.selectbox("Opponent (OUA)", [""] + TEAMS)
    game_date = st.date_input("Date", value=date.today())
    if st.button("Create game"):
        if game_name.strip():
            game_id = str(uuid4())
            st.session_state.games.insert(
                0,
                {
                    "id": game_id,
                    "name": game_name.strip(),
                    "opponent": opponent,
                    "date": game_date.isoformat(),
                    "possessions": [],
                },
            )
            st.session_state.active_game_id = game_id

    st.markdown("---")
    st.subheader("Active games")
    if not st.session_state.games:
        st.caption("No games yet.")
    for game in st.session_state.games:
        is_active = game["id"] == st.session_state.active_game_id
        st.markdown(f"**{game['name']}**" + (" (active)" if is_active else ""))
        st.caption(f"{game.get('opponent') or 'No opponent set'} · {game.get('date')}")
        st.caption(f"{len(game.get('possessions', []))} logged")
        col_select, col_delete = st.columns(2)
        with col_select:
            if st.button("Select", key=f"select_{game['id']}"):
                st.session_state.active_game_id = game["id"]
        with col_delete:
            if st.button("Delete", key=f"delete_{game['id']}"):
                st.session_state.pending_delete_game_id = game["id"]
        if st.session_state.pending_delete_game_id == game["id"]:
            st.warning("Delete this game from the database? This cannot be undone.")
            confirm_col, cancel_col = st.columns(2)
            with confirm_col:
                if st.button("Confirm delete", key=f"confirm_delete_{game['id']}"):
                    engine = get_engine()
                    if engine:
                        try:
                            delete_game(engine, game["id"])
                        except SQLAlchemyError as exc:
                            st.error(f"Failed to delete from database: {exc}")
                    st.session_state.games = [g for g in st.session_state.games if g["id"] != game["id"]]
                    if st.session_state.active_game_id == game["id"]:
                        st.session_state.active_game_id = (
                            st.session_state.games[0]["id"] if st.session_state.games else None
                        )
                    st.session_state.pending_delete_game_id = None
                    st.rerun()
            with cancel_col:
                if st.button("Cancel", key=f"cancel_delete_{game['id']}"):
                    st.session_state.pending_delete_game_id = None
    st.markdown("---")
    analytics_focus = st.toggle("Full game analysis (full width)", value=False)


active_game = get_active_game()
if analytics_focus:
    analytics_view = st.selectbox(
        "Analytics view",
        ["Full game", "First half", "Second half", "Q1", "Q2", "Q3", "Q4"],
        index=0,
        key="analytics_view",
    )
    selected_quarter = None
    selected_half = None
    if analytics_view == "First half":
        selected_half = 1
    elif analytics_view == "Second half":
        selected_half = 2
    elif analytics_view != "Full game":
        selected_quarter = int(analytics_view[1])
    render_analytics(active_game, selected_quarter, selected_half)
else:
    main_col, analytics_col = st.columns([1.7, 1])

if not analytics_focus:
    with main_col:
        if not active_game:
            st.info("Select a game to start tracking.")
        else:
            header_left, header_right = st.columns([3, 1])
            with header_left:
                st.markdown("<div style='letter-spacing:0.3em;text-transform:uppercase;font-size:11px;color:#5d4936;'>Active game</div>", unsafe_allow_html=True)
                st.subheader(active_game["name"])
                st.caption(f"{active_game.get('opponent') or 'Opponent TBD'} · {active_game.get('date')}")
            with header_right:
                st.session_state.quarter = st.selectbox("Quarter", [1, 2, 3, 4], index=st.session_state.quarter - 1)
                rows = get_rows_for_quarter(st.session_state.quarter)
                quarter_possessions = [
                    p for p in active_game.get("possessions", []) if p.get("quarter") == st.session_state.quarter
                ]
                st.caption(f"Logged {len(quarter_possessions)}/{rows}")

        st.markdown("---")
        header = st.columns([0.7, 1, 1, 1.1, 1.1, 1.2, 3, 0.4])
        header[0].markdown("**Poss**")
        header[1].markdown("**Paint Touch (0/1)**")
        header[2].markdown("**Trans (0/1)**")
        header[3].markdown("**Points**")
        header[4].markdown("**Def**")
        header[5].markdown("**Shot Q**")
        header[6].markdown("**Outcome**")

        rows = get_rows_for_quarter(st.session_state.quarter)
        quarter_possessions = [
            p for p in active_game.get("possessions", []) if p.get("quarter") == st.session_state.quarter
        ]
        possession_map = {p["number"]: p for p in quarter_possessions}

        for number in range(1, rows + 1):
            entry = possession_map.get(number)
            paint_touch = entry.get("paint_touch") if entry else None
            transition = entry.get("transition") if entry else None
            points = entry.get("points") if entry else None
            outcome = entry.get("outcome") if entry else ""
            defense = entry.get("defense") if entry else ""
            shot_quality = entry.get("shot_quality") if entry else ""

            with st.container(border=True):
                header_cols = st.columns([6, 1])
                header_cols[0].markdown(f"**#{number}**")
                with header_cols[1]:
                    if st.button("🗑", key=f"delete_{st.session_state.quarter}_{number}"):
                        delete_possession(active_game, st.session_state.quarter, number)
                        current_rows = get_rows_for_quarter(st.session_state.quarter)
                        st.session_state.rows_by_quarter[str(st.session_state.quarter)] = max(1, current_rows - 1)
                        st.rerun()

                field_cols = st.columns([1, 1, 1, 1.1, 1.2, 3])
                with field_cols[0]:
                    paint_index = None
                    if paint_touch is True:
                        paint_index = 1
                    elif paint_touch is False:
                        paint_index = 0
                    paint_choice = st.radio(
                        "Paint Touch (0/1)",
                        [0, 1],
                        horizontal=True,
                        index=paint_index,
                        key=f"paint_{st.session_state.quarter}_{number}",
                    )
                    if paint_choice is not None and paint_choice != (
                        1 if paint_touch else 0 if paint_touch is False else None
                    ):
                        update_possession(
                            active_game,
                            st.session_state.quarter,
                            number,
                            {"paint_touch": bool(paint_choice)},
                        )

                with field_cols[1]:
                    transition_index = None
                    if transition is True:
                        transition_index = 1
                    elif transition is False:
                        transition_index = 0
                    transition_choice = st.radio(
                        "Trans (0/1)",
                        [0, 1],
                        horizontal=True,
                        index=transition_index,
                        key=f"transition_{st.session_state.quarter}_{number}",
                    )
                    if transition_choice is not None and transition_choice != (
                        1 if transition else 0 if transition is False else None
                    ):
                        update_possession(
                            active_game,
                            st.session_state.quarter,
                            number,
                            {"transition": bool(transition_choice)},
                        )

                with field_cols[2]:
                    points_index = None
                    if points is not None:
                        points_index = POINT_OPTIONS.index(points)
                    points_choice = st.radio(
                        "Points",
                        POINT_OPTIONS,
                        horizontal=True,
                        index=points_index,
                        key=f"points_{st.session_state.quarter}_{number}",
                    )
                    if points_choice is not None and points_choice != points:
                        update_possession(
                            active_game,
                            st.session_state.quarter,
                            number,
                            {"points": points_choice},
                        )

                with field_cols[3]:
                    defense_options = ["Man", "Zone"]
                    defense_index = None
                    if defense and defense.lower() in ("man", "zone"):
                        defense_index = 0 if defense.lower() == "man" else 1
                    defense_choice = st.radio(
                        "Def",
                        defense_options,
                        horizontal=True,
                        index=defense_index,
                        key=f"defense_{st.session_state.quarter}_{number}",
                    )
                    if defense_choice:
                        defense_value = defense_choice.lower()
                        if defense_value != (defense or "").lower():
                            update_possession(
                                active_game,
                                st.session_state.quarter,
                                number,
                                {"defense": defense_value},
                            )

                with field_cols[4]:
                    shot_options = ["Good", "Bad"]
                    shot_index = None
                    if shot_quality and shot_quality.lower() in ("good", "bad"):
                        shot_index = 0 if shot_quality.lower() == "good" else 1
                    shot_choice = st.radio(
                        "Shot Q",
                        shot_options,
                        horizontal=True,
                        index=shot_index,
                        key=f"shot_quality_{st.session_state.quarter}_{number}",
                    )
                    if shot_choice:
                        shot_value = shot_choice.lower()
                        if shot_value != (shot_quality or "").lower():
                            update_possession(
                                active_game,
                                st.session_state.quarter,
                                number,
                                {"shot_quality": shot_value},
                            )

                with field_cols[5]:
                    outcome_labels = [item["label"] for item in OUTCOMES]
                    outcome_values = [item["value"] for item in OUTCOMES]
                    outcome_index = None
                    if outcome in outcome_values:
                        outcome_index = outcome_values.index(outcome)
                    outcome_choice = st.radio(
                        "Outcome",
                        outcome_labels,
                        horizontal=True,
                        index=outcome_index,
                        key=f"outcome_{st.session_state.quarter}_{number}",
                    )
                    if outcome_choice and outcome_choice != "":
                        selected_value = OUTCOMES[outcome_labels.index(outcome_choice)]["value"]
                        if selected_value != outcome:
                            update_possession(
                                active_game,
                                st.session_state.quarter,
                                number,
                                {"outcome": selected_value},
                            )

        if st.button("Add possession row", key=f"add_possession_{st.session_state.quarter}"):
            current_rows = get_rows_for_quarter(st.session_state.quarter)
            st.session_state.rows_by_quarter[str(st.session_state.quarter)] = current_rows + 1
            st.rerun()

        if active_game.get("possessions"):
            export_rows = [
                [
                    p.get("number"),
                    p.get("quarter"),
                    "yes" if p.get("paint_touch") else "no",
                    "yes" if p.get("transition") else "no",
                    p.get("points") if p.get("points") is not None else "",
                    p.get("defense") or "",
                    p.get("shot_quality") or "",
                    p.get("outcome") or "",
                ]
                for p in sorted(active_game.get("possessions", []), key=lambda x: (x["quarter"], x["number"]))
            ]
            export_df = pd.DataFrame(
                export_rows,
                columns=[
                    "possession_number",
                    "quarter",
                    "paint_touch",
                    "transition",
                    "points",
                    "defense",
                    "shot_quality",
                    "outcome",
                ],
            )
            export_col, sync_col = st.columns([1, 1])
            with export_col:
                st.download_button(
                    "Export CSV",
                    export_df.to_csv(index=False),
                    file_name=f"{active_game.get('name','game')}_{active_game.get('date')}.csv",
                    mime="text/csv",
                )
            with sync_col:
                if st.button("Sync to DB"):
                    engine = get_engine()
                    if not engine:
                        st.error("DATABASE_URL is not set.")
                    else:
                        try:
                            init_db(engine)
                            synced = sync_game(engine, active_game)
                            st.success(f"Synced {synced} possessions.")
                        except SQLAlchemyError as exc:
                            st.error(f"Sync failed: {exc}")

    with analytics_col:
        render_analytics(active_game, st.session_state.quarter, None)
