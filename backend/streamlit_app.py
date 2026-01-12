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
            "points": None,
            "outcome": "",
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
                    points INTEGER,
                    outcome TEXT,
                    timestamp TEXT NOT NULL
                )
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

        synced = 0
        for possession in game.get("possessions", []):
            conn.execute(
                text(
                    """
                    INSERT INTO possessions
                        (client_id, game_id, number, quarter, paint_touch, points, outcome, timestamp)
                    VALUES
                        (:client_id, :game_id, :number, :quarter, :paint_touch, :points, :outcome, :timestamp)
                    ON CONFLICT (client_id) DO UPDATE SET
                        number = EXCLUDED.number,
                        quarter = EXCLUDED.quarter,
                        paint_touch = EXCLUDED.paint_touch,
                        points = EXCLUDED.points,
                        outcome = EXCLUDED.outcome,
                        timestamp = EXCLUDED.timestamp
                    """
                ),
                {
                    "client_id": possession.get("id"),
                    "game_id": game_id,
                    "number": possession.get("number"),
                    "quarter": possession.get("quarter"),
                    "paint_touch": possession.get("paint_touch") is True,
                    "points": possession.get("points"),
                    "outcome": possession.get("outcome"),
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
                    SELECT client_id, number, quarter, paint_touch, points, outcome, timestamp
                    FROM possessions
                    WHERE game_id = :game_id
                    ORDER BY quarter, number
                    """
                ),
                {"game_id": row["id"]},
            ).mappings().all()
            games.append(
                {
                    "id": row["client_id"],
                    "name": row["name"],
                    "opponent": row["opponent"],
                    "date": row["game_date"],
                    "possessions": [
                        {
                            "id": possession["client_id"],
                            "number": possession["number"],
                            "quarter": possession["quarter"],
                            "paint_touch": possession["paint_touch"],
                            "points": possession["points"],
                            "outcome": possession["outcome"],
                            "timestamp": possession["timestamp"],
                        }
                        for possession in possessions
                    ],
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


def render_analytics(active_game: dict | None, quarter_filter: int | None) -> None:
    st.markdown(
        "<div style='letter-spacing:0.3em;text-transform:uppercase;font-size:11px;color:#5d4936;'>Analytics</div>",
        unsafe_allow_html=True,
    )
    if quarter_filter is None:
        st.subheader("Full game analysis")
        st.caption("All possessions across the game.")
    else:
        st.subheader(f"Quarter {quarter_filter} snapshot")
        st.caption("Key outcomes + paint touch performance.")

    if not active_game:
        st.info("Select a game to see analytics.")
        return

    if quarter_filter is None:
        analytics_possessions = list(active_game.get("possessions", []))
    else:
        analytics_possessions = [
            p for p in active_game.get("possessions", []) if p.get("quarter") == quarter_filter
        ]
    total = len(analytics_possessions)
    paint_touches = sum(1 for p in analytics_possessions if p.get("paint_touch"))
    points = sum(p.get("points") or 0 for p in analytics_possessions)
    paint_rate = round((paint_touches / total) * 100) if total else 0
    ppp = round(points / total, 2) if total else 0
    paint_scores = sum(
        1 for p in analytics_possessions if p.get("paint_touch") and (p.get("points") or 0) > 0
    )
    paint_score_rate = round((paint_scores / paint_touches) * 100) if paint_touches else 0

    stat_cols = st.columns(3)
    stat_cols[0].metric("Possessions logged", total)
    stat_cols[1].metric("Paint touch rate", f"{paint_rate}%")
    stat_cols[2].metric("Points per possession", f"{ppp:.2f}")

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

    st.markdown("---")
    st.markdown("**Paint touch performance**")
    perf_cols = st.columns(2)
    perf_cols[0].metric("Score on paint touches", f"{paint_score_rate}%")
    perf_cols[1].metric("Paint touch scores", f"{paint_scores}/{paint_touches}")

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
st.title("In Game performance tracker")

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
        ["Full game", "Q1", "Q2", "Q3", "Q4"],
        index=0,
        key="analytics_view",
    )
    selected_quarter = None
    if analytics_view != "Full game":
        selected_quarter = int(analytics_view[1])
    render_analytics(active_game, selected_quarter)
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
        header = st.columns([0.7, 1, 1.1, 3, 0.4])
        header[0].markdown("**Poss**")
        header[1].markdown("**Paint Touch (0/1)**")
        header[2].markdown("**Points**")
        header[3].markdown("**Outcome**")

        rows = get_rows_for_quarter(st.session_state.quarter)
        quarter_possessions = [
            p for p in active_game.get("possessions", []) if p.get("quarter") == st.session_state.quarter
        ]
        possession_map = {p["number"]: p for p in quarter_possessions}

        for number in range(1, rows + 1):
            entry = possession_map.get(number)
            paint_touch = entry.get("paint_touch") if entry else None
            points = entry.get("points") if entry else None
            outcome = entry.get("outcome") if entry else ""

            with st.container(border=True):
                header_cols = st.columns([6, 1])
                header_cols[0].markdown(f"**#{number}**")
                with header_cols[1]:
                    if st.button("🗑", key=f"delete_{st.session_state.quarter}_{number}"):
                        delete_possession(active_game, st.session_state.quarter, number)
                        current_rows = get_rows_for_quarter(st.session_state.quarter)
                        st.session_state.rows_by_quarter[str(st.session_state.quarter)] = max(1, current_rows - 1)
                        st.rerun()

                field_cols = st.columns([1, 1, 3])
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
                            active_game, st.session_state.quarter, number, {"paint_touch": bool(paint_choice)}
                        )

                with field_cols[1]:
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
                        update_possession(active_game, st.session_state.quarter, number, {"points": points_choice})

                with field_cols[2]:
                    outcome_labels = [item["label"] for item in OUTCOMES]
                    outcome_values = [item["value"] for item in OUTCOMES]
                    outcome_index = None
                    if outcome in outcome_values:
                        outcome_index = outcome_values.index(outcome)
                    outcome_choice = st.radio(
                        "Outcome",
                        outcome_labels,
                        index=outcome_index,
                        key=f"outcome_{st.session_state.quarter}_{number}",
                    )
                    if outcome_choice and outcome_choice != "":
                        selected_value = OUTCOMES[outcome_labels.index(outcome_choice)]["value"]
                        if selected_value != outcome:
                            update_possession(
                                active_game, st.session_state.quarter, number, {"outcome": selected_value}
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
                    p.get("points") if p.get("points") is not None else "",
                    p.get("outcome") or "",
                ]
                for p in sorted(active_game.get("possessions", []), key=lambda x: (x["quarter"], x["number"]))
            ]
            export_df = pd.DataFrame(
                export_rows,
                columns=["possession_number", "quarter", "paint_touch", "points", "outcome"],
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
        render_analytics(active_game, st.session_state.quarter)
