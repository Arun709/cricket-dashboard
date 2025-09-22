import os
import streamlit as st
import pandas as pd
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import closing
import psycopg2.sql as sql
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


DB = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "cric_buzz"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}


RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST")


def get_conn():
    try:
        conn_str = (
            f"host={DB['host']} port={DB['port']} dbname={DB['dbname']} "
            f"user={DB['user']} password={DB['password']}"
        )
        return psycopg2.connect(conn_str, cursor_factory=RealDictCursor)
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None


def list_tables(schema="public"):
    q = """
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = %s AND table_type='BASE TABLE'
    ORDER BY table_name;"""
    with closing(get_conn()) as conn:
        if conn:
            with conn.cursor() as cur:
                cur.execute(q, (schema,))
                return [r["table_name"] for r in cur.fetchall()]
    return []


def get_table_columns(table, schema="public"):
    q = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position;"""
    with closing(get_conn()) as conn:
        if conn:
            with conn.cursor() as cur:
                cur.execute(q, (schema, table))
                return cur.fetchall()
    return []


def get_primary_key_columns(table, schema="public"):
    q = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
    WHERE tc.table_schema = %s AND tc.table_name = %s
      AND tc.constraint_type = 'PRIMARY KEY'
    ORDER BY kcu.ordinal_position;"""
    with closing(get_conn()) as conn:
        if conn:
            with conn.cursor() as cur:
                cur.execute(q, (schema, table))
                return [r["column_name"] for r in cur.fetchall()]
    return []


def fetch_table_rows(table, limit=100, schema="public"):
    q = sql.SQL("SELECT * FROM {}.{} LIMIT %s").format(
        sql.Identifier(schema), sql.Identifier(table)
    )
    with closing(get_conn()) as conn:
        if conn:
            with conn.cursor() as cur:
                cur.execute(q, (limit,))
                return cur.fetchall()
    return []


def upsert_row(table, row, pk_cols, schema="public"):
    cols = list(row.keys())
    if not cols:
        return
    values = [row[c] for c in cols]
    if pk_cols and all(row.get(pk) is not None for pk in pk_cols):
        set_cols = [c for c in cols if c not in pk_cols]
        if set_cols:
            set_clause = sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(c)) for c in set_cols
            )
            where_clause = sql.SQL(" AND ").join(
                sql.SQL("{} = %s").format(sql.Identifier(pk)) for pk in pk_cols
            )
            q = (
                sql.SQL("UPDATE {}.{} SET ").format(
                    sql.Identifier(schema), sql.Identifier(table)
                ) + set_clause + sql.SQL(" WHERE ") + where_clause
            )
            params = [row[c] for c in set_cols] + [row[pk] for pk in pk_cols]
            with closing(get_conn()) as conn:
                if conn:
                    with conn.cursor() as cur:
                        cur.execute(q, params)
                        conn.commit()
            return
    q = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in cols),
        sql.SQL(", ").join(sql.Placeholder() * len(cols)),
    )
    with closing(get_conn()) as conn:
        if conn:
            with conn.cursor() as cur:
                cur.execute(q, values)
                conn.commit()


def delete_row(table, pk_cols, pk_vals, schema="public"):
    where_clause = sql.SQL(" AND ").join(
        sql.SQL("{} = %s").format(sql.Identifier(pk)) for pk in pk_cols
    )
    q = sql.SQL("DELETE FROM {}.{} WHERE ").format(
        sql.Identifier(schema), sql.Identifier(table)
    ) + where_clause
    with closing(get_conn()) as conn:
        if conn:
            with conn.cursor() as cur:
                cur.execute(q, pk_vals)
                conn.commit()


def fetch_live_matches():
    url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/live"
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST,
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def fetch_match_details(match_id):
    url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}"
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST,
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def fetch_match_scorecard(match_id):
    url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}/scard"
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST,
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}

# Parse live matches data
def parse_live_matches(data):
    matches = []
    if not isinstance(data, dict):
        return matches
    def extract_matches_recursive(obj):
        extracted = []
        if isinstance(obj, dict):
            if obj.get("matchInfo") and obj.get("matchInfo", {}).get("matchId"):
                mi = obj.get("matchInfo", {})
                team1 = mi.get("team1", {})
                team2 = mi.get("team2", {})
                match_data = {
                    "match_id": mi.get("matchId"),
                    "team1": team1.get("teamSName", ""),
                    "team2": team2.get("teamSName", ""),
                    "teams": f"{team1.get('teamSName', '')} vs {team2.get('teamSName', '')}",
                    "format": mi.get("matchFormat") or mi.get("mFormat", ""),
                    "venue": mi.get("venueInfo", {}).get("ground", "") or mi.get("venue", ""),
                    "city": mi.get("venueInfo", {}).get("city", ""),
                    "status": obj.get("statusText", ""),
                    "start_time": mi.get("startDate"),
                    "series": mi.get("seriesName", ""),
                    "match_desc": mi.get("matchDesc", ""),
                    "state": mi.get("state", "")
                }
                extracted.append(match_data)
            for value in obj.values():
                if isinstance(value, (dict, list)):
                    extracted.extend(extract_matches_recursive(value))
        elif isinstance(obj, list):
            for item in obj:
                extracted.extend(extract_matches_recursive(item))
        return extracted
    matches = extract_matches_recursive(data)
    return matches


def parse_match_live_score(match_details):
    if not isinstance(match_details, dict):
        return None
    match_header = match_details.get("matchHeader", {})
    match_info = match_header.get("matchInfo", {})
    team1 = match_info.get("team1", {})
    team2 = match_info.get("team2", {})
    status = match_header.get("status", "")
    state = match_header.get("state", "")
    miniscore = match_details.get("miniscore", {})
    live_score = {
        "match_id": match_info.get("matchId"),
        "team1": team1.get("teamSName", ""),
        "team2": team2.get("teamSName", ""),
        "team1_score": "",
        "team2_score": "",
        "current_over": "",
        "status": status,
        "state": state,
        "toss": match_info.get("tossResults", {}).get("tossWinnerName", ""),
        "venue": f"{match_info.get('venueInfo', {}).get('ground', '')} - {match_info.get('venueInfo', {}).get('city', '')}",
        "series": match_info.get("seriesName", ""),
        "match_format": match_info.get("matchFormat", ""),
        "result": match_header.get("result", {}).get("resultText", ""),
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    if miniscore:
        innings_scores = miniscore.get("inningsScores", [])
        if len(innings_scores) >= 1:
            live_score["team1_score"] = f"{innings_scores[0].get('runs', 0)}/{innings_scores[0].get('wickets', 0)} ({innings_scores[0].get('overs', 0)} ov)"
        if len(innings_scores) >= 2:
            live_score["team2_score"] = f"{innings_scores[1].get('runs', 0)}/{innings_scores[1].get('wickets', 0)} ({innings_scores[1].get('overs', 0)} ov)"
        over_summary = miniscore.get("overSummary", {})
        if over_summary:
            live_score["current_over"] = f"Over {over_summary.get('overNum', '')}: {over_summary.get('runs', 0)} runs"
    return live_score


def parse_match_scorecard(scorecard_data):
    batting_stats = []
    bowling_stats = []
    if not isinstance(scorecard_data, dict):
        return batting_stats, bowling_stats
    scorecard = scorecard_data.get("scoreCard", [])
    for innings in scorecard:
        innings_number = innings.get("inningsId", 0)
        batting_team = innings.get("batTeamName", "")
        bowling_team = innings.get("bowlTeamName", "")
        bat_team_details = innings.get("batTeamDetails", {})
        batsmen = bat_team_details.get("batsmenData", {})
        for player_id, player_data in batsmen.items():
            batting_stats.append({
                "match_id": scorecard_data.get("matchId", ""),
                "innings": innings_number,
                "team": batting_team,
                "player": player_data.get("name", ""),
                "runs": player_data.get("runs", 0),
                "balls": player_data.get("balls", 0),
                "fours": player_data.get("fours", 0),
                "sixes": player_data.get("sixes", 0),
                "strike_rate": player_data.get("strikeRate", 0.0),
                "status": player_data.get("outDesc", "not out")
            })
        bowl_team_details = innings.get("bowlTeamDetails", {})
        bowlers = bowl_team_details.get("bowlersData", {})
        for player_id, player_data in bowlers.items():
            bowling_stats.append({
                "match_id": scorecard_data.get("matchId", ""),
                "innings": innings_number,
                "team": bowling_team,
                "player": player_data.get("name", ""),
                "overs": player_data.get("overs", 0.0),
                "maidens": player_data.get("maidens", 0),
                "runs": player_data.get("runs", 0),
                "wickets": player_data.get("wickets", 0),
                "economy": player_data.get("economy", 0.0),
                "wides": player_data.get("wides", 0),
                "noballs": player_data.get("noBalls", 0)
            })
    return batting_stats, bowling_stats


st.set_page_config(page_title="Live Cricket Dashboard", layout="wide")
st.markdown("""
<style>
.metric-card { background-color: #f0f2f6; padding: 1rem; border-radius: 0.5rem; margin: 0.5rem 0;}
.live-indicator { color: #ff4444; font-weight: bold; animation: blink 1s infinite;}
@keyframes blink {0% { opacity: 1; } 50% { opacity: 0.5; } 100% { opacity: 1; }}
.score-display { font-size: 1.2em; font-weight: bold; color: #1f77b4;}
</style>
""", unsafe_allow_html=True)

st.sidebar.title("🏏 Cricket Dashboard")
page = st.sidebar.radio("Navigate", ["🔴 Live Matches", "📊 Player Analytics"], index=0)

if page == "🔴 Live Matches":
    st.title("🔴 Live Cricket Matches")
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("🔄 Refresh Now"):
            st.rerun()
    with col2:
        show_detailed = st.checkbox("📋 Show Detailed Stats", value=True)
    with st.spinner("Fetching live matches..."):
        data = fetch_live_matches()
    if isinstance(data, dict) and data.get("error"):
        st.error(f"❌ API Error: {data['error']}")
    else:
        matches = parse_live_matches(data)
        if not matches:
            st.info("ℹ️ No live matches found at the moment.")
            if st.expander("🔍 Debug: Raw API Response"):
                st.json(data)
        else:
            st.subheader(f"📊 Live Matches Overview ({len(matches)} matches)")
            matches_df = pd.DataFrame(matches)
            for idx, match in enumerate(matches):
                with st.container():
                    st.markdown("---")
                    col1, col2, col3 = st.columns([3, 1, 1])
                    with col1:
                        st.markdown(f"### 🏏 {match['teams']}")
                        st.markdown(f"**{match['series']}** - {match['match_desc']}")
                    with col2:
                        st.markdown('<div class="live-indicator">● LIVE</div>', unsafe_allow_html=True)
                    with col3:
                        st.markdown(f"**{match['format']}**")
                    col1a, col2a, col3a = st.columns([2, 2, 1])
                    with col1a:
                        st.markdown(f"📍 **Venue:** {match['venue']}, {match['city']}")
                    with col2a:
                        st.markdown(f"📊 **Status:** {match['status']}")
                    if match['match_id']:
                        with st.spinner("Loading match scores..."):
                            match_details = fetch_match_details(match['match_id'])
                            if not (isinstance(match_details, dict) and match_details.get("error")):
                                live_score = parse_match_live_score(match_details)
                                if live_score:
                                    score_col1, score_col2, score_col3 = st.columns(3)
                                    with score_col1:
                                        st.markdown(f'<div class="score-display">{live_score["team1"]}: {live_score["team1_score"]}</div>', unsafe_allow_html=True)
                                    with score_col2:
                                        st.markdown(f'<div class="score-display">{live_score["team2"]}: {live_score["team2_score"]}</div>', unsafe_allow_html=True)
                                    with score_col3:
                                        if live_score["current_over"]:
                                            st.markdown(f'**{live_score["current_over"]}**')
                                    if live_score["result"]:
                                        st.success(f"🏆 **Result:** {live_score['result']}")
                                    st.write(f"🕓 Last updated: {live_score['last_updated']}")
                            else:
                                st.warning(f"⚠️ Could not fetch scores for match {match['match_id']}")
                    if show_detailed and match['match_id']:
                        scorecard_data = fetch_match_scorecard(match['match_id'])
                        if not (isinstance(scorecard_data, dict) and scorecard_data.get("error")):
                            batting_stats, bowling_stats = parse_match_scorecard(scorecard_data)
                            if batting_stats or bowling_stats:
                                tab1, tab2 = st.tabs(["🏏 Batting Stats", "⚾ Bowling Stats"])
                                with tab1:
                                    if batting_stats:
                                        batting_df = pd.DataFrame(batting_stats)
                                        teams = batting_df['team'].unique()
                                        for team in teams:
                                            st.subheader(f"{team} - Batting")
                                            team_batting = batting_df[batting_df['team'] == team]
                                            team_batting_display = team_batting[['player', 'runs', 'balls', 'fours', 'sixes', 'strike_rate', 'status']]
                                            st.dataframe(team_batting_display, hide_index=True)
                                    else:
                                        st.info("No batting statistics available")
                                with tab2:
                                    if bowling_stats:
                                        bowling_df = pd.DataFrame(bowling_stats)
                                        teams = bowling_df['team'].unique()
                                        for team in teams:
                                            st.subheader(f"{team} - Bowling")
                                            team_bowling = bowling_df[bowling_df['team'] == team]
                                            team_bowling_display = team_bowling[['player', 'overs', 'maidens', 'runs', 'wickets', 'economy', 'wides', 'noballs']]
                                            st.dataframe(team_bowling_display, hide_index=True)
                                    else:
                                        st.info("No bowling statistics available")
                        else:
                            st.warning(f"⚠️ Could not fetch detailed scorecard for match {match['match_id']}")

elif page == "📊 Player Analytics":
    st.header("📊 Player Analytics (Database Editor)")
    st.sidebar.subheader("Controls")
    schema = st.selectbox("Schema", ["public"])
    tables = list_tables(schema)
    search = st.text_input("Search tables")
    filtered = [t for t in tables if search.lower() in t.lower()] if search else tables
    selected = st.selectbox("Select table", filtered) if filtered else None
    if not selected:
        st.info("No table selected or no tables found.")
    else:
        cols_meta = get_table_columns(selected, schema)
        col_names = [c["column_name"] for c in cols_meta]
        pk_cols = get_primary_key_columns(selected, schema)
        st.sidebar.subheader("Columns to display")
        chosen_cols = st.sidebar.multiselect("Columns", col_names, default=col_names)
        st.subheader(f"Table: {selected}")
        st.write("Columns:")
        for c in cols_meta:
            st.write(
                f"- **{c['column_name']}** — {c['data_type']} — nullable: {c['is_nullable']}"
            )
        limit = st.number_input(
            "Rows to load", min_value=1, max_value=1000, value=100, step=1
        )
        rows = fetch_table_rows(selected, limit, schema)
        if not rows:
            st.info("No rows found.")
        else:
            df = pd.DataFrame(rows)
            display_df = df[chosen_cols] if chosen_cols else df
            st.dataframe(display_df, use_container_width=True)
            for i, row in df.iterrows():
                with st.expander(f"Edit Row {i}"):
                    inputs = {}
                    for col in df.columns:
                        dtype = next((c["data_type"] for c in cols_meta if c["column_name"] == col), "")
                        val = row[col]
                        init = "" if pd.isna(val) else val
                        key = f"{i}_{col}"
                        if "char" in dtype or "text" in dtype:
                            inputs[col] = st.text_input(col, value=str(init), key=key)
                        elif "int" in dtype:
                            try:
                                inputs[col] = st.number_input(
                                    col, value=int(init) if init != "" else 0, step=1, format="%d", key=key,
                                )
                            except Exception:
                                inputs[col] = st.text_input(col, value=str(init), key=key)
                        elif "bool" in dtype:
                            inputs[col] = st.checkbox(col, value=bool(init), key=key)
                        else:
                            inputs[col] = st.text_input(col, value=str(init), key=key)
                    col_save, col_del = st.columns(2)
                    if col_save.button("💾 Save", key=f"save_{i}"):
                        to_save = {k: (None if isinstance(v, str) and v == "" else v) for k, v in inputs.items()}
                        try:
                            upsert_row(selected, to_save, pk_cols, schema)
                            st.success("✅ Saved successfully!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Save error: {e}")
                    if col_del.button("🗑️ Delete", key=f"del_{i}"):
                        if not pk_cols:
                            st.error("❌ Cannot delete: table has no primary key.")
                        else:
                            pk_vals = [row[pk] for pk in pk_cols]
                            try:
                                delete_row(selected, pk_cols, pk_vals, schema)
                                st.success("✅ Deleted successfully!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Delete error: {e}")
            st.markdown("---")
            st.subheader("➕ Insert New Row")
            new_inputs = {}
            for c in cols_meta:
                cname = c["column_name"]
                dtype = c["data_type"]
                key = f"ins_{cname}"
                if "char" in dtype or "text" in dtype:
                    new_inputs[cname] = st.text_input(cname, key=key)
                elif "int" in dtype:
                    new_inputs[cname] = st.number_input(cname, step=1, format="%d", key=key)
                elif "bool" in dtype:
                    new_inputs[cname] = st.checkbox(cname, key=key)
                else:
                    new_inputs[cname] = st.text_input(cname, key=key)
            if st.button("➕ Insert Row"):
                to_insert = {k: (None if isinstance(v, str) and v == "" else v) for k, v in new_inputs.items()}
                try:
                    upsert_row(selected, to_insert, pk_cols=[], schema=schema)
                    st.success("✅ Row inserted successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Insert error: {e}")

st.markdown("---")
st.markdown("📱 **Live Cricket Dashboard** - Real-time updates | Built with Streamlit & CricBuzz API")
