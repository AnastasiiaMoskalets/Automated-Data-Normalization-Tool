import streamlit as st
from db_connection import connect_db, get_tables, get_table_data
from analysis import detect_1nf_violations, detect_2nf_violations, detect_3nf_violations
from transform import fix_1nf, fix_2nf, fix_3nf

st.set_page_config(layout="wide")
st.title("Automated Data Normalization Tool for SQLite")


db_path = st.text_input("Enter path to your SQLite database:")
if not db_path:
    st.info("Please enter a database path above to connect.")
    st.stop()

conn = connect_db(db_path)
if not conn:
    st.error("Could not connect. Please check the file path.")
    st.stop()

tables = get_tables(conn)
if not tables:
    st.warning("No tables found in this database.")
    st.stop()

if "table_idx" not in st.session_state:
    st.session_state.table_idx = 0

if "selected_table_name" not in st.session_state:
    st.session_state.selected_table_name = tables[0]

col1, col2 = st.columns(2)
with col1:
    if st.button("Refresh Table"):
        # Reload table – no action needed here; new data will be fetched below
        pass
with col2:
    if st.button("Next Table"):
        idx = (tables.index(st.session_state.selected_table_name) + 1) % len(tables)
        st.session_state.selected_table_name = tables[idx]

table_name = st.selectbox(
    "Select a table to analyze:",
    options=tables,
    index=tables.index(st.session_state.selected_table_name),
    key="selected_table_name"
)

# Load data
df = get_table_data(conn, table_name)
cursor = conn.cursor()
cursor.execute(f"PRAGMA table_info({table_name})")
primary_keys = [r[1] for r in cursor.fetchall() if r[5] > 0]

st.subheader(f"Preview: `{table_name}`")
st.dataframe(df)

st.subheader("Detected Normalization Issues")
c1, c2, c3 = st.columns(3)

with c1:
    v1 = detect_1nf_violations(df)
    st.write("**1NF Violations**", v1 or "None")
    if v1 and st.button("Fix 1NF", key=f"fix1_{table_name}"):
        fix_1nf(conn, table_name)
        st.success(f"1NF normalized on `{table_name}`")

with c2:
    v2 = detect_2nf_violations(df, primary_keys)
    st.write("**2NF Violations**", v2 or "None")
    if v2 and st.button("Fix 2NF", key=f"fix2_{table_name}"):
        fix_2nf(conn, table_name, primary_keys)
        st.success(f"2NF normalized on `{table_name}`")

with c3:
    v3 = detect_3nf_violations(df, primary_keys)
    st.write("**3NF Violations**", v3 or "None")
    if v3 and st.button("Fix 3NF", key=f"fix3_{table_name}"):
        fix_3nf(conn, table_name, primary_keys)
        st.success(f"3NF normalized on `{table_name}`")