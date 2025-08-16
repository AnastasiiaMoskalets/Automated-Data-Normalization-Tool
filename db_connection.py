import re
import sqlite3
import pandas as pd

def connect_db(db_path):
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        print(f"Database connection failed: {e}")
        return None

def get_tables(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    return tables

def get_table_schema(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    return cursor.fetchall()

def get_table_data(conn, table_name):
    if not re.match(r'^[A-Za-z0-9_]+$', table_name):
        raise ValueError("Invalid table name")
    return pd.read_sql_query(f"SELECT * FROM \"{table_name}\";", conn)


def get_primary_keys(conn, table_name):
    schema = get_table_schema(conn, table_name)
    return [col[1] for col in schema if col[5] > 0]



