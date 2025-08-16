import sqlite3
import pandas as pd
import itertools
from analysis import (
    detect_1nf_violations,
    detect_2nf_violations,
    detect_3nf_violations,
)
from db_connection import get_primary_keys, get_table_data, get_table_schema

def fix_1nf(conn: sqlite3.Connection, table_name: str) -> None:
    df = get_table_data(conn, table_name)
    violations = detect_1nf_violations(df)
    if not violations:
        return  

    schema = get_table_schema(conn, table_name)
    pks = get_primary_keys(conn, table_name)
    if not pks:
        raise RuntimeError(f"No primary key defined on {table_name}")

    for col in violations:
        child = f"{table_name}_{col}_1nf"
        conn.execute(f"DROP TABLE IF EXISTS {child}")

        parts = []
        for cid, name, dtype, notnull, default, pkflag in schema:
            if name in pks:
                nn = " NOT NULL" if notnull else ""
                parts.append(f"{name} {dtype}{nn}")
        orig_type = next(typ for (_,n,typ,_,_,_) in schema if n == col)
        parts.append(f"{col} {orig_type}")
        conn.execute(f"CREATE TABLE {child} ({', '.join(parts)})")

        for _, row in df.iterrows():
            pk_vals = [row[k] for k in pks]
            for piece in map(str.strip, str(row[col]).split(",")):
                conn.execute(
                    f"INSERT INTO {child} VALUES ({', '.join('?' for _ in pks)}, ?)",
                    (*pk_vals, piece),
                )

        tmp = f"{table_name}__old"
        conn.execute(f"ALTER TABLE {table_name} RENAME TO {tmp}")

        keep = [c for c in df.columns if c != col]
        defs = []
        for cid, name, dtype, notnull, default, pkflag in schema:
            if name in keep:
                nn = " NOT NULL" if notnull else ""
                dfault = f" DEFAULT {default}" if default is not None else ""
                defs.append(f"{name} {dtype}{nn}{dfault}")
        pk_clause = f", PRIMARY KEY ({', '.join(pks)})"
        conn.execute(f"CREATE TABLE {table_name} ({', '.join(defs)}{pk_clause})")

        placeholders = ", ".join("?" for _ in keep)
        for _, row in df[keep].iterrows():
            conn.execute(
                f"INSERT INTO {table_name} VALUES ({placeholders})",
                tuple(row[c] for c in keep),
            )
        conn.execute(f"DROP TABLE {tmp}")
    conn.commit()



def fix_2nf(conn: sqlite3.Connection, table_name: str, primary_keys: list) -> None:
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    violations = detect_2nf_violations(df, primary_keys)
    if not violations:
        return

    for dep in violations:
        lhs, rhs = map(str.strip, dep.split("->"))
        cols = [c.strip() for c in lhs.strip("()").split(",")]
        child = f"{table_name}_{rhs}_2nf"

        conn.execute(f"DROP TABLE IF EXISTS {child}")
        lhs_defs = ", ".join(f"{c} TEXT" for c in cols)
        conn.execute(f"CREATE TABLE {child} ({lhs_defs}, {rhs} TEXT, PRIMARY KEY ({', '.join(cols)}))")

        grouped = df[cols + [rhs]].drop_duplicates()
        for _, row in grouped.iterrows():
            conn.execute(
                f"INSERT INTO {child} VALUES ({', '.join('?' for _ in row)})",
                tuple(row),
            )
            
        tmp = f"{table_name}__old"
        conn.execute(f"ALTER TABLE {table_name} RENAME TO {tmp}")
        keep = [c for c in df.columns if c != rhs]
        defs = ", ".join(f"{c} TEXT" for c in keep)
        conn.execute(f"CREATE TABLE {table_name} ({defs})")
        for _, row in df[keep].iterrows():
            conn.execute(
                f"INSERT INTO {table_name} VALUES ({', '.join('?' for _ in keep)})",
                tuple(row),
            )
        conn.execute(f"DROP TABLE {tmp}")
    conn.commit()


def fix_3nf(conn: sqlite3.Connection, table_name: str, primary_keys: list) -> None:
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    violations = detect_3nf_violations(df, primary_keys)
    if not violations:
        return

    for dep in violations:
        X, Y = map(str.strip, dep.split("->"))
        child = f"{table_name}_{Y}_3nf"

        conn.execute(f"DROP TABLE IF EXISTS {child}")
        conn.execute(f"CREATE TABLE {child} ({X} TEXT PRIMARY KEY, {Y} TEXT)")

        grouped = df[[X, Y]].drop_duplicates()
        for _, row in grouped.iterrows():
            conn.execute(f"INSERT INTO {child} VALUES (?, ?)", (row[X], row[Y]))

        tmp = f"{table_name}__old"
        conn.execute(f"ALTER TABLE {table_name} RENAME TO {tmp}")
        keep = [c for c in df.columns if c != Y]
        defs = ", ".join(f"{c} TEXT" for c in keep)
        conn.execute(f"CREATE TABLE {table_name} ({defs})")
        
        for _, row in df[keep].iterrows():
            conn.execute(
                f"INSERT INTO {table_name} VALUES ({', '.join('?' for _ in keep)})",
                tuple(row),
            )
        conn.execute(f"DROP TABLE {tmp}")
    conn.commit()
