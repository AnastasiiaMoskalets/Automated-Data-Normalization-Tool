import re
import itertools
import pandas as pd

def detect_1nf_violations(df: pd.DataFrame) -> list[str]:
    violations = []
    pattern = re.compile(r"[,;|\n]")
    for col in df.columns:
        if df[col].dtype == object:
            if df[col].dropna().astype(str).str.contains(pattern).any():
                violations.append(col)
    return violations


def detect_3nf_violations(df: pd.DataFrame, primary_keys: list[str]) -> list[str]:
    one_nf = set(detect_1nf_violations(df))
    non_primes = [
        c for c in df.columns
        if c not in primary_keys and c not in one_nf
    ]
    n = len(df)

    raw = []
    for X, Y in itertools.permutations(non_primes, 2):
        if df[X].nunique(dropna=True) == n:
            continue
        sub = df.dropna(subset=[X, Y])
        if sub.groupby(X)[Y].nunique(dropna=True).max() == 1:
            raw.append((X, Y))

    fds = []
    seen = set()
    for X, Y in raw:
        if (Y, X) in seen:
            continue
        seen.add((X, Y))
        fds.append((X, Y))

    minimal = []
    for X, Z in fds:
        implied = any((X, Y) in fds and (Y, Z) in fds
                      for Y in non_primes if Y not in (X, Z))
        if not implied:
            minimal.append((X, Z))
    return [f"{X} -> {Z}" for X, Z in sorted(minimal)]


def detect_2nf_violations(df: pd.DataFrame, primary_keys: list[str]) -> list[str]:
    if len(primary_keys) < 2:
        return []
    one_nf = set(detect_1nf_violations(df))
    three_nf_rhs = {
        rhs.strip()
        for rule in detect_3nf_violations(df, primary_keys)
        for rhs in [rule.split("->")[1]]
    }
    violations = []
    non_primes = [
        c for c in df.columns
        if c not in primary_keys and c not in one_nf and c not in three_nf_rhs
    ]
    for r in range(1, len(primary_keys)):
        for subset in itertools.combinations(primary_keys, r):
            subset = list(subset)
            if not df.duplicated(subset=subset).any():
                continue
            grp = df.dropna(subset=subset + non_primes)
            for attr in non_primes:
                if grp.groupby(subset)[attr].nunique(dropna=True).max() == 1:
                    if len(subset) == 1:
                        lhs = subset[0]
                    else:
                        lhs = "(" + ",".join(subset) + ")"
                    violations.append(f"{lhs} -> {attr}")
    minimal = []
    for rule in set(violations):
        lhs, rhs = map(str.strip, rule.split("->"))
        lhs_cols = lhs.strip("()").split(",") if lhs.startswith("(") else [lhs]
        smaller = [
            r for r in violations
            if r.endswith(f"-> {rhs}") and
               set(r.split("->")[0].strip("()").split(",")) < set(lhs_cols)
        ]
        if not smaller:
            minimal.append(rule)
    return sorted(minimal)
