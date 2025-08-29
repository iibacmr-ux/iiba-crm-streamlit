
# storage_backend.py — DataFrames <-> CSV/Google Sheets avec cache anti-429
from __future__ import annotations
from pathlib import Path
import hashlib
import pandas as pd
import streamlit as st

try:
    import gspread
    from gspread_dataframe import set_with_dataframe, get_as_dataframe
except Exception:  # pragma: no cover
    gspread = None
    def set_with_dataframe(*a, **k): raise RuntimeError("gspread indisponible")
    def get_as_dataframe(*a, **k): raise RuntimeError("gspread indisponible")

AUDIT_COLS = ["Created_At","Created_By","Updated_At","Updated_By"]
SHEET_NAME = {
    "contacts":"contacts","inter":"interactions","events":"evenements","parts":"participations",
    "pay":"paiements","cert":"certifications","entreprises":"entreprises","params":"parametres",
    "users":"users","orgparts":"entreprise_participations",
}

def compute_etag(df: pd.DataFrame, name: str) -> str:
    if df is None or df.empty:
        return "empty"
    payload = df.astype(str).fillna("").to_csv(index=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def _dfcache_key(tab: str) -> str:
    return f"__DF_CACHE__::{tab}"

def ensure_df_source(name: str, cols: list, paths: dict = None, ws_func=None) -> pd.DataFrame:
    full_cols = list(dict.fromkeys(cols + AUDIT_COLS))
    backend = st.secrets.get("storage_backend", "csv")
    # ---- Google Sheets
    if backend == "gsheets":
        if ws_func is None:
            raise RuntimeError("ws_func requis pour backend gsheets")
        tab = SHEET_NAME.get(name, name)
        # cache DF local pour limiter les lectures répétées
        cache_key = _dfcache_key(tab)
        if cache_key in st.session_state:
            df = st.session_state[cache_key].copy()
        else:
            wsh = ws_func(tab)   # ws(...) est lui-même mis en cache (gs_client)
            try:
                df = get_as_dataframe(wsh, evaluate_formulas=True, header=0)
            except Exception as e:
                raise
            if df is None or df.empty:
                df = pd.DataFrame(columns=full_cols)
                set_with_dataframe(wsh, df, include_index=False, include_column_header=True, resize=True)
            # normalisation colonnes
            for c in full_cols:
                if c not in df.columns:
                    df[c] = ""
            df = df[full_cols].astype(str).fillna("")
            st.session_state[cache_key] = df.copy()
        st.session_state[f"etag_{name}"] = compute_etag(df, name)
        return df

    # ---- CSV (fallback)
    if paths is None or name not in paths:
        raise RuntimeError("PATHS manquant pour CSV backend")
    path = paths[name]
    path.parent.mkdir(exist_ok=True)
    if not path.exists():
        df = pd.DataFrame(columns=full_cols)
        df.to_csv(path, index=False, encoding="utf-8")
    else:
        try:
            df = pd.read_csv(path, dtype=str).fillna("")
        except Exception:
            df = pd.DataFrame(columns=full_cols)
    for c in full_cols:
        if c not in df.columns: df[c] = ""
    df = df[full_cols]
    st.session_state[f"etag_{name}"] = compute_etag(df, name)
    return df

def save_df_target(name: str, df: pd.DataFrame, paths: dict = None, ws_func=None):
    backend = st.secrets.get("storage_backend", "csv")
    # ---- Google Sheets
    if backend == "gsheets":
        if ws_func is None:
            raise RuntimeError("ws_func requis pour backend gsheets")
        tab = SHEET_NAME.get(name, name)
        wsh = ws_func(tab)
        # optimistic lock simple basé sur cache local
        cache_key = _dfcache_key(tab)
        df_remote = None
        if cache_key in st.session_state:
            df_remote = st.session_state[cache_key]
        else:
            try:
                df_remote = get_as_dataframe(wsh, evaluate_formulas=True, header=0)
            except Exception:
                df_remote = None
        if df_remote is None:
            df_remote = pd.DataFrame(columns=df.columns)
        expected = st.session_state.get(f"etag_{name}")
        current = compute_etag(df_remote, name)
        if expected and expected != current:
            st.error(f"Conflit de modification détecté sur '{tab}'. Veuillez recharger la page.")
            st.stop()
        set_with_dataframe(wsh, df, include_index=False, include_column_header=True, resize=True)
        st.session_state[cache_key] = df.copy()
        st.session_state[f"etag_{name}"] = compute_etag(df, name)
        return

    # ---- CSV (fallback)
    if paths is None or name not in paths:
        raise RuntimeError("PATHS manquant pour CSV backend")
    path = paths[name]
    path.parent.mkdir(exist_ok=True)
    # optimistic lock
    try:
        cur = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        cur = pd.DataFrame(columns=df.columns)
    expected = st.session_state.get(f"etag_{name}")
    current = compute_etag(cur, name)
    if expected and expected != current:
        st.error(f"Conflit de modification détecté sur '{name}'. Veuillez recharger la page.")
        st.stop()
    df.to_csv(path, index=False, encoding="utf-8")
    st.session_state[f"etag_{name}"] = compute_etag(df, name)
