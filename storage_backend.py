
from typing import Dict, List, Mapping, Optional
from datetime import datetime
import hashlib
import pandas as pd
from pathlib import Path
import streamlit as st

# Optional gsheets deps are imported in app when backend == gsheets
try:
    import gspread  # type: ignore
    from gspread_dataframe import set_with_dataframe, get_as_dataframe  # type: ignore
except Exception:  # pragma: no cover
    gspread = None  # type: ignore
    def set_with_dataframe(*args, **kwargs):  # type: ignore
        raise RuntimeError("gspread indisponible")
    def get_as_dataframe(*args, **kwargs):  # type: ignore
        raise RuntimeError("gspread indisponible")


AUDIT_COLS = ["Created_At", "Created_By", "Updated_At", "Updated_By"]

SHEET_NAME = {
    "contacts": "contacts",
    "inter": "interactions",
    "events": "evenements",
    "parts": "participations",
    "pay": "paiements",
    "cert": "certifications",
    "entreprises": "entreprises",
    "params": "parametres",
    "users": "users",
}

def _id_col_for(name: str) -> str:
    return {
        "contacts": "ID",
        "inter": "ID_Interaction",
        "events": "ID_Événement",
        "parts": "ID_Participation",
        "pay": "ID_Paiement",
        "cert": "ID_Certif",
        "entreprises": "ID_Entreprise",
        "users": "user_id",
    }.get(name, "ID")


def _compute_etag(df: pd.DataFrame, name: str) -> str:
    if df is None or df.empty:
        return "empty"
    idc = _id_col_for(name)
    cols = [c for c in [idc, "Updated_At"] if c in df.columns]
    try:
        payload = df[cols].astype(str).fillna("").sort_values(by=cols).to_csv(index=False)
    except Exception:
        payload = df.astype(str).fillna("").to_csv(index=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ensure_df_source(name: str, cols: list, paths: dict = None, ws_func=None) -> pd.DataFrame:
    full_cols = cols + [c for c in AUDIT_COLS if c not in cols]
    backend = st.secrets.get("storage_backend", "csv")
    st.session_state.setdefault(f"etag_{name}", "empty")

    if backend == "gsheets":
        if ws_func is None:
            raise RuntimeError("ws_func requis pour backend gsheets")
        tab = SHEET_NAME.get(name, name)
        ws = ws_func(tab)
        df = _get_as_dataframe(ws, evaluate_formulas=True, header=0)
        if df is None or df.empty:
            df = pd.DataFrame(columns=full_cols)
            _set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
        else:
            for c in full_cols:
                if c not in df.columns:
                    df[c] = ""
            df = df[full_cols]
        st.session_state[f"etag_{name}"] = _compute_etag(df, name)
        return df

    # CSV fallback
    if paths is None or name not in paths:
        raise RuntimeError("PATHS manquant pour CSV backend")
    path = paths[name]
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
    st.session_state[f"etag_{name}"] = _compute_etag(df, name)
    return df

    # CSV fallback
    if paths is None or name not in paths:
        raise RuntimeError("PATHS manquant pour CSV backend")
    path = paths[name]
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
    st.session_state[f"etag_{name}"] = _compute_etag(df, name)
    return df


def save_df_target(name: str, df: pd.DataFrame, paths: Optional[Dict[str, Path]] = None, ws_func=None):
    backend = st.secrets.get("storage_backend", "csv")

    if backend == "gsheets":
        if ws_func is None:
            raise RuntimeError("ws_func requis pour backend gsheets")
        tab = SHEET_NAME.get(name, name)
        ws = ws_func(tab)
        # optimistic locking
        df_remote = get_as_dataframe(ws, evaluate_formulas=True, header=0)
        if df_remote is None:
            df_remote = pd.DataFrame(columns=df.columns)
        expected = st.session_state.get(f"etag_{name}")
        current = _compute_etag(df_remote, name)
        if expected and expected != current:
            st.error(f"Conflit de modification détecté sur '{tab}'. Veuillez recharger la page.")
            st.stop()
        set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
        st.session_state[f"etag_{name}"] = _compute_etag(df, name)
        return

    # CSV fallback
    if paths is None or name not in paths:
        raise RuntimeError("PATHS manquant pour CSV backend")
    path = paths[name]
    # optimistic lock
    try:
        cur = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        cur = pd.DataFrame(columns=df.columns)
    expected = st.session_state.get(f"etag_{name}")
    current = _compute_etag(cur, name)
    if expected and expected != current:
        st.error(f"Conflit de modification détecté sur '{name}'. Veuillez recharger la page.")
        st.stop()
    df.to_csv(path, index=False, encoding="utf-8")
    st.session_state[f"etag_{name}"] = _compute_etag(df, name)
