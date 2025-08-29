# storage_backend.py — backend CSV / Google Sheets avec ETag & verrou optimiste assoupli
from __future__ import annotations
from typing import Dict, Optional
from pathlib import Path
import hashlib
import pandas as pd
import streamlit as st

try:
    from gspread_dataframe import set_with_dataframe as _set_with_dataframe, get_as_dataframe as _get_as_dataframe
except Exception:
    _set_with_dataframe = None
    _get_as_dataframe = None

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
        "params": "Param",
    }.get(name, "ID")

def compute_etag(df: pd.DataFrame, name: str) -> str:
    """ETag stable basé sur colonnes d'identité + Updated_At si présente."""
    if df is None or df.empty:
        return "empty"
    idc = _id_col_for(name)
    cols = [c for c in [idc, "Updated_At"] if c in df.columns]
    try:
        payload = df[cols].astype(str).fillna("").sort_values(by=cols).to_csv(index=False)
    except Exception:
        payload = df.astype(str).fillna("").to_csv(index=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def _lock_enabled() -> bool:
    # secret "optimistic_lock" = off/0/false pour désactiver le verrou globalement
    val = str(st.secrets.get("optimistic_lock", "on")).lower()
    if val in ("0","off","false","no"):
        return False
    # flag de session pour désactiver ponctuellement
    if st.session_state.get("_lock_disable", False):
        return False
    return True

def ensure_df_source(name: str, cols: list, paths: Dict[str, Path] = None, ws_func=None) -> pd.DataFrame:
    """Charge une table depuis CSV ou Google Sheets et prépare les colonnes."""
    full_cols = list(dict.fromkeys(cols + [c for c in AUDIT_COLS if c not in cols]))
    backend = st.secrets.get("storage_backend", "csv")

    if backend == "gsheets":
        if ws_func is None:
            raise RuntimeError("ws_func requis pour backend gsheets")
        tab = SHEET_NAME.get(name, name)
        ws = ws_func(tab)
        if _get_as_dataframe is None:
            raise RuntimeError("gspread indisponible")
        df = _get_as_dataframe(ws, evaluate_formulas=True, header=0)
        if df is None or df.empty:
            df = pd.DataFrame(columns=full_cols)
            if _set_with_dataframe:
                _set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
        else:
            df = df.fillna("")
            for c in full_cols:
                if c not in df.columns:
                    df[c] = ""
            df = df[full_cols]
        st.session_state[f"etag_{name}"] = compute_etag(df, name)
        return df

    # CSV fallback
    if paths is None or name not in paths:
        raise RuntimeError("PATHS manquant pour CSV backend")
    path = paths[name]
    if not path.exists():
        df = pd.DataFrame(columns=full_cols)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False, encoding="utf-8")
    else:
        try:
            df = pd.read_csv(path, dtype=str).fillna("")
        except Exception:
            df = pd.DataFrame(columns=full_cols)
    for c in full_cols:
        if c not in df.columns:
            df[c] = ""
    df = df[full_cols]
    st.session_state[f"etag_{name}"] = compute_etag(df, name)
    return df

def save_df_target(name: str, df: pd.DataFrame, paths: Optional[Dict[str, Path]] = None, ws_func=None, override: bool=False):
    """Sauvegarde avec verrou optimiste (désactivable) et mode 'override'."""
    backend = st.secrets.get("storage_backend", "csv")

    if backend == "gsheets":
        if ws_func is None:
            raise RuntimeError("ws_func requis pour backend gsheets")
        tab = SHEET_NAME.get(name, name)
        ws = ws_func(tab)
        if _get_as_dataframe is None or _set_with_dataframe is None:
            raise RuntimeError("gspread indisponible")
        df_remote = _get_as_dataframe(ws, evaluate_formulas=True, header=0)
        if df_remote is None:
            df_remote = pd.DataFrame(columns=df.columns)
        expected = st.session_state.get(f"etag_{name}")
        current = compute_etag(df_remote, name)
        if _lock_enabled() and (not override) and expected and expected != current:
            st.error(f"Conflit de modification détecté sur '{tab}'. Veuillez recharger la page ou cocher 'Forcer la sauvegarde'.")
            st.stop()
        _set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
        st.session_state[f"etag_{name}"] = compute_etag(df, name)
        return

    # CSV
    if paths is None or name not in paths:
        raise RuntimeError("PATHS manquant pour CSV backend")
    path = paths[name]
    try:
        cur = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        cur = pd.DataFrame(columns=df.columns)
    expected = st.session_state.get(f"etag_{name}")
    current = compute_etag(cur, name)
    if _lock_enabled() and (not override) and expected and expected != current:
        st.error(f"Conflit de modification détecté sur '{name}'. Veuillez recharger la page ou cocher 'Forcer la sauvegarde'.")
        st.stop()
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
    st.session_state[f"etag_{name}"] = compute_etag(df, name)
