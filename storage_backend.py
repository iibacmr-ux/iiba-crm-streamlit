# storage_backend.py — accès unifié CSV / Google Sheets + ETag simple
from __future__ import annotations
import hashlib
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import streamlit as st

# gspread_dataframe (optional)
try:
    from gspread_dataframe import set_with_dataframe as _set_with_dataframe_gs
    from gspread_dataframe import get_as_dataframe as _get_as_dataframe_gs
except Exception:
    _set_with_dataframe_gs = None
    _get_as_dataframe_gs = None

AUDIT_COLS = ["Created_At","Created_By","Updated_At","Updated_By"]
SHEET_NAME = {
    "contacts":"contacts",
    "inter":"interactions",
    "events":"evenements",
    "parts":"participations",
    "pay":"paiements",
    "cert":"certifications",
    "entreprises":"entreprises",
    "params":"parametres",
    "users":"users",
    "entreprise_parts":"entreprise_participations",
}

def _backend_effective() -> str:
    """Lecture prioritaire depuis session (override quand GSheets down), sinon secrets."""
    b = st.session_state.get("BACKEND_EFFECTIVE", "").strip().lower()
    if b:
        return b
    return st.secrets.get("storage_backend","csv").strip().lower()

def compute_etag(df: pd.DataFrame, name: str) -> str:
    try:
        if df is None or df.empty:
            return "empty"
        cols = [c for c in df.columns if c.lower() in {"id","updated_at","id_événement","user_id"}]
        if cols:
            payload = df[cols].astype(str).fillna("").sort_values(by=cols).to_csv(index=False)
        else:
            payload = df.astype(str).fillna("").to_csv(index=False)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    except Exception:
        return "empty"

def _get_as_dataframe(ws, **kwargs) -> pd.DataFrame:
    if _get_as_dataframe_gs is None:
        raise RuntimeError("gspread_dataframe non disponible")
    return _get_as_dataframe_gs(ws, **kwargs)

def _set_with_dataframe(ws, df: pd.DataFrame, **kwargs):
    if _set_with_dataframe_gs is None:
        raise RuntimeError("gspread_dataframe non disponible")
    return _set_with_dataframe_gs(ws, df, **kwargs)

def ensure_df_source(name: str, cols: list, paths: Dict[str, Path] = None, ws_func=None) -> pd.DataFrame:
    """Charge une table depuis Google Sheets si ws_func est fourni et backend effectif=='gsheets',
       sinon depuis CSV. Crée la structure si manquante. Met à jour st.session_state ETag."""
    full_cols = cols + [c for c in AUDIT_COLS if c not in cols]
    backend = _backend_effective()
    st.session_state.setdefault(f"etag_{name}", "empty")

    if backend == "gsheets" and ws_func is not None:
        tab = SHEET_NAME.get(name, name)
        try:
            ws = ws_func(tab)
            df = _get_as_dataframe(ws, evaluate_formulas=True, header=0)
        except Exception as e:
            st.warning(f"Lecture Google Sheets échouée ({tab}), fallback CSV: {e}")
            backend = "csv"  # bascule locale pour cette lecture
        else:
            if df is None or df.empty:
                df = pd.DataFrame(columns=full_cols)
                try:
                    _set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
                except Exception as e:
                    st.warning(f"Init vide '{tab}' non écrit: {e}")
            else:
                for c in full_cols:
                    if c not in df.columns:
                        df[c] = ""
                df = df[full_cols].fillna("")
            st.session_state[f"etag_{name}"] = compute_etag(df, name)
            return df

    # CSV fallback
    paths = paths or {}
    path = paths.get(name, Path(f"data/{name}.csv"))
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        df = pd.DataFrame(columns=full_cols)
        df.to_csv(path, index=False, encoding="utf-8")
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

def save_df_target(name: str, df: pd.DataFrame, paths: Optional[Dict[str, Path]] = None, ws_func=None):
    """Sauvegarde avec verrou optimiste simple via ETag (sur la session)."""
    backend = _backend_effective()
    if backend == "gsheets" and ws_func is not None:
        tab = SHEET_NAME.get(name, name)
        try:
            ws = ws_func(tab)
            try:
                df_remote = _get_as_dataframe(ws, evaluate_formulas=True, header=0)
            except Exception:
                df_remote = pd.DataFrame(columns=df.columns)
            expected = st.session_state.get(f"etag_{name}")
            current = compute_etag(df_remote, name)
            if expected and expected != current:
                st.error(f"Conflit de modification détecté sur '{tab}'. Veuillez recharger la page.")
                st.stop()
            _set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
            st.session_state[f"etag_{name}"] = compute_etag(df, name)
            return
        except Exception as e:
            st.warning(f"Écriture Google Sheets échouée ({tab}), fallback CSV: {e}")

    # CSV fallback
    paths = paths or {}
    path = paths.get(name, Path(f"data/{name}.csv"))
    path.parent.mkdir(parents=True, exist_ok=True)
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
