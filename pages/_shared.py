# _shared.py — utilitaires communs (filtres, pagination, statusbar, chargements, exports)
from __future__ import annotations
import io
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd
import streamlit as st

# ==== Import backends existants ====
try:
    from storage_backend import (
        AUDIT_COLS, SHEET_NAME,
        compute_etag, ensure_df_source, save_df_target
    )
except Exception:
    # Garde-fous si le module n'existe pas (dev local minimal)
    AUDIT_COLS = ["Created_At","Created_By","Updated_At","Updated_By"]
    SHEET_NAME = {
        "contacts":"contacts","inter":"interactions","events":"evenements",
        "parts":"participations","pay":"paiements","cert":"certifications",
        "entreprises":"entreprises","params":"parametres","users":"users",
        "entreprise_parts":"entreprise_participations"
    }
    def compute_etag(df, name):  # pragma: no cover
        try:
            payload = df.astype(str).fillna("").to_csv(index=False)
            import hashlib
            return hashlib.sha256(payload.encode("utf-8")).hexdigest()
        except Exception:
            return "empty"
    def ensure_df_source(name: str, cols: list, paths: dict=None, ws_func=None) -> pd.DataFrame:  # pragma: no cover
        p = (paths or {}).get(name, Path(f"data/{name}.csv"))
        p.parent.mkdir(exist_ok=True, parents=True)
        if not p.exists():
            df = pd.DataFrame(columns=cols + [c for c in AUDIT_COLS if c not in cols])
            df.to_csv(p, index=False, encoding="utf-8")
            return df
        return pd.read_csv(p, dtype=str).fillna("")
    def save_df_target(name: str, df: pd.DataFrame, paths: dict=None, ws_func=None):  # pragma: no cover
        p = (paths or {}).get(name, Path(f"data/{name}.csv"))
        p.parent.mkdir(exist_ok=True, parents=True)
        df.to_csv(p, index=False, encoding="utf-8")

# ==== Schémas colonnes minimaux ====
C_COLS = ["ID","Nom","Prenom","Email","Telephone","Type","Statut","Entreprise","Fonction","Pays","Ville",
          "Top20","Created_At","Created_By","Updated_At","Updated_By","Genre"]
ENT_COLS = ["ID_Entreprise","Nom_Entreprise","Secteur","Contact_Principal_ID","CA_Annuel","Nb_Employes",
            "Pays","Ville","Created_At","Created_By","Updated_At","Updated_By"]
E_COLS = ["ID_Événement","Nom_Événement","Type","Date","Ville","Pays",
          "Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total",
          "Created_At","Created_By","Updated_At","Updated_By"]
PART_COLS = ["ID_Participation","ID","ID_Événement","Rôle","Note","Created_At","Created_By","Updated_At","Updated_By"]
PAY_COLS  = ["ID_Paiement","ID","ID_Événement","Montant","Statut","Date_Paiement","Created_At","Created_By","Updated_At","Updated_By"]
CERT_COLS = ["ID_Certif","ID","Intitulé","Résultat","Date_Obtention","Date_Examen","Created_At","Created_By","Updated_At","Updated_By"]
INTER_COLS = ["ID_Interaction","ID","Canal","Objet","Date","Responsable","Cible","ID_Cible",
              "Created_At","Created_By","Updated_At","Updated_By"]
EPART_COLS = ["ID_EntPart","ID_Entreprise","ID_Événement","Type_Lien","Nb_Employes","Sponsoring_FCFA",
              "Created_At","Created_By","Updated_At","Updated_By"]

# ==== Backend & chemins ====
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True, parents=True)
DEFAULT_PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "entreprises": DATA_DIR / "entreprises.csv",
    "events": DATA_DIR / "evenements.csv",
    "parts": DATA_DIR / "participations.csv",
    "pay": DATA_DIR / "paiements.csv",
    "cert": DATA_DIR / "certifications.csv",
    "inter": DATA_DIR / "interactions.csv",
    "entreprise_parts": DATA_DIR / "entreprise_participations.csv",
    "params": DATA_DIR / "parametres.csv",
    "users": DATA_DIR / "users.csv",
}

def _paths() -> Dict[str, Path]:
    return st.session_state.get("PATHS", DEFAULT_PATHS)

def _ws_func():
    return st.session_state.get("WS_FUNC", None)

# ==== Chargement groupé ====
def load_all_tables() -> Dict[str, pd.DataFrame]:
    paths = _paths()
    ws = _ws_func() if st.secrets.get("storage_backend","csv")=="gsheets" else None

    def _norm(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        df = (df or pd.DataFrame(columns=cols)).copy()
        for c in cols:
            if c not in df.columns:
                df[c] = ""
        ordered = [c for c in cols if c in df.columns] + [c for c in df.columns if c not in cols]
        return df[ordered].fillna("")

    dfs = {}
    dfs["contacts"] = _norm(ensure_df_source("contacts", C_COLS, paths, ws), C_COLS)
    dfs["entreprises"] = _norm(ensure_df_source("entreprises", ENT_COLS, paths, ws), ENT_COLS)
    dfs["events"] = _norm(ensure_df_source("events", E_COLS, paths, ws), E_COLS)
    dfs["parts"] = _norm(ensure_df_source("parts", PART_COLS, paths, ws), PART_COLS)
    dfs["pay"] = _norm(ensure_df_source("pay", PAY_COLS, paths, ws), PAY_COLS)
    dfs["cert"] = _norm(ensure_df_source("cert", CERT_COLS, paths, ws), CERT_COLS)
    _inter = ensure_df_source("inter", INTER_COLS, paths, ws)
    for nc in ["Cible","ID_Cible"]:
        if nc not in _inter.columns:
            _inter[nc] = ""
    dfs["inter"] = _norm(_inter, INTER_COLS)
    dfs["entreprise_parts"] = _norm(ensure_df_source("entreprise_parts", EPART_COLS, paths, ws), EPART_COLS)
    dfs["params"] = ensure_df_source("params", ["key","value"], paths, ws)
    dfs["users"]  = ensure_df_source("users", ["user_id","email","password_hash","role","is_active","display_name",
                                               "Created_At","Created_By","Updated_At","Updated_By"], paths, ws)
    return dfs

def save_table(name: str, df: pd.DataFrame) -> None:
    paths = _paths()
    ws = _ws_func() if st.secrets.get("storage_backend","csv")=="gsheets" else None
    save_df_target(name, df, paths, ws)

# ==== Helpers divers ====
def generate_id(prefix: str, series_like) -> str:
    try:
        existing = pd.Series(series_like).astype(str)
        nums = existing.str.extract(rf"{prefix}(\d+)", expand=False).dropna().astype(int)
        nxt = (nums.max() + 1) if not nums.empty else 1
    except Exception:
        nxt = 1
    return f"{prefix}{nxt:05d}"

def to_int_safe(x, default=0) -> int:
    try:
        if pd.isna(x): return default
        s = str(x).strip().replace(" ", "").replace("\u00a0","")
        return int(float(s)) if s != "" else default
    except Exception:
        return default

def parse_date(x):
    try:
        if pd.isna(x) or str(x).strip()=="": return None
        return pd.to_datetime(x).date()
    except Exception:
        return None

def add_year_month(df: pd.DataFrame, date_col: str, year_col="Année", month_col="Mois") -> pd.DataFrame:
    d = pd.to_datetime(df[date_col], errors="coerce")
    df[year_col] = d.dt.year.astype("Int64")
    df[month_col] = d.dt.month.astype("Int64")
    return df

# ==== Barre d'agrégats + Filtres & Pagination ====
def _sum_numeric(df: pd.DataFrame, cols: List[str]) -> Dict[str, float]:
    out = {}
    for c in cols:
        if c in df.columns:
            out[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).sum()
    return out

def statusbar(df: pd.DataFrame, numeric_keys: List[str] = None, key: str = "statusbar"):
    numeric_keys = numeric_keys or []
    sums = _sum_numeric(df, numeric_keys)
    parts = [f"lignes : **{len(df)}**"]
    for k, v in sums.items():
        parts.append(f"{k} = **{int(v):,}**".replace(",", " "))
    st.caption(" | ".join(parts))

def smart_suggested_filters(df: pd.DataFrame, extra: List[str]=None, max_cols: int = 6) -> List[str]:
    """Propose des colonnes catégorielles pertinentes pour les filtres."""
    extra = extra or []
    candidates = [
        "Type","Statut","Entreprise","Fonction","Secteur","Pays","Ville",
        "Responsable","Canal","Résultat","Rôle","Top20"
    ]
    # Ajouter les extra en priorité
    ordered = extra + [c for c in candidates if c not in extra]
    present = [c for c in ordered if c in df.columns]
    # Exclure colonnes ID et numériques évidentes
    def _is_numeric_series(s: pd.Series) -> bool:
        try:
            return pd.api.types.is_numeric_dtype(pd.to_numeric(s, errors="coerce"))
        except Exception:
            return False
    present = [c for c in present if c.lower() not in {"id","id_événement","id_paiement","id_participation",
                                                       "id_interaction","id_certif","id_entreprise"}]
    present = [c for c in present if not _is_numeric_series(df[c])]
    return present[:max_cols]

def filter_and_paginate(df: pd.DataFrame,
                        key_prefix: str,
                        page_size_default: int = 20,
                        suggested_filters: List[str] = None,
                        enable_sort: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Renvoie (df_page, df_filtered). Dessine UI filtres + pagination."""
    if df is None: df = pd.DataFrame()
    df = df.copy()
    if suggested_filters is None:
        suggested_filters = smart_suggested_filters(df)

    with st.expander("🔎 Filtres avancés", expanded=False):
        # Recherche globale (colonnes texte)
        global_q = st.text_input("Recherche globale (contient)", key=f"{key_prefix}_q").strip()
        if global_q:
            mask = pd.Series(False, index=df.index)
            for c in df.columns:
                if df[c].dtype == object:
                    mask = mask | df[c].astype(str).str.contains(global_q, case=False, na=False)
            df = df[mask]

        # Filtres catégoriels proposés (si présents)
        cols_present = [c for c in suggested_filters if c in df.columns]
        if cols_present:
            cols = st.columns(min(4, len(cols_present)))
            # Répartir les filtres sur 4 colonnes max
            for i, c in enumerate(cols_present):
                col = cols[i % len(cols)]
                vals = sorted([v for v in df[c].dropna().astype(str).unique() if v!=""])
                sel = col.multiselect(c, vals, default=[], key=f"{key_prefix}_f_{c}")
                if sel:
                    df = df[df[c].astype(str).isin(sel)]

        # Tri (optionnel)
        if enable_sort and not df.empty:
            sort_cols = ["(aucun)"] + df.columns.tolist()
            sc = st.selectbox("Tri par", options=sort_cols, index=0, key=f"{key_prefix}_sortcol")
            if sc != "(aucun)":
                asc = st.checkbox("Tri ascendant", value=True, key=f"{key_prefix}_sortasc")
                try:
                    df = df.sort_values(by=sc, ascending=asc, kind="mergesort")
                except Exception:
                    pass

        # Page size
        page_size = st.number_input("Taille de page", min_value=5, max_value=200,
                                    value=page_size_default, step=5, key=f"{key_prefix}_pagesize")

    # Pagination
    total = len(df)
    if total == 0:
        st.info("Aucune donnée à afficher.")
        return df, df  # vide

    import math
    pages = max(1, math.ceil(total / page_size))
    col_p1, col_p2, col_p3 = st.columns([1,2,1])
    with col_p1:
        page_idx = st.number_input("Page", min_value=1, max_value=pages, value=1, step=1, key=f"{key_prefix}_page")
    with col_p2:
        st.caption(f"{total} lignes • {pages} pages • {page_size} par page")
    with col_p3:
        if st.button("⟳ Rafraîchir", key=f"{key_prefix}_refresh"):
            st.experimental_rerun()

    start = (page_idx - 1) * page_size
    end = start + page_size
    df_page = df.iloc[start:end].copy()

    return df_page, df

# ==== Export utilitaire (multi-feuilles) ====
def export_filtered_excel(dfs: Dict[str, pd.DataFrame], filename_prefix: str = "export"):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet, df in dfs.items():
            try:
                df.to_excel(writer, sheet_name=str(sheet)[:31], index=False)
            except Exception:
                pd.DataFrame().to_excel(writer, sheet_name=str(sheet)[:31], index=False)
    st.download_button(
        "⬇ Export Excel (filtres appliqués)",
        data=buf.getvalue(),
        file_name=f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
