# _shared.py — utilitaires communs (filtres, pagination, statusbar, chargements, exports)
from __future__ import annotations
import io
import re
from datetime import date, datetime
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
    backend_eff = st.session_state.get("BACKEND_EFFECTIVE", st.secrets.get("storage_backend","csv")).strip().lower()
    ws = _ws_func() if backend_eff == "gsheets" else None

    def _norm(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        # Évite l'ambiguïté pandas: 'DataFrame is not truthy'
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            df = pd.DataFrame(columns=cols)
        else:
            df = df.copy()
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
    backend_eff = st.session_state.get("BACKEND_EFFECTIVE", st.secrets.get("storage_backend","csv")).strip().lower()
    ws = _ws_func() if backend_eff == "gsheets" else None
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

# === _shared.py : Filtres globaux inter-pages =================================
def _safe_unique(series: pd.Series):
    if series is None or series.empty:
        return []
    vals = series.dropna().astype(str).str.strip()
    vals = vals[vals!=""].unique().tolist()
    vals.sort()
    return vals

def get_global_filters(defaults: dict | None = None) -> dict:
    """Récupère l'état des filtres globaux depuis la session (avec valeurs par défaut)."""
    base = {
        "search": "",
        "year": "Toutes",      # "Toutes" ou int (ex: 2025)
        "month": "Tous",       # "Tous"   ou int (1-12)
        "entreprise_ids": [],  # liste d'ID_Entreprise
        "secteurs": [],
        "pays": [],
        "villes": [],
        "types_contact": [],
        "statuts_contact": [],
        "types_event": [],
        "responsables": [],
    }
    if defaults:
        base.update({k:v for k,v in defaults.items() if k in base})
    st.session_state.setdefault("GLOBAL_FILTERS", base)
    # on recolle avec schéma (au cas où des clés aient évolué)
    gf = {**base, **st.session_state["GLOBAL_FILTERS"]}
    st.session_state["GLOBAL_FILTERS"] = gf
    return gf

def set_global_filters(new_values: dict):
    gf = get_global_filters()
    gf.update({k:v for k,v in new_values.items() if k in gf})
    st.session_state["GLOBAL_FILTERS"] = gf

def render_global_filter_panel(dfs: dict):
    """Rend le panneau de filtres globaux dans la sidebar et met à jour l'état.
       dfs : dictionnaire de DataFrames (load_all_tables()).
    """
    gf = get_global_filters()

    # Collecte des options à partir des tables (tout est caché par @cache_data au-dessus)
    dfc  = dfs.get("contacts", pd.DataFrame())
    dfe  = dfs.get("events", pd.DataFrame())
    dfen = dfs.get("entreprises", pd.DataFrame())
    dfi  = dfs.get("inter", pd.DataFrame())

    # Options – Contacts
    opt_types_c   = _safe_unique(dfc.get("Type", pd.Series(dtype=str)))
    opt_statuts_c = _safe_unique(dfc.get("Statut", pd.Series(dtype=str)))
    opt_resp      = _safe_unique(dfi.get("Responsable", pd.Series(dtype=str))) if not dfi.empty else []

    # Options – Entreprises
    opt_ent_ids   = _safe_unique(dfen.get("ID_Entreprise", pd.Series(dtype=str)))
    opt_secteurs  = _safe_unique(dfen.get("Secteur", pd.Series(dtype=str)))
    opt_pays      = _safe_unique(dfen.get("Pays", pd.Series(dtype=str)))
    opt_villes    = _safe_unique(dfen.get("Ville", pd.Series(dtype=str)))

    # Options – Evénements
    opt_types_e   = _safe_unique(dfe.get("Type", pd.Series(dtype=str)))

    with st.sidebar.expander("🔎 Filtre global", expanded=True):
        gf["search"] = st.text_input("Recherche globale", value=gf.get("search",""))
        col_y, col_m = st.columns(2)
        with col_y:
            years = ["Toutes"]
            # années disponibles à partir de diverses dates
            all_dates = []
            for s in [
                dfc.get("Date_Creation", pd.Series(dtype=str)),
                dfe.get("Date", pd.Series(dtype=str)),
                dfs.get("pay", pd.DataFrame()).get("Date_Paiement", pd.Series(dtype=str)),
                dfs.get("cert", pd.DataFrame()).get("Date_Obtention", pd.Series(dtype=str)),
                dfs.get("inter", pd.DataFrame()).get("Date", pd.Series(dtype=str)),
            ]:
                if not s.empty:
                    dd = pd.to_datetime(s, errors="coerce")
                    all_dates.append(dd)
            if all_dates:
                years_avail = pd.concat(all_dates).dt.year.dropna().astype(int).unique().tolist()
                years_avail.sort(reverse=True)
                years += years_avail
            gf["year"] = st.selectbox("Année", options=years, index=(years.index(gf.get("year")) if gf.get("year") in years else 0))
        with col_m:
            months = ["Tous"] + list(range(1,13))
            try:
                idx = months.index(gf.get("month"))
            except Exception:
                idx = 0
            gf["month"] = st.selectbox("Mois", options=months, index=idx)

        st.markdown("**Contacts**")
        gf["types_contact"]   = st.multiselect("Type",   options=opt_types_c,   default=[x for x in gf.get("types_contact",[]) if x in opt_types_c])
        gf["statuts_contact"] = st.multiselect("Statut", options=opt_statuts_c, default=[x for x in gf.get("statuts_contact",[]) if x in opt_statuts_c])

        st.markdown("**Entreprises**")
        gf["entreprise_ids"]  = st.multiselect("ID Entreprise", options=opt_ent_ids, default=[x for x in gf.get("entreprise_ids",[]) if x in opt_ent_ids])
        col_s, col_pv = st.columns(2)
        with col_s:
            gf["secteurs"]      = st.multiselect("Secteurs", options=opt_secteurs, default=[x for x in gf.get("secteurs",[]) if x in opt_secteurs])
        with col_pv:
            gf["pays"]          = st.multiselect("Pays", options=opt_pays, default=[x for x in gf.get("pays",[]) if x in opt_pays])
            gf["villes"]        = st.multiselect("Villes", options=opt_villes, default=[x for x in gf.get("villes",[]) if x in opt_villes])

        st.markdown("**Événements & Interactions**")
        gf["types_event"]     = st.multiselect("Type d'événement", options=opt_types_e, default=[x for x in gf.get("types_event",[]) if x in opt_types_e])
        gf["responsables"]    = st.multiselect("Responsable (interactions)", options=opt_resp, default=[x for x in gf.get("responsables",[]) if x in opt_resp])

        if st.button("↩ Réinitialiser", use_container_width=True):
            gf = get_global_filters({})  # restaure les défauts
            st.experimental_rerun()

    # Mettre à jour session
    set_global_filters(gf)

def _match_year_month(dt: pd.Series, year_sel, month_sel):
    if dt is None or dt.empty:
        return pd.Series([True]*0, dtype=bool)
    d = pd.to_datetime(dt, errors="coerce")
    mask = pd.Series([True]*len(d), index=d.index)
    if year_sel != "Toutes":
        mask = mask & (d.dt.year == int(year_sel))
    if month_sel != "Tous":
        mask = mask & (d.dt.month == int(month_sel))
    return mask.fillna(False)

def _contains_any(text_series: pd.Series, needle: str) -> pd.Series:
    if not needle:
        return pd.Series([True]*len(text_series), index=text_series.index) if not text_series.empty else pd.Series([], dtype=bool)
    pattern = re.escape(needle.strip().lower())
    s = text_series.fillna("").astype(str).str.lower()
    # recherche plein-texte naïve : la plus robuste et rapide sur colonnes textuelles
    return s.str.contains(pattern, na=False)

def apply_global_filters(df: pd.DataFrame, domain: str, gf: dict | None = None) -> pd.DataFrame:
    """Applique le filtre global à une table selon son domaine.
       domain in {"contacts","entreprises","events","inter","parts","pay","cert","entreprise_parts"}
    """
    if df is None or df.empty:
        return df
    gf = gf or get_global_filters()
    out = df.copy()

    # --- 1) Recherche globale (plein-texte) ---
    text_cols = [c for c in out.columns if out[c].dtype == object or out[c].dtype == "string"]
    if text_cols:
        mask_text = pd.Series([False]*len(out), index=out.index)
        if gf.get("search", "").strip():
            for c in text_cols:
                mask_text = mask_text | _contains_any(out[c], gf["search"])
        else:
            mask_text = pd.Series([True]*len(out), index=out.index)
        out = out[mask_text]

    # --- 2) Filtres année/mois selon domaine ---
    if domain == "contacts":
        # Date_Creation
        if "Date_Creation" in out.columns:
            m = _match_year_month(out["Date_Creation"], gf.get("year","Toutes"), gf.get("month","Tous"))
            out = out[m]
        # Type / Statut
        if gf.get("types_contact"):   out = out[out.get("Type","").isin(gf["types_contact"])]
        if gf.get("statuts_contact"): out = out[out.get("Statut","").isin(gf["statuts_contact"])]
        # Filtre d’appartenance à une entreprise (si demandé)
        if gf.get("entreprise_ids") and "ID_Entreprise" in out.columns:
            out = out[out["ID_Entreprise"].astype(str).isin(gf["entreprise_ids"])]

    elif domain == "entreprises":
        # Filtre sectoriel / géographique
        if gf.get("secteurs") and "Secteur" in out.columns:
            out = out[out["Secteur"].isin(gf["secteurs"])]
        if gf.get("pays") and "Pays" in out.columns:
            out = out[out["Pays"].isin(gf["pays"])]
        if gf.get("villes") and "Ville" in out.columns:
            out = out[out["Ville"].isin(gf["villes"])]

    elif domain == "events":
        # Date
        if "Date" in out.columns:
            m = _match_year_month(out["Date"], gf.get("year","Toutes"), gf.get("month","Tous"))
            out = out[m]
        if gf.get("types_event") and "Type" in out.columns:
            out = out[out["Type"].isin(gf["types_event"])]

    elif domain == "inter":
        # Date + Responsable
        if "Date" in out.columns:
            m = _match_year_month(out["Date"], gf.get("year","Toutes"), gf.get("month","Tous"))
            out = out[m]
        if gf.get("responsables") and "Responsable" in out.columns:
            out = out[out["Responsable"].isin(gf["responsables"])]

    elif domain == "parts":
        # Participations — si on veut filtrer par date d'événement, il faut la joindre côté page
        # Ici on peut filtrer par entreprise/contact via filtres si dispo
        pass

    elif domain == "pay":
        if "Date_Paiement" in out.columns:
            m = _match_year_month(out["Date_Paiement"], gf.get("year","Toutes"), gf.get("month","Tous"))
            out = out[m]

    elif domain == "cert":
        # On prend Date_Obtention (ou Date_Examen si présente)
        d1 = out.get("Date_Obtention")
        d2 = out.get("Date_Examen")
        if d1 is not None or d2 is not None:
            m1 = _match_year_month(d1, gf.get("year","Toutes"), gf.get("month","Tous")) if d1 is not None else None
            m2 = _match_year_month(d2, gf.get("year","Toutes"), gf.get("month","Tous")) if d2 is not None else None
            if m1 is not None and m2 is not None:
                m = (m1 | m2).fillna(False)
            else:
                m = m1 if m1 is not None else m2
            out = out[m]

    elif domain == "entreprise_parts":
        # Participations officielles d'entreprise — pas de date native (selon votre modèle),
        # mais on pourrait filtrer par entreprise si gf["entreprise_ids"] est rempli
        if gf.get("entreprise_ids") and "ID_Entreprise" in out.columns:
            out = out[out["ID_Entreprise"].astype(str).isin(gf["entreprise_ids"])]

    # Rien d’autre : retour du DataFrame filtré
    return out