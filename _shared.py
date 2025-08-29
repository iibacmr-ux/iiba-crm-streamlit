
# _shared.py — Helpers communs (cache TTL + filtres globaux + fallback CSV)
# ----------------------------------------------------------------------------
# Objectifs
# - Réduire les erreurs 429 côté Google Sheets en privilégiant la lecture
#   cache-only pour l'affichage (TTL configuré), et en réservant les lectures
#   "fraîches" aux opérations d'écriture via ensure_df_source() dans les pages.
# - Centraliser le filtre global (année / mois) et son panneau UI.
# - Fournir utilitaires génériques (generate_id, to_int_safe, etc.).
# ----------------------------------------------------------------------------

from __future__ import annotations
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st

# ==== Backend helpers (importés si dispo, sinon fallback CSV) =================
try:
    from storage_backend import ensure_df_source, save_df_target  # noqa: F401
except Exception:
    def ensure_df_source(name: str, cols: list, paths: Dict[str, Path]=None, ws_func=None) -> pd.DataFrame:  # type: ignore
        p = (paths or PATHS)[name]
        p = Path(p)
        if p.exists():
            try:
                df = pd.read_csv(p, dtype=str).fillna("")
            except Exception:
                df = pd.DataFrame(columns=cols)
        else:
            df = pd.DataFrame(columns=cols)
        for c in cols:
            if c not in df.columns:
                df[c] = ""
        return df[cols]

# ==== Répertoire local CSV (fallback / dev local) ============================
DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ==== Mapping des chemins CSV par table ======================================
PATHS: Dict[str, Path] = {
    "contacts": DATA_DIR / "contacts.csv",
    "interactions": DATA_DIR / "interactions.csv",
    "evenements": DATA_DIR / "evenements.csv",
    "participations": DATA_DIR / "participations.csv",
    "paiements": DATA_DIR / "paiements.csv",
    "certifications": DATA_DIR / "certifications.csv",
    "entreprises": DATA_DIR / "entreprises.csv",
    "parametres": DATA_DIR / "parametres.csv",
    "users": DATA_DIR / "users.csv",
    "entreprise_participations": DATA_DIR / "entreprise_participations.csv",
}

# ==== Schémas de colonnes minimaux (ajustez au besoin) =======================
C_COLS = ["ID","Nom","Prénom","Genre","Titre","Société","Secteur","Email","Téléphone",
          "LinkedIn","Ville","Pays","Type","Source","Statut","Score_Engagement","Notes",
          "Top20","Date_Creation","Created_At","Created_By","Updated_At","Updated_By"]

I_COLS = ["ID_Interaction","ID","Date","Canal","Objet","Résumé","Résultat",
          "Prochaine_Action","Relance","Responsable","Created_At","Created_By","Updated_At","Updated_By"]

E_COLS = ["ID_Événement","Nom_Événement","Type","Date","Lieu","Cout_Salle","Cout_Formateur","Cout_Logistique",
          "Cout_Pub","Cout_Autres","Cout_Total","Created_At","Created_By","Updated_At","Updated_By"]

PART_COLS = ["ID_Participation","ID","ID_Événement","Rôle","Feedback","Note","Commentaire",
             "Created_At","Created_By","Updated_At","Updated_By"]

PAY_COLS = ["ID_Paiement","ID","ID_Événement","Date_Paiement","Montant","Moyen","Statut",
            "Référence","Commentaire","Created_At","Created_By","Updated_At","Updated_By"]

CERT_COLS = ["ID_Certif","ID","Type_Certif","Date_Examen","Résultat","Score","Date_Obtention","Commentaire",
             "Created_At","Created_By","Updated_At","Updated_By"]

ENT_COLS = ["ID_Entreprise","Nom_Entreprise","Secteur","Adresse","Ville","Pays",
            "Site_Web","Email","Téléphone","Contact_Principal_ID","CA_Annuel","Nb_Employés",
            "Notes","Created_At","Created_By","Updated_At","Updated_By"]

PARAM_COLS = ["clé","valeur"]
U_COLS = ["user_id","email","password_hash","role","is_active","display_name",
          "Created_At","Created_By","Updated_At","Updated_By"]

EP_COLS = ["ID_EntPart","ID_Entreprise","ID_Événement","Type_Lien","Nb_Employés","Sponsoring_FCFA",
           "Commentaire","Created_At","Created_By","Updated_At","Updated_By"]

# ==== Paramètres courants (alimentés depuis parametres) ======================
PARAMS: Dict[str, object] = {}

# ==== Utilitaires ============================================================
def to_int_safe(v, default=0) -> int:
    try:
        if v is None: return default
        s = str(v).strip()
        if s == "": return default
        s = s.replace(" ", "").replace(",", ".")
        return int(float(s))
    except Exception:
        return default

def generate_id(prefix: str, df: pd.DataFrame, col: str) -> str:
    """
    Génère un ID unique de la forme PREFIX00001 à partir de la plus grande
    terminaison numérique trouvée dans df[col].
    """
    try:
        if df is None or df.empty or col not in df.columns:
            return f"{prefix}00001"
        nums = (
            df[col].astype(str)
                  .str.extract(r"(\d+)$")[0]
                  .dropna()
                  .astype(int)
                  .tolist()
        )
        n = max(nums) + 1 if nums else 1
        return f"{prefix}{n:05d}"
    except Exception:
        # fallback si parsing échoue
        return f"{prefix}{(len(df) + 1):05d}"

# ==== Filtre global ==========================================================
_GLOBAL_FILTERS_DEFAULT = {"annee": "Toutes", "mois": "Tous"}

def get_global_filters() -> Dict[str, str]:
    return st.session_state.get("GLOBAL_FILTERS", _GLOBAL_FILTERS_DEFAULT.copy())

def set_global_filters(annee: str, mois: str) -> None:
    st.session_state["GLOBAL_FILTERS"] = {"annee": annee, "mois": mois}

def _years_months_from_dfs(dfs: Dict[str, pd.DataFrame]) -> Tuple[List[str], List[str]]:
    """
    Scanne plusieurs tables pour inférer les années / mois disponibles selon
    les colonnes date usuelles.
    """
    years = set()
    months = set()
    date_cols = ["Date_Creation","Date","Date_Paiement","Date_Obtention","Date_Examen"]
    for df in dfs.values():
        if df is None or df.empty: 
            continue
        for c in date_cols:
            if c in df.columns:
                s = pd.to_datetime(df[c], errors="coerce")
                years.update(s.dt.year.dropna().astype(int).tolist())
                months.update(s.dt.month.dropna().astype(int).tolist())
    ylist = ["Toutes"] + sorted({str(y) for y in years})
    mlist = ["Tous"] + [str(m) for m in range(1,13)] if not months else ["Tous"] + sorted({str(m) for m in months}, key=lambda x:int(x))
    return ylist, mlist

def render_global_filter_panel(dfs: Optional[Dict[str, pd.DataFrame]]=None, location="sidebar") -> Dict[str, str]:
    """
    Rend le panneau de filtre global (Année/Mois). Sauvegarde le choix dans
    st.session_state["GLOBAL_FILTERS"].
    """
    container = st.sidebar if location == "sidebar" else st
    with container.expander("🌍 Filtre global", expanded=True):
        # Pour proposer des valeurs pertinentes, on peut dériver des tables chargées (cache)
        if dfs is None:
            dfs = st.session_state.get("__CACHED_LAST_DFS__", {})
        years, months = _years_months_from_dfs(dfs if isinstance(dfs, dict) else {})
        gf = get_global_filters()
        col1, col2 = st.columns(2)
        with col1:
            annee = st.selectbox("Année", years, index=years.index(gf.get("annee","Toutes")) if gf.get("annee","Toutes") in years else 0, key="__global_year")
        with col2:
            mois = st.selectbox("Mois", months, index=months.index(gf.get("mois","Tous")) if gf.get("mois","Tous") in months else 0, key="__global_month")
        set_global_filters(annee, mois)
        return get_global_filters()

def apply_global_filters(df: pd.DataFrame, table_name: str, gf: Optional[Dict[str, str]]=None) -> pd.DataFrame:
    """
    Applique Année/Mois s'ils sont différents de "Toutes"/"Tous".
    Heuristique de colonne date: on préfère "Date_Creation" si dispo, sinon
    "Date", "Date_Paiement", "Date_Obtention", "Date_Examen".
    """
    if df is None or df.empty:
        return df
    gf = gf or get_global_filters()
    year_sel = gf.get("annee", "Toutes")
    month_sel = gf.get("mois", "Tous")
    if year_sel == "Toutes" and month_sel == "Tous":
        return df
    # Choix de la colonne date
    for c in ["Date_Creation","Date","Date_Paiement","Date_Obtention","Date_Examen"]:
        if c in df.columns:
            s = pd.to_datetime(df[c], errors="coerce")
            mask = pd.Series(True, index=df.index)
            if year_sel != "Toutes":
                mask = mask & (s.dt.year == int(year_sel))
            if month_sel != "Tous":
                mask = mask & (s.dt.month == int(month_sel))
            return df.loc[mask].copy()
    return df

# ==== Cache TTL pour réduire les 429 =========================================
def _cache_key() -> str:
    """
    Construit une clé de cache *hashable* indépendante des objets non sérialisables.
    On ne met PAS les filtres dedans (lecture "globale"), les filtres s'appliquent ensuite.
    """
    backend = str(st.secrets.get("storage_backend","csv"))
    sid = str(st.secrets.get("gsheet_spreadsheet_id",""))
    stitle = str(st.secrets.get("gsheet_spreadsheet",""))
    # On incorpore aussi la présence d'un WS_FUNC (booléen)
    has_ws = "1" if bool(st.session_state.get("WS_FUNC")) else "0"
    return f"{backend}|{sid}|{stitle}|{has_ws}"

@st.cache_data(show_spinner=False, ttl=120)
def _read_all_tables_cached(key: str) -> Dict[str, pd.DataFrame]:
    """
    Lecture groupée des tables, *mise en cache* (TTL=120s). Réduit fortement
    le volume d'appels à l'API Sheets. Les pages devraient utiliser
    load_all_tables(use_cache_only=True) pour l'affichage.
    """
    ws = st.session_state.get("WS_FUNC")
    backend = st.secrets.get("storage_backend","csv")

    def _read(name: str, cols: List[str]) -> pd.DataFrame:
        # Tentative GSheets
        if backend == "gsheets" and ws is not None:
            try:
                return ensure_df_source(name, cols, PATHS, ws).copy()
            except Exception as e:
                st.sidebar.caption(f"Lecture Google Sheets échouée ({name}), fallback CSV: {e}")
        # Fallback CSV
        p = PATHS[name]
        p = Path(p)
        if p.exists():
            try:
                df = pd.read_csv(p, dtype=str).fillna("")
            except Exception:
                df = pd.DataFrame(columns=cols)
        else:
            df = pd.DataFrame(columns=cols)
        for c in cols:
            if c not in df.columns:
                df[c] = ""
        return df[cols]

    dfs = {
        "contacts": _read("contacts", C_COLS),
        "interactions": _read("interactions", I_COLS),
        "evenements": _read("evenements", E_COLS),
        "participations": _read("participations", PART_COLS),
        "paiements": _read("paiements", PAY_COLS),
        "certifications": _read("certifications", CERT_COLS),
        "entreprises": _read("entreprises", ENT_COLS),
        "parametres": _read("parametres", PARAM_COLS),
        "users": _read("users", U_COLS),
        "entreprise_participations": _read("entreprise_participations", EP_COLS),
    }

    # Exposer aux autres fonctions (ex: panneau filtre global)
    st.session_state["__CACHED_LAST_DFS__"] = dfs

    # Construire PARAMS (dict) à partir de df parametres (clé/valeur)
    try:
        dfp = dfs.get("parametres", pd.DataFrame())
        params = {}
        if dfp is not None and not dfp.empty and {"clé","valeur"}.issubset(dfp.columns):
            for _, r in dfp.iterrows():
                k = str(r.get("clé","")).strip()
                v = r.get("valeur","")
                if k:
                    params[k] = v
        # garder dans session + global
        st.session_state["PARAMS"] = params
        global PARAMS
        PARAMS = params
    except Exception:
        pass

    return dfs

def load_all_tables(use_cache_only: bool = False) -> Dict[str, pd.DataFrame]:
    """
    Chargement des tables. Par défaut, renvoie la version *cache* (TTL=120s).
    Utilisez use_cache_only=True pour garantir zéro appel Sheets.
    Les écritures doivent faire leurs relectures ciblées via ensure_df_source().
    """
    key = _cache_key()
    if use_cache_only:
        return _read_all_tables_cached(key)
    # Sinon on s'appuie tout de même sur le cache (TTL court). Pour forcer une
    # relecture totale ici, on pourrait invalider le cache ; on évite pour limiter
    # les 429 et on réserve la relecture "fraîche" aux opérations de sauvegarde.
    return _read_all_tables_cached(key)

# ==== Export util pour d'autres pages ========================================
__all__ = [
    "PATHS",
    "C_COLS","I_COLS","E_COLS","PART_COLS","PAY_COLS","CERT_COLS","ENT_COLS","PARAM_COLS","U_COLS","EP_COLS",
    "PARAMS",
    "to_int_safe","generate_id",
    "get_global_filters","set_global_filters","render_global_filter_panel","apply_global_filters",
    "load_all_tables",
]
