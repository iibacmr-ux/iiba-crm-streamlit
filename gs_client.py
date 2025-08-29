# gs_client.py — gestion des secrets Google, client gspread et diagnostics
from __future__ import annotations
import json
from typing import Dict, Optional, Mapping, Any
import streamlit as st

# compat TOML (string) parsing
try:
    import tomllib  # Python 3.11+
except Exception:  # pragma: no cover
    tomllib = None  # type: ignore

def _mapping_to_dict(m: Mapping[str, Any]) -> Dict[str, Any]:
    """Convertit un objet mapping 'Secrets' de Streamlit en dict plat."""
    return {k: m.get(k) for k in m.keys()} if hasattr(m, "keys") else dict(m)

def read_service_account_secret() -> Dict:
    """Lit st.secrets pour la clé de service.
    Accepte:
      - Table TOML (st.secrets[google_service_account]) -> mapping
      - Chaîne JSON brute -> {...}
      - Chaîne TOML -> key="value" (optionnellement avec section [google_service_account])
    """
    sec = st.secrets.get("google_service_account", None)
    if sec is None:
        raise RuntimeError("Secret 'google_service_account' manquant.")

    # Cas 1: mapping (TOML table)
    if isinstance(sec, Mapping):
        info = _mapping_to_dict(sec)
    else:
        s = str(sec).strip()
        # Retirer triples guillemets éventuels
        if s.startswith('"""') and s.endswith('"""'):
            s = s[3:-3].strip()

        # Essai JSON
        if s.startswith("{"):
            info = json.loads(s)
        else:
            # Essai TOML si dispo
            if tomllib is None:
                raise RuntimeError("Le secret n'est ni JSON ni table TOML; installez 'tomli' ou fournissez du JSON.")
            data = tomllib.loads(s)
            # Si section [google_service_account] présente, l'utiliser
            if isinstance(data, dict) and "google_service_account" in data:
                info = data["google_service_account"]
            else:
                info = data

    # Normaliser private_key
    pk = info.get("private_key", "")
    if isinstance(pk, str) and "\\n" in pk and "-----BEGIN" in pk:
        info["private_key"] = pk.replace("\\n", "\n")
    return info  # dict JSON-compatible

@st.cache_resource(show_spinner=False)
def get_gspread_client(info: Dict):
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def make_ws_func(GC, spreadsheet_id: Optional[str] = None, spreadsheet_title: Optional[str] = None):
    """Retourne une fonction ws(name) qui renvoie la worksheet demandée.
       - Si spreadsheet_id est fourni -> open_by_key
       - Sinon -> open par titre
       La fonction cache le Spreadsheet et les worksheets pour limiter les requêtes.
    """
    import gspread
    from functools import lru_cache

    if not spreadsheet_id and not spreadsheet_title:
        spreadsheet_title = "IIBA CRM DB"

    # Cache le Spreadsheet (évite les requêtes répétées)
    @st.cache_resource(show_spinner=False)
    def _open_spreadsheet():
        if spreadsheet_id:
            return GC.open_by_key(spreadsheet_id)
        return GC.open(spreadsheet_title)

    ss = _open_spreadsheet()

    @lru_cache(maxsize=64)
    def _get_ws(name: str):
        try:
            return ss.worksheet(name)
        except gspread.WorksheetNotFound:
            ws = ss.add_worksheet(title=name, rows=1000, cols=40)
            return ws

    def ws(name: str):
        return _get_ws(name)

    return ws

def show_diagnostics_sidebar(spreadsheet_title_or_id: str, sheet_name_map: Dict[str, str]):
    with st.sidebar.expander("🩺 Diagnostics (Google Sheets)"):
        st.write("Backend déclaré:", st.secrets.get("storage_backend","csv"))
        st.write("Backend effectif:", st.session_state.get("BACKEND_EFFECTIVE","csv"))
        st.write("Spreadsheet (title/id):", spreadsheet_title_or_id)
        st.write("Feuilles attendues:", list(sheet_name_map.values()))
        # Détection du format du secret
        sec = st.secrets.get("google_service_account", None)
        if isinstance(sec, Mapping):
            st.success("google_service_account: Table TOML (mapping) détectée.")
        elif isinstance(sec, str):
            s = sec.strip()
            if s.startswith("{"):
                st.success("google_service_account: Chaîne JSON détectée.")
            else:
                st.info("google_service_account: Chaîne TOML détectée (ou autre).")
        else:
            st.warning("google_service_account: format inattendu.")
        st.caption("Astuce : fournissez gsheet_spreadsheet_id pour éviter les collisions de titre.")
