# gs_client.py — gestion des secrets Google, client gspread et diagnostics
from __future__ import annotations
import json
from typing import Dict, Optional
import streamlit as st

def read_service_account_secret() -> Dict:
    """Lit st.secrets pour la clé de service.
    Accepte:
      - st.secrets["google_service_account"] (dict TOML ou str JSON)
      - ou un bloc JSON brut dans st.secrets["google_service_account"]
    """
    sec = st.secrets.get("google_service_account", None)
    if sec is None:
        raise RuntimeError("Secret 'google_service_account' manquant.")
    if isinstance(sec, dict):
        info = dict(sec)
    else:
        s = str(sec).strip()
        # Enlever éventuels triples guillemets
        if s.startswith('"""') and s.endswith('"""'):
            s = s[3:-3]
        info = json.loads(s)
    # Normaliser la clé privée (certains systèmes insèrent des \n littéraux)
    pk = info.get("private_key", "")
    if isinstance(pk, str):
        if "\\n" in pk and "-----BEGIN" in pk:
            info["private_key"] = pk.replace("\\n", "\n")
    return info

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
            # crée la feuille si elle n'existe pas
            ws = ss.add_worksheet(title=name, rows=1000, cols=40)
            return ws

    def ws(name: str):
        return _get_ws(name)

    return ws

def show_diagnostics_sidebar(spreadsheet_title_or_id: str, sheet_name_map: Dict[str, str]):
    with st.sidebar.expander("🩺 Diagnostics (Google Sheets)"):
        st.write("Backend:", st.secrets.get("storage_backend","csv"))
        st.write("Spreadsheet (title/id):", spreadsheet_title_or_id)
        st.write("Feuilles attendues:", list(sheet_name_map.values()))
        st.caption("Astuce : donnez *gsheet_spreadsheet_id* pour éviter les collisions de titre.")
