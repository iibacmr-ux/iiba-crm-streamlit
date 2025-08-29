
# gs_client.py — Google Sheets client helpers (caching + backoff)
from __future__ import annotations
import json, time
import streamlit as st

try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception as _e:  # pragma: no cover
    gspread = None
    Credentials = None

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]

def read_service_account_secret() -> dict:
    info = st.secrets.get("google_service_account")
    if not info:
        raise RuntimeError("Secret [google_service_account] introuvable (TOML ou JSON).")
    if isinstance(info, str):
        info = json.loads(info)
    pk = info.get("private_key", "")
    # normalisation des clés collées en triple quotes / TOML
    info["private_key"] = pk.replace("\\n", "\n")
    return info

@st.cache_resource(show_spinner=False)
def get_gspread_client(info: dict):
    if gspread is None or Credentials is None:
        raise RuntimeError("gspread / google-auth non disponibles (requirements)")
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)

def make_ws_func(GC):
    """
    Retourne une fonction ws(name) qui:
    - ouvre le spreadsheet 1 seule fois (open_by_key si gsheet_spreadsheet_id, sinon open par titre)
    - met en cache les Worksheet par nom (dans st.session_state)
    - applique un backoff basique si 429
    """
    def ws(name: str):
        ss_id = (st.secrets.get("gsheet_spreadsheet_id","") or "").strip()
        ss_title = (st.secrets.get("gsheet_spreadsheet","") or "IIBA CRM DB").strip()
        # caches session
        if "__GS_SS__" not in st.session_state:
            st.session_state["__GS_SS__"] = None
        if "__WS_CACHE__" not in st.session_state:
            st.session_state["__WS_CACHE__"] = {}

        if st.session_state["__GS_SS__"] is None:
            last_err = None
            for attempt in range(3):
                try:
                    s = GC.open_by_key(ss_id) if ss_id else GC.open(ss_title)
                    st.session_state["__GS_SS__"] = s
                    break
                except Exception as e:
                    last_err = e
                    # 429 / quotas -> backoff
                    time.sleep(1.5 * (attempt + 1))
            if st.session_state["__GS_SS__"] is None:
                raise last_err
        s = st.session_state["__GS_SS__"]
        cache = st.session_state["__WS_CACHE__"]
        if name in cache:
            return cache[name]
        # worksheet par nom (créé si absent)
        try:
            w = s.worksheet(name)
        except Exception:
            # création paresseuse pour éviter des lectures inutiles en boucle
            w = s.add_worksheet(title=name, rows=100, cols=50)
        cache[name] = w
        return w
    return ws
