
# gs_client.py — Google Sheets client helpers (caching + backoff + diagnostics)
from __future__ import annotations
import json, time, traceback
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
    """
    Lit le secret [google_service_account] depuis st.secrets.
    IMPORTANT: Retourne une copie dict mutable (et non l'objet Secrets) pour éviter
    l'erreur "Secrets does not support item assignment".
    Normalise la clé privée (remplace \\n par \n).
    """
    info = st.secrets.get("google_service_account")
    if not info:
        raise RuntimeError("Secret [google_service_account] introuvable (TOML ou JSON).")
    if isinstance(info, str):
        info = json.loads(info)
    else:
        # copie défensive pour éviter l'écriture dans st.secrets
        info = dict(info)
    pk = info.get("private_key", "")
    if isinstance(pk, str) and "\\n" in pk and "\n" not in pk:
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
    - ouvre le spreadsheet une seule fois (open_by_key si gsheet_spreadsheet_id, sinon open par titre)
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

def show_diagnostics_sidebar(spreadsheet_label: str = "", sheet_name_map: dict | None = None):
    """
    Affiche un panneau de diagnostic dans la sidebar:
    - Vérifie la présence des secrets et leur parsing
    - Tente d'ouvrir le spreadsheet (par ID ou par titre)
    - Liste les feuilles disponibles
    - Option de test lecture/écriture minimale (désactivée par défaut)
    """
    with st.sidebar.expander("🩺 Diagnostics (Google Sheets)"):
        st.caption("Backend: **gsheets**" if st.secrets.get("storage_backend","csv")=="gsheets" else "Backend: **csv**")
        if st.secrets.get("storage_backend","csv") != "gsheets":
            st.info("Le backend actuel est CSV — pas de diagnostics Google Sheets nécessaires.")
            return

        # Parsing secret
        try:
            info = read_service_account_secret()
            st.success("Secret [google_service_account] OK.")
        except Exception as e:
            st.error(f"Secret invalide: {e}")
            st.code(traceback.format_exc())
            return

        # Client
        try:
            GC = get_gspread_client(info)
            st.success("Client gspread initialisé.")
        except Exception as e:
            st.error(f"Initialisation gspread échouée: {e}")
            st.code(traceback.format_exc())
            return

        # Spreadsheet
        sid = (st.secrets.get("gsheet_spreadsheet_id","") or "").strip()
        title = (st.secrets.get("gsheet_spreadsheet","") or spreadsheet_label or "IIBA CRM DB").strip()
        try:
            s = GC.open_by_key(sid) if sid else GC.open(title)
            st.success(f"Ouverture spreadsheet OK: {(sid or title)}")
            try:
                wss = [w.title for w in s.worksheets()]
                st.write("Feuilles:", ", ".join(wss) if wss else "(aucune)")
            except Exception as e2:
                st.warning(f"Impossible de lister les feuilles: {e2}")
        except Exception as e:
            st.error(f"❌ Ouverture Google Sheet échouée\n{e}")
            st.code(traceback.format_exc())
            return

        # Test minimal (optionnel)
        do_test = st.checkbox("Exécuter un test de lecture/écriture minimal (peut consommer du quota)", value=False)
        if do_test:
            try:
                w = s.worksheet("_diag") if "_diag" in [w.title for w in s.worksheets()] else s.add_worksheet(title="_diag", rows=10, cols=5)
                import pandas as pd
                from gspread_dataframe import set_with_dataframe, get_as_dataframe
                df = pd.DataFrame({"ts":[pd.Timestamp.now().isoformat()]})  # une cellule
                set_with_dataframe(w, df, include_index=False, include_column_header=True, resize=True)
                back = get_as_dataframe(w, evaluate_formulas=True, header=0)
                st.write("Test R/W OK — dernière valeur:", back.tail(1))
            except Exception as e:
                st.error(f"Test R/W échoué: {e}")
