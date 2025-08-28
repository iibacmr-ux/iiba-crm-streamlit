"""
gs_client.py — Gestion des secrets Google et client gspread + diagnostics
Expose:
- read_service_account_secret()
- get_gspread_client(info: dict|None) -> gspread.Client
- make_ws_func(GC) -> callable(name)->Worksheet
- show_diagnostics_sidebar(spreadsheet_name: str, sheet_map: Mapping[str, str])
"""
from __future__ import annotations
from typing import Any, Dict, Optional, Mapping, Callable
import ast
import json
import streamlit as st

try:
    from google.oauth2.service_account import Credentials  # type: ignore
    import gspread  # type: ignore
    from gspread.exceptions import APIError, SpreadsheetNotFound  # type: ignore
except Exception:  # pragma: no cover
    Credentials = None  # type: ignore
    gspread = None  # type: ignore
    APIError = Exception  # type: ignore
    SpreadsheetNotFound = Exception  # type: ignore

def _as_mapping(obj: Any) -> Optional[Mapping]:
    try:
        from collections.abc import Mapping as _Mapping
    except Exception:  # pragma: no cover
        _Mapping = dict
    if isinstance(obj, _Mapping):
        return obj
    if hasattr(obj, "keys") and hasattr(obj, "__getitem__"):
        return obj  # Streamlit Secrets
    return None

def _parse_secret_value(val: Any) -> Optional[Dict[str, Any]]:
    m = _as_mapping(val)
    if m is not None:
        try:
            return dict(m)  # type: ignore
        except Exception:
            try:
                return {k: m[k] for k in m.keys()}  # type: ignore
            except Exception:
                return None
    if isinstance(val, str):
        s = val.strip()
        try:
            return json.loads(s)
        except Exception:
            try:
                d = ast.literal_eval(s)
                if isinstance(d, dict):
                    return d
            except Exception:
                pass
    return None

def _normalize_private_key(info: Dict[str, Any]) -> Dict[str, Any]:
    pk = info.get("private_key")
    if isinstance(pk, str) and "\\n" in pk and "\n" not in pk:
        info["private_key"] = pk.replace("\\n", "\n")
    return info

def read_service_account_secret(secret_key: str = "google_service_account", 
                                secrets: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    if secrets is None:
        secrets = st.secrets
    try:
        keys = list(secrets.keys())  # type: ignore
    except Exception:
        keys = []
    if secret_key not in keys:
        raise ValueError(f"Clé '{secret_key}' absente dans Secrets. Clés disponibles: {', '.join(keys) or 'aucune'}.")
    raw = secrets[secret_key]  # type: ignore
    info = _parse_secret_value(raw)
    if not isinstance(info, dict):
        raise ValueError(
            "Le secret 'google_service_account' n'est pas au bon format. "
            "Utilisez: JSON triple-quoted ou table TOML [google_service_account]."
        )
    info = _normalize_private_key(info)
    required = ["type", "project_id", "private_key_id", "private_key", "client_email", "client_id", "token_uri"]
    missing = [k for k in required if k not in info or not info[k]]
    if missing:
        raise ValueError("Champs manquants dans le secret: " + ", ".join(missing))
    return info

def get_gspread_client(info: Optional[Dict[str, Any]] = None):
    if info is None:
        info = read_service_account_secret()
    if Credentials is None or gspread is None:  # pragma: no cover
        raise RuntimeError("google-auth/gspread indisponibles")
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)  # type: ignore
    return gspread.authorize(creds)  # type: ignore

def make_ws_func(GC) -> Callable[[str], "gspread.Worksheet"]:
    """Fabrique un ws(name) qui ouvre le Spreadsheet par ID ou par titre, et crée l'onglet si manquant."""
    sid = (st.secrets.get("gsheet_spreadsheet_id") or "").strip()
    sname = (st.secrets.get("gsheet_spreadsheet") or "").strip()

    def ws(name: str):
        if GC is None:
            raise RuntimeError("Client gspread non initialisé")
        try:
            if sid:
                sh = GC.open_by_key(sid)
            elif sname:
                sh = GC.open(sname)
            else:
                raise RuntimeError("Ni 'gsheet_spreadsheet_id' ni 'gsheet_spreadsheet' dans secrets.")
            try:
                w = sh.worksheet(name)
            except Exception:
                sh.add_worksheet(title=name, rows=2000, cols=50)
                w = sh.worksheet(name)
            return w
        except Exception as e:
            st.sidebar.error("❌ Ouverture Google Sheet échouée")
            st.sidebar.code(str(e))
            raise
    return ws

def show_diagnostics_sidebar(spreadsheet_name: str, sheet_map: Mapping[str, str]):
    with st.sidebar.expander("🩺 Diagnostics (Google Sheets)", expanded=False):
        backend = st.secrets.get("storage_backend", "csv")
        st.write(f"**Backend** : `{backend}`")
        ss_id = st.secrets.get("gsheet_spreadsheet_id", "").strip() if "gsheet_spreadsheet_id" in st.secrets else ""
        st.write(f"**Spreadsheet (nom)** : `{spreadsheet_name or '—'}`")
        st.write(f"**Spreadsheet (ID)** : `{ss_id or '—'}`")

        st.markdown("**1) Analyse du secret `google_service_account`**")
        try:
            all_keys = list(st.secrets.keys())  # type: ignore
        except Exception:
            all_keys = []
        st.write(f"- Clés en Secrets : {', '.join(all_keys) or '—'}")
        try:
            raw = st.secrets.get("google_service_account", None)
            st.write(f"- Type brut: `{type(raw).__name__}`")
            if raw is not None:
                preview = str(raw).replace("private_key", "private_key(…masqué…)")
                st.code(preview[:900] + (' …' if len(preview) > 900 else ''), language="text")
            info = read_service_account_secret()
            st.success("Secret parsé ✅")
            st.write(f"- `client_email`: `{info.get('client_email','—')}`")
        except Exception as e:
            st.error(f"Parsing secret: {e}")
            return

        st.markdown("---")
        st.markdown("**2) Connexion Google Sheets & onglets**")
        try:
            gc = get_gspread_client(info)
            if ss_id:
                try:
                    sh = gc.open_by_key(ss_id)
                except APIError as e:
                    msg = str(e).lower()
                    if "not supported for this document" in msg or "operation is not supported" in msg:
                        st.warning("L’ID ne correspond pas à un Google Sheet natif. Fallback par titre si disponible.")
                        sh = gc.open(spreadsheet_name)
                    else:
                        raise
            else:
                sh = gc.open(spreadsheet_name)
            ws_list = [w.title for w in sh.worksheets()]
            st.success(f"Connexion OK. **{len(ws_list)}** onglet(s).")
            st.write("**Onglets détectés** :", ", ".join(ws_list) or "—")
            required_tabs = list({v for v in sheet_map.values()})
            missing = [t for t in required_tabs if t not in ws_list]
            if missing:
                st.warning("Onglets manquants : " + ", ".join(missing))
                if st.button("🛠️ Créer les onglets manquants"):
                    for t in missing:
                        try:
                            sh.add_worksheet(title=t, rows=2, cols=50)
                        except Exception as _e:
                            st.error(f"Impossible de créer '{t}' : {_e}")
                    st.experimental_rerun()
            else:
                st.info("Toutes les tables attendues existent.")
        except Exception as e:
            st.error(f"Connexion échouée: {e}")
