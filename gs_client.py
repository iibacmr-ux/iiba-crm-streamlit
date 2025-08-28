
import json
import ast
from typing import Any, Dict, Mapping, Optional, Tuple, List
import streamlit as st

try:
    from google.oauth2.service_account import Credentials  # type: ignore
    import gspread  # type: ignore
except Exception:  # pragma: no cover
    Credentials = None  # type: ignore
    gspread = None  # type: ignore


def _as_mapping(obj: Any) -> Optional[Mapping]:
    """Return obj as mapping-like if possible."""
    try:
        from collections.abc import Mapping as _Mapping
    except Exception:  # pragma: no cover
        _Mapping = dict  # fallback
    if isinstance(obj, _Mapping):
        return obj
    if hasattr(obj, "keys") and hasattr(obj, "__getitem__"):
        # Streamlit Secrets behaves like a mapping
        return obj  # type: ignore
    return None


def _parse_secret_value(val: Any) -> Optional[Dict[str, Any]]:
    """Parse a secrets value into a dict, accepting Mapping or JSON string or Python-literal dict string."""
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
        # Try JSON first
        try:
            return json.loads(s)
        except Exception:
            # Try Python literal (e.g., pasted dict)
            try:
                d = ast.literal_eval(s)
                if isinstance(d, dict):
                    return d
            except Exception:
                pass
    return None


def _normalize_private_key(info: Dict[str, Any]) -> Dict[str, Any]:
    """Convert '\\n' sequences to real newlines ONLY if there are no real newlines yet."""
    pk = info.get("private_key")
    if isinstance(pk, str):
        # If user pasted JSON with \\n and there are no real newlines
        if "\\n" in pk and "\n" not in pk:
            info["private_key"] = pk.replace("\\n", "\n")
    return info


def read_service_account_secret(
    secret_key: str = "google_service_account",
    secrets: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Read and parse the service account from Streamlit secrets or a provided mapping.
    Raises ValueError with verbose message on failure.
    """
    if secrets is None:
        secrets = st.secrets
    # List keys to help diagnostics
    try:
        keys = list(secrets.keys())  # type: ignore
    except Exception:
        keys = []

    if secret_key not in keys:
        raise ValueError(
            f"Clé '{secret_key}' absente dans Secrets. Clés disponibles: {', '.join(keys) or 'aucune'}."
        )

    raw = secrets[secret_key]  # type: ignore
    info = _parse_secret_value(raw)
    if not isinstance(info, dict):
        raise ValueError(
            "Le secret 'google_service_account' n'est pas un dictionnaire exploitable. "
            "Utilisez soit: 1) un bloc JSON entre triples guillemets, soit 2) une table TOML [google_service_account]."
        )

    info = _normalize_private_key(info)

    # Validation minimale
    required = ["type", "project_id", "private_key_id", "private_key", "client_email", "client_id", "token_uri"]
    missing = [k for k in required if k not in info or not info[k]]
    if missing:
        raise ValueError(f"Champs manquants dans le secret: {', '.join(missing)}")

    return info


def get_gspread_client(info: Optional[Dict[str, Any]] = None):
    """Return an authorized gspread client from a service-account info dict (or from secrets)."""
    if info is None:
        info = read_service_account_secret()
    if Credentials is None or gspread is None:  # pragma: no cover
        raise RuntimeError("Modules google-auth/gspread non disponibles.")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)  # type: ignore
    return gspread.authorize(creds)  # type: ignore


def show_diagnostics_sidebar(spreadsheet_name: str, sheet_map: Mapping[str, str]):
    """Render a verbose diagnostics panel in Streamlit sidebar."""
    with st.sidebar.expander("🩺 Diagnostics (Google Sheets)", expanded=False):
        st.caption("Vérification détaillée de la configuration et accès Google Sheets.")

        backend = st.secrets.get("storage_backend", "csv")
        st.write(f"**Backend** : `{backend}`")
        st.write(f"**Spreadsheet** : `{spreadsheet_name}`")

        # 1) Analyse du secret
        st.markdown("**1) Analyse du secret `google_service_account`**")
        try:
            # list keys
            try:
                all_keys = list(st.secrets.keys())  # type: ignore
            except Exception:
                all_keys = []
            st.write(f"- Clés disponibles dans Secrets : {', '.join(all_keys) or '—'}")

            raw = st.secrets.get("google_service_account", None)
            st.write(f"- Type brut: `{type(raw).__name__}`")
            if raw is not None:
                preview = str(raw).replace("private_key", "private_key(…masqué…)")
                st.code(preview[:1000] + (" …" if len(preview) > 1000 else ""), language="text")

            info = read_service_account_secret()
            pk = info.get("private_key", "")
            st.success("Secret parsé ✅")
            st.write(f"- `private_key` longueur: {len(pk) if isinstance(pk, str) else '—'}")
            st.write(f"- Contient séquences `\\n` : {isinstance(pk, str) and ('\\n' in pk)}")
            st.write(f"- Contient vrais retours `\\n` : {isinstance(pk, str) and ('\\n' in pk.replace('\\\\n',''))}")

        except Exception as e:
            st.error(f"Parsing secret: {e}")
            return  # stop diagnostics here

        # 2) Connexion et onglets
        st.markdown("---")
        st.markdown("**2) Connexion Google Sheets & onglets**")
        try:
            gc = get_gspread_client(info)
            sh = gc.open(spreadsheet_name)
            ws_list = [w.title for w in sh.worksheets()]
            st.success(f"Connexion OK. **{len(ws_list)}** onglet(s).")
            st.write("**Onglets détectés** :", ", ".join(ws_list) or "—")

            required_tabs = list({v for v in sheet_map.values()})
            missing = [t for t in required_tabs if t not in ws_list]
            if missing:
                st.warning("Onglets manquants : " + ", ".join(missing))
                if st.button("🛠️ Créer les onglets manquants"):
                    for t in missing:
                        try:
                            sh.add_worksheet(title=t, rows=2, cols=50)
                        except Exception as _e:
                            st.error(f"Impossible de créer '{t}' : {_e}")
                    st.experimental_rerun()
            else:
                st.info("Toutes les tables attendues existent.")
        except Exception as e:
            st.error(f"Connexion échouée: {e}")
