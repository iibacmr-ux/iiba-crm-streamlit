# app.py — IIBA Cameroun CRM (refactor stable : backend CSV/Google Sheets + auth + nav)
from __future__ import annotations
from pathlib import Path
from datetime import datetime
import hashlib
import pandas as pd
import streamlit as st
from _shared import load_all_tables, render_global_filter_panel

from storage_backend import (
    AUDIT_COLS, SHEET_NAME,
    compute_etag, ensure_df_source, save_df_target
)
from gs_client import (
    read_service_account_secret, get_gspread_client, make_ws_func, show_diagnostics_sidebar
)

st.set_page_config(page_title="IIBA Cameroun — CRM", page_icon="📊", layout="wide")

# ---------- Schéma utilisateurs ----------
U_COLS = ["user_id","email","password_hash","role","is_active","display_name",
          "Created_At","Created_By","Updated_At","Updated_By"]

# ---------- Panneau tests rapides ----------
with st.sidebar.expander("🧪 Tests rapides"):
    if st.checkbox("🧪 Test ETag rapide", value=False, key="diag_etag"):
        _df = pd.DataFrame({"user_id":[1,2], "Updated_At":["2025-01-01 00:00:00","2025-02-01 12:34:00"]})
        st.write("ETag:", compute_etag(_df, "users"))
        st.info("Ce test n'accède pas à Google Sheets.")
        st.stop()

# ---------- Backend & chemins ----------
BACKEND_DECLARED = st.secrets.get("storage_backend", "csv").strip().lower()
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True, parents=True)
# Tous les chemins utilisés par _shared.py et les pages
PATHS = {
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
st.session_state["PATHS"] = PATHS  # partagé avec _shared.py

# ---------- Initialisation Google Sheets (optionnel) ----------
WS_FUNC = None
GC = None
BACKEND_EFFECTIVE = "csv"

if BACKEND_DECLARED == "gsheets":
    try:
        info = read_service_account_secret()
        GC = get_gspread_client(info)  # @cache_resource
        # Support ID (recommandé) OU titre (fallback)
        sid = st.secrets.get("gsheet_spreadsheet_id", "").strip()
        sname = st.secrets.get("gsheet_spreadsheet", "IIBA CRM DB").strip()
        WS_FUNC = make_ws_func(GC, spreadsheet_id=(sid or None), spreadsheet_title=(None if sid else sname))
        st.session_state["WS_FUNC"] = WS_FUNC
        BACKEND_EFFECTIVE = "gsheets"
        st.session_state["BACKEND_EFFECTIVE"] = BACKEND_EFFECTIVE
        st.sidebar.success("Google Sheets prêt ✅")
    except Exception as e:
        st.sidebar.error(f"Initialisation Google Sheets échouée : {e}")
        st.info("Bascule automatique en CSV local (./data).")
        BACKEND_EFFECTIVE = "csv"
        st.session_state["BACKEND_EFFECTIVE"] = BACKEND_EFFECTIVE
        st.session_state["WS_FUNC"] = None
else:
    BACKEND_EFFECTIVE = "csv"
    st.session_state["BACKEND_EFFECTIVE"] = BACKEND_EFFECTIVE
    st.sidebar.info("Backend : CSV (./data)")

# ---------- Diagnostics Google Sheets ----------
if st.sidebar.checkbox("🩺 Ouvrir le panneau Diagnostics", value=False, key="diag_gs"):
    show_diagnostics_sidebar(
        st.secrets.get("gsheet_spreadsheet_id", st.secrets.get("gsheet_spreadsheet","IIBA CRM DB")),
        SHEET_NAME
    )

# ---------- Auth utils ----------
def _hash_pwd(p: str) -> str:
    return hashlib.sha256(("iiba-cmr::" + str(p)).encode("utf-8")).hexdigest()

def ensure_default_users(df_users: pd.DataFrame) -> pd.DataFrame:
    for c in U_COLS:
        if c not in df_users.columns:
            df_users[c] = ""
    emails = df_users["email"].fillna("").str.lower().tolist() if not df_users.empty else []
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    seed = []
    if "admin2@iiba.cm" not in emails:
        seed.append({
            "user_id":"U0001","email":"admin2@iiba.cm","password_hash":_hash_pwd("123456"),
            "role":"admin","is_active":"1","display_name":"Admin 2",
            "Created_At":ts,"Created_By":"seed","Updated_At":ts,"Updated_By":"seed"
        })
    if "admin@iiba.cm" not in emails:
        seed.append({
            "user_id":"U0002","email":"admin@iiba.cm","password_hash":_hash_pwd("admin"),
            "role":"admin","is_active":"1","display_name":"Admin",
            "Created_At":ts,"Created_By":"seed","Updated_At":ts,"Updated_By":"seed"
        })
    if seed:
        df_users = pd.concat([df_users, pd.DataFrame(seed)], ignore_index=True)
    return df_users[[c for c in U_COLS]] if not df_users.empty else pd.DataFrame(columns=U_COLS)

def authenticate_user(email: str, password: str, df_users: pd.DataFrame):
    em = (email or "").strip().lower()
    hp = _hash_pwd(password or "")
    if df_users is None or df_users.empty: return None
    row = df_users[df_users["email"].fillna("").str.lower() == em]
    if row.empty: return None
    row = row.iloc[0]
    if str(row.get("is_active","1")) not in ("1","true","True"):
        return None
    if row.get("password_hash","") != hp:
        return None
    return row.to_dict()

# ---------- Chargement et seed Users ----------
@st.cache_data(show_spinner=False)
def load_users(paths):
    ws_func = None
    if st.session_state.get("BACKEND_EFFECTIVE") == "gsheets":
        ws_func = st.session_state.get("WS_FUNC")
    return ensure_df_source("users", U_COLS, paths, ws_func)

df_users = load_users(PATHS)
before_len = len(df_users)
df_users = ensure_default_users(df_users)
after_len = len(df_users)
if after_len != before_len:
    # Seed écrit immédiatement dans le backend choisi
    save_df_target("users", df_users, PATHS, st.session_state.get("WS_FUNC"))
st.session_state["df_users"] = df_users

# ---------- UI ----------
st.title("IIBA Cameroun — CRM (refactor)")
st.subheader("Connexion")

col1, col2 = st.columns(2)
with col1:
    em = st.text_input("Email", value="admin2@iiba.cm", key="login_email")
with col2:
    pw = st.text_input("Mot de passe", value="", type="password", key="login_pwd")

if st.button("Se connecter", key="btn_login"):
    dfu = st.session_state.get("df_users")
    if dfu is None or dfu.empty:
        st.error("Données utilisateurs indisponibles.")
    else:
        u = authenticate_user(em, pw, dfu)
        if u:
            st.session_state["auth_user"] = u
            st.success(f"Bienvenue, {u.get('display_name','')} !")
            st.rerun()
        else:
            st.error("Utilisateur introuvable ou mot de passe incorrect.")

# ---------- Navigation pages ----------
st.sidebar.markdown("### 📚 Navigation")
LINKS = [
    ("🛠️ Admin",        "pages/00_Admin.py"),
    ("👤 Contacts",      "pages/01_Contacts.py"),
    ("📋 CRM (Contacts)", "pages/01_CRM.py"),
    ("🏢 Entreprises",   "pages/02_Entreprises.py"),
    ("📅 Événements",    "pages/03_Evenements.py"),
    ("📈 Rapports",      "pages/04_Rapports.py"),
]
for label, page in LINKS:
    try:
        st.sidebar.page_link(page, label=label)
    except Exception as e:
        st.sidebar.caption(f"⚠️ {label} indisponible : {e}")

if "auth_user" in st.session_state:
    st.sidebar.success(f"Connecté : {st.session_state['auth_user'].get('email')}")
    st.write("🟢 Vous êtes connecté. Utilisez le menu de gauche pour accéder aux pages.")
    # ——— Filtre global inter-pages ———
    try:
        dfs_for_filters = load_all_tables()  # cache -> pas de surcoût
        render_global_filter_panel(dfs_for_filters)  # met à jour st.session_state["GLOBAL_FILTERS"]
    except Exception as e:
        st.sidebar.warning(f"Filtre global indisponible : {e}")
else:
    st.info("Veuillez vous connecter pour accéder au CRM.")

