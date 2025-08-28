# app.py — IIBA Cameroun CRM (refactor propre: helpers externalisés, ordre robuste)
from __future__ import annotations
from pathlib import Path
from datetime import datetime
import pandas as pd
import streamlit as st

from storage_backend import (
    AUDIT_COLS, SHEET_NAME,
    compute_etag, ensure_df_source, save_df_target
)
from gs_client import (
    read_service_account_secret, get_gspread_client, make_ws_func, show_diagnostics_sidebar
)

st.set_page_config(page_title="IIBA Cameroun — CRM", page_icon="📊", layout="wide")

# ---------- Schémas ----------
U_COLS = ["user_id","email","password_hash","role","is_active","display_name",
          "Created_At","Created_By","Updated_At","Updated_By"]

# ---------- Panneau test ETag (autonome) ----------
with st.sidebar.expander("🧪 Tests rapides"):
    if st.checkbox("🧪 Test ETag rapide", value=False, key="diag_etag"):
        _df = pd.DataFrame({"user_id":[1,2], "Updated_At":["2025-01-01 00:00:00","2025-02-01 12:34:00"]})
        st.write("ETag:", compute_etag(_df, "users"))
        st.info("Ce test n'accède pas à Google Sheets.")
        st.stop()

# ---------- Backend & chemins ----------
BACKEND = st.secrets.get("storage_backend", "csv")
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
PATHS = {"users": DATA_DIR / "users.csv"}  # étendez à volonté pour les autres tables

WS_FUNC = None
GC = None
if BACKEND == "gsheets":
    try:
        info = read_service_account_secret()
        GC = get_gspread_client(info)
        WS_FUNC = make_ws_func(GC)
    except Exception as e:
        st.error(f"Initialisation Google Sheets échouée : {e}")
        st.stop()

# ---------- Diagnostics Google Sheets (optionnel) ----------
if st.sidebar.checkbox("🩺 Ouvrir le panneau Diagnostics", value=False, key="diag_gs"):
    show_diagnostics_sidebar(st.secrets.get("gsheet_spreadsheet","IIBA CRM DB"), SHEET_NAME)

# ---------- Utilitaires Auth ----------
import hashlib
def _hash_pwd(p: str) -> str:
    return hashlib.sha256(("iiba-cmr::" + str(p)).encode("utf-8")).hexdigest()

def ensure_default_users(df_users: pd.DataFrame) -> pd.DataFrame:
    # Garantit colonnes
    for c in U_COLS:
        if c not in df_users.columns:
            df_users[c] = ""
    emails = df_users["email"].fillna("").str.lower().tolist()
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    seed = []
    if "admin2@iiba.cm" not in emails:
        seed.append({
            "user_id":"U0001", "email":"admin2@iiba.cm", "password_hash":_hash_pwd("123456"),
            "role":"admin", "is_active":"1", "display_name":"Admin 2",
            "Created_At":ts,"Created_By":"seed","Updated_At":ts,"Updated_By":"seed"
        })
    if "admin@iiba.cm" not in emails:
        seed.append({
            "user_id":"U0002", "email":"admin@iiba.cm", "password_hash":_hash_pwd("admin"),
            "role":"admin", "is_active":"1", "display_name":"Admin",
            "Created_At":ts,"Created_By":"seed","Updated_At":ts,"Updated_By":"seed"
        })
    if seed:
        df_users = pd.concat([df_users, pd.DataFrame(seed)], ignore_index=True)
    return df_users[[c for c in U_COLS]]

def authenticate_user(email: str, password: str, df_users: pd.DataFrame):
    em = (email or "").strip().lower()
    hp = _hash_pwd(password or "")
    row = df_users[df_users["email"].fillna("").str.lower() == em]
    if row.empty:
        return None
    row = row.iloc[0]
    if str(row.get("is_active","1")) not in ("1","true","True"):
        return None
    if row.get("password_hash","") != hp:
        return None
    return row.to_dict()

# ---------- Chargement Users (cache) ----------
@st.cache_data(show_spinner=False)
def load_users() -> pd.DataFrame:
    return ensure_df_source("users", U_COLS, PATHS, WS_FUNC)

df_users = load_users()
df_users = ensure_default_users(df_users)
save_df_target("users", df_users, PATHS, WS_FUNC)
st.session_state["df_users"] = df_users

# ---------- UI Login simple ----------
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

if "auth_user" in st.session_state:
    st.sidebar.success(f"Connecté : {st.session_state['auth_user'].get('email')}")
    st.write("🟢 Vous êtes connecté. (Placez ici vos pages CRM : Contacts, Entreprises, Événements, etc.)")
else:
    st.info("Veuillez vous connecter pour accéder au CRM.")

# Dans app.py, après la partie Connexion (et PAS dans un form)
st.sidebar.markdown("### 📚 Navigation")
LINKS = [
    ("👤 Contacts",      "pages/01_Contacts.py"),
    ("🏢 Entreprises",   "pages/02_Entreprises.py"),
    ("📅 Événements",    "pages/03_Evenements.py"),
    ("📈 Rapports",      "pages/04_Rapports.py"),
    ("🛠️ Admin",        "pages/00_Admin.py"),
]
for label, page in LINKS:
    try:
        st.sidebar.page_link(page, label=label)
    except Exception as e:
        st.sidebar.caption(f"⚠️ {label} indisponible : {e}")
