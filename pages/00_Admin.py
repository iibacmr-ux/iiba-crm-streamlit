# pages/00_Admin.py — Administration (utilisateurs, paramètres)
from __future__ import annotations
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st

from storage_backend import ensure_df_source, save_df_target
from gs_client import read_service_account_secret, get_gspread_client, make_ws_func
from ui_common import require_login

st.set_page_config(page_title="CRM — Admin", page_icon="🛠️", layout="wide")
require_login()

# --- Vérifier rôle ---
user = st.session_state.get("auth_user", {})
if (user.get("role") or "").lower() != "admin":
    st.error("Accès restreint : administrateurs uniquement.")
    st.stop()

# --- Backend init ---
BACKEND = st.secrets.get("storage_backend", "csv")
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
PATHS = {"users": DATA_DIR / "users.csv"}

WS_FUNC = None
if BACKEND == "gsheets":
    try:
        info = read_service_account_secret()
        GC = get_gspread_client(info)
        WS_FUNC = make_ws_func(GC)
    except Exception as e:
        st.error(f"Initialisation Google Sheets échouée : {e}")
        st.stop()

# --- Schéma ---
U_COLS = ["user_id","email","password_hash","role","is_active","display_name",
          "Created_At","Created_By","Updated_At","Updated_By"]

# --- Helpers ---
import hashlib
def _hash_pwd(p: str) -> str:
    return hashlib.sha256(("iiba-cmr::" + str(p)).encode("utf-8")).hexdigest()

# --- Chargement ---
df_users = ensure_df_source("users", U_COLS, PATHS, WS_FUNC)

st.title("Administration")
st.caption("Gestion des utilisateurs et paramètres globaux.")

# --- Tableau des utilisateurs ---
st.subheader("👥 Utilisateurs")
st.dataframe(df_users, use_container_width=True, height=320)

st.markdown("---")
st.subheader("Créer / Mettre à jour un utilisateur")

with st.form("user_form"):
    colA, colB, colC, colD = st.columns(4)
    with colA:
        mode = st.radio("Mode", ["Créer", "Mettre à jour"], horizontal=True)
        email = st.text_input("Email").strip().lower()
        display = st.text_input("Nom affiché").strip()
    with colB:
        role = st.selectbox("Rôle", ["admin","staff","viewer"], index=1)
        is_active = st.selectbox("Actif ?", ["1","0"], index=0)
    with colC:
        pwd = st.text_input("Mot de passe (plain)", type="password")
        pwd2 = st.text_input("Confirmer", type="password")
    with colD:
        user_id = st.text_input("ID (pour mise à jour)", value="").strip()

    submitted = st.form_submit_button("Enregistrer")

    if submitted:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        if mode == "Créer":
            if not email or not pwd or pwd != pwd2:
                st.error("Email et mot de passe requis (et identiques).")
            else:
                new_id = f"U{int(datetime.utcnow().timestamp())}"
                new_row = {
                    "user_id": new_id, "email": email, "password_hash": _hash_pwd(pwd),
                    "role": role, "is_active": is_active, "display_name": display,
                    "Created_At": now, "Created_By": user.get('email','ui'),
                    "Updated_At": now, "Updated_By": user.get('email','ui'),
                }
                df_users = pd.concat([df_users, pd.DataFrame([new_row])], ignore_index=True)
                save_df_target("users", df_users, PATHS, WS_FUNC)
                st.success(f"Utilisateur {email} créé ({new_id}).")
                st.experimental_rerun()
        else:
            if not user_id:
                st.error("Veuillez préciser l'ID de l'utilisateur à mettre à jour.")
            else:
                idx = df_users.index[df_users["user_id"] == user_id]
                if len(idx) == 0:
                    st.error("ID utilisateur introuvable.")
                else:
                    i = idx[0]
                    updates = {"email": email, "display_name": display, "role": role, "is_active": is_active,
                               "Updated_At": now, "Updated_By": user.get('email','ui')}
                    if pwd:
                        if pwd != pwd2:
                            st.error("Les mots de passe ne correspondent pas.")
                            st.stop()
                        updates["password_hash"] = _hash_pwd(pwd)
                    for k,v in updates.items():
                        df_users.at[i, k] = v
                    save_df_target("users", df_users, PATHS, WS_FUNC)
                    st.success(f"Utilisateur {user_id} mis à jour.")
                    st.experimental_rerun()

st.markdown("---")
st.subheader("🔄 Import/Export des utilisateurs (CSV)")
c1, c2 = st.columns(2)
with c1:
    file = st.file_uploader("Importer un CSV (colonnes conformes au schéma)", type=["csv"])
    if file is not None:
        try:
            new_df = pd.read_csv(file, dtype=str).fillna("")
            df_users = new_df[[c for c in U_COLS]]
            save_df_target("users", df_users, PATHS, WS_FUNC)
            st.success("Import réalisé.")
            st.experimental_rerun()
        except Exception as e:
            st.error(f"Import échoué : {e}")
with c2:
    st.download_button("Exporter (CSV)", data=df_users.to_csv(index=False).encode("utf-8"),
                       file_name="users_export.csv", mime="text/csv")
