# pages/00_Admin.py — Administration
from __future__ import annotations
from datetime import datetime
from pathlib import Path
import hashlib
import pandas as pd
import streamlit as st

from storage_backend import ensure_df_source, save_df_target
from gs_client import read_service_account_secret, get_gspread_client, make_ws_func
from ui_common import require_login, aggrid_table

st.set_page_config(page_title="CRM — Admin", page_icon="🛠️", layout="wide")
require_login()

# --- Rôle ---
user = st.session_state.get("auth_user", {})
if (user.get("role") or "").lower() != "admin":
    st.error("Accès restreint : administrateurs uniquement.")
    st.stop()

# --- Backend ---
BACKEND = st.secrets.get("storage_backend", "csv")
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
PATHS = {
    "users": DATA_DIR / "users.csv",
    "params": DATA_DIR / "parametres.csv",
}

WS_FUNC = None
if BACKEND == "gsheets":
    try:
        info = read_service_account_secret()
        GC = get_gspread_client(info)
        WS_FUNC = make_ws_func(GC)
    except Exception as e:
        st.error(f"Initialisation Google Sheets échouée : {e}")
        st.stop()

# --- Schémas ---
U_COLS = ["user_id","email","password_hash","role","is_active","display_name",
          "Created_At","Created_By","Updated_At","Updated_By"]
P_COLS = ["Param","Valeur","Created_At","Created_By","Updated_At","Updated_By"]

def _hash_pwd(p: str) -> str:
    return hashlib.sha256(("iiba-cmr::" + str(p)).encode("utf-8")).hexdigest()

df_users = ensure_df_source("users", U_COLS, PATHS, WS_FUNC)
df_params = ensure_df_source("params", P_COLS, PATHS, WS_FUNC)

st.sidebar.checkbox("⚠️ Forcer la sauvegarde (ignore verrou)", value=False, key="override_save_admin")

st.title("Administration")
tabs = st.tabs(["👥 Utilisateurs", "⚙️ Paramètres (listes)"])

with tabs[0]:
    st.subheader("Utilisateurs")
    aggrid_table(df_users, page_size=20, selection='single')
    st.markdown("---")
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
            pwd = st.text_input("Mot de passe", type="password")
            pwd2 = st.text_input("Confirmer", type="password")
        with colD:
            user_id = st.text_input("ID (pour MAJ)", value="").strip()
        submitted = st.form_submit_button("Enregistrer")

    if submitted:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        if mode == "Créer":
            if not email or not pwd or pwd != pwd2:
                st.error("Email/mot de passe requis (et identiques).")
            else:
                new_id = f"U{int(datetime.utcnow().timestamp())}"
                new_row = {
                    "user_id": new_id, "email": email, "password_hash": _hash_pwd(pwd),
                    "role": role, "is_active": "1", "display_name": display,
                    "Created_At": now, "Created_By": user.get('email','ui'),
                    "Updated_At": now, "Updated_By": user.get('email','ui'),
                }
                df_users = pd.concat([df_users, pd.DataFrame([new_row])], ignore_index=True)
                save_df_target("users", df_users, PATHS, WS_FUNC, override=st.session_state.get("override_save_admin", False))
                st.success(f"Utilisateur {email} créé.")
                st.experimental_rerun()
        else:
            if not user_id:
                st.error("ID requis pour mise à jour.")
            else:
                idx = df_users.index[df_users["user_id"] == user_id]
                if len(idx)==0:
                    st.error("ID utilisateur introuvable.")
                else:
                    i = idx[0]
                    updates = {"email": email, "display_name": display, "role": role, "is_active": is_active,
                               "Updated_At": now, "Updated_By": user.get('email','ui')}
                    if pwd:
                        if pwd != pwd2:
                            st.error("Les mots de passe ne correspondent pas."); st.stop()
                        updates["password_hash"] = _hash_pwd(pwd)
                    for k,v in updates.items():
                        df_users.at[i, k] = v
                    save_df_target("users", df_users, PATHS, WS_FUNC, override=st.session_state.get("override_save_admin", False))
                    st.success(f"Utilisateur {user_id} mis à jour."); st.experimental_rerun()

with tabs[1]:
    st.subheader("Paramètres (listes contrôlées)")
    st.caption("Alimente : Secteur (Entreprises), Fonction / Pays / Ville (Contacts).")

    def list_values(df_params, key):
        return df_params[df_params["Param"] == key]["Valeur"].dropna().astype(str).tolist()

    def editor_block(param_key, label):
        vals = list_values(df_params, param_key)
        st.write(f"**{label}** :", ", ".join(vals) if vals else "—")
        col1, col2 = st.columns([2,1])
        with col1:
            value = st.text_input(f"Ajouter/Supprimer dans {label}", key=f"in_{param_key}").strip()
        with col2:
            act = st.selectbox("Action", ["Ajouter","Supprimer"], key=f"act_{param_key}")
        if st.button(f"Valider ({label})", key=f"btn_{param_key}"):
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            if act == "Ajouter":
                if value and value not in vals:
                    row = {"Param": param_key, "Valeur": value,
                           "Created_At": now, "Created_By": user.get('email','ui'),
                           "Updated_At": now, "Updated_By": user.get('email','ui')}
                    df_params = pd.concat([df_params, pd.DataFrame([row])], ignore_index=True)
                    save_df_target("params", df_params, PATHS, WS_FUNC, override=st.session_state.get("override_save_admin", False))
                    st.success(f"Ajouté à {label} : '{value}'"); st.experimental_rerun()
            else:
                sel = df_params.index[(df_params["Param"]==param_key) & (df_params["Valeur"]==value)]
                if len(sel)>0:
                    df_params = df_params.drop(sel)
                    save_df_target("params", df_params, PATHS, WS_FUNC, override=st.session_state.get("override_save_admin", False))
                    st.success(f"Supprimé de {label} : '{value}'"); st.experimental_rerun()

    editor_block("Secteur", "Secteurs (Entreprises)")
    st.markdown("---")
    editor_block("Fonction", "Fonctions (Contacts)")
    editor_block("Pays", "Pays (Contacts)")
    editor_block("Ville", "Villes (Contacts)")
