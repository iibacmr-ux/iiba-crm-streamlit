# pages/01_Contacts.py
from __future__ import annotations
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st

from storage_backend import ensure_df_source, save_df_target, SHEET_NAME
from gs_client import read_service_account_secret, get_gspread_client, make_ws_func

st.set_page_config(page_title="CRM — Contacts", page_icon="👤", layout="wide")

# --- Backend init (local à la page) ---
BACKEND = st.secrets.get("storage_backend", "csv")
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "entreprises": DATA_DIR / "entreprises.csv",
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
C_COLS = [
    "ID","Civilité","Nom","Prénom","Email","Téléphone","Entreprise","Fonction",
    "Adresse","Ville","Pays","Notes",
    "Created_At","Created_By","Updated_At","Updated_By"
]
E_COLS = [
    "ID_Entreprise","Raison_Sociale","CA_Annuel","Nb_Employés","Secteur","Contact_Principal",
    "Adresse","Ville","Pays","Site_Web","Notes",
    "Created_At","Created_By","Updated_At","Updated_By"
]

# --- Chargement ---
df_contacts = ensure_df_source("contacts", C_COLS, PATHS, WS_FUNC)
df_entreprises = ensure_df_source("entreprises", E_COLS, PATHS, WS_FUNC)

st.title("Contacts")

# --- Filtre ---
c1, c2, c3 = st.columns([2,2,1])
with c1:
    filt_nom = st.text_input("Filtrer par nom contient", "")
with c2:
    entreprises = ["(toutes)"] + sorted([e for e in df_entreprises["Raison_Sociale"].dropna().astype(str).unique() if e])
    filt_ent = st.selectbox("Entreprise", entreprises, index=0)
with c3:
    st.caption(" ")

df_view = df_contacts.copy()
if filt_nom:
    f = filt_nom.lower()
    df_view = df_view[df_view["Nom"].fillna("").str.lower().str.contains(f) | df_view["Prénom"].fillna("").str.lower().str.contains(f)]
if filt_ent and filt_ent != "(toutes)":
    df_view = df_view[df_view["Entreprise"].fillna("") == filt_ent]

st.dataframe(df_view, use_container_width=True, height=420)

st.markdown("---")
st.subheader("Créer / Modifier un contact")

# --- Formulaire ---
with st.form("contact_form", clear_on_submit=False):
    colA, colB, colC = st.columns(3)
    with colA:
        civilite = st.selectbox("Civilité", ["","M.","Mme","Dr"], index=0)
        nom = st.text_input("Nom").strip()
        prenom = st.text_input("Prénom").strip()
    with colB:
        email = st.text_input("Email").strip()
        tel = st.text_input("Téléphone").strip()
        ent = st.selectbox("Entreprise", [""] + sorted([e for e in df_entreprises["Raison_Sociale"].dropna().astype(str).unique() if e]))
    with colC:
        fonction = st.text_input("Fonction").strip()
        pays = st.text_input("Pays").strip()
        ville = st.text_input("Ville").strip()
    adresse = st.text_area("Adresse")
    notes = st.text_area("Notes")
    mode = st.radio("Mode", ["Créer nouveau", "Mettre à jour existant"], horizontal=True)
    id_modif = None
    if mode == "Mettre à jour existant":
        opts = [""] + [f"{r['ID']} — {r['Nom']} {r['Prénom']} ({r['Entreprise']})" for _, r in df_contacts.iterrows()]
        choix = st.selectbox("Sélectionner un contact", opts, index=0)
        if choix:
            id_modif = choix.split(" — ")[0]

    submitted = st.form_submit_button("Enregistrer")

    if submitted:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        if mode == "Créer nouveau":
            new_id = f"C{int(datetime.utcnow().timestamp())}"
            new_row = {
                "ID": new_id, "Civilité": civilite, "Nom": nom, "Prénom": prenom,
                "Email": email, "Téléphone": tel, "Entreprise": ent, "Fonction": fonction,
                "Adresse": adresse, "Ville": ville, "Pays": pays, "Notes": notes,
                "Created_At": now, "Created_By": "ui", "Updated_At": now, "Updated_By": "ui",
            }
            df_contacts = pd.concat([df_contacts, pd.DataFrame([new_row])], ignore_index=True)
            save_df_target("contacts", df_contacts, PATHS, WS_FUNC)
            st.success(f"Contact {new_id} créé.")
            st.experimental_rerun()
        else:
            if not id_modif:
                st.error("Veuillez choisir un contact à modifier.")
            else:
                idx = df_contacts.index[df_contacts["ID"] == id_modif]
                if len(idx) == 0:
                    st.error("ID introuvable.")
                else:
                    i = idx[0]
                    df_contacts.loc[i, ["Civilité","Nom","Prénom","Email","Téléphone","Entreprise","Fonction","Adresse","Ville","Pays","Notes","Updated_At","Updated_By"]] = \
                        [civilite,nom,prenom,email,tel,ent,fonction,adresse,ville,pays,notes, now,"ui"]
                    save_df_target("contacts", df_contacts, PATHS, WS_FUNC)
                    st.success(f"Contact {id_modif} mis à jour.")
                    st.experimental_rerun()
