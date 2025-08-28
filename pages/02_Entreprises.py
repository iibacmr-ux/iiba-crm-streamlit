# pages/02_Entreprises.py
from __future__ import annotations
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st

from storage_backend import ensure_df_source, save_df_target, SHEET_NAME
from gs_client import read_service_account_secret, get_gspread_client, make_ws_func

st.set_page_config(page_title="CRM — Entreprises", page_icon="🏢", layout="wide")

# --- Backend init ---
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

def to_int_safe(x, default=0):
    try:
        if x is None: return default
        s = str(x).strip()
        if s == "": return default
        s = s.replace(" ", "").replace("\u00A0","").replace(",", ".")
        return int(float(s))
    except Exception:
        return default

# --- Chargement ---
df_contacts = ensure_df_source("contacts", C_COLS, PATHS, WS_FUNC)
df_entreprises = ensure_df_source("entreprises", E_COLS, PATHS, WS_FUNC)

st.title("Entreprises")

# --- Sélecteur + stats ---
colL, colR = st.columns([3,2])
with colL:
    opts = ["(nouvelle)"] + [f"{r['ID_Entreprise']} — {r['Raison_Sociale']}" for _, r in df_entreprises.iterrows()]
    choix = st.selectbox("Sélectionner une entreprise", opts, index=0)
    ent_id_sel = None if choix == "(nouvelle)" else choix.split(" — ")[0]
with colR:
    # Statistiques robustes
    ca_total_ent = pd.to_numeric(df_entreprises["CA_Annuel"], errors="coerce").fillna(0).sum()
    nb_ent = len(df_entreprises.index)
    st.metric("CA total (somme)", f"{int(ca_total_ent):,}".replace(",", " "))
    st.metric("Nombre d'entreprises", nb_ent)

# --- Contact principal: liste des options (ID - Nom Prenom - Entreprise) ---
def contact_label(row):
    return f"{row.get('ID','')} - {row.get('Nom','')} {row.get('Prénom','')} - {row.get('Entreprise','')}"

contacts_opts = [""] + [contact_label(r) for _, r in df_contacts.iterrows()]
contacts_map = {label: label.split(" - ")[0] for label in contacts_opts if label}

# --- Formulaire entreprise ---
with st.form("entreprise_form", clear_on_submit=False):
    colA, colB, colC = st.columns(3)
    if ent_id_sel:
        row_init = df_entreprises[df_entreprises["ID_Entreprise"] == ent_id_sel].iloc[0].to_dict()
    else:
        row_init = {}

    with colA:
        rs = st.text_input("Raison sociale", row_init.get("Raison_Sociale","")).strip()
        secteur = st.text_input("Secteur", row_init.get("Secteur","")).strip()
        site = st.text_input("Site web", row_init.get("Site_Web","")).strip()
    with colB:
        ca_annuel = st.number_input("CA Annuel (FCFA)", min_value=0, step=1_000_000,
                                    value=to_int_safe(row_init.get("CA_Annuel"), 0))
        nb_emp = st.number_input("Nb Employés", min_value=0, step=1, value=to_int_safe(row_init.get("Nb_Employés"), 0))
        pays = st.text_input("Pays", row_init.get("Pays","")).strip()
    with colC:
        ville = st.text_input("Ville", row_init.get("Ville","")).strip()
        adresse = st.text_area("Adresse", row_init.get("Adresse",""))
        notes = st.text_area("Notes", row_init.get("Notes",""))

    contact_init_id = str(row_init.get("Contact_Principal","") or "")
    contact_init_label = ""
    if contact_init_id:
        # reconstituer le label si existant
        row_c = df_contacts[df_contacts["ID"] == contact_init_id]
        if not row_c.empty:
            rc = row_c.iloc[0]
            contact_init_label = contact_label(rc)
    contact_label_sel = st.selectbox("Contact principal (ID - Nom Prénom - Entreprise)",
                                     options=contacts_opts, index=contacts_opts.index(contact_init_label) if contact_init_label in contacts_opts else 0)

    submitted = st.form_submit_button("Enregistrer")

    if submitted:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        cp_id = contacts_map.get(contact_label_sel, "")

        if not ent_id_sel:  # création
            new_id = f"E{int(datetime.utcnow().timestamp())}"
            new_row = {
                "ID_Entreprise": new_id, "Raison_Sociale": rs, "CA_Annuel": ca_annuel, "Nb_Employés": nb_emp,
                "Secteur": secteur, "Contact_Principal": cp_id, "Adresse": adresse, "Ville": ville, "Pays": pays,
                "Site_Web": site, "Notes": notes, "Created_At": now, "Created_By": "ui", "Updated_At": now, "Updated_By": "ui"
            }
            df_entreprises = pd.concat([df_entreprises, pd.DataFrame([new_row])], ignore_index=True)
            save_df_target("entreprises", df_entreprises, PATHS, WS_FUNC)
            st.success(f"Entreprise {new_id} créée.")
            st.experimental_rerun()
        else:
            idx = df_entreprises.index[df_entreprises["ID_Entreprise"] == ent_id_sel]
            if len(idx) == 0:
                st.error("Entreprise introuvable.")
            else:
                i = idx[0]
                df_entreprises.loc[i, ["Raison_Sociale","CA_Annuel","Nb_Employés","Secteur","Contact_Principal","Adresse","Ville","Pays","Site_Web","Notes","Updated_At","Updated_By"]] = \
                    [rs, ca_annuel, nb_emp, secteur, cp_id, adresse, ville, pays, site, notes, now, "ui"]
                save_df_target("entreprises", df_entreprises, PATHS, WS_FUNC)
                st.success(f"Entreprise {ent_id_sel} mise à jour.")
                st.experimental_rerun()
