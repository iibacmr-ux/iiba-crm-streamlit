# pages/02_Entreprises.py
from __future__ import annotations
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st

from storage_backend import ensure_df_source, save_df_target
from gs_client import read_service_account_secret, get_gspread_client, make_ws_func
from ui_common import require_login, aggrid_table

st.set_page_config(page_title="CRM — Entreprises", page_icon="🏢", layout="wide")
require_login()

BACKEND = st.secrets.get("storage_backend", "csv")
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "entreprises": DATA_DIR / "entreprises.csv",
    "params": DATA_DIR / "parametres.csv",
    "events": DATA_DIR / "evenements.csv",
    "parts": DATA_DIR / "participations.csv",
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

C_COLS = ["ID","Civilité","Nom","Prénom","Email","Téléphone","Entreprise","Fonction",
          "Adresse","Ville","Pays","Notes","Created_At","Created_By","Updated_At","Updated_By"]
E_COLS = ["ID_Entreprise","Raison_Sociale","CA_Annuel","Nb_Employés","Secteur","Contact_Principal",
          "Adresse","Ville","Pays","Site_Web","Notes","Created_At","Created_By","Updated_At","Updated_By"]
P_COLS = ["Param","Valeur","Created_At","Created_By","Updated_At","Updated_By"]
EV_COLS = ["ID_Événement","Titre","Date","Heure","Lieu","Ville","Pays","Description","Type",
           "Created_At","Created_By","Updated_At","Updated_By"]
PART_COLS = ["ID_Participation","Cible_Type","Cible_ID","ID_Événement","Role","Created_At","Created_By","Updated_At","Updated_By"]

df_contacts = ensure_df_source("contacts", C_COLS, PATHS, WS_FUNC)
df_entreprises = ensure_df_source("entreprises", E_COLS, PATHS, WS_FUNC)
df_params = ensure_df_source("params", P_COLS, PATHS, WS_FUNC)
df_events = ensure_df_source("events", EV_COLS, PATHS, WS_FUNC)
df_parts = ensure_df_source("parts", PART_COLS, PATHS, WS_FUNC)

st.sidebar.checkbox("⚠️ Forcer la sauvegarde (ignore verrou)", value=False, key="override_save")

st.title("Entreprises")

grid = aggrid_table(df_entreprises[["ID_Entreprise","Raison_Sociale","Secteur","Ville","Pays","CA_Annuel","Nb_Employés"]],
                    page_size=20, selection='single')
sel = grid.selected_rows[0] if grid.selected_rows else None
st.caption(f"{len(df_entreprises)} entreprise(s)")

colS1, colS2 = st.columns(2)
with colS1:
    ca_total_ent = pd.to_numeric(df_entreprises["CA_Annuel"], errors="coerce").fillna(0).sum()
    st.metric("CA total (somme)", f"{int(ca_total_ent):,}".replace(",", " "))
with colS2:
    st.metric("Nombre d'entreprises", len(df_entreprises.index))

st.markdown("---")
st.subheader("Créer / Modifier une entreprise")

secteurs = [""] + sorted(df_params[df_params["Param"]=="Secteur"]["Valeur"].dropna().astype(str).unique().tolist())

def contact_label(row):
    return f"{row.get('ID','')} - {row.get('Nom','')} {row.get('Prénom','')} - {row.get('Entreprise','')}"

cp_opts = [""] + [contact_label(r) for _, r in df_contacts.iterrows()]
cp_map = {label: label.split(" - ")[0] for label in cp_opts if label}

with st.form("entreprise_form"):
    row_init = sel or {}
    ent_id_sel = (sel["ID_Entreprise"] if sel is not None else None)

    colA, colB, colC = st.columns(3)
    with colA:
        rs = st.text_input("Raison sociale", row_init.get("Raison_Sociale","")).strip()
        secteur = st.selectbox("Secteur", secteurs, index=(secteurs.index(row_init.get("Secteur","")) if row_init.get("Secteur","") in secteurs else 0))
        site = st.text_input("Site web", row_init.get("Site_Web","")).strip()
    with colB:
        def to_int_safe(x, default=0):
            try:
                if x is None: return default
                s = str(x).strip().replace(" ", "").replace("\u00A0","").replace(",", ".")
                if s == "": return default
                return int(float(s))
            except Exception:
                return default
        ca_annuel = st.number_input("CA Annuel (FCFA)", min_value=0, step=1_000_000, value=to_int_safe(row_init.get("CA_Annuel"), 0))
        nb_emp = st.number_input("Nb Employés", min_value=0, step=1, value=to_int_safe(row_init.get("Nb_Employés"), 0))
        pays = st.text_input("Pays", row_init.get("Pays","")).strip()
    with colC:
        ville = st.text_input("Ville", row_init.get("Ville","")).strip()
        adresse = st.text_area("Adresse", row_init.get("Adresse",""))
        notes = st.text_area("Notes", row_init.get("Notes",""))
    cp_label_init = ""
    if row_init.get("Contact_Principal"):
        r = df_contacts[df_contacts["ID"] == row_init.get("Contact_Principal")]
        if not r.empty:
            rc = r.iloc[0]
            cp_label_init = contact_label(rc)
    contact_label_sel = st.selectbox("Contact principal (ID - Nom Prénom - Entreprise)",
                                     options=cp_opts, index=(cp_opts.index(cp_label_init) if cp_label_init in cp_opts else 0))

    submitted = st.form_submit_button("Enregistrer")

    if submitted:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        cp_id = cp_map.get(contact_label_sel, "")
        if ent_id_sel is None:
            new_id = f"E{int(datetime.utcnow().timestamp())}"
            new_row = {"ID_Entreprise": new_id, "Raison_Sociale": rs, "CA_Annuel": ca_annuel, "Nb_Employés": nb_emp,
                       "Secteur": secteur, "Contact_Principal": cp_id, "Adresse": adresse, "Ville": ville, "Pays": pays,
                       "Site_Web": site, "Notes": notes, "Created_At": now, "Created_By": "ui", "Updated_At": now, "Updated_By": "ui"}
            df_entreprises = pd.concat([df_entreprises, pd.DataFrame([new_row])], ignore_index=True)
            save_df_target("entreprises", df_entreprises, PATHS, WS_FUNC, override=st.session_state.get("override_save", False))
            st.success(f"Entreprise {new_id} créée."); st.experimental_rerun()
        else:
            idx = df_entreprises.index[df_entreprises["ID_Entreprise"] == ent_id_sel]
            if len(idx)==0:
                st.error("Entreprise introuvable.")
            else:
                i = idx[0]
                df_entreprises.loc[i, ["Raison_Sociale","CA_Annuel","Nb_Employés","Secteur","Contact_Principal","Adresse","Ville","Pays","Site_Web","Notes","Updated_At","Updated_By"]] = \
                    [rs, ca_annuel, nb_emp, secteur, cp_id, adresse, ville, pays, site, notes, now, "ui"]
                save_df_target("entreprises", df_entreprises, PATHS, WS_FUNC, override=st.session_state.get("override_save", False))
                st.success(f"Entreprise {ent_id_sel} mise à jour."); st.experimental_rerun()

st.markdown("---")
st.subheader("Contacts employés de l'entreprise")
if sel is None:
    st.info("Sélectionnez une entreprise dans la grille.")
else:
    emp = df_contacts[df_contacts["Entreprise"].fillna("") == sel["Raison_Sociale"]]
    aggrid_table(emp, page_size=10, selection='none')
    st.caption(f"{len(emp)} contact(s) rattaché(s)")

st.markdown("---")
st.subheader("Participations de l'entreprise à des événements")
if sel is None:
    st.info("Sélectionnez une entreprise pour gérer ses participations.")
else:
    ent_id = sel["ID_Entreprise"]
    ev_labels = [f"{r['ID_Événement']} — {r['Titre']}" for _, r in df_events.iterrows()]
    ev_map = {lab: r["ID_Événement"] for lab, (_, r) in zip(ev_labels, df_events.iterrows())}
    add_ev = st.selectbox("Ajouter l'entreprise à l'événement", [""] + ev_labels, index=0)
    if st.button("Ajouter participation entreprise", disabled=(add_ev=="" or ent_id=="")):
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        new_row = {"ID_Participation": f"P{int(datetime.utcnow().timestamp())}","Cible_Type":"entreprise","Cible_ID": ent_id,
                   "ID_Événement": ev_map.get(add_ev,""), "Role":"participant",
                   "Created_At": now,"Created_By":"ui","Updated_At": now,"Updated_By":"ui"}
        df_parts = pd.concat([df_parts, pd.DataFrame([new_row])], ignore_index=True)
        save_df_target("parts", df_parts, PATHS, WS_FUNC, override=st.session_state.get("override_save", False))
        st.success("Participation ajoutée."); st.experimental_rerun()
    aggrid_table(df_parts[df_parts["Cible_Type"].eq("entreprise") & df_parts["Cible_ID"].eq(ent_id)], page_size=10, selection='none')
