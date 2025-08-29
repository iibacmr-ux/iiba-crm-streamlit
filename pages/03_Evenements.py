# pages/03_Evenements.py
from __future__ import annotations
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st

from storage_backend import ensure_df_source, save_df_target
from gs_client import read_service_account_secret, get_gspread_client, make_ws_func
from ui_common import require_login, aggrid_table

st.set_page_config(page_title="CRM — Événements", page_icon="📅", layout="wide")
require_login()

BACKEND = st.secrets.get("storage_backend", "csv")
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
PATHS = {
    "events": DATA_DIR / "evenements.csv",
    "parts": DATA_DIR / "participations.csv",
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

EV_COLS = ["ID_Événement","Titre","Date","Heure","Lieu","Ville","Pays","Description","Type",
           "Created_At","Created_By","Updated_At","Updated_By"]
PART_COLS = ["ID_Participation","Cible_Type","Cible_ID","ID_Événement","Role","Created_At","Created_By","Updated_At","Updated_By"]
C_COLS = ["ID","Nom","Prénom","Entreprise","Email","Téléphone","Ville","Pays","Created_At","Created_By","Updated_At","Updated_By"]
E_COLS = ["ID_Entreprise","Raison_Sociale","Ville","Pays","Created_At","Created_By","Updated_At","Updated_By"]

df_events = ensure_df_source("events", EV_COLS, PATHS, WS_FUNC)
df_parts = ensure_df_source("parts", PART_COLS, PATHS, WS_FUNC)
df_contacts = ensure_df_source("contacts", C_COLS, PATHS, WS_FUNC)
df_ent = ensure_df_source("entreprises", E_COLS, PATHS, WS_FUNC)

st.sidebar.checkbox("⚠️ Forcer la sauvegarde (ignore verrou)", value=False, key="override_save")

st.title("Événements")

grid = aggrid_table(df_events, page_size=20, selection='single')
sel = grid.selected_rows[0] if grid.selected_rows else None
st.caption(f"{len(df_events)} événement(s)")

st.markdown("---")
st.subheader("Créer / Modifier un événement")
with st.form("event_form"):
    row = sel or {}
    ev_id_sel = row.get("ID_Événement")
    col1, col2, col3 = st.columns(3)
    with col1:
        titre = st.text_input("Titre", row.get("Titre","")).strip()
        date = st.date_input("Date")
    with col2:
        heure = st.time_input("Heure")
        type_ev = st.selectbox("Type", ["Atelier","Webinar","Réunion","Autre"], index=0)
    with col3:
        lieu = st.text_input("Lieu", row.get("Lieu","")).strip()
        ville = st.text_input("Ville", row.get("Ville","")).strip()
        pays = st.text_input("Pays", row.get("Pays","")).strip()
    desc = st.text_area("Description", row.get("Description",""))
    submitted = st.form_submit_button("Enregistrer")

    if submitted:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        if ev_id_sel is None:
            new_id = f"EV{int(datetime.utcnow().timestamp())}"
            new_row = {"ID_Événement": new_id,"Titre": titre,"Date": str(date),"Heure": str(heure),
                       "Lieu": lieu,"Ville": ville,"Pays": pays,"Description": desc,"Type": type_ev,
                       "Created_At": now,"Created_By":"ui","Updated_At": now,"Updated_By":"ui"}
            df_events = pd.concat([df_events, pd.DataFrame([new_row])], ignore_index=True)
            save_df_target("events", df_events, PATHS, WS_FUNC, override=st.session_state.get("override_save", False))
            st.success(f"Événement {new_id} créé."); st.experimental_rerun()
        else:
            idx = df_events.index[df_events["ID_Événement"] == ev_id_sel]
            if len(idx):
                i = idx[0]
                df_events.loc[i, ["Titre","Date","Heure","Lieu","Ville","Pays","Description","Type","Updated_At","Updated_By"]] = \
                    [titre, str(date), str(heure), lieu, ville, pays, desc, type_ev, now, "ui"]
                save_df_target("events", df_events, PATHS, WS_FUNC, override=st.session_state.get("override_save", False))
                st.success(f"Événement {ev_id_sel} mis à jour."); st.experimental_rerun()

st.markdown("---")
st.subheader("Participants")
if sel is None:
    st.info("Sélectionnez un événement dans la grille.")
else:
    evid = sel["ID_Événement"]
    colA, colB = st.columns(2)
    with colA:
        st.caption("👤 Contacts")
        labels = [f"{r['ID']} — {r['Nom']} {r['Prénom']} ({r['Entreprise']})" for _, r in df_contacts.iterrows()]
        cmap = {lab: r["ID"] for lab, (_, r) in zip(labels, df_contacts.iterrows())}
        add = st.selectbox("Ajouter un contact", [""] + labels, index=0)
        if st.button("Ajouter (contact)", disabled=(add=="")):
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            row = {"ID_Participation": f"P{int(datetime.utcnow().timestamp())}","Cible_Type":"contact",
                   "Cible_ID": cmap.get(add,""), "ID_Événement": evid, "Role":"participant",
                   "Created_At": now,"Created_By":"ui","Updated_At": now,"Updated_By":"ui"}
            df_parts = pd.concat([df_parts, pd.DataFrame([row])], ignore_index=True)
            save_df_target("parts", df_parts, PATHS, WS_FUNC, override=st.session_state.get("override_save", False)); st.experimental_rerun()
        aggrid_table(df_parts[df_parts["Cible_Type"].eq("contact") & df_parts["ID_Événement"].eq(evid)], page_size=10, selection='none')
    with colB:
        st.caption("🏢 Entreprises")
        labels = [f"{r['ID_Entreprise']} — {r['Raison_Sociale']}" for _, r in df_ent.iterrows()]
        emap = {lab: r["ID_Entreprise"] for lab, (_, r) in zip(labels, df_ent.iterrows())}
        add2 = st.selectbox("Ajouter une entreprise", [""] + labels, index=0)
        if st.button("Ajouter (entreprise)", disabled=(add2=="")):
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            row = {"ID_Participation": f"P{int(datetime.utcnow().timestamp())}","Cible_Type":"entreprise",
                   "Cible_ID": emap.get(add2,""), "ID_Événement": evid, "Role":"participant",
                   "Created_At": now,"Created_By":"ui","Updated_At": now,"Updated_By":"ui"}
            df_parts = pd.concat([df_parts, pd.DataFrame([row])], ignore_index=True)
            save_df_target("parts", df_parts, PATHS, WS_FUNC, override=st.session_state.get("override_save", False)); st.experimental_rerun()
        aggrid_table(df_parts[df_parts["Cible_Type"].eq("entreprise") & df_parts["ID_Événement"].eq(evid)], page_size=10, selection='none')
