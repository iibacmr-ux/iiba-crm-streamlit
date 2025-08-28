# pages/03_Evenements.py
from __future__ import annotations
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st

from storage_backend import ensure_df_source, save_df_target
from gs_client import read_service_account_secret, get_gspread_client, make_ws_func

st.set_page_config(page_title="CRM — Événements", page_icon="📅", layout="wide")

# --- Backend init ---
BACKEND = st.secrets.get("storage_backend", "csv")
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
PATHS = {
    "events": DATA_DIR / "evenements.csv",
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
E_COLS = [
    "ID_Événement","Titre","Date","Heure","Lieu","Ville","Pays","Description","Type",
    "Created_At","Created_By","Updated_At","Updated_By"
]

# --- Chargement ---
df_events = ensure_df_source("events", E_COLS, PATHS, WS_FUNC)

st.title("Événements")

# --- Liste ---
st.subheader("Liste des événements")
st.dataframe(df_events, use_container_width=True, height=380)

st.markdown("---")
st.subheader("Créer un événement")

with st.form("event_form"):
    c1, c2, c3 = st.columns(3)
    with c1:
        titre = st.text_input("Titre").strip()
        date = st.date_input("Date")
    with c2:
        heure = st.time_input("Heure")
        type_ev = st.selectbox("Type", ["Atelier","Webinar","Réunion","Autre"])
    with c3:
        lieu = st.text_input("Lieu").strip()
        ville = st.text_input("Ville").strip()
        pays = st.text_input("Pays").strip()
    desc = st.text_area("Description")

    submitted = st.form_submit_button("Créer")

    if submitted:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        new_id = f"EV{int(datetime.utcnow().timestamp())}"
        new_row = {
            "ID_Événement": new_id, "Titre": titre, "Date": str(date), "Heure": str(heure),
            "Lieu": lieu, "Ville": ville, "Pays": pays, "Description": desc, "Type": type_ev,
            "Created_At": now, "Created_By": "ui", "Updated_At": now, "Updated_By": "ui"
        }
        df_events = pd.concat([df_events, pd.DataFrame([new_row])], ignore_index=True)
        save_df_target("events", df_events, PATHS, WS_FUNC)
        st.success(f"Événement {new_id} créé.")
        st.experimental_rerun()
