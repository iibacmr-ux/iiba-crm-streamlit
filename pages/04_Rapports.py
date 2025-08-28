# pages/04_Rapports.py — Rapports transverses (KPI & graphiques)
from __future__ import annotations
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
import pandas as pd
import streamlit as st

from storage_backend import ensure_df_source, SHEET_NAME
from gs_client import read_service_account_secret, get_gspread_client, make_ws_func
from ui_common import require_login, render_global_filters

st.set_page_config(page_title="CRM — Rapports", page_icon="📈", layout="wide")
require_login()

# --- Backend init ---
BACKEND = st.secrets.get("storage_backend", "csv")
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "entreprises": DATA_DIR / "entreprises.csv",
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
C_COLS = ["ID","Civilité","Nom","Prénom","Email","Téléphone","Entreprise","Fonction",
          "Adresse","Ville","Pays","Notes","Created_At","Created_By","Updated_At","Updated_By"]
E_COLS = ["ID_Entreprise","Raison_Sociale","CA_Annuel","Nb_Employés","Secteur","Contact_Principal",
          "Adresse","Ville","Pays","Site_Web","Notes","Created_At","Created_By","Updated_At","Updated_By"]
EV_COLS = ["ID_Événement","Titre","Date","Heure","Lieu","Ville","Pays","Description","Type",
           "Created_At","Created_By","Updated_At","Updated_By"]

# --- Chargement ---
df_c = ensure_df_source("contacts", C_COLS, PATHS, WS_FUNC)
df_e = ensure_df_source("entreprises", E_COLS, PATHS, WS_FUNC)
df_ev = ensure_df_source("events", EV_COLS, PATHS, WS_FUNC)

st.title("Rapports")

# --- Filtres globaux ---
gf = render_global_filters()

# --- KPIs ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Contacts", len(df_c.index))
with col2:
    st.metric("Entreprises", len(df_e.index))
with col3:
    # total CA robuste
    ca_total = pd.to_numeric(df_e["CA_Annuel"], errors="coerce").fillna(0).sum()
    st.metric("CA (somme)", f"{int(ca_total):,}".replace(",", " "))
with col4:
    st.metric("Événements", len(df_ev.index))

st.markdown("---")
st.subheader("Répartition contacts par entreprise (Top 15)")

top = (df_c.groupby("Entreprise")["ID"].count()
         .sort_values(ascending=False).head(15).reset_index()
         .rename(columns={"ID":"Nb_Contacts"}))
st.bar_chart(top.set_index("Entreprise"))

st.markdown("---")
st.subheader("Événements par mois (12 derniers mois)")

# Construire un mois AAAA-MM à partir de Date (si vide -> NaT)
ev = df_ev.copy()
ev["Date"] = pd.to_datetime(ev["Date"], errors="coerce")
min_month = (pd.Timestamp.utcnow() - pd.DateOffset(months=11)).to_period("M").to_timestamp()
ev = ev[ev["Date"] >= min_month]
ev["YYYY-MM"] = ev["Date"].dt.to_period("M").astype(str)
counts = ev.groupby("YYYY-MM")["ID_Événement"].count().reset_index()
st.line_chart(counts.set_index("YYYY-MM"))

st.markdown("---")
st.subheader("Exports")

c1, c2 = st.columns(2)
with c1:
    st.download_button("Exporter contacts (CSV)", data=df_c.to_csv(index=False).encode("utf-8"),
                       file_name="contacts_export.csv", mime="text/csv")
    st.download_button("Exporter entreprises (CSV)", data=df_e.to_csv(index=False).encode("utf-8"),
                       file_name="entreprises_export.csv", mime="text/csv")
with c2:
    st.download_button("Exporter événements (CSV)", data=df_ev.to_csv(index=False).encode("utf-8"),
                       file_name="evenements_export.csv", mime="text/csv")

# Export Excel multi-feuilles
buf = BytesIO()
with pd.ExcelWriter(buf, engine="xlsxwriter") as xw:
    df_c.to_excel(xw, index=False, sheet_name="contacts")
    df_e.to_excel(xw, index=False, sheet_name="entreprises")
    df_ev.to_excel(xw, index=False, sheet_name="evenements")
st.download_button("📒 Export Excel (3 feuilles)", data=buf.getvalue(),
                   file_name="crm_export.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
