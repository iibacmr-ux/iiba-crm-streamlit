# pages/01_Contacts.py — Grille Contacts (filtres + pagination + statusbar)
from __future__ import annotations
import streamlit as st
import pandas as pd
from _shared import load_all_tables, statusbar, filter_and_paginate, smart_suggested_filters

st.set_page_config(page_title="Contacts — IIBA Cameroun CRM", page_icon="👤", layout="wide")
st.title("👤 Contacts — Grille centrale")

dfs = load_all_tables()
dfc = dfs["contacts"]

# Filtres & pagination (bonnes pratiques CRM 2025)
base_filters = ["Type","Statut","Entreprise","Fonction","Pays","Ville","Genre","Top20"]
suggested = [c for c in base_filters if c in dfc.columns]
if not suggested:
    suggested = smart_suggested_filters(dfc)

page_df, filtered_df = filter_and_paginate(dfc, key_prefix="contacts",
                                           page_size_default=20,
                                           suggested_filters=suggested)

# Status bar
statusbar(filtered_df, numeric_keys=[])

# Affichage
st.dataframe(page_df, use_container_width=True, hide_index=True)

# Sélecteur de contact pour actions complémentaires
if not filtered_df.empty and "ID" in filtered_df.columns:
    st.subheader("✏️ Opérations rapides")
    opt = ["—"] + filtered_df["ID"].astype(str).tolist()
    sel = st.selectbox("Sélectionner un contact (ID)", options=opt, index=0, key="sel_contact_id")
    if sel and sel != "—":
        st.info(f"Contact sélectionné : {sel}. (Placez ici vos formulaires de modification/assignations.)")
