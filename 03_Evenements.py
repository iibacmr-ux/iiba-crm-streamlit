# pages/03_Evenements.py — Grille Événements + entreprises via employés & officielles
from __future__ import annotations
import streamlit as st
import pandas as pd
from _shared import load_all_tables, statusbar, filter_and_paginate, parse_date

st.set_page_config(page_title="Événements — IIBA Cameroun", page_icon="📅", layout="wide")
st.title("📅 Événements")

dfs = load_all_tables()
dfev = dfs["events"].copy()
dfp  = dfs["parts"].copy()
dfc  = dfs["contacts"].copy()
df_ep = dfs["entreprise_parts"].copy()

# ===== Grille événements avec filtres/pagination =====
# Aide : colonnes Type, Ville, Pays + Année/Mois sur "Date"
if "Date" in dfev.columns:
    dfev["_annee"] = pd.to_datetime(dfev["Date"], errors="coerce").dt.year.astype("Int64")
    dfev["_mois"]  = pd.to_datetime(dfev["Date"], errors="coerce").dt.month.astype("Int64")
suggested = ["Type","Ville","Pays","_annee","_mois"]
page_df, filtered_df = filter_and_paginate(dfev, key_prefix="ev", page_size_default=20, suggested_filters=suggested)
statusbar(filtered_df, numeric_keys=["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total"])
st.dataframe(page_df.drop(columns=["_annee","_mois"], errors="ignore"), use_container_width=True, hide_index=True)

# ===== Détails par événement sélectionné =====
sel_ids = ["—"] + filtered_df.get("ID_Événement", pd.Series([], dtype=str)).astype(str).tolist()
sel_evt = st.selectbox("Sélectionner un événement (ID_Événement)", options=sel_ids, index=0, key="evt_sel")
if sel_evt and sel_evt != "—":
    ev = dfev[dfev["ID_Événement"].astype(str)==sel_evt]
    if not ev.empty:
        row = ev.iloc[0].to_dict()
        st.subheader(f"🗂️ Fiche — {row.get('Nom_Événement','(sans nom)')}")
        c1,c2,c3,c4 = st.columns(4)
        c1.text_input("ID_Événement", row.get("ID_Événement",""), disabled=True)
        c2.text_input("Créé le", row.get("Created_At",""), disabled=True)
        c3.text_input("Modifié le", row.get("Updated_At",""), disabled=True)
        c4.text_input("Type", row.get("Type",""), disabled=True)

        st.markdown("---")
        tab_parts, tab_emp, tab_off = st.tabs(["🎟 Participations (personnes)","🏢 Entreprises via employés","🏢 Entreprises officielles"])

        with tab_parts:
            parts = dfp[dfp.get("ID_Événement","").astype(str)==sel_evt].copy()
            page_p, filt_p = filter_and_paginate(parts, key_prefix="evt_parts", page_size_default=20,
                                                 suggested_filters=["Rôle"])
            statusbar(filt_p, numeric_keys=[])
            st.dataframe(page_p, use_container_width=True, hide_index=True)

        with tab_emp:
            # Entreprises "via employés" = entreprise du contact participant
            parts = dfp[dfp.get("ID_Événement","").astype(str)==sel_evt].copy()
            if not parts.empty and "ID" in parts.columns and "Entreprise" in dfc.columns:
                emp_ent = parts.merge(dfc[["ID","Entreprise"]], on="ID", how="left")
                agg = emp_ent.groupby("Entreprise")["ID_Participation"].count().reset_index().rename(columns={"ID_Participation":"Nb_Participants"})
            else:
                agg = pd.DataFrame(columns=["Entreprise","Nb_Participants"])
            page_emp, filt_emp = filter_and_paginate(agg, key_prefix="evt_emp", page_size_default=20,
                                                     suggested_filters=["Entreprise"])
            statusbar(filt_emp, numeric_keys=["Nb_Participants"])
            st.dataframe(page_emp, use_container_width=True, hide_index=True)

        with tab_off:
            # Entreprises officielles (au nom de l'entreprise)
            off = df_ep[df_ep.get("ID_Événement","").astype(str)==sel_evt].copy()
            page_off, filt_off = filter_and_paginate(off, key_prefix="evt_off", page_size_default=20,
                                                     suggested_filters=["Type_Lien"])
            statusbar(filt_off, numeric_keys=["Nb_Employes","Sponsoring_FCFA"])
            st.dataframe(page_off, use_container_width=True, hide_index=True)
