
from __future__ import annotations
import pandas as pd
import streamlit as st
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
    HAS_AG = True
except Exception:
    HAS_AG = False

from _shared import load_all_tables, aggregates_for_contacts

st.set_page_config(page_title="CRM", page_icon="🧩", layout="wide")

dfs = load_all_tables()
dfc = dfs["contacts"]
ag = aggregates_for_contacts(dfs)

# Vue enrichie (join sur ID)
dfc_show = dfc.copy()
if "ID" not in dfc_show.columns:
    dfc_show["ID"] = ""
dfc_show = dfc_show.merge(ag, on="ID", how="left")

st.title("🧩 CRM — Grille centrale")

# Filtres simples
with st.expander("🔎 Filtres"):
    colf1, colf2, colf3, colf4 = st.columns(4)
    f_secteur = colf1.selectbox("Secteur", ["(Tous)"] + sorted(set(dfc_show.get("Secteur","").unique())))
    f_pays    = colf2.selectbox("Pays",    ["(Tous)"] + sorted(set(dfc_show.get("Pays","").unique())))
    f_ville   = colf3.selectbox("Ville",   ["(Tous)"] + sorted(set(dfc_show.get("Ville","").unique())))
    f_type    = colf4.selectbox("Type",    ["(Tous)"] + sorted(set(dfc_show.get("Type","").unique())))
    mask = pd.Series(True, index=dfc_show.index)
    if f_secteur != "(Tous)": mask &= (dfc_show["Secteur"] == f_secteur)
    if f_pays    != "(Tous)": mask &= (dfc_show["Pays"] == f_pays)
    if f_ville   != "(Tous)": mask &= (dfc_show["Ville"] == f_ville)
    if f_type    != "(Tous)": mask &= (dfc_show["Type"] == f_type)
    dfc_show = dfc_show[mask]

# Grille
show_cols = [c for c in dfc_show.columns if c not in ("Created_At","Created_By","Updated_At","Updated_By")]
if HAS_AG:
    gb = GridOptionsBuilder.from_dataframe(dfc_show[show_cols])
    gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
    gb.configure_default_column(filter=True, sortable=True, resizable=True)
    gb.configure_grid_options(statusBar={"statusPanels":[
        {"statusPanel":"agTotalRowCountComponent","align":"left"},
        {"statusPanel":"agFilteredRowCountComponent"},
        {"statusPanel":"agSelectedRowCountComponent"},
        {"statusPanel":"agAggregationComponent"}
    ]})
    gb.configure_side_bar()
    gb.configure_selection("single", use_checkbox=False)
    AgGrid(dfc_show[show_cols], gridOptions=gb.build(), update_mode=GridUpdateMode.NO_UPDATE, height=520, theme="streamlit")
else:
    st.info("st-aggrid non disponible — affichage standard.")
    st.dataframe(dfc_show[show_cols], use_container_width=True)
