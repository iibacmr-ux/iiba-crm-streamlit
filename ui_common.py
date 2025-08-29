# ui_common.py — composants UI partagés
from __future__ import annotations
from typing import Dict, Any
import streamlit as st

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
except Exception:
    AgGrid = None
    GridOptionsBuilder = None
    GridUpdateMode = None
    DataReturnMode = None

def require_login():
    if "auth_user" not in st.session_state:
        st.warning("🔐 Veuillez vous connecter depuis la page principale pour accéder à cette section.")
        st.stop()

def render_global_filters() -> Dict[str, Any]:
    key = "global_filters"
    st.sidebar.markdown("### 🔎 Filtres globaux")
    gf = st.session_state.get(key, {"search":"", "pays":"", "ville":""})
    gf["search"] = st.sidebar.text_input("Recherche (nom/email/titre…)", value=gf.get("search",""))
    col1, col2 = st.sidebar.columns(2)
    with col1:
        gf["pays"] = st.text_input("Pays (global)", value=gf.get("pays",""))
    with col2:
        gf["ville"] = st.text_input("Ville (global)", value=gf.get("ville",""))
    st.session_state[key] = gf
    return gf

def aggrid_table(df, *, height=460, page_size=20, selection='single', enable_sidebar=False, fit_columns=True):
    if AgGrid is None:
        st.info("ℹ️ Ajoutez 'streamlit-aggrid' à requirements.txt pour filtres/pagination/agrégats.")
        st.dataframe(df, use_container_width=True, height=height)
        class Dummy:
            selected_rows = []
            data = df
        return Dummy()
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=page_size)
    gb.configure_default_column(filter=True, sortable=True, resizable=True, enableValue=True, enableRowGroup=True, enablePivot=True)
    gb.configure_side_bar(enable_sidebar)
    gb.configure_selection(selection_mode=selection, use_checkbox=True)
    gb.configure_status_bar(statusPanels=[
        {'statusPanel': 'agTotalRowCountComponent', 'align': 'left'},
        {'statusPanel': 'agAggregationComponent', 'align': 'right'}
    ])
    gridOptions = gb.build()
    grid = AgGrid(df, gridOptions=gridOptions, height=height, fit_columns_on_grid_load=fit_columns,
                  data_return_mode=DataReturnMode.AS_INPUT,
                  update_mode=GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.VALUE_CHANGED)
    return grid
