# ui_common.py — composants UI partagés (barre de filtres, garde login)
from __future__ import annotations
from typing import Dict, Any
import streamlit as st
from datetime import date

def require_login():
    """Stoppe la page si l'utilisateur n'est pas authentifié."""
    if "auth_user" not in st.session_state:
        st.warning("🔐 Veuillez vous connecter depuis la page principale pour accéder à cette section.")
        st.stop()

def render_global_filters() -> Dict[str, Any]:
    """Barre de filtres transverses ; valeurs stockées dans st.session_state['global_filters']"""
    key = "global_filters"
    st.sidebar.markdown("### 🔎 Filtres globaux")
    gf = st.session_state.get(key, {
        "search": "",
        "pays": "",
        "ville": "",
        "date_from": None,
        "date_to": None,
    })
    gf["search"] = st.sidebar.text_input("Recherche (nom/email/titre…)", value=gf.get("search",""))
    col1, col2 = st.sidebar.columns(2)
    with col1:
        gf["pays"] = st.text_input("Pays (global)", value=gf.get("pays",""))
    with col2:
        gf["ville"] = st.text_input("Ville (global)", value=gf.get("ville",""))
    col3, col4 = st.sidebar.columns(2)
    with col3:
        gf["date_from"] = st.date_input("Date min", value=gf.get("date_from") or date(2020,1,1))
    with col4:
        gf["date_to"] = st.date_input("Date max", value=gf.get("date_to") or date.today())
    st.session_state[key] = gf
    return gf
