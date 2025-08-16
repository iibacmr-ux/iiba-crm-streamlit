import streamlit as st
import pandas as pd
import os
from datetime import datetime, date
import json

# --- CONFIGURATION ---
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon="📊", layout="wide")

# Fichiers de données
FILES = {
    "contacts": "contacts.csv",
    "interactions": "interactions.csv", 
    "evenements": "evenements.csv",
    "participations": "participations.csv",
    "paiements": "paiements.csv",
    "certifications": "certifications.csv",
    "settings": "settings.json"
}

# --- DONNÉES DE PARAMÉTRAGE (DROPDOWNS) ---
DEFAULT_SETTINGS = {
    "types_contact": ["Membre", "Prospect", "Formateur", "Partenaire"],
    "sources": ["Afterwork", "Formation", "LinkedIn", "Recommandation", "Site Web", "Salon", "Autre"],
    "statuts_engagement": ["Actif", "Inactif", "À relancer"],
    "secteurs": ["IT", "Finance", "Éducation", "Santé", "Consulting", "Autre"],
    "pays": ["Cameroun", "France", "Canada", "Belgique", "Autre"],
    "canaux": ["Email", "Téléphone", "WhatsApp", "LinkedIn", "Réunion", "Autre"],
    "types_evenements": ["Atelier", "Conférence", "Formation", "Webinaire", "Afterwork"],
    "moyens_paiement": ["Chèque", "Espèces", "Virement", "CB", "Mobile Money", "Autre"]
}

# --- FONCTIONS UTILITAIRES ---
@st.cache_data
def load_settings():
    """Charge les paramètres depuis le fichier JSON"""
    if os.path.exists(FILES["settings"]):
        with open(FILES["settings"], 'r',
