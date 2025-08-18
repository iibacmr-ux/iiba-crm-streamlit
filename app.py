import streamlit as st
import pandas as pd
import os, json
from datetime import datetime, date
from st_aggrid import AgGrid, GridOptionsBuilder
import io
import openpyxl
import traceback

# --- CONFIGURATION ---
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon="📊", layout="wide")

DATA = {
    "contacts": "contacts.csv",
    "interactions": "interactions.csv",
    "evenements": "evenements.csv",
    "participations": "participations.csv",
    "paiements": "paiements.csv",
    "certifications": "certifications.csv",
    "settings": "settings.json"
}

DEFAULT = {
    "statuts_paiement": ["Réglé", "Partiel", "Non payé"],
    "resultats_inter": ["Positif", "Négatif", "Neutre", "À relancer", "À suivre", "Sans suite"],
    "types_contact": ["Membre", "Prospect", "Formateur", "Partenaire"],
    "sources": ["Afterwork", "Formation", "LinkedIn", "Recommandation", "Site Web", "Salon", "Autre"],
    "statuts_engagement": ["Actif", "Inactif", "À relancer"],
    "secteurs": ["IT", "Finance", "Éducation", "Santé", "Consulting", "Autre", "Côte d’Ivoire", "Sénégal"],
    "pays": ["Cameroun", "France", "Canada", "Belgique", "Autre"],
    "canaux": ["Email", "Téléphone", "WhatsApp", "LinkedIn", "Réunion", "Autre"],
    "types_evenements": ["Atelier", "Conférence", "Formation", "Webinaire", "Afterwork", "BA MEET UP", "Groupe d’étude"],
    "moyens_paiement": ["Chèque", "Espèces", "Virement", "CB", "Mobile Money", "Autre"]
}

@st.cache_data
def load_settings():
    if os.path.exists(DATA["settings"]):
        return json.load(open(DATA["settings"], encoding="utf-8"))
    json.dump(DEFAULT, open(DATA["settings"], "w", encoding="utf-8"), indent=2)
    return DEFAULT

def save_settings(s):
    json.dump(s, open(DATA["settings"], "w", encoding="utf-8"), indent=2)
    st.cache_data.clear()

SET = load_settings()

def load_df(file, cols):
    if os.path.exists(file):
        df = pd.read_csv(file, encoding="utf-8")
    else:
        df = pd.DataFrame(columns=cols)
    for c, v in cols.items():
        if c not in df.columns:
            df[c] = v() if callable(v) else v
    return df[list(cols.keys())]

def save_df(df, file):
    df.to_csv(file, index=False, encoding="utf-8")

def generate_id(prefix, df, col):
    nums = [int(str(x).split("_")[1]) for x in df[col] if isinstance(x, str) and "_" in str(x)]
    n = max(nums) if nums else 0
    return f"{prefix}_{n+1:03d}"

C_COLS = {
    "ID": lambda: None, "Nom": "", "Prénom": "", "Genre": "", "Titre": "",
    "Société": "", "Secteur": SET['secteurs'][0], "Email": "", "Téléphone": "",
    "Ville": "", "Pays": SET['pays'][0], "Type": SET['types_contact'][0], "Source": SET['sources'][0],
    "Statut": SET['statuts_paiement'][0], "LinkedIn": "", "Notes": "", "Date_Creation": lambda: date.today().isoformat()
}

I_COLS = {
    "ID_Interaction": lambda: None, "ID": "", "Date": date.today().isoformat(), "Canal": SET['canaux'][0],
    "Objet": "", "Résumé": "", "Résultat": SET['resultats_inter'][0], "Responsable": "",
    "Prochaine_Action": "", "Relance": ""
}

E_COLS = {
    "ID_Événement": lambda: None, "Nom_Événement": "", "Type": SET['types_evenements'][0], "Date": date.today().isoformat(),
    "Durée_h": 0.0, "Lieu": "", "Formateur(s)": "", "Invité(s)": "", "Objectif": "", "Période": "Matinée",
    "Notes": "", "Coût_Total": 0.0, "Recettes": 0.0, "Bénéfice": 0.0
}

P_COLS = {
    "ID_Participation": lambda: None, "ID": "", "ID_Événement": "", "Rôle": "Participant", 
    "Inscription": date.today().isoformat(), "Arrivée": "", "Temps_Present": "AUTO", "Feedback": 3, 
    "Note": 0, "Commentaire": "", "Nom Participant": "", "Nom Événement": ""
}

PAY_COLS = {
    "ID_Paiement": lambda: None, "ID": "", "ID_Événement": "", "Date_Paiement": date.today().isoformat(),
    "Montant": 0.0, "Moyen": SET['moyens_paiement'][0], "Statut": SET['statuts_paiement'][0],
    "Référence": "", "Notes": "", "Relance": "", "Nom Contact": "", "Nom Événement": ""
}

CERT_COLS = {
    "ID_Certif": lambda: None, "ID": "", "Type_Certif": SET['types_contact'][0], "Date_Examen": date.today().isoformat(),
    "Résultat": "Réussi", "Score": 0, "Date_Obtention": date.today().isoformat(),
    "Validité": "", "Renouvellement": "", "Notes": "", "Nom Contact": ""
}

# --- Handle navigation redirection ---

if "redirect_page" in st.session_state:
    page = st.session_state.pop("redirect_page")
else:
    page = st.sidebar.selectbox("Menu", ["Dashboard", "Vue 360°", "Contacts", "Interactions", "Evenements", "Participations", "Paiements", "Certifications", "Rapports", "Migration", "Paramètres"])

# --------- Pages ---------

if page == "Dashboard":
    st.title("Dashboard global (à développer)")
    st.write("Contenu à implémenter")

elif page == "Vue 360°":
    st.title("👁 Vue 360° des Contacts")
    df = load_df(DATA["contacts"], C_COLS)

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    gb.configure_selection('single', use_checkbox=True)
    grid_response = AgGrid(df, gb.build(), height=350, fit_columns_on_grid_load=True, key='vue360')
    selected = grid_response['selected_rows']

    col_add, col_edit, col_inter, col_part, col_pay = st.columns(5)

    if col_add.button("➕ Nouveau contact"):
        st.session_state["redirect_page"] = "Contacts"
        st.session_state["contact_action"] = "new"
        st.session_state["contact_id"] = None
        st.experimental_rerun()

    if selected:
        sel_id = selected[0]['ID']
        st.write(f"Selected contact: **{sel_id}** {selected[0].get('Nom','')} {selected[0].get('Prénom','')}")

        if col_edit.button("✏️ Editer contact"):
            st.session_state["redirect_page"] = "Contacts"
            st.session_state["contact_action"] = "edit"
            st.session_state["contact_id"] = sel_id
            st.experimental_rerun()
        if col_inter.button("💬 Interactions"):
            st.session_state["redirect_page"] = "Interactions"
            st.session_state["focus_contact"] = sel_id
            st.experimental_rerun()
        if col_part.button("🙋 Participations"):
            st.session_state["redirect_page"] = "Participations"
            st.session_state["focus_contact"] = sel_id
            st.experimental_rerun()
        if col_pay.button("💳 Paiements"):
            st.session_state["redirect_page"] = "Paiements"
            st.session_state["focus_contact"] = sel_id
            st.experimental_rerun()
    else:
        st.info("Sélectionnez un contact dans le tableau ci-dessus pour activer les actions.")

elif page == "Migration":
    st.title("Migration: à compléter selon code précédent")

elif page == "Rapports":
    st.title("Rapports: à compléter selon code précédent")

elif page == "Contacts":
    df = load_df(DATA["contacts"], C_COLS)
    contact_action = st.session_state.get('contact_action', 'view')
    contact_id = st.session_state.get('contact_id', None)
    
    if contact_action == 'edit' and contact_id:
        rec = df.loc[df['ID'] == contact_id].squeeze()
    else:
        rec = None

    st.title("Gestion des Contacts")

    with st.form("form_contact"):
        if rec is not None:
            st.text_input("ID", rec["ID"], disabled=True)
        nom = st.text_input("Nom", rec["Nom"] if rec is not None else "")
        prenom = st.text_input("Prénom", rec["Prénom"] if rec is not None else "")
        genre = st.selectbox("Genre", ["", "Homme", "Femme", "Autre"],
                             index=(["", "Homme", "Femme", "Autre"].index(rec["Genre"]) if rec is not None else 0))
        # (autres champs ici selon C_COLS)
        submit = st.form_submit_button("Sauvegarder")
    
    if submit:
        if rec is not None:
            idx = df.loc[df['ID'] == rec['ID']].index[0]
            df.at[idx, 'Nom'] = nom
            df.at[idx, 'Prénom'] = prenom
            # (mettre à jour autres champs)
        else:
            new_id = generate_id("CNT", df, "ID")
            new_row = {'ID': new_id, 'Nom': nom, 'Prénom': prenom}
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        save_df(df, DATA["contacts"])
        st.success("Contact enregistré!")
        st.session_state.pop('contact_action', None)
        st.session_state.pop('contact_id', None)

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    gb.configure_selection('single', use_checkbox=True)
    AgGrid(df, gb.build(), height=350)

# (Similar minimal code for Interactions, Evenements, Participations, Paiements, Certifications, Paramètres...)

elif page == "Interactions":
    df = load_df(DATA["interactions"], I_COLS)
    focus_contact = st.session_state.get('focus_contact', None)
    if focus_contact:
        df = df[df['ID'] == focus_contact]
    st.title("Interactions")
    st.dataframe(df)

elif page == "Evenements":
    df = load_df(DATA["evenements"], E_COLS)
    st.title("Evénements")
    st.dataframe(df)

elif page == "Participations":
    df = load_df(DATA["participations"], P_COLS)
    focus_contact = st.session_state.get('focus_contact', None)
    if focus_contact:
        df = df[df['ID'] == focus_contact]
    st.title("Participations")
    st.dataframe(df)

elif page == "Paiements":
    df = load_df(DATA["paiements"], PAY_COLS)
    focus_contact = st.session_state.get('focus_contact', None)
    if focus_contact:
        df = df[df['ID'] == focus_contact]
    st.title("Paiements")
    st.dataframe(df)

elif page == "Certifications":
    df = load_df(DATA["certifications"], CERT_COLS)
    focus_contact = st.session_state.get('focus_contact', None)
    if focus_contact:
        df = df[df['ID'] == focus_contact]
    st.title("Certifications")
    st.dataframe(df)

elif page == "Paramètres":
    st.title("Paramètres")
    col1, col2 = st.columns(2)
    with col1:
        sp = st.text_area("Statuts Paiement", "\n".join(SET['statuts_paiement']))
        ri = st.text_area("Resultats Interaction", "\n".join(SET['resultats_inter']))
        tc = st.text_area("Types Contact", "\n".join(SET['types_contact']))
        so = st.text_area("Sources", "\n".join(SET['sources']))
    with col2:
        se = st.text_area("Statuts Engagement", "\n".join(SET['statuts_engagement']))
        sectr = st.text_area("Secteurs", "\n".join(SET['secteurs']))
        pa = st.text_area("Pays", "\n".join(SET['pays']))
        ca = st.text_area("Canaux", "\n".join(SET['canaux']))
    if st.button("Sauvegarder Paramètres"):
        SET['statuts_paiement'] = sp.split("\n")
        SET['resultats_inter'] = ri.split("\n")
        SET['types_contact'] = tc.split("\n")
        SET['sources'] = so.split("\n")
        SET['statuts_engagement'] = se.split("\n")
        SET['secteurs'] = sectr.split("\n")
        SET['pays'] = pa.split("\n")
        SET['canaux'] = ca.split("\n")
        save_settings(SET)
        st.success("Paramètres sauvegardés!")

