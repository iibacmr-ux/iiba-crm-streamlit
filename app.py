import streamlit as st
import pandas as pd
import os
from datetime import datetime, date
import json


# --- CONFIGURATION ---
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon="ðŸ“Š", layout="wide")

# Fichiers de donnÃ©es
FILES = {
    "contacts": "contacts.csv",
    "interactions": "interactions.csv", 
    "evenements": "evenements.csv",
    "participations": "participations.csv",
    "paiements": "paiements.csv",
    "certifications": "certifications.csv",
    "settings": "settings.json"
}

# --- DONNÃ‰ES DE PARAMÃ‰TRAGE (DROPDOWNS) ---
DEFAULT_SETTINGS = {
    "types_contact": ["Membre", "Prospect", "Formateur", "Partenaire"],
    "sources": ["Afterwork", "Formation", "LinkedIn", "Recommandation", "Site Web", "Salon", "Autre"],
    "statuts_engagement": ["Actif", "Inactif", "Ã€ relancer"],
    "secteurs": ["IT", "Finance", "Ã‰ducation", "SantÃ©", "Consulting", "Autre"],
    "pays": ["Cameroun", "France", "Canada", "Belgique", "Autre"],
    "canaux": ["Email", "TÃ©lÃ©phone", "WhatsApp", "LinkedIn", "RÃ©union", "Autre"],
    "types_evenements": ["Atelier", "ConfÃ©rence", "Formation", "Webinaire", "Afterwork"],
    "moyens_paiement": ["ChÃ¨que", "EspÃ¨ces", "Virement", "CB", "Mobile Money", "Autre"]
}

# --- FONCTIONS UTILITAIRES ---
@st.cache_data
def load_settings():
    """Charge les paramÃ¨tres depuis le fichier JSON"""
    if os.path.exists(FILES["settings"]):
        with open(FILES["settings"], 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        save_settings(DEFAULT_SETTINGS)
        return DEFAULT_SETTINGS

def save_settings(settings):
    """Sauvegarde les paramÃ¨tres dans le fichier JSON"""
    with open(FILES["settings"], 'w', encoding='utf-8') as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)
    st.cache_data.clear()


@st.cache_data
def load_data(file_path):
    """Charge les donnÃ©es depuis un fichier CSV avec migration automatique"""
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # Migration automatique pour contacts
            if "contacts" in file_path:
                required_columns = ['ID', 'Nom', 'PrÃ©nom', 'Genre', 'Titre', 'SociÃ©tÃ©', 'Secteur', 
                                  'Email', 'TÃ©lÃ©phone', 'Ville', 'Pays', 'Type_Contact', 'Source', 
                                  'Statut_Engagement', 'LinkedIn', 'Notes', 'Date_Creation']
                
                # Ajouter les colonnes manquantes avec valeurs par dÃ©faut
                for col in required_columns:
                    if col not in df.columns:
                        if col == 'Type_Contact':
                            df[col] = 'Prospect'  # Valeur par dÃ©faut
                        elif col == 'Source':
                            df[col] = 'Autre'
                        elif col == 'Statut_Engagement':
                            df[col] = 'Actif'
                        else:
                            df[col] = ''
                
                # RÃ©organiser les colonnes dans le bon ordre
                df = df[required_columns]
                
                # Sauvegarder le fichier migrÃ©
                save_data(df, file_path)
                st.success(f"âœ… Migration automatique effectuÃ©e pour {file_path}")
            
            return df
            
        except Exception as e:
            st.error(f"Erreur lors du chargement de {file_path}: {e}")
            # Retourner un DataFrame vide en cas d'erreur
            if "contacts" in file_path:
                return pd.DataFrame(columns=['ID', 'Nom', 'PrÃ©nom', 'Genre', 'Titre', 'SociÃ©tÃ©', 'Secteur', 
                                           'Email', 'TÃ©lÃ©phone', 'Ville', 'Pays', 'Type_Contact', 'Source', 
                                           'Statut_Engagement', 'LinkedIn', 'Notes', 'Date_Creation'])
            elif "interactions" in file_path:
                return pd.DataFrame(columns=['ID_Interaction', 'ID_Contact', 'Date', 'Canal', 'Objet', 
                                           'RÃ©sumÃ©', 'RÃ©sultat', 'Responsable', 'Prochaine_Action', 'Relance'])
            elif "paiements" in file_path:
                return pd.DataFrame(columns=['ID_Paiement', 'ID_Contact', 'ID_Ã‰vÃ©nement', 'Date_Paiement', 
                                           'Montant', 'Moyen', 'Statut', 'RÃ©fÃ©rence', 'Notes'])
            return pd.DataFrame()
    else:
        # CrÃ©er les DataFrames vides avec les bonnes colonnes
        if "contacts" in file_path:
            return pd.DataFrame(columns=['ID', 'Nom', 'PrÃ©nom', 'Genre', 'Titre', 'SociÃ©tÃ©', 'Secteur', 
                                        'Email', 'TÃ©lÃ©phone', 'Ville', 'Pays', 'Type_Contact', 'Source', 
                                        'Statut_Engagement', 'LinkedIn', 'Notes', 'Date_Creation'])
        elif "interactions" in file_path:
            return pd.DataFrame(columns=['ID_Interaction', 'ID_Contact', 'Date', 'Canal', 'Objet', 
                                        'RÃ©sumÃ©', 'RÃ©sultat', 'Responsable', 'Prochaine_Action', 'Relance'])
        elif "paiements" in file_path:
            return pd.DataFrame(columns=['ID_Paiement', 'ID_Contact', 'ID_Ã‰vÃ©nement', 'Date_Paiement', 
                                        'Montant', 'Moyen', 'Statut', 'RÃ©fÃ©rence', 'Notes'])
        return pd.DataFrame()


def save_data(data, file_path):
    """Sauvegarde les donnÃ©es dans un fichier CSV"""
    data.to_csv(file_path, index=False, encoding='utf-8')
    st.cache_data.clear()

def generate_id(prefix, existing_df):
    """GÃ©nÃ¨re un nouvel ID unique"""
    if len(existing_df) == 0:
        return f"{prefix}_001"
    last_id = existing_df.iloc[-1, 0] if len(existing_df) > 0 else f"{prefix}_000"
    num = int(str(last_id).split('_')[1]) + 1
    return f"{prefix}_{num:03d}"

# --- STYLES CSS AMÃ‰LIORÃ‰S ---
st.markdown("""
<style>
.search-box {background-color: #f0f2f6; padding: 15px; border-radius: 10px; margin-bottom: 20px;}
.contact-card {background-color: white; padding: 15px; border-radius: 10px; margin: 5px; border-left: 4px solid #1f77b4;}
.kpi-card {background-color: #f8f9fa; padding: 20px; border-radius: 10px; text-align: center; margin: 10px;}
.readonly-field {background-color: #f5f5f5; border: 1px solid #ddd; padding: 5px; border-radius: 5px;}
div[data-baseweb="tooltip"] {width: 300px !important; max-width: none !important;}
.help-tooltip {font-size: 12px; color: #666;}
</style>
""", unsafe_allow_html=True)

# --- NAVIGATION ---
PAGES = ["Dashboard 360", "Contacts", "Interactions", "Ã‰vÃ©nements", "Participations", "Paiements", "Certifications", "ParamÃ¨tres"]
choice = st.sidebar.selectbox("Navigation IIBA CRM", PAGES)

# Chargement des paramÃ¨tres
settings = load_settings()

# --- PAGE 1 : DASHBOARD 360 (avec KPI enrichis) ---
if choice == "Dashboard 360":
    st.title("ðŸŽ¯ Dashboard 360 - IIBA Cameroun")
    
    # Chargement des donnÃ©es pour les KPI
    df_contacts = load_data(FILES["contacts"])
    df_interactions = load_data(FILES["interactions"])
    df_paiements = load_data(FILES["paiements"])
    df_certifications = load_data(FILES["certifications"])
    
    # --- KPI CARDS ---
    st.subheader("ðŸ“Š Indicateurs ClÃ©s de Performance")
    
    # Calculs des KPI
    current_year = datetime.now().year
    prospects_actifs = len(df_contacts[df_contacts['Type_Contact'] == 'Prospect']) if not df_contacts.empty else 0
    membres_iiba = len(df_contacts[df_contacts['Type_Contact'] == 'Membre']) if not df_contacts.empty else 0
    ca_total = df_paiements[df_paiements['Statut'] == 'PayÃ©']['Montant'].sum() if not df_paiements.empty else 0
    paiements_attente = len(df_paiements[df_paiements['Statut'] == 'En attente']) if not df_paiements.empty else 0
    certifs_obtenues = len(df_certifications[df_certifications['RÃ©sultat'] == 'RÃ©ussi']) if not df_certifications.empty else 0
    
    # Affichage des KPI en colonnes
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("ðŸ‘¥ Prospects Actifs", prospects_actifs)
        st.metric("ðŸ† Membres IIBA", membres_iiba)
    
    with col2:
        st.metric(f"ðŸ“… Ã‰vÃ©nements {current_year}", 0)  # Ã€ calculer selon tes donnÃ©es
        st.metric("ðŸŽ“ Participations Totales", 0)  # Ã€ calculer
    
    with col3:
        st.metric("ðŸ’° CA Total RÃ©glÃ©", f"{ca_total:,.0f} FCFA")
        st.metric("â³ Paiements en Attente", paiements_attente)
    
    with col4:
        st.metric("ðŸŽ¯ Certifications Obtenues", certifs_obtenues)
        st.metric("ðŸ“ˆ Score Moyen Engagement", "N/A")  # Ã€ calculer
    
    # --- SECTION RECHERCHE ---
    st.markdown('<div class="search-box">', unsafe_allow_html=True)
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        search_term = st.text_input("ðŸ” Rechercher un contact", "", 
                                   help="Tapez un nom, sociÃ©tÃ©, email ou tÃ©lÃ©phone pour rechercher")
    with col2:
        secteur_filter = st.selectbox("Filtrer par secteur", 
                                     ["Tous"] + settings["secteurs"])
    with col3:
        type_filter = st.selectbox("Filtrer par type", 
                                  ["Tous"] + settings["types_contact"])
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Filtrage et affichage des rÃ©sultats
    if not df_contacts.empty:
        filtered_df = df_contacts.copy()
        
        if search_term:
            mask = (
                filtered_df['Nom'].str.contains(search_term, case=False, na=False) |
                filtered_df['PrÃ©nom'].str.contains(search_term, case=False, na=False) |
                filtered_df['SociÃ©tÃ©'].str.contains(search_term, case=False, na=False) |
                filtered_df['Email'].str.contains(search_term, case=False, na=False)
            )
            filtered_df = filtered_df[mask]
        
        if secteur_filter != "Tous":
            filtered_df = filtered_df[filtered_df['Secteur'] == secteur_filter]
        if type_filter != "Tous":
            filtered_df = filtered_df[filtered_df['Type_Contact'] == type_filter]
        
        st.subheader(f"ðŸ“‹ Liste des contacts ({len(filtered_df)} rÃ©sultat(s))")
        
        if len(filtered_df) > 0:
            # SÃ©lection d'un contact pour modification
            selected_contact = st.selectbox("SÃ©lectionner un contact Ã  modifier", 
                                           [""] + filtered_df['ID'].tolist())
            
            if selected_contact:
                st.session_state.edit_contact_id = selected_contact
                st.session_state.page_redirect = "Contacts"
                st.rerun()
            
            st.dataframe(
                filtered_df[['ID', 'Nom', 'PrÃ©nom', 'SociÃ©tÃ©', 'Type_Contact', 'Email', 'TÃ©lÃ©phone']],
                use_container_width=True
            )
        else:
            st.info("Aucun contact trouvÃ© avec ces critÃ¨res.")
    else:
        st.info("Aucun contact enregistrÃ©. Ajoutez votre premier contact ! ðŸ‘†")

# --- PAGE 2 : CONTACTS (avec modification) ---
elif choice == "Contacts":
    st.title("ðŸ‘¤ Gestion des Contacts")
    
    df_contacts = load_data(FILES["contacts"])
    
    # Mode modification si un contact est sÃ©lectionnÃ©
    edit_mode = False
    edit_contact = None
    
    if hasattr(st.session_state, 'edit_contact_id') and st.session_state.edit_contact_id:
        edit_mode = True
        edit_contact = df_contacts[df_contacts['ID'] == st.session_state.edit_contact_id].iloc[0]
        st.info(f"âœï¸ Modification du contact {edit_contact['ID']} - {edit_contact['Nom']} {edit_contact['PrÃ©nom']}")
    
    with st.form("form_contacts"):
        if edit_mode:
            st.subheader("âœï¸ Modifier le contact")
        else:
            st.subheader("âž• Ajouter un nouveau contact")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Champ ID en readonly si mode modification
            if edit_mode:
                st.text_input("ID Contact", value=edit_contact['ID'], disabled=True,
                             help="Identifiant unique du contact (non modifiable)")
            
            nom = st.text_input("Nom*", 
                               value=edit_contact['Nom'] if edit_mode else "",
                               help="Nom de famille du contact")
            
            prenom = st.text_input("PrÃ©nom*", 
                                  value=edit_contact['PrÃ©nom'] if edit_mode else "",
                                  help="PrÃ©nom du contact")
            
            genre = st.selectbox("Genre", ["", "Homme", "Femme", "Autre"],
                               index=["", "Homme", "Femme", "Autre"].index(edit_contact['Genre']) if edit_mode and edit_contact['Genre'] in ["", "Homme", "Femme", "Autre"] else 0,
                               help="Genre du contact")
            
            titre = st.text_input("Titre/Position", 
                                 value=edit_contact['Titre'] if edit_mode else "",
                                 help="Fonction ou titre professionnel")
            
            societe = st.text_input("SociÃ©tÃ©", 
                                   value=edit_contact['SociÃ©tÃ©'] if edit_mode else "",
                                   help="Nom de l'entreprise oÃ¹ travaille le contact")
            
            secteur = st.selectbox("Secteur d'activitÃ©", 
                                  [""] + settings["secteurs"],
                                  index=([""] + settings["secteurs"]).index(edit_contact['Secteur']) if edit_mode and edit_contact['Secteur'] in settings["secteurs"] else 0,
                                  help="Domaine d'activitÃ© de l'entreprise")
            
            type_contact = st.selectbox("Type de Contact*", 
                                       [""] + settings["types_contact"],
                                       index=([""] + settings["types_contact"]).index(edit_contact['Type_Contact']) if edit_mode and edit_contact['Type_Contact'] in settings["types_contact"] else 0,
                                       help="CatÃ©gorie du contact : Membre, Prospect, Formateur ou Partenaire")
        
        with col2:
            email = st.text_input("Email*", 
                                 value=edit_contact['Email'] if edit_mode else "",
                                 help="Adresse email principale du contact")
            
            telephone = st.text_input("TÃ©lÃ©phone*", 
                                     value=edit_contact['TÃ©lÃ©phone'] if edit_mode else "",
                                     help="NumÃ©ro de tÃ©lÃ©phone (format: +237XXXXXXXXX)")
            
            ville = st.text_input("Ville", 
                                 value=edit_contact['Ville'] if edit_mode else "",
                                 help="Ville de rÃ©sidence ou de travail")
            
            pays = st.selectbox("Pays", 
                               [""] + settings["pays"],
                               index=([""] + settings["pays"]).index(edit_contact['Pays']) if edit_mode and edit_contact['Pays'] in settings["pays"] else 0,
                               help="Pays de rÃ©sidence")
            
            source = st.selectbox("Source*", 
                                 [""] + settings["sources"],
                                 index=([""] + settings["sources"]).index(edit_contact['Source']) if edit_mode and edit_contact['Source'] in settings["sources"] else 0,
                                 help="Comment avez-vous connu ce contact ?")
            
            statut = st.selectbox("Statut Engagement*", 
                                 [""] + settings["statuts_engagement"],
                                 index=([""] + settings["statuts_engagement"]).index(edit_contact['Statut_Engagement']) if edit_mode and edit_contact['Statut_Engagement'] in settings["statuts_engagement"] else 0,
                                 help="Niveau d'engagement actuel du contact")
            
            linkedin = st.text_input("LinkedIn", 
                                    value=edit_contact['LinkedIn'] if edit_mode else "",
                                    help="URL du profil LinkedIn (optionnel)")
            
            # Date de crÃ©ation - modifiable seulement en mode modification pour migration
            if edit_mode:
                date_creation = st.text_input("Date de CrÃ©ation", 
                                            value=edit_contact['Date_Creation'],
                                            help="Date de crÃ©ation (modifiable pour migration de donnÃ©es)")
            else:
                date_creation = datetime.now().strftime("%d/%m/%Y %H:%M")
        
        notes = st.text_area("Notes", 
                           value=edit_contact['Notes'] if edit_mode else "",
                           help="Informations complÃ©mentaires, observations, historique...")
        
        submitted = st.form_submit_button("âœ… Mettre Ã  jour Contact" if edit_mode else "âœ… Enregistrer Contact")
        
        if submitted:
            # Validation
            if not nom or not prenom or not email or not telephone or not type_contact or not source or not statut:
                st.error("âŒ Veuillez remplir tous les champs obligatoires (*)")
            else:
                if edit_mode:
                    # Mise Ã  jour du contact existant
                    idx = df_contacts[df_contacts['ID'] == st.session_state.edit_contact_id].index[0]
                    df_contacts.loc[idx] = [
                        st.session_state.edit_contact_id, nom, prenom, genre, titre, societe, secteur,
                        email, telephone, ville, pays, type_contact, source, statut, linkedin, notes, date_creation
                    ]
                    save_data(df_contacts, FILES["contacts"])
                    st.success(f"âœ… Contact {st.session_state.edit_contact_id} mis Ã  jour !")
                    # Reset du mode modification
                    del st.session_state.edit_contact_id
                    st.rerun()
                else:
                    # CrÃ©ation d'un nouveau contact
                    new_id = generate_id("CNT", df_contacts)
                    new_contact = {
                        'ID': new_id, 'Nom': nom, 'PrÃ©nom': prenom, 'Genre': genre, 'Titre': titre,
                        'SociÃ©tÃ©': societe, 'Secteur': secteur, 'Email': email, 'TÃ©lÃ©phone': telephone,
                        'Ville': ville, 'Pays': pays, 'Type_Contact': type_contact, 'Source': source,
                        'Statut_Engagement': statut, 'LinkedIn': linkedin, 'Notes': notes, 'Date_Creation': date_creation
                    }
                    df_contacts = pd.concat([df_contacts, pd.DataFrame([new_contact])], ignore_index=True)
                    save_data(df_contacts, FILES["contacts"])
                    st.success(f"âœ… Contact '{nom} {prenom}' enregistrÃ© avec l'ID {new_id} !")
                    st.balloons()
    
    # Bouton d'annulation en mode modification
    if edit_mode:
        if st.button("âŒ Annuler la modification"):
            del st.session_state.edit_contact_id
            st.rerun()

# --- PAGE 3 : INTERACTIONS (amÃ©liorÃ©e) ---
elif choice == "Interactions":
    st.title("ðŸ’¬ Gestion des Interactions")
    
    df_interactions = load_data(FILES["interactions"])
    df_contacts = load_data(FILES["contacts"])
    
    contact_options = [""] + [f"{row['ID']} - {row['Nom']} {row['PrÃ©nom']}" for _, row in df_contacts.iterrows()] if not df_contacts.empty else [""]
    
    with st.form("form_interactions"):
        st.subheader("âž• Ajouter une nouvelle interaction")
        
        col1, col2 = st.columns(2)
        
        with col1:
            contact_choice = st.selectbox("Contact*", contact_options,
                                        help="SÃ©lectionnez le contact concernÃ© par cette interaction")
            
            date_interaction = st.date_input("Date de l'interaction*", datetime.now(),
                                           help="Date Ã  laquelle l'interaction a eu lieu")
            
            canal = st.selectbox("Canal de communication*", [""] + settings["canaux"],
                               help="Moyen utilisÃ© pour cette interaction")
            
            objet = st.text_input("Titre de l'interaction*",
                                help="Titre ou objet principal de l'interaction")
        
        with col2:
            resume = st.text_area("RÃ©sumÃ© de l'interaction*",
                                help="Description dÃ©taillÃ©e de ce qui s'est dit/fait")
            
            resultat = st.selectbox("RÃ©sultat", ["", "Positif", "NÃ©gatif", "Neutre", "Ã€ relancer"],
                                  help="Ã‰valuation du rÃ©sultat de cette interaction")
            
            responsable = st.text_input("Responsable IIBA*",
                                      help="Membre IIBA qui a menÃ© cette interaction")
            
            prochaine_action = st.text_area("Prochaine action prÃ©vue",
                                          help="Actions Ã  entreprendre suite Ã  cette interaction")
            
            relance = st.date_input("Date de relance (optionnelle)", value=None,
                                  help="Date Ã  laquelle relancer ce contact")
        
        submitted = st.form_submit_button("âœ… Enregistrer Interaction")
        
        if submitted:
            if not contact_choice or not objet or not resume or not responsable or not canal:
                st.error("âŒ Veuillez remplir tous les champs obligatoires (*)")
            else:
                id_contact = contact_choice.split(" - ")[0]
                new_id = generate_id("INT", df_interactions)
                
                new_interaction = {
                    'ID_Interaction': new_id, 'ID_Contact': id_contact,
                    'Date': date_interaction.strftime("%d/%m/%Y"), 'Canal': canal,
                    'Objet': objet, 'RÃ©sumÃ©': resume, 'RÃ©sultat': resultat,
                    'Responsable': responsable, 'Prochaine_Action': prochaine_action,
                    'Relance': relance.strftime("%d/%m/%Y") if relance else ""
                }
                
                df_interactions = pd.concat([df_interactions, pd.DataFrame([new_interaction])], ignore_index=True)
                save_data(df_interactions, FILES["interactions"])
                st.success(f"âœ… Interaction {new_id} enregistrÃ©e !")

# --- PAGE PARAMÃˆTRES ---
elif choice == "ParamÃ¨tres":
    st.title("âš™ï¸ Configuration des ParamÃ¨tres")
    st.write("GÃ©rez ici les listes de valeurs utilisÃ©es dans les formulaires.")
    
    settings = load_settings()
    
    # Interface pour modifier les paramÃ¨tres
    tab1, tab2, tab3, tab4 = st.tabs(["Types & Statuts", "Secteurs & Pays", "Communication", "Ã‰vÃ©nements"])
    
    with tab1:
        st.subheader("Types de Contact")
        types_contact = st.text_area("Types de Contact (un par ligne)", 
                                    value="\n".join(settings["types_contact"]))
        
        st.subheader("Sources")
        sources = st.text_area("Sources (un par ligne)", 
                              value="\n".join(settings["sources"]))
        
        st.subheader("Statuts d'Engagement")
        statuts = st.text_area("Statuts d'Engagement (un par ligne)", 
                              value="\n".join(settings["statuts_engagement"]))
    
    with tab2:
        st.subheader("Secteurs d'ActivitÃ©")
        secteurs = st.text_area("Secteurs (un par ligne)", 
                               value="\n".join(settings["secteurs"]))
        
        st.subheader("Pays")
        pays = st.text_area("Pays (un par ligne)", 
                           value="\n".join(settings["pays"]))
    
    with tab3:
        st.subheader("Canaux de Communication")
        canaux = st.text_area("Canaux (un par ligne)", 
                             value="\n".join(settings["canaux"]))
    
    with tab4:
        st.subheader("Types d'Ã‰vÃ©nements")
        types_events = st.text_area("Types d'Ã‰vÃ©nements (un par ligne)", 
                                   value="\n".join(settings["types_evenements"]))
        
        st.subheader("Moyens de Paiement")
        moyens_paiement = st.text_area("Moyens de Paiement (un par ligne)", 
                                      value="\n".join(settings["moyens_paiement"]))
    
    if st.button("ðŸ’¾ Sauvegarder les ParamÃ¨tres"):
        new_settings = {
            "types_contact": [x.strip() for x in types_contact.split('\n') if x.strip()],
            "sources": [x.strip() for x in sources.split('\n') if x.strip()],
            "statuts_engagement": [x.strip() for x in statuts.split('\n') if x.strip()],
            "secteurs": [x.strip() for x in secteurs.split('\n') if x.strip()],
            "pays": [x.strip() for x in pays.split('\n') if x.strip()],
            "canaux": [x.strip() for x in canaux.split('\n') if x.strip()],
            "types_evenements": [x.strip() for x in types_events.split('\n') if x.strip()],
            "moyens_paiement": [x.strip() for x in moyens_paiement.split('\n') if x.strip()]
        }
        save_settings(new_settings)
        st.success("âœ… ParamÃ¨tres sauvegardÃ©s !")
        st.rerun()

# --- SIDEBAR STATISTIQUES ENRICHIES ---
st.sidebar.markdown("---")
st.sidebar.subheader("ðŸ“Š Statistiques Rapides")

df_contacts = load_data(FILES["contacts"])
df_interactions = load_data(FILES["interactions"])
df_paiements = load_data(FILES["paiements"])

st.sidebar.metric("ðŸ‘¥ Total Contacts", len(df_contacts))
st.sidebar.metric("ðŸ’¬ Total Interactions", len(df_interactions))
if not df_contacts.empty:
    prospects = len(df_contacts[df_contacts['Type_Contact'] == 'Prospect'])
    st.sidebar.metric("ðŸŽ¯ Prospects", prospects)
if not df_paiements.empty:
    ca_sidebar = df_paiements[df_paiements['Statut'] == 'PayÃ©']['Montant'].sum()
    st.sidebar.metric("ðŸ’° CA RÃ©glÃ©", f"{ca_sidebar:,.0f}")

st.sidebar.markdown("---")
st.sidebar.info("ðŸ’¡ **Aide** : Survolez les champs avec (?) pour voir les descriptions dÃ©taillÃ©es")

# Redirection depuis Dashboard si nÃ©cessaire
if hasattr(st.session_state, 'page_redirect'):
    del st.session_state.page_redirect
