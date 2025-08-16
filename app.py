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
        with open(FILES["settings"], 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        save_settings(DEFAULT_SETTINGS)
        return DEFAULT_SETTINGS

def save_settings(settings):
    """Sauvegarde les paramètres dans le fichier JSON"""
    with open(FILES["settings"], 'w', encoding='utf-8') as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)
    st.cache_data.clear()

@st.cache_data
def load_data(file_path):
    """Charge les données depuis un fichier CSV"""
    if os.path.exists(file_path):
        return pd.read_csv(file_path, encoding='utf-8')
    else:
        if "contacts" in file_path:
            return pd.DataFrame(columns=['ID', 'Nom', 'Prénom', 'Genre', 'Titre', 'Société', 'Secteur', 
                                        'Email', 'Téléphone', 'Ville', 'Pays', 'Type_Contact', 'Source', 
                                        'Statut_Engagement', 'LinkedIn', 'Notes', 'Date_Creation'])
        elif "interactions" in file_path:
            return pd.DataFrame(columns=['ID_Interaction', 'ID_Contact', 'Date', 'Canal', 'Objet', 
                                        'Résumé', 'Résultat', 'Responsable', 'Prochaine_Action', 'Relance'])
        elif "paiements" in file_path:
            return pd.DataFrame(columns=['ID_Paiement', 'ID_Contact', 'ID_Événement', 'Date_Paiement', 
                                        'Montant', 'Moyen', 'Statut', 'Référence', 'Notes'])
        return pd.DataFrame()

def save_data(data, file_path):
    """Sauvegarde les données dans un fichier CSV"""
    data.to_csv(file_path, index=False, encoding='utf-8')
    st.cache_data.clear()

def generate_id(prefix, existing_df):
    """Génère un nouvel ID unique"""
    if len(existing_df) == 0:
        return f"{prefix}_001"
    last_id = existing_df.iloc[-1, 0] if len(existing_df) > 0 else f"{prefix}_000"
    num = int(str(last_id).split('_')[1]) + 1
    return f"{prefix}_{num:03d}"

# --- STYLES CSS AMÉLIORÉS ---
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
PAGES = ["Dashboard 360", "Contacts", "Interactions", "Événements", "Participations", "Paiements", "Certifications", "Paramètres"]
choice = st.sidebar.selectbox("Navigation IIBA CRM", PAGES)

# Chargement des paramètres
settings = load_settings()

# --- PAGE 1 : DASHBOARD 360 (avec KPI enrichis) ---
if choice == "Dashboard 360":
    st.title("🎯 Dashboard 360 - IIBA Cameroun")
    
    # Chargement des données pour les KPI
    df_contacts = load_data(FILES["contacts"])
    df_interactions = load_data(FILES["interactions"])
    df_paiements = load_data(FILES["paiements"])
    df_certifications = load_data(FILES["certifications"])
    
    # --- KPI CARDS ---
    st.subheader("📊 Indicateurs Clés de Performance")
    
    # Calculs des KPI
    current_year = datetime.now().year
    prospects_actifs = len(df_contacts[df_contacts['Type_Contact'] == 'Prospect']) if not df_contacts.empty else 0
    membres_iiba = len(df_contacts[df_contacts['Type_Contact'] == 'Membre']) if not df_contacts.empty else 0
    ca_total = df_paiements[df_paiements['Statut'] == 'Payé']['Montant'].sum() if not df_paiements.empty else 0
    paiements_attente = len(df_paiements[df_paiements['Statut'] == 'En attente']) if not df_paiements.empty else 0
    certifs_obtenues = len(df_certifications[df_certifications['Résultat'] == 'Réussi']) if not df_certifications.empty else 0
    
    # Affichage des KPI en colonnes
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("👥 Prospects Actifs", prospects_actifs)
        st.metric("🏆 Membres IIBA", membres_iiba)
    
    with col2:
        st.metric(f"📅 Événements {current_year}", 0)  # À calculer selon tes données
        st.metric("🎓 Participations Totales", 0)  # À calculer
    
    with col3:
        st.metric("💰 CA Total Réglé", f"{ca_total:,.0f} FCFA")
        st.metric("⏳ Paiements en Attente", paiements_attente)
    
    with col4:
        st.metric("🎯 Certifications Obtenues", certifs_obtenues)
        st.metric("📈 Score Moyen Engagement", "N/A")  # À calculer
    
    # --- SECTION RECHERCHE ---
    st.markdown('<div class="search-box">', unsafe_allow_html=True)
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        search_term = st.text_input("🔍 Rechercher un contact", "", 
                                   help="Tapez un nom, société, email ou téléphone pour rechercher")
    with col2:
        secteur_filter = st.selectbox("Filtrer par secteur", 
                                     ["Tous"] + settings["secteurs"])
    with col3:
        type_filter = st.selectbox("Filtrer par type", 
                                  ["Tous"] + settings["types_contact"])
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Filtrage et affichage des résultats
    if not df_contacts.empty:
        filtered_df = df_contacts.copy()
        
        if search_term:
            mask = (
                filtered_df['Nom'].str.contains(search_term, case=False, na=False) |
                filtered_df['Prénom'].str.contains(search_term, case=False, na=False) |
                filtered_df['Société'].str.contains(search_term, case=False, na=False) |
                filtered_df['Email'].str.contains(search_term, case=False, na=False)
            )
            filtered_df = filtered_df[mask]
        
        if secteur_filter != "Tous":
            filtered_df = filtered_df[filtered_df['Secteur'] == secteur_filter]
        if type_filter != "Tous":
            filtered_df = filtered_df[filtered_df['Type_Contact'] == type_filter]
        
        st.subheader(f"📋 Liste des contacts ({len(filtered_df)} résultat(s))")
        
        if len(filtered_df) > 0:
            # Sélection d'un contact pour modification
            selected_contact = st.selectbox("Sélectionner un contact à modifier", 
                                           [""] + filtered_df['ID'].tolist())
            
            if selected_contact:
                st.session_state.edit_contact_id = selected_contact
                st.session_state.page_redirect = "Contacts"
                st.rerun()
            
            st.dataframe(
                filtered_df[['ID', 'Nom', 'Prénom', 'Société', 'Type_Contact', 'Email', 'Téléphone']],
                use_container_width=True
            )
        else:
            st.info("Aucun contact trouvé avec ces critères.")
    else:
        st.info("Aucun contact enregistré. Ajoutez votre premier contact ! 👆")

# --- PAGE 2 : CONTACTS (avec modification) ---
elif choice == "Contacts":
    st.title("👤 Gestion des Contacts")
    
    df_contacts = load_data(FILES["contacts"])
    
    # Mode modification si un contact est sélectionné
    edit_mode = False
    edit_contact = None
    
    if hasattr(st.session_state, 'edit_contact_id') and st.session_state.edit_contact_id:
        edit_mode = True
        edit_contact = df_contacts[df_contacts['ID'] == st.session_state.edit_contact_id].iloc[0]
        st.info(f"✏️ Modification du contact {edit_contact['ID']} - {edit_contact['Nom']} {edit_contact['Prénom']}")
    
    with st.form("form_contacts"):
        if edit_mode:
            st.subheader("✏️ Modifier le contact")
        else:
            st.subheader("➕ Ajouter un nouveau contact")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Champ ID en readonly si mode modification
            if edit_mode:
                st.text_input("ID Contact", value=edit_contact['ID'], disabled=True,
                             help="Identifiant unique du contact (non modifiable)")
            
            nom = st.text_input("Nom*", 
                               value=edit_contact['Nom'] if edit_mode else "",
                               help="Nom de famille du contact")
            
            prenom = st.text_input("Prénom*", 
                                  value=edit_contact['Prénom'] if edit_mode else "",
                                  help="Prénom du contact")
            
            genre = st.selectbox("Genre", ["", "Homme", "Femme", "Autre"],
                               index=["", "Homme", "Femme", "Autre"].index(edit_contact['Genre']) if edit_mode and edit_contact['Genre'] in ["", "Homme", "Femme", "Autre"] else 0,
                               help="Genre du contact")
            
            titre = st.text_input("Titre/Position", 
                                 value=edit_contact['Titre'] if edit_mode else "",
                                 help="Fonction ou titre professionnel")
            
            societe = st.text_input("Société", 
                                   value=edit_contact['Société'] if edit_mode else "",
                                   help="Nom de l'entreprise où travaille le contact")
            
            secteur = st.selectbox("Secteur d'activité", 
                                  [""] + settings["secteurs"],
                                  index=([""] + settings["secteurs"]).index(edit_contact['Secteur']) if edit_mode and edit_contact['Secteur'] in settings["secteurs"] else 0,
                                  help="Domaine d'activité de l'entreprise")
            
            type_contact = st.selectbox("Type de Contact*", 
                                       [""] + settings["types_contact"],
                                       index=([""] + settings["types_contact"]).index(edit_contact['Type_Contact']) if edit_mode and edit_contact['Type_Contact'] in settings["types_contact"] else 0,
                                       help="Catégorie du contact : Membre, Prospect, Formateur ou Partenaire")
        
        with col2:
            email = st.text_input("Email*", 
                                 value=edit_contact['Email'] if edit_mode else "",
                                 help="Adresse email principale du contact")
            
            telephone = st.text_input("Téléphone*", 
                                     value=edit_contact['Téléphone'] if edit_mode else "",
                                     help="Numéro de téléphone (format: +237XXXXXXXXX)")
            
            ville = st.text_input("Ville", 
                                 value=edit_contact['Ville'] if edit_mode else "",
                                 help="Ville de résidence ou de travail")
            
            pays = st.selectbox("Pays", 
                               [""] + settings["pays"],
                               index=([""] + settings["pays"]).index(edit_contact['Pays']) if edit_mode and edit_contact['Pays'] in settings["pays"] else 0,
                               help="Pays de résidence")
            
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
            
            # Date de création - modifiable seulement en mode modification pour migration
            if edit_mode:
                date_creation = st.text_input("Date de Création", 
                                            value=edit_contact['Date_Creation'],
                                            help="Date de création (modifiable pour migration de données)")
            else:
                date_creation = datetime.now().strftime("%d/%m/%Y %H:%M")
        
        notes = st.text_area("Notes", 
                           value=edit_contact['Notes'] if edit_mode else "",
                           help="Informations complémentaires, observations, historique...")
        
        submitted = st.form_submit_button("✅ Mettre à jour Contact" if edit_mode else "✅ Enregistrer Contact")
        
        if submitted:
            # Validation
            if not nom or not prenom or not email or not telephone or not type_contact or not source or not statut:
                st.error("❌ Veuillez remplir tous les champs obligatoires (*)")
            else:
                if edit_mode:
                    # Mise à jour du contact existant
                    idx = df_contacts[df_contacts['ID'] == st.session_state.edit_contact_id].index[0]
                    df_contacts.loc[idx] = [
                        st.session_state.edit_contact_id, nom, prenom, genre, titre, societe, secteur,
                        email, telephone, ville, pays, type_contact, source, statut, linkedin, notes, date_creation
                    ]
                    save_data(df_contacts, FILES["contacts"])
                    st.success(f"✅ Contact {st.session_state.edit_contact_id} mis à jour !")
                    # Reset du mode modification
                    del st.session_state.edit_contact_id
                    st.rerun()
                else:
                    # Création d'un nouveau contact
                    new_id = generate_id("CNT", df_contacts)
                    new_contact = {
                        'ID': new_id, 'Nom': nom, 'Prénom': prenom, 'Genre': genre, 'Titre': titre,
                        'Société': societe, 'Secteur': secteur, 'Email': email, 'Téléphone': telephone,
                        'Ville': ville, 'Pays': pays, 'Type_Contact': type_contact, 'Source': source,
                        'Statut_Engagement': statut, 'LinkedIn': linkedin, 'Notes': notes, 'Date_Creation': date_creation
                    }
                    df_contacts = pd.concat([df_contacts, pd.DataFrame([new_contact])], ignore_index=True)
                    save_data(df_contacts, FILES["contacts"])
                    st.success(f"✅ Contact '{nom} {prenom}' enregistré avec l'ID {new_id} !")
                    st.balloons()
    
    # Bouton d'annulation en mode modification
    if edit_mode:
        if st.button("❌ Annuler la modification"):
            del st.session_state.edit_contact_id
            st.rerun()

# --- PAGE 3 : INTERACTIONS (améliorée) ---
elif choice == "Interactions":
    st.title("💬 Gestion des Interactions")
    
    df_interactions = load_data(FILES["interactions"])
    df_contacts = load_data(FILES["contacts"])
    
    contact_options = [""] + [f"{row['ID']} - {row['Nom']} {row['Prénom']}" for _, row in df_contacts.iterrows()] if not df_contacts.empty else [""]
    
    with st.form("form_interactions"):
        st.subheader("➕ Ajouter une nouvelle interaction")
        
        col1, col2 = st.columns(2)
        
        with col1:
            contact_choice = st.selectbox("Contact*", contact_options,
                                        help="Sélectionnez le contact concerné par cette interaction")
            
            date_interaction = st.date_input("Date de l'interaction*", datetime.now(),
                                           help="Date à laquelle l'interaction a eu lieu")
            
            canal = st.selectbox("Canal de communication*", [""] + settings["canaux"],
                               help="Moyen utilisé pour cette interaction")
            
            objet = st.text_input("Titre de l'interaction*",
                                help="Titre ou objet principal de l'interaction")
        
        with col2:
            resume = st.text_area("Résumé de l'interaction*",
                                help="Description détaillée de ce qui s'est dit/fait")
            
            resultat = st.selectbox("Résultat", ["", "Positif", "Négatif", "Neutre", "À relancer"],
                                  help="Évaluation du résultat de cette interaction")
            
            responsable = st.text_input("Responsable IIBA*",
                                      help="Membre IIBA qui a mené cette interaction")
            
            prochaine_action = st.text_area("Prochaine action prévue",
                                          help="Actions à entreprendre suite à cette interaction")
            
            relance = st.date_input("Date de relance (optionnelle)", value=None,
                                  help="Date à laquelle relancer ce contact")
        
        submitted = st.form_submit_button("✅ Enregistrer Interaction")
        
        if submitted:
            if not contact_choice or not objet or not resume or not responsable or not canal:
                st.error("❌ Veuillez remplir tous les champs obligatoires (*)")
            else:
                id_contact = contact_choice.split(" - ")[0]
                new_id = generate_id("INT", df_interactions)
                
                new_interaction = {
                    'ID_Interaction': new_id, 'ID_Contact': id_contact,
                    'Date': date_interaction.strftime("%d/%m/%Y"), 'Canal': canal,
                    'Objet': objet, 'Résumé': resume, 'Résultat': resultat,
                    'Responsable': responsable, 'Prochaine_Action': prochaine_action,
                    'Relance': relance.strftime("%d/%m/%Y") if relance else ""
                }
                
                df_interactions = pd.concat([df_interactions, pd.DataFrame([new_interaction])], ignore_index=True)
                save_data(df_interactions, FILES["interactions"])
                st.success(f"✅ Interaction {new_id} enregistrée !")

# --- PAGE PARAMÈTRES ---
elif choice == "Paramètres":
    st.title("⚙️ Configuration des Paramètres")
    st.write("Gérez ici les listes de valeurs utilisées dans les formulaires.")
    
    settings = load_settings()
    
    # Interface pour modifier les paramètres
    tab1, tab2, tab3, tab4 = st.tabs(["Types & Statuts", "Secteurs & Pays", "Communication", "Événements"])
    
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
        st.subheader("Secteurs d'Activité")
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
        st.subheader("Types d'Événements")
        types_events = st.text_area("Types d'Événements (un par ligne)", 
                                   value="\n".join(settings["types_evenements"]))
        
        st.subheader("Moyens de Paiement")
        moyens_paiement = st.text_area("Moyens de Paiement (un par ligne)", 
                                      value="\n".join(settings["moyens_paiement"]))
    
    if st.button("💾 Sauvegarder les Paramètres"):
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
        st.success("✅ Paramètres sauvegardés !")
        st.rerun()

# --- SIDEBAR STATISTIQUES ENRICHIES ---
st.sidebar.markdown("---")
st.sidebar.subheader("📊 Statistiques Rapides")

df_contacts = load_data(FILES["contacts"])
df_interactions = load_data(FILES["interactions"])
df_paiements = load_data(FILES["paiements"])

st.sidebar.metric("👥 Total Contacts", len(df_contacts))
st.sidebar.metric("💬 Total Interactions", len(df_interactions))
if not df_contacts.empty:
    prospects = len(df_contacts[df_contacts['Type_Contact'] == 'Prospect'])
    st.sidebar.metric("🎯 Prospects", prospects)
if not df_paiements.empty:
    ca_sidebar = df_paiements[df_paiements['Statut'] == 'Payé']['Montant'].sum()
    st.sidebar.metric("💰 CA Réglé", f"{ca_sidebar:,.0f}")

st.sidebar.markdown("---")
st.sidebar.info("💡 **Aide** : Survolez les champs avec (?) pour voir les descriptions détaillées")

# Redirection depuis Dashboard si nécessaire
if hasattr(st.session_state, 'page_redirect'):
    del st.session_state.page_redirect
