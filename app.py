import streamlit as st
import pandas as pd
import os
from datetime import datetime

# --- CONFIGURATION ---
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon="📊", layout="wide")

# Fichiers de données CSV
FILES = {
    "contacts": "contacts.csv",
    "interactions": "interactions.csv", 
    "evenements": "evenements.csv",
    "participations": "participations.csv",
    "paiements": "paiements.csv",
    "certifications": "certifications.csv"
}

# --- FONCTIONS DE SAUVEGARDE ---
@st.cache_data
def load_data(file_path):
    """Charge les données depuis un fichier CSV"""
    if os.path.exists(file_path):
        return pd.read_csv(file_path, encoding='utf-8')
    else:
        # Retourne un DataFrame vide avec les bonnes colonnes
        if "contacts" in file_path:
            return pd.DataFrame(columns=['ID', 'Nom', 'Prénom', 'Genre', 'Titre', 'Société', 'Secteur', 
                                        'Email', 'Téléphone', 'Ville', 'Pays', 'LinkedIn', 'Notes', 
                                        'Attentes', 'Date_Creation'])
        elif "interactions" in file_path:
            return pd.DataFrame(columns=['ID_Interaction', 'ID_Contact', 'Date', 'Canal', 'Objet', 
                                        'Résumé', 'Résultat', 'Responsable', 'Prochaine_Action', 'Relance'])
        # Ajouter les autres structures selon les besoins
        return pd.DataFrame()

def save_data(data, file_path):
    """Sauvegarde les données dans un fichier CSV"""
    data.to_csv(file_path, index=False, encoding='utf-8')
    st.cache_data.clear()  # Rafraîchit le cache

def generate_id(prefix, existing_df):
    """Génère un nouvel ID unique"""
    if len(existing_df) == 0:
        return f"{prefix}_001"
    last_id = existing_df['ID'].iloc[-1] if 'ID' in existing_df.columns else f"{prefix}_000"
    num = int(last_id.split('_')[1]) + 1
    return f"{prefix}_{num:03d}"

# --- STYLES CSS ---
st.markdown("""
<style>
.search-box {background-color: #f0f2f6; padding: 10px; border-radius: 10px; margin-bottom: 20px;}
.contact-card {background-color: white; padding: 15px; border-radius: 10px; margin: 5px; border-left: 4px solid #1f77b4;}
.success-msg {color: green; font-weight: bold;}
</style>
""", unsafe_allow_html=True)

# --- NAVIGATION ---
PAGES = ["Dashboard 360", "Contacts", "Interactions", "Événements", "Participations", "Paiements", "Certifications"]
choice = st.sidebar.selectbox("Navigation IIBA CRM", PAGES)

# --- PAGE 1 : DASHBOARD 360 (avec recherche) ---
if choice == "Dashboard 360":
    st.title("🎯 Dashboard 360 - IIBA Cameroun")
    
    # Chargement des données
    df_contacts = load_data(FILES["contacts"])
    
    # Section recherche
    st.markdown('<div class="search-box">', unsafe_allow_html=True)
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        search_term = st.text_input("🔍 Rechercher un contact (nom, société, email...)", "")
    with col2:
        secteur_filter = st.selectbox("Filtrer par secteur", ["Tous"] + df_contacts['Secteur'].dropna().unique().tolist() if not df_contacts.empty else ["Tous"])
    with col3:
        pays_filter = st.selectbox("Filtrer par pays", ["Tous"] + df_contacts['Pays'].dropna().unique().tolist() if not df_contacts.empty else ["Tous"])
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Filtrage des données
    if not df_contacts.empty:
        filtered_df = df_contacts.copy()
        
        # Filtre par terme de recherche
        if search_term:
            mask = (
                filtered_df['Nom'].str.contains(search_term, case=False, na=False) |
                filtered_df['Prénom'].str.contains(search_term, case=False, na=False) |
                filtered_df['Société'].str.contains(search_term, case=False, na=False) |
                filtered_df['Email'].str.contains(search_term, case=False, na=False)
            )
            filtered_df = filtered_df[mask]
        
        # Filtres par dropdowns
        if secteur_filter != "Tous":
            filtered_df = filtered_df[filtered_df['Secteur'] == secteur_filter]
        if pays_filter != "Tous":
            filtered_df = filtered_df[filtered_df['Pays'] == pays_filter]
        
        # Affichage des résultats
        st.subheader(f"📋 Liste des contacts ({len(filtered_df)} résultat(s))")
        
        if len(filtered_df) > 0:
            # Affichage sous forme de tableau
            st.dataframe(
                filtered_df[['ID', 'Nom', 'Prénom', 'Société', 'Email', 'Téléphone', 'Date_Creation']],
                use_container_width=True
            )
            
            # Option d'export
            csv_data = filtered_df.to_csv(index=False, encoding='utf-8')
            st.download_button(
                label="📥 Télécharger la liste (CSV)",
                data=csv_data,
                file_name=f"contacts_iiba_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.info("Aucun contact trouvé avec ces critères.")
    else:
        st.info("Aucun contact enregistré pour le moment. Ajoutez votre premier contact ! 👆")

# --- PAGE 2 : CONTACTS (avec sauvegarde) ---
elif choice == "Contacts":
    st.title("👤 Gestion des Contacts")
    
    # Chargement des contacts existants
    df_contacts = load_data(FILES["contacts"])
    
    with st.form("form_contacts"):
        st.subheader("➕ Ajouter un nouveau contact")
        
        col1, col2 = st.columns(2)
        with col1:
            nom = st.text_input("Nom*", "")
            prenom = st.text_input("Prénom*", "")
            genre = st.selectbox("Genre*", ["", "Homme", "Femme", "Autre"])
            titre = st.text_input("Titre / Fonction")
            societe = st.text_input("Société")
            secteur = st.selectbox("Secteur d'activité", ["", "IT", "Finance", "Éducation", "Santé", "Consulting", "Autre"])
        
        with col2:
            email = st.text_input("Email*", "")
            telephone = st.text_input("Téléphone*", "")
            ville = st.text_input("Ville")
            pays = st.selectbox("Pays", ["", "Cameroun", "France", "Canada", "Autre"])
            linkedin = st.text_input("LinkedIn")
        
        notes = st.text_area("Notes")
        attentes = st.multiselect("Attentes", ["Contact commercial", "Newsletter", "Invitation événement", "Formation", "Certification"])
        
        submitted = st.form_submit_button("✅ Enregistrer Contact")
        
        if submitted:
            # Validation des champs obligatoires
            if not nom or not prenom or not email or not telephone:
                st.error("❌ Veuillez remplir tous les champs obligatoires (*)")
            else:
                # Génération du nouvel ID
                new_id = generate_id("CNT", df_contacts)
                
                # Création du nouveau contact
                new_contact = {
                    'ID': new_id,
                    'Nom': nom,
                    'Prénom': prenom,
                    'Genre': genre,
                    'Titre': titre,
                    'Société': societe,
                    'Secteur': secteur,
                    'Email': email,
                    'Téléphone': telephone,
                    'Ville': ville,
                    'Pays': pays,
                    'LinkedIn': linkedin,
                    'Notes': notes,
                    'Attentes': ", ".join(attentes),
                    'Date_Creation': datetime.now().strftime("%d/%m/%Y %H:%M")
                }
                
                # Ajout au DataFrame
                df_contacts = pd.concat([df_contacts, pd.DataFrame([new_contact])], ignore_index=True)
                
                # Sauvegarde
                save_data(df_contacts, FILES["contacts"])
                
                st.success(f"✅ Contact '{nom} {prenom}' enregistré avec l'ID {new_id} !")
                st.balloons()

# --- PAGE 3 : INTERACTIONS (avec sauvegarde) ---
elif choice == "Interactions":
    st.title("💬 Gestion des Interactions")
    
    # Chargement des données
    df_interactions = load_data(FILES["interactions"])
    df_contacts = load_data(FILES["contacts"])
    
    # Liste des contacts pour le dropdown
    contact_options = [""] + [f"{row['ID']} - {row['Nom']} {row['Prénom']}" for _, row in df_contacts.iterrows()] if not df_contacts.empty else [""]
    
    with st.form("form_interactions"):
        st.subheader("➕ Ajouter une nouvelle interaction")
        
        contact_choice = st.selectbox("Contact*", contact_options)
        date_interaction = st.date_input("Date", datetime.now())
        canal = st.selectbox("Canal", ["", "Email", "Téléphone", "WhatsApp", "LinkedIn", "Réunion", "Autre"])
        objet = st.text_input("Objet*")
        resume = st.text_area("Résumé*")
        resultat = st.selectbox("Résultat", ["", "Positif", "Négatif", "Neutre", "À relancer"])
        responsable = st.text_input("Responsable*")
        prochaine_action = st.text_area("Prochaine action")
        relance = st.date_input("Date de relance (optionnelle)", value=None)
        
        submitted = st.form_submit_button("✅ Enregistrer Interaction")
        
        if submitted:
            if not contact_choice or not objet or not resume or not responsable:
                st.error("❌ Veuillez remplir tous les champs obligatoires (*)")
            else:
                # Extraction de l'ID contact
                id_contact = contact_choice.split(" - ")[0]
                
                # Génération du nouvel ID
                new_id = generate_id("INT", df_interactions)
                
                # Création de la nouvelle interaction
                new_interaction = {
                    'ID_Interaction': new_id,
                    'ID_Contact': id_contact,
                    'Date': date_interaction.strftime("%d/%m/%Y"),
                    'Canal': canal,
                    'Objet': objet,
                    'Résumé': resume,
                    'Résultat': resultat,
                    'Responsable': responsable,
                    'Prochaine_Action': prochaine_action,
                    'Relance': relance.strftime("%d/%m/%Y") if relance else ""
                }
                
                # Ajout au DataFrame
                df_interactions = pd.concat([df_interactions, pd.DataFrame([new_interaction])], ignore_index=True)
                
                # Sauvegarde
                save_data(df_interactions, FILES["interactions"])
                
                st.success(f"✅ Interaction enregistrée avec l'ID {new_id} !")

# Ajouter les autres pages (Événements, Participations, etc.) avec le même principe...

# --- AFFICHAGE DES STATISTIQUES (sidebar) ---
st.sidebar.markdown("---")
st.sidebar.subheader("📊 Statistiques")

# Chargement de toutes les données pour les stats
df_contacts = load_data(FILES["contacts"])
df_interactions = load_data(FILES["interactions"])

st.sidebar.metric("👥 Total Contacts", len(df_contacts))
st.sidebar.metric("💬 Total Interactions", len(df_interactions))
