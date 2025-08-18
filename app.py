import streamlit as st
import pandas as pd
import os, json, hashlib, re
from datetime import datetime, date, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder
import io, openpyxl, traceback, logging
from typing import Optional, Dict, Any

# --- CONFIGURATION ---
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon="📊", layout="wide")

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CSS MODERNE ---
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #1f4e79 0%, #2e86de 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 2rem;
    }
    
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #2e86de;
    }
    
    .alert-success {
        background-color: #d4edda;
        color: #155724;
        padding: 0.75rem;
        border-radius: 0.375rem;
        border: 1px solid #c3e6cb;
    }
    
    .alert-error {
        background-color: #f8d7da;
        color: #721c24;
        padding: 0.75rem;
        border-radius: 0.375rem;
        border: 1px solid #f5c6cb;
    }
</style>
""", unsafe_allow_html=True)

# --- CONSTANTES ---
DATA_FILES = {
    "contacts": "contacts.csv",
    "interactions": "interactions.csv", 
    "evenements": "evenements.csv",
    "participations": "participations.csv",
    "paiements": "paiements.csv",
    "certifications": "certifications.csv",
    "settings": "settings.json",
    "users": "users.json"
}

DEFAULT_SETTINGS = {
    "statuts_paiement": ["Réglé", "Partiel", "Non payé"],
    "resultats_inter": ["Positif", "Négatif", "Neutre", "À relancer", "À suivre", "Sans suite"],
    "types_contact": ["Membre", "Prospect", "Formateur", "Partenaire"],
    "sources": ["Afterwork", "Formation", "LinkedIn", "Recommandation", "Site Web", "Salon", "Autre"],
    "statuts_engagement": ["Actif", "Inactif", "À relancer"],
    "secteurs": ["IT", "Finance", "Éducation", "Santé", "Consulting", "Autre"],
    "pays": ["Cameroun", "France", "Canada", "Belgique", "Autre"],
    "canaux": ["Email", "Téléphone", "WhatsApp", "LinkedIn", "Réunion", "Autre"],
    "types_evenements": ["Atelier", "Conférence", "Formation", "Webinaire", "Afterwork", "BA MEET UP", "Groupe d'étude"],
    "moyens_paiement": ["Chèque", "Espèces", "Virement", "CB", "Mobile Money", "Autre"]
}

# --- CORRECTION CRITIQUE: MAPPING DES FEUILLES EXCEL ---
def get_excel_sheet_mapping():
    """Mapping correct entre les clés de schéma et les noms de feuilles Excel"""
    return {
        "contacts": "Contacts",
        "interactions": "Interactions", 
        "evenements": "Événements",  # CORRECTION: avec accent comme dans Excel
        "participations": "Participations",
        "paiements": "Paiements",
        "certifications": "Certifications"
    }

# --- FONCTIONS UTILITAIRES ---

def hash_password(password: str) -> str:
    """Hash un mot de passe avec SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

def show_success(message: str):
    """Affiche un message de succès stylé"""
    st.markdown(f'<div class="alert-success">{message}</div>', unsafe_allow_html=True)

def show_error(message: str):
    """Affiche un message d'erreur stylé"""
    st.markdown(f'<div class="alert-error">{message}</div>', unsafe_allow_html=True)

@st.cache_data
def load_settings() -> Dict[str, Any]:
    """Charge les paramètres de configuration"""
    try:
        if os.path.exists(DATA_FILES["settings"]):
            with open(DATA_FILES["settings"], "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            save_settings(DEFAULT_SETTINGS)
            return DEFAULT_SETTINGS
    except Exception as e:
        logger.error(f"Erreur chargement settings: {e}")
        return DEFAULT_SETTINGS

def save_settings(settings: Dict[str, Any]):
    """Sauvegarde les paramètres"""
    try:
        with open(DATA_FILES["settings"], "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=2, ensure_ascii=False)
        st.cache_data.clear()
        logger.info("Paramètres sauvegardés")
    except Exception as e:
        logger.error(f"Erreur sauvegarde settings: {e}")
        st.error(f"Erreur lors de la sauvegarde: {e}")

def load_users() -> Dict[str, str]:
    """Charge les utilisateurs"""
    try:
        if os.path.exists(DATA_FILES["users"]):
            with open(DATA_FILES["users"], "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            default_users = {"admin": hash_password("iiba2024")}
            with open(DATA_FILES["users"], "w", encoding="utf-8") as f:
                json.dump(default_users, f)
            return default_users
    except Exception as e:
        logger.error(f"Erreur chargement users: {e}")
        return {"admin": hash_password("iiba2024")}

def generate_id(prefix: str, df: pd.DataFrame, col: str) -> str:
    """Génère un ID unique avec préfixe"""
    try:
        if df.empty:
            return f"{prefix}_001"
        
        nums = []
        for x in df[col]:
            if isinstance(x, str) and "_" in x:
                try:
                    nums.append(int(x.split("_")[1]))
                except (ValueError, IndexError):
                    continue
        
        n = max(nums) if nums else 0
        return f"{prefix}_{n+1:03d}"
    except Exception as e:
        logger.error(f"Erreur génération ID: {e}")
        return f"{prefix}_001"

def safe_load_df(file: str, cols: Dict[str, Any]) -> pd.DataFrame:
    """Charge un DataFrame de façon sécurisée"""
    try:
        if os.path.exists(file):
            df = pd.read_csv(file, encoding="utf-8")
            for c, v in cols.items():
                if c not in df.columns:
                    df[c] = v() if callable(v) else v
            return df[list(cols.keys())]
        else:
            return pd.DataFrame(columns=list(cols.keys()))
    except Exception as e:
        logger.error(f"Erreur chargement {file}: {e}")
        return pd.DataFrame(columns=list(cols.keys()))

def safe_save_df(df: pd.DataFrame, file: str):
    """Sauvegarde un DataFrame de façon sécurisée"""
    try:
        df.to_csv(file, index=False, encoding="utf-8")
        logger.info(f"Fichier sauvegardé: {file}")
    except Exception as e:
        logger.error(f"Erreur sauvegarde {file}: {e}")
        st.error(f"Erreur lors de la sauvegarde: {e}")

# --- AUTHENTIFICATION ---
def check_authentication():
    """Vérifie l'authentification utilisateur"""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.markdown('<div class="main-header"><h1>🔐 Connexion IIBA Cameroun CRM</h1></div>', unsafe_allow_html=True)
        
        with st.form("login_form"):
            username = st.text_input("Nom d'utilisateur")
            password = st.text_input("Mot de passe", type="password")
            submit = st.form_submit_button("Se connecter")
            
            if submit:
                users = load_users()
                if username in users and users[username] == hash_password(password):
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    logger.info(f"Connexion réussie pour {username}")
                    st.rerun()
                else:
                    show_error("Nom d'utilisateur ou mot de passe incorrect")
        
        st.info("💡 Utilisateur par défaut: admin / Mot de passe: iiba2024")
        return False
    return True

# --- INITIALISATION ---
if not check_authentication():
    st.stop()

SET = load_settings()

# Schémas des données
def get_schemas():
    return {
        "contacts": {
            "ID": lambda: None, "Nom": "", "Prénom": "", "Genre": "", "Titre": "",
            "Société": "", "Secteur": SET['secteurs'][0], "Email": "", "Téléphone": "",
            "Ville": "", "Pays": SET['pays'][0], "Type": SET['types_contact'][0], 
            "Source": SET['sources'][0], "Statut": SET['statuts_paiement'][0], 
            "LinkedIn": "", "Notes": "", "Date_Creation": lambda: date.today().isoformat()
        },
        "interactions": {
            "ID_Interaction": lambda: None, "ID": "", "Date": date.today().isoformat(), 
            "Canal": SET['canaux'][0], "Objet": "", "Résumé": "", 
            "Résultat": SET['resultats_inter'][0], "Responsable": "",
            "Prochaine_Action": "", "Relance": ""
        },
        "evenements": {
            "ID_Événement": lambda: None, "Nom_Événement": "", "Type": SET['types_evenements'][0], 
            "Date": date.today().isoformat(), "Durée_h": 0.0, "Lieu": "",
            "Formateur(s)": "", "Invité(s)": "", "Objectif": "", "Période": "Matinée",
            "Notes": "", "Coût_Total": 0.0, "Recettes": 0.0, "Bénéfice": 0.0
        },
        "participations": {
            "ID_Participation": lambda: None, "ID": "", "ID_Événement": "", "Rôle": "Participant",
            "Inscription": date.today().isoformat(), "Arrivée": "", "Temps_Present": "AUTO", 
            "Feedback": 3, "Note": 0, "Commentaire": "", "Nom Participant": "", "Nom Événement": ""
        },
        "paiements": {
            "ID_Paiement": lambda: None, "ID": "", "ID_Événement": "", 
            "Date_Paiement": date.today().isoformat(), "Montant": 0.0, 
            "Moyen": SET['moyens_paiement'][0], "Statut": SET['statuts_paiement'][0],
            "Référence": "", "Notes": "", "Relance": "", "Nom Contact": "", "Nom Événement": ""
        },
        "certifications": {
            "ID_Certif": lambda: None, "ID": "", "Type_Certif": SET['types_contact'][0], 
            "Date_Examen": date.today().isoformat(), "Résultat": "Réussi", "Score": 0,
            "Date_Obtention": date.today().isoformat(), "Validité": "", "Renouvellement": "",
            "Notes": "", "Nom Contact": ""
        }
    }

SCHEMAS = get_schemas()

# --- NAVIGATION ---
def handle_navigation():
    """Gère la navigation entre les pages"""
    if "redirect_page" in st.session_state:
        return st.session_state.pop("redirect_page")
    
    with st.sidebar:
        st.markdown(f"👤 **{st.session_state.username}**")
        if st.button("🚪 Déconnexion"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
        st.markdown("---")
        
    return st.sidebar.selectbox(
        "📋 Navigation", 
        ["Dashboard", "Contacts", "Migration", "Paramètres"]
    )

page = handle_navigation()

# --- PAGES PRINCIPALES ---

if page == "Dashboard":
    st.markdown('<div class="main-header"><h1>📈 Tableau de Bord IIBA Cameroun</h1></div>', unsafe_allow_html=True)
    
    dfc = safe_load_df(DATA_FILES["contacts"], SCHEMAS["contacts"])
    st.metric("📊 Total Contacts", len(dfc))
    
    if st.button("⬇️ Export CSV"):
        csv_data = dfc.to_csv(index=False)
        st.download_button("Télécharger", csv_data, file_name="contacts.csv")

elif page == "Contacts":
    st.markdown('<div class="main-header"><h1>👤 Gestion des Contacts</h1></div>', unsafe_allow_html=True)
    
    df = safe_load_df(DATA_FILES["contacts"], SCHEMAS["contacts"])
    
    with st.form("form_contact"):
        col1, col2 = st.columns(2)
        with col1:
            nom = st.text_input("Nom *")
            prenom = st.text_input("Prénom *")
            email = st.text_input("Email")
        with col2:
            telephone = st.text_input("Téléphone")
            societe = st.text_input("Société")
            type_contact = st.selectbox("Type", SET["types_contact"])
        
        submit = st.form_submit_button("💾 Enregistrer")
        
        if submit and nom and prenom:
            try:
                new_id = generate_id("CNT", df, "ID")
                new_record = {
                    "ID": new_id, "Nom": nom, "Prénom": prenom, "Genre": "", 
                    "Titre": "", "Société": societe, "Secteur": SET['secteurs'][0],
                    "Email": email, "Téléphone": telephone, "Ville": "", 
                    "Pays": SET['pays'][0], "Type": type_contact, "Source": SET['sources'][0],
                    "Statut": SET['statuts_paiement'][0], "LinkedIn": "", "Notes": "",
                    "Date_Creation": date.today().isoformat()
                }
                df = pd.concat([df, pd.DataFrame([new_record])], ignore_index=True)
                safe_save_df(df, DATA_FILES["contacts"])
                show_success("Contact créé avec succès!")
            except Exception as e:
                show_error(f"Erreur: {e}")
    
    if not df.empty:
        st.dataframe(df, use_container_width=True)

elif page == "Migration":
    st.markdown('<div class="main-header"><h1>📦 Migration et Import</h1></div>', unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["📥 Template", "📤 Import"])
    
    with tab1:
        st.header("Télécharger le template Excel")
        
        try:
            # CORRECTION: Utiliser le mapping correct pour le template
            sheet_mapping = get_excel_sheet_mapping()
            output = io.BytesIO()
            
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                for schema_key, schema in SCHEMAS.items():
                    # Utiliser le nom de feuille correct avec accent
                    sheet_name = sheet_mapping[schema_key]
                    df_template = pd.DataFrame(columns=list(schema.keys()))
                    df_template.to_excel(writer, sheet_name=sheet_name, index=False)
            
            output.seek(0)
            st.download_button(
                label="📥 Télécharger template Excel",
                data=output,
                file_name=f"template_iiba_cameroun_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            show_success("✅ Template généré avec les noms de feuilles corrects!")
            
        except Exception as e:
            logger.error(f"Erreur génération template: {e}")
            show_error("Erreur lors de la génération du template")
    
    with tab2:
        st.header("Importer un fichier Excel")
        
        uploaded_file = st.file_uploader("📁 Fichier Excel", type=["xlsx"])
        
        if uploaded_file:
            try:
                wb = openpyxl.load_workbook(uploaded_file)
                
                # CORRECTION CRITIQUE: Utiliser le mapping correct
                sheet_mapping = get_excel_sheet_mapping()
                required_sheets = {}
                
                for schema_key, schema in SCHEMAS.items():
                    excel_sheet_name = sheet_mapping[schema_key]
                    required_sheets[excel_sheet_name] = {"schema": schema, "key": schema_key}
                
                missing_sheets = [s for s in required_sheets if s not in wb.sheetnames]
                
                if missing_sheets:
                    show_error(f"Feuilles manquantes dans le fichier: {missing_sheets}")
                    st.info(f"Feuilles trouvées: {wb.sheetnames}")
                    st.info(f"Feuilles attendues: {list(required_sheets.keys())}")
                else:
                    show_success("✅ Toutes les feuilles requises sont présentes!")
                    
                    data_to_import = {}
                    validation_errors = []
                    
                    # Validation des données
                    for sheet_name, sheet_info in required_sheets.items():
                        try:
                            df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
                            schema = sheet_info["schema"]
                            schema_key = sheet_info["key"]
                            
                            missing_cols = [c for c in schema.keys() if c not in df.columns]
                            
                            if missing_cols:
                                validation_errors.append(f"Feuille {sheet_name}: colonnes manquantes {missing_cols}")
                            else:
                                data_to_import[schema_key] = df
                                
                        except Exception as e:
                            validation_errors.append(f"Erreur lecture feuille {sheet_name}: {e}")
                    
                    if validation_errors:
                        for error in validation_errors:
                            show_error(error)
                    else:
                        show_success("✅ Fichier validé avec succès!")
                        
                        # Aperçu des données
                        for schema_key, df in data_to_import.items():
                            with st.expander(f"Aperçu - {schema_key.capitalize()} ({len(df)} lignes)"):
                                st.dataframe(df.head(10))
                        
                        # Confirmation d'import
                        if st.button("🚀 Confirmer l'import"):
                            success_count = 0
                            
                            try:
                                for schema_key, new_df in data_to_import.items():
                                    if schema_key in DATA_FILES:
                                        existing_df = safe_load_df(DATA_FILES[schema_key], SCHEMAS[schema_key])
                                        
                                        # Simple concaténation pour cette version
                                        if not existing_df.empty:
                                            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                                        else:
                                            combined_df = new_df
                                        
                                        safe_save_df(combined_df, DATA_FILES[schema_key])
                                        success_count += 1
                                        logger.info(f"Import réussi pour {schema_key}")
                                
                                show_success(f"🎉 Import terminé! {success_count} feuilles importées.")
                                
                            except Exception as e:
                                logger.error(f"Erreur import: {e}")
                                show_error(f"Erreur lors de l'import: {e}")
                
            except Exception as e:
                logger.error(f"Erreur traitement fichier: {e}")
                show_error(f"Erreur lors du traitement: {e}")

elif page == "Paramètres":
    st.markdown('<div class="main-header"><h1>⚙️ Paramètres</h1></div>', unsafe_allow_html=True)
    
    st.subheader("Configuration des référentiels")
    
    col1, col2 = st.columns(2)
    
    with col1:
        types_contact = st.text_area("Types de contact", "\n".join(SET["types_contact"]))
        sources = st.text_area("Sources", "\n".join(SET["sources"]))
        
    with col2:
        secteurs = st.text_area("Secteurs", "\n".join(SET["secteurs"]))
        pays = st.text_area("Pays", "\n".join(SET["pays"]))
    
    if st.button("💾 Sauvegarder"):
        try:
            new_settings = {
                **SET,
                "types_contact": [s.strip() for s in types_contact.split("\n") if s.strip()],
                "sources": [s.strip() for s in sources.split("\n") if s.strip()],
                "secteurs": [s.strip() for s in secteurs.split("\n") if s.strip()],
                "pays": [s.strip() for s in pays.split("\n") if s.strip()]
            }
            
            save_settings(new_settings)
            show_success("✅ Paramètres sauvegardés!")
            st.rerun()
            
        except Exception as e:
            logger.error(f"Erreur sauvegarde: {e}")
            show_error(f"Erreur: {e}")

# Footer
st.markdown("---")
st.markdown("**IIBA Cameroun CRM** - Version Corrigée | Migration des accents résolue ✅")
