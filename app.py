import streamlit as st
import pandas as pd
import os, json, hashlib, re, logging
from datetime import datetime, date
from st_aggrid import AgGrid, GridOptionsBuilder
import io, openpyxl, traceback
from typing import Dict, Any, Optional, List

# ===================== CONFIGURATION =====================
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon="📊", layout="wide")

# Configuration logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# CSS ultra-moderne
st.markdown("""<style>
.main-header {background: linear-gradient(90deg, #1f4e79 0%, #2e86de 100%); padding: 1rem; border-radius: 10px; color: white; margin-bottom: 2rem;}
.metric-card {background: white; padding: 1rem; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); border-left: 4px solid #2e86de;}
.stButton > button {border-radius: 8px; border: none; background: linear-gradient(45deg, #2e86de, #1f4e79); color: white; transition: all 0.3s;}
.stButton > button:hover {transform: translateY(-2px); box-shadow: 0 4px 8px rgba(0,0,0,0.2);}
.alert-success {background-color: #d4edda; color: #155724; padding: 0.75rem; border-radius: 0.375rem; border: 1px solid #c3e6cb;}
.alert-error {background-color: #f8d7da; color: #721c24; padding: 0.75rem; border-radius: 0.375rem; border: 1px solid #f5c6cb;}
</style>""", unsafe_allow_html=True)

# ===================== CONSTANTES ET CONFIGURATION =====================
FILES = {
    "contacts": "contacts.csv", "interactions": "interactions.csv", "evenements": "evenements.csv",
    "participations": "participations.csv", "paiements": "paiements.csv", "certifications": "certifications.csv",
    "settings": "settings.json", "users": "users.json"
}

DEFAULT_CONFIG = {
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

PAGES = ["Dashboard", "Vue 360°", "Contacts", "Interactions", "Événements", "Participations", "Paiements", "Certifications", "Rapports", "Migration", "Paramètres"]

# ===================== CORRECTION CRITIQUE: MAPPING EXCEL =====================
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

# ===================== FONCTIONS UTILITAIRES =====================
def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def validate_email(email: str) -> bool:
    return re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email) is not None

def validate_phone(phone: str) -> bool:
    return re.match(r'^[\+]?[1-9][\d]{0,15}$', phone.replace(' ', '').replace('-', '')) is not None

def safe_index(lst: list, item: Any, default: int = 0) -> int:
    try: return lst.index(item)
    except (ValueError, TypeError): return default

def show_alert(message: str, type: str = "success"):
    st.markdown(f'<div class="alert-{type}">{message}</div>', unsafe_allow_html=True)

def create_backup(file: str):
    if os.path.exists(file):
        import shutil
        backup = f"{file}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        shutil.copy2(file, backup)
        logger.info(f"Backup: {backup}")

@st.cache_data
def load_config() -> Dict[str, Any]:
    try:
        if os.path.exists(FILES["settings"]):
            with open(FILES["settings"], "r", encoding="utf-8") as f:
                return json.load(f)
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG
    except Exception as e:
        logger.error(f"Erreur config: {e}")
        return DEFAULT_CONFIG

def save_config(config: Dict[str, Any]):
    try:
        create_backup(FILES["settings"])
        with open(FILES["settings"], "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        st.cache_data.clear()
    except Exception as e:
        logger.error(f"Erreur sauvegarde config: {e}")

def load_users() -> Dict[str, str]:
    try:
        if os.path.exists(FILES["users"]):
            with open(FILES["users"], "r", encoding="utf-8") as f:
                return json.load(f)
        default = {"admin": hash_password("iiba2024")}
        with open(FILES["users"], "w", encoding="utf-8") as f:
            json.dump(default, f)
        return default
    except Exception as e:
        logger.error(f"Erreur users: {e}")
        return {"admin": hash_password("iiba2024")}

def generate_id(prefix: str, df: pd.DataFrame, col: str) -> str:
    try:
        if df.empty: return f"{prefix}_001"
        nums = [int(x.split("_")[1]) for x in df[col] if isinstance(x, str) and "_" in x and x.split("_")[1].isdigit()]
        return f"{prefix}_{max(nums, default=0) + 1:03d}"
    except Exception as e:
        logger.error(f"Erreur ID: {e}")
        return f"{prefix}_001"

def safe_load_df(file: str, schema: Dict[str, Any]) -> pd.DataFrame:
    try:
        if os.path.exists(file):
            df = pd.read_csv(file, encoding="utf-8")
            for col, default in schema.items():
                if col not in df.columns:
                    df[col] = default() if callable(default) else default
            return df[list(schema.keys())]
        return pd.DataFrame(columns=list(schema.keys()))
    except Exception as e:
        logger.error(f"Erreur load {file}: {e}")
        return pd.DataFrame(columns=list(schema.keys()))

def safe_save_df(df: pd.DataFrame, file: str):
    try:
        create_backup(file)
        df.to_csv(file, index=False, encoding="utf-8")
        logger.info(f"Sauvegarde: {file}")
    except Exception as e:
        logger.error(f"Erreur save {file}: {e}")
        show_alert(f"Erreur sauvegarde: {e}", "error")

def create_grid(df: pd.DataFrame, height: int = 400) -> AgGrid:
    try:
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(sortable=True, filterable=True, resizable=True)
        gb.configure_selection('single', use_checkbox=True)
        return AgGrid(df, gb.build(), height=height, fit_columns_on_grid_load=True)
    except Exception as e:
        logger.error(f"Erreur grille: {e}")
        st.dataframe(df)
        return {"selected_rows": []}

# ===================== AUTHENTIFICATION =====================
def check_auth() -> bool:
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.markdown('<div class="main-header"><h1>🔐 IIBA Cameroun CRM - Connexion</h1></div>', unsafe_allow_html=True)
        
        with st.form("login"):
            username = st.text_input("Utilisateur")
            password = st.text_input("Mot de passe", type="password")
            
            if st.form_submit_button("Connexion"):
                users = load_users()
                if username in users and users[username] == hash_password(password):
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    logger.info(f"Connexion: {username}")
                    st.rerun()
                else:
                    show_alert("Identifiants invalides", "error")
        
        st.info("💡 Admin: admin / iiba2024")
        return False
    return True

# ===================== SCHÉMAS DONNÉES =====================
def get_schemas(config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        "contacts": {
            "ID": lambda: None, "Nom": "", "Prénom": "", "Genre": "", "Titre": "", "Société": "", 
            "Secteur": config['secteurs'][0], "Email": "", "Téléphone": "", "Ville": "", "Pays": config['pays'][0], 
            "Type": config['types_contact'][0], "Source": config['sources'][0], "Statut": config['statuts_paiement'][0], 
            "LinkedIn": "", "Notes": "", "Date_Creation": lambda: date.today().isoformat()
        },
        "interactions": {
            "ID_Interaction": lambda: None, "ID": "", "Date": date.today().isoformat(), "Canal": config['canaux'][0],
            "Objet": "", "Résumé": "", "Résultat": config['resultats_inter'][0], "Responsable": "", "Prochaine_Action": "", "Relance": ""
        },
        "evenements": {
            "ID_Événement": lambda: None, "Nom_Événement": "", "Type": config['types_evenements'][0], "Date": date.today().isoformat(),
            "Durée_h": 0.0, "Lieu": "", "Formateur(s)": "", "Invité(s)": "", "Objectif": "", "Période": "Matinée",
            "Notes": "", "Coût_Total": 0.0, "Recettes": 0.0, "Bénéfice": 0.0
        },
        "participations": {
            "ID_Participation": lambda: None, "ID": "", "ID_Événement": "", "Rôle": "Participant",
            "Inscription": date.today().isoformat(), "Arrivée": "", "Temps_Present": "AUTO", "Feedback": 3,
            "Note": 0, "Commentaire": "", "Nom Participant": "", "Nom Événement": ""
        },
        "paiements": {
            "ID_Paiement": lambda: None, "ID": "", "ID_Événement": "", "Date_Paiement": date.today().isoformat(),
            "Montant": 0.0, "Moyen": config['moyens_paiement'][0], "Statut": config['statuts_paiement'][0],
            "Référence": "", "Notes": "", "Relance": "", "Nom Contact": "", "Nom Événement": ""
        },
        "certifications": {
            "ID_Certif": lambda: None, "ID": "", "Type_Certif": config['types_contact'][0], "Date_Examen": date.today().isoformat(),
            "Résultat": "Réussi", "Score": 0, "Date_Obtention": date.today().isoformat(),
            "Validité": "", "Renouvellement": "", "Notes": "", "Nom Contact": ""
        }
    }

# ===================== NAVIGATION =====================
def handle_navigation() -> str:
    if "redirect_page" in st.session_state:
        return st.session_state.pop("redirect_page")
    
    with st.sidebar:
        st.markdown(f"👤 **{st.session_state.username}**")
        if st.button("🚪 Déconnexion"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
        st.markdown("---")
        
    return st.sidebar.selectbox("📋 Navigation", PAGES)

# ===================== FONCTIONS PAGES =====================
def filter_by_date(df: pd.DataFrame, col: str, year: str, month: str) -> pd.DataFrame:
    try:
        if df.empty: return df
        mask = (df[col].str[:4] == year) & ((month == "Tous") | (df[col].str[5:7] == month))
        return df[mask]
    except Exception: return df

def render_kpi_metrics(dfc_f, dfe_f, dfp_f, dfpay_f, dfcert_f):
    try:
        c1, c2, c3, c4 = st.columns(4)
        
        with c1:
            st.metric("👥 Prospects", len(dfc_f[dfc_f["Type"] == "Prospect"]))
            st.metric("🏆 Membres", len(dfc_f[dfc_f["Type"] == "Membre"]))
        
        with c2:
            st.metric("📅 Événements", len(dfe_f))
            st.metric("🙋 Participations", len(dfp_f))
        
        ca_total = dfpay_f[dfpay_f["Statut"] == "Réglé"]["Montant"].sum()
        impayes = len(dfpay_f[dfpay_f["Statut"] != "Réglé"])
        
        with c3:
            st.metric("💰 CA (FCFA)", f"{ca_total:,.0f}")
            st.metric("⏳ Impayés", impayes)
        
        with c4:
            certifs = len(dfcert_f[dfcert_f["Résultat"] == "Réussi"])
            engagement = dfp_f["Feedback"].mean() if not dfp_f.empty else 0
            st.metric("📜 Certifications", certifs)
            st.metric("📈 Engagement", f"{engagement:.1f}")
            
    except Exception as e:
        logger.error(f"Erreur métriques: {e}")
        show_alert("Erreur calcul métriques", "error")

def render_contact_form(config: Dict, schemas: Dict, contact_data: Optional[pd.Series] = None):
    with st.form("contact_form"):
        if contact_data is not None:
            st.text_input("ID", contact_data["ID"], disabled=True)
        
        col1, col2 = st.columns(2)
        with col1:
            nom = st.text_input("Nom *", contact_data.get("Nom", "") if contact_data is not None else "")
            prenom = st.text_input("Prénom *", contact_data.get("Prénom", "") if contact_data is not None else "")
            genre = st.selectbox("Genre", ["", "Homme", "Femme", "Autre"],
                index=safe_index(["", "Homme", "Femme", "Autre"], contact_data.get("Genre", "") if contact_data is not None else ""))
        
        with col2:
            titre = st.text_input("Titre", contact_data.get("Titre", "") if contact_data is not None else "")
            societe = st.text_input("Société", contact_data.get("Société", "") if contact_data is not None else "")
            secteur = st.selectbox("Secteur", config["secteurs"],
                index=safe_index(config["secteurs"], contact_data.get("Secteur", "") if contact_data is not None else ""))
        
        col3, col4 = st.columns(2)
        with col3:
            type_contact = st.selectbox("Type", config["types_contact"],
                index=safe_index(config["types_contact"], contact_data.get("Type", "") if contact_data is not None else ""))
            source = st.selectbox("Source", config["sources"],
                index=safe_index(config["sources"], contact_data.get("Source", "") if contact_data is not None else ""))
        
        with col4:
            statut = st.selectbox("Statut", config["statuts_paiement"],
                index=safe_index(config["statuts_paiement"], contact_data.get("Statut", "") if contact_data is not None else ""))
            pays = st.selectbox("Pays", config["pays"],
                index=safe_index(config["pays"], contact_data.get("Pays", "") if contact_data is not None else ""))
        
        email = st.text_input("Email", contact_data.get("Email", "") if contact_data is not None else "")
        tel = st.text_input("Téléphone", contact_data.get("Téléphone", "") if contact_data is not None else "")
        ville = st.text_input("Ville", contact_data.get("Ville", "") if contact_data is not None else "")
        linkedin = st.text_input("LinkedIn", contact_data.get("LinkedIn", "") if contact_data is not None else "")
        notes = st.text_area("Notes", contact_data.get("Notes", "") if contact_data is not None else "")
        
        submitted = st.form_submit_button("💾 Enregistrer")
        
        if submitted:
            # Validation
            errors = []
            if not nom.strip(): errors.append("Nom obligatoire")
            if not prenom.strip(): errors.append("Prénom obligatoire")
            if email and not validate_email(email): errors.append("Email invalide")
            if tel and not validate_phone(tel): errors.append("Téléphone invalide")
            
            if errors:
                for error in errors:
                    show_alert(error, "error")
                return None
            
            return {
                "Nom": nom.strip(), "Prénom": prenom.strip(), "Genre": genre, "Titre": titre.strip(),
                "Société": societe.strip(), "Secteur": secteur, "Email": email.strip(), "Téléphone": tel.strip(),
                "Ville": ville.strip(), "Pays": pays, "Type": type_contact, "Source": source,
                "Statut": statut, "LinkedIn": linkedin.strip(), "Notes": notes.strip(),
                "Date_Creation": contact_data.get("Date_Creation", date.today().isoformat()) if contact_data is not None else date.today().isoformat()
            }
        return None

def render_action_buttons(selected_contact: Dict, page_name: str):
    if selected_contact:
        contact_id = selected_contact.get("ID", "")
        nom_complet = f"{selected_contact.get('Nom', '')} {selected_contact.get('Prénom', '')}"
        st.markdown(f"**{nom_complet}** (ID: {contact_id})")
        
        actions = {
            "✏️ Modifier": ("Contacts", {"contact_action": "edit", "contact_id": contact_id}),
            "💬 Interactions": ("Interactions", {"focus_contact": contact_id}),
            "🙋 Participations": ("Participations", {"focus_contact": contact_id}),
            "💳 Paiements": ("Paiements", {"focus_contact": contact_id})
        }
        
        for label, (target_page, params) in actions.items():
            if st.button(label, key=f"{page_name}_{label}_{contact_id}"):
                st.session_state["redirect_page"] = target_page
                for key, value in params.items():
                    st.session_state[key] = value
                st.rerun()

# ===================== INITIALISATION =====================
if not check_auth(): st.stop()

config = load_config()
schemas = get_schemas(config)
page = handle_navigation()

# ===================== PAGES PRINCIPALES =====================
if page == "Dashboard":
    st.markdown('<div class="main-header"><h1>📈 Tableau de Bord IIBA Cameroun</h1></div>', unsafe_allow_html=True)
    
    # Chargement données
    data = {name: safe_load_df(FILES[name], schema) for name, schema in schemas.items()}
    
    # Filtres temporels
    col1, col2 = st.columns(2)
    try:
        years = sorted(set(d[:4] for d in data["contacts"]["Date_Creation"] if isinstance(d, str) and len(d) >= 4)) or [str(date.today().year)]
    except: years = [str(date.today().year)]
    
    year = col1.selectbox("📅 Année", years)
    month = col2.selectbox("📅 Mois", ["Tous"] + [f"{i:02d}" for i in range(1, 13)])
    
    # Application filtres et métriques
    filtered_data = {
        "contacts": filter_by_date(data["contacts"], "Date_Creation", year, month),
        "evenements": filter_by_date(data["evenements"], "Date", year, month),
        "participations": filter_by_date(data["participations"], "Inscription", year, month),
        "paiements": filter_by_date(data["paiements"], "Date_Paiement", year, month),
        "certifications": filter_by_date(data["certifications"], "Date_Obtention", year, month)
    }
    
    render_kpi_metrics(
        filtered_data["contacts"],
        filtered_data["evenements"],
        filtered_data["participations"],
        filtered_data["paiements"],
        filtered_data["certifications"]
    )
    
    # Export unifié
    if st.button("📥 Export global CSV"):
        try:
            merged = data["contacts"].merge(data["interactions"], on="ID", how="left").merge(data["participations"], on="ID", how="left")
            csv_data = merged.to_csv(index=False)
            st.download_button("Télécharger", csv_data, file_name=f"export_crm_{date.today()}.csv")
        except Exception as e:
            logger.error(f"Erreur export: {e}")
            show_alert("Erreur export", "error")

elif page == "Vue 360°":
    st.markdown('<div class="main-header"><h1>👁 Vue 360° des Contacts</h1></div>', unsafe_allow_html=True)
    
    df = safe_load_df(FILES["contacts"], schemas["contacts"])
    
    if df.empty:
        st.info("Aucun contact. Créez-en un!")
        if st.button("➕ Premier contact"):
            st.session_state["redirect_page"] = "Contacts"
            st.session_state["contact_action"] = "new"
            st.rerun()
    else:
        grid_response = create_grid(df)
        selected = grid_response.get('selected_rows', [])
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        if col1.button("➕ Nouveau"):
            st.session_state.update({"redirect_page": "Contacts", "contact_action": "new", "contact_id": None})
            st.rerun()
        
        if selected:
            contact = selected[0]
            contact_id = contact.get('ID', '')
            st.write(f"**Sélectionné:** {contact.get('Nom', '')} {contact.get('Prénom', '')} (ID: {contact_id})")
            
            buttons = [
                (col2, "✏️ Éditer", "Contacts", {"contact_action": "edit", "contact_id": contact_id}),
                (col3, "💬 Interactions", "Interactions", {"focus_contact": contact_id}),
                (col4, "🙋 Participations", "Participations", {"focus_contact": contact_id}),
                (col5, "💳 Paiements", "Paiements", {"focus_contact": contact_id})
            ]
            
            for column, label, target, params in buttons:
                if column.button(label):
                    st.session_state["redirect_page"] = target
                    st.session_state.update(params)
                    st.rerun()
        else:
            st.info("Sélectionnez un contact pour voir les actions.")

elif page == "Contacts":
    st.markdown('<div class="main-header"><h1>👤 Gestion des Contacts</h1></div>', unsafe_allow_html=True)
    
    df = safe_load_df(FILES["contacts"], schemas["contacts"])
    contact_action = st.session_state.get('contact_action', 'view')
    contact_id = st.session_state.get('contact_id')
    
    # Récupération contact à éditer
    contact_data = None
    if contact_action == 'edit' and contact_id:
        try:
            matches = df[df['ID'] == contact_id]
            if not matches.empty:
                contact_data = matches.iloc[0]
        except Exception as e:
            logger.error(f"Erreur contact: {e}")
    
    # Formulaire
    form_data = render_contact_form(config, schemas, contact_data)
    
    if form_data:
        try:
            if contact_data is not None:
                # Mise à jour
                idx = df[df["ID"] == contact_id].index[0]
                for key, value in form_data.items():
                    df.loc[idx, key] = value
            else:
                # Création
                form_data["ID"] = generate_id("CNT", df, "ID")
                df = pd.concat([df, pd.DataFrame([form_data])], ignore_index=True)
            
            safe_save_df(df, FILES["contacts"])
            show_alert("✅ Contact enregistré!")
            
            # Nettoyage session
            for key in ["contact_action", "contact_id"]:
                st.session_state.pop(key, None)
            
            st.rerun()
            
        except Exception as e:
            logger.error(f"Erreur sauvegarde contact: {e}")
            show_alert(f"Erreur sauvegarde: {e}", "error")
    
    # Affichage liste
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.markdown("### 📋 Liste des contacts")
        grid_response = create_grid(df) if not df.empty else {"selected_rows": []}
        selected = grid_response.get("selected_rows", [])
    
    with col2:
        st.markdown("### ⚡ Actions")
        render_action_buttons(selected[0] if selected else {}, "contacts")
    
    # Export
    if st.button("📥 Exporter contacts"):
        try:
            csv_data = df.to_csv(index=False)
            st.download_button("Télécharger", csv_data, file_name=f"contacts_{date.today()}.csv")
        except Exception as e:
            logger.error(f"Erreur export: {e}")
            show_alert("Erreur export", "error")

elif page in ["Interactions", "Événements", "Participations", "Paiements", "Certifications"]:
    # Pages génériques simplifiées avec même structure
    page_key = page.lower().replace("é", "e")  # événements -> evenements
    
    st.markdown(f'<div class="main-header"><h1>{page}</h1></div>', unsafe_allow_html=True)
    
    df = safe_load_df(FILES[page_key], schemas[page_key])
    
    # Filtrage par contact si focus
    focus_contact = st.session_state.get("focus_contact")
    if focus_contact and "ID" in df.columns:
        df_filtered = df[df["ID"] == focus_contact]
        st.info(f"🎯 Filtré pour contact: {focus_contact}")
        if st.button("🔄 Voir tout"):
            st.session_state.pop("focus_contact", None)
            st.rerun()
    else:
        df_filtered = df
    
    # Formulaire basique (à personnaliser selon page)
    with st.expander(f"➕ Ajouter {page[:-1].lower()}"):
        with st.form(f"form_{page_key}"):
            if page == "Interactions":
                contacts = safe_load_df(FILES["contacts"], schemas["contacts"])
                contact = st.selectbox("Contact", [""] + contacts["ID"].tolist())
                date_inter = st.date_input("Date", date.today())
                canal = st.selectbox("Canal", config["canaux"])
                objet = st.text_input("Objet")
                resume = st.text_area("Résumé")
                resultat = st.selectbox("Résultat", config["resultats_inter"])
                
                if st.form_submit_button("Enregistrer") and contact:
                    new_record = {
                        "ID_Interaction": generate_id("INT", df, "ID_Interaction"),
                        "ID": contact, "Date": date_inter.isoformat(), "Canal": canal,
                        "Objet": objet, "Résumé": resume, "Résultat": resultat,
                        "Responsable": "", "Prochaine_Action": "", "Relance": ""
                    }
                    df = pd.concat([df, pd.DataFrame([new_record])], ignore_index=True)
                    safe_save_df(df, FILES[page_key])
                    show_alert("✅ Interaction enregistrée!")
                    st.rerun()
                    
            elif page == "Événements":
                nom_evt = st.text_input("Nom de l'événement")
                type_evt = st.selectbox("Type", config["types_evenements"])
                date_evt = st.date_input("Date", date.today())
                duree = st.number_input("Durée (heures)", min_value=0.0, step=0.5)
                lieu = st.text_input("Lieu")
                
                if st.form_submit_button("Enregistrer") and nom_evt:
                    new_record = {
                        "ID_Événement": generate_id("EVT", df, "ID_Événement"),
                        "Nom_Événement": nom_evt, "Type": type_evt, "Date": date_evt.isoformat(),
                        "Durée_h": duree, "Lieu": lieu, "Formateur(s)": "", "Invité(s)": "",
                        "Objectif": "", "Période": "Matinée", "Notes": "",
                        "Coût_Total": 0.0, "Recettes": 0.0, "Bénéfice": 0.0
                    }
                    df = pd.concat([df, pd.DataFrame([new_record])], ignore_index=True)
                    safe_save_df(df, FILES[page_key])
                    show_alert("✅ Événement enregistré!")
                    st.rerun()
                    
            elif page == "Participations":
                contacts = safe_load_df(FILES["contacts"], schemas["contacts"])
                evenements = safe_load_df(FILES["evenements"], schemas["evenements"])
                contact = st.selectbox("Contact", [""] + contacts["ID"].tolist())
                evenement = st.selectbox("Événement", [""] + evenements["ID_Événement"].tolist())
                role = st.selectbox("Rôle", ["Participant", "Organisateur", "Formateur"])
                
                if st.form_submit_button("Enregistrer") and contact and evenement:
                    new_record = {
                        "ID_Participation": generate_id("PAR", df, "ID_Participation"),
                        "ID": contact, "ID_Événement": evenement, "Rôle": role,
                        "Inscription": date.today().isoformat(), "Arrivée": "",
                        "Temps_Present": "AUTO", "Feedback": 3, "Note": 0,
                        "Commentaire": "", "Nom Participant": "", "Nom Événement": ""
                    }
                    df = pd.concat([df, pd.DataFrame([new_record])], ignore_index=True)
                    safe_save_df(df, FILES[page_key])
                    show_alert("✅ Participation enregistrée!")
                    st.rerun()
                    
            elif page == "Paiements":
                contacts = safe_load_df(FILES["contacts"], schemas["contacts"])
                evenements = safe_load_df(FILES["evenements"], schemas["evenements"])
                contact = st.selectbox("Contact", [""] + contacts["ID"].tolist())
                evenement = st.selectbox("Événement", [""] + evenements["ID_Événement"].tolist())
                montant = st.number_input("Montant (FCFA)", min_value=0.0, step=1000.0)
                moyen = st.selectbox("Moyen", config["moyens_paiement"])
                statut = st.selectbox("Statut", config["statuts_paiement"])
                
                if st.form_submit_button("Enregistrer") and contact and evenement:
                    new_record = {
                        "ID_Paiement": generate_id("PAY", df, "ID_Paiement"),
                        "ID": contact, "ID_Événement": evenement,
                        "Date_Paiement": date.today().isoformat(), "Montant": montant,
                        "Moyen": moyen, "Statut": statut, "Référence": "", "Notes": "",
                        "Relance": "", "Nom Contact": "", "Nom Événement": ""
                    }
                    df = pd.concat([df, pd.DataFrame([new_record])], ignore_index=True)
                    safe_save_df(df, FILES[page_key])
                    show_alert("✅ Paiement enregistré!")
                    st.rerun()
                    
            elif page == "Certifications":
                contacts = safe_load_df(FILES["contacts"], schemas["contacts"])
                contact = st.selectbox("Contact", [""] + contacts["ID"].tolist())
                type_certif = st.selectbox("Type certification", ["ECBA", "CCBA", "CBAP", "PBA", "PMI-PBA"])
                resultat = st.selectbox("Résultat", ["Réussi", "Échoué", "En attente"])
                score = st.number_input("Score", min_value=0, max_value=100)
                
                if st.form_submit_button("Enregistrer") and contact:
                    new_record = {
                        "ID_Certif": generate_id("CER", df, "ID_Certif"),
                        "ID": contact, "Type_Certif": type_certif,
                        "Date_Examen": date.today().isoformat(), "Résultat": resultat,
                        "Score": score, "Date_Obtention": date.today().isoformat(),
                        "Validité": "", "Renouvellement": "", "Notes": "", "Nom Contact": ""
                    }
                    df = pd.concat([df, pd.DataFrame([new_record])], ignore_index=True)
                    safe_save_df(df, FILES[page_key])
                    show_alert("✅ Certification enregistrée!")
                    st.rerun()
    
    # Affichage données
    if not df_filtered.empty:
        create_grid(df_filtered)
    else:
        st.info(f"Aucun(e) {page[:-1].lower()} trouvé(e)")

elif page == "Migration":
    st.markdown('<div class="main-header"><h1>📦 Migration de Données</h1></div>', unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["📥 Template", "📤 Import", "📋 Logs"])
    
    with tab1:
        st.header("Template Excel")
        try:
            # CORRECTION CRITIQUE: Utiliser le mapping correct
            sheet_mapping = get_excel_sheet_mapping()
            output = io.BytesIO()
            
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                for schema_key, schema in schemas.items():
                    # Utiliser le nom de feuille correct avec accent
                    sheet_name = sheet_mapping[schema_key]
                    pd.DataFrame(columns=list(schema.keys())).to_excel(writer, sheet_name=sheet_name, index=False)
            
            output.seek(0)
            st.download_button("📥 Télécharger", output, file_name=f"template_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as e:
            logger.error(f"Erreur template: {e}")
            show_alert("Erreur génération template", "error")
    
    with tab2:
        st.header("Import Excel")
        uploaded = st.file_uploader("📁 Fichier Excel", type=["xlsx"])
        
        if uploaded:
            try:
                wb = openpyxl.load_workbook(uploaded)
                
                # CORRECTION CRITIQUE: Utiliser le mapping correct
                sheet_mapping = get_excel_sheet_mapping()
                required_sheets = {}
                
                for schema_key, schema in schemas.items():
                    excel_sheet_name = sheet_mapping[schema_key]
                    required_sheets[excel_sheet_name] = {"schema": schema, "key": schema_key}
                
                missing = [s for s in required_sheets if s not in wb.sheetnames]
                if missing:
                    show_alert(f"Feuilles manquantes: {missing}", "error")
                    st.info(f"Feuilles trouvées: {wb.sheetnames}")
                    st.info(f"Feuilles attendues: {list(required_sheets.keys())}")
                else:
                    data_to_import = {}
                    errors = []
                    
                    for sheet_name, sheet_info in required_sheets.items():
                        try:
                            import_df = pd.read_excel(uploaded, sheet_name=sheet_name)
                            schema = sheet_info["schema"]
                            schema_key = sheet_info["key"]
                            
                            missing_cols = [c for c in schema.keys() if c not in import_df.columns]
                            if missing_cols:
                                errors.append(f"{sheet_name}: colonnes manquantes {missing_cols}")
                            else:
                                data_to_import[schema_key] = import_df
                        except Exception as e:
                            errors.append(f"Erreur {sheet_name}: {e}")
                    
                    if errors:
                        for error in errors:
                            show_alert(error, "error")
                    else:
                        show_alert("✅ Fichier validé!")
                        
                        for schema_key, import_df in data_to_import.items():
                            with st.expander(f"Aperçu {schema_key} ({len(import_df)} lignes)"):
                                st.dataframe(import_df.head())
                        
                        if st.button("🚀 Confirmer import"):
                            success_count = 0
                            try:
                                for schema_key, new_df in data_to_import.items():
                                    if schema_key in FILES and schema_key in schemas:
                                        existing_df = safe_load_df(FILES[schema_key], schemas[schema_key])
                                        id_col = list(schemas[schema_key].keys())[0]
                                        
                                        if not existing_df.empty:
                                            existing_ids = set(existing_df[id_col].dropna())
                                            new_ids = set(new_df[id_col].dropna())
                                            filtered_existing = existing_df[~existing_df[id_col].isin(existing_ids & new_ids)]
                                            combined = pd.concat([filtered_existing, new_df], ignore_index=True)
                                        else:
                                            combined = new_df
                                        
                                        safe_save_df(combined, FILES[schema_key])
                                        success_count += 1
                                
                                # Log import
                                with open("migrations.log", "a", encoding="utf-8") as f:
                                    f.write(f"{datetime.now()} - Import par {st.session_state.username} - {success_count} feuilles\n")
                                
                                show_alert(f"🎉 Import réussi! {success_count} feuilles traitées.")
                                
                            except Exception as e:
                                logger.error(f"Erreur import: {e}")
                                show_alert(f"Erreur import: {e}", "error")
                                
            except Exception as e:
                logger.error(f"Erreur fichier: {e}")
                show_alert(f"Erreur traitement fichier: {e}", "error")
    
    with tab3:
        st.header("Historique")
        try:
            if os.path.exists("migrations.log"):
                with open("migrations.log", "r", encoding="utf-8") as f:
                    content = f.read()
                    if content.strip():
                        st.text_area("📋 Logs", content, height=400)
                    else:
                        st.info("Aucun log")
            else:
                st.info("Pas de fichier log")
        except Exception as e:
            logger.error(f"Erreur logs: {e}")
            show_alert("Erreur lecture logs", "error")

elif page == "Rapports":
    st.markdown('<div class="main-header"><h1>📊 Rapports Avancés</h1></div>', unsafe_allow_html=True)
    
    # Chargement données
    data = {name: safe_load_df(FILES[name], schema) for name, schema in schemas.items()}
    
    # Filtres
    col1, col2 = st.columns(2)
    try:
        years = sorted(set(d[:4] for d in data["contacts"]["Date_Creation"] if isinstance(d, str) and len(d) >= 4)) or [str(date.today().year)]
    except: years = [str(date.today().year)]
    
    year = col1.selectbox("📅 Année", years, key="rapport_year")
    month = col2.selectbox("📅 Mois", ["Tous"] + [f"{i:02d}" for i in range(1, 13)], key="rapport_month")
    
    # Données filtrées
    filtered = {
        name: filter_by_date(df, 
                           {"contacts": "Date_Creation", "evenements": "Date", "participations": "Inscription", 
                            "paiements": "Date_Paiement", "certifications": "Date_Obtention"}.get(name, "Date"), 
                           year, month)
        for name, df in data.items()
    }
    
    # Calculs KPIs
    try:
        kpis = {
            "Total contacts": len(filtered["contacts"]),
            "Prospects": len(filtered["contacts"][filtered["contacts"]["Type"] == "Prospect"]),
            "Membres": len(filtered["contacts"][filtered["contacts"]["Type"] == "Membre"]),
            "Événements": len(filtered["evenements"]),
            "Participations": len(filtered["participations"]),
            "CA encaissé (FCFA)": filtered["paiements"][filtered["paiements"]["Statut"] == "Réglé"]["Montant"].sum(),
            "Impayés": len(filtered["paiements"][filtered["paiements"]["Statut"] != "Réglé"]),
            "Certifications": len(filtered["certifications"][filtered["certifications"]["Résultat"] == "Réussi"])
        }
        
        # Taux calculés
        kpis["Taux conversion (%)"] = (kpis["Membres"] / max(kpis["Prospects"] + kpis["Membres"], 1)) * 100
        kpis["Participation/Événement"] = kpis["Participations"] / max(kpis["Événements"], 1)
        
        # Affichage tableau KPIs
        st.markdown("### 📈 Indicateurs clés")
        kpi_df = pd.DataFrame(list(kpis.items()), columns=["Indicateur", "Valeur"])
        st.dataframe(kpi_df, use_container_width=True, hide_index=True)
        
        # Export rapport
        if st.button("📊 Exporter rapport Excel"):
            try:
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    kpi_df.to_excel(writer, sheet_name='KPIs', index=False)
                    for name, df in filtered.items():
                        if not df.empty:
                            df.to_excel(writer, sheet_name=name.capitalize(), index=False)
                
                output.seek(0)
                st.download_button("📥 Télécharger", output, 
                    file_name=f"rapport_{year}_{month}_{date.today()}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            except Exception as e:
                logger.error(f"Erreur export rapport: {e}")
                show_alert("Erreur export rapport", "error")
                
    except Exception as e:
        logger.error(f"Erreur calcul KPIs: {e}")
        show_alert("Erreur calcul indicateurs", "error")

elif page == "Paramètres":
    st.markdown('<div class="main-header"><h1>⚙️ Paramètres Système</h1></div>', unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["📋 Référentiels", "👤 Utilisateurs", "🔧 Système"])
    
    with tab1:
        st.header("Configuration des référentiels")
        
        col1, col2 = st.columns(2)
        config_fields = {}
        
        with col1:
            config_fields["statuts_paiement"] = st.text_area("💰 Statuts paiement", "\n".join(config["statuts_paiement"]))
            config_fields["resultats_inter"] = st.text_area("📨 Résultats interaction", "\n".join(config["resultats_inter"]))
            config_fields["types_contact"] = st.text_area("👥 Types contact", "\n".join(config["types_contact"]))
            config_fields["sources"] = st.text_area("📋 Sources", "\n".join(config["sources"]))
            config_fields["statuts_engagement"] = st.text_area("⚡ Statuts engagement", "\n".join(config["statuts_engagement"]))
        
        with col2:
            config_fields["secteurs"] = st.text_area("🏢 Secteurs", "\n".join(config["secteurs"]))
            config_fields["pays"] = st.text_area("🌍 Pays", "\n".join(config["pays"]))
            config_fields["canaux"] = st.text_area("🛠 Canaux", "\n".join(config["canaux"]))
            config_fields["types_evenements"] = st.text_area("🎫 Types événements", "\n".join(config["types_evenements"]))
            config_fields["moyens_paiement"] = st.text_area("💵 Moyens paiement", "\n".join(config["moyens_paiement"]))
        
        if st.button("💾 Sauvegarder référentiels"):
            try:
                new_config = {key: [s.strip() for s in value.split("\n") if s.strip()] 
                             for key, value in config_fields.items()}
                save_config(new_config)
                show_alert("✅ Référentiels sauvegardés!")
                st.rerun()
            except Exception as e:
                logger.error(f"Erreur config: {e}")
                show_alert(f"Erreur sauvegarde: {e}", "error")
    
    with tab2:
        st.header("Gestion utilisateurs")
        
        users = load_users()
        
        # Liste existants
        st.subheader("👤 Utilisateurs")
        for username in users.keys():
            col1, col2 = st.columns([3, 1])
            col1.write(f"**{username}**")
            if col2.button("🗑️", key=f"del_{username}") and username != "admin":
                del users[username]
                with open(FILES["users"], "w", encoding="utf-8") as f:
                    json.dump(users, f)
                show_alert(f"Utilisateur {username} supprimé")
                st.rerun()
        
        # Ajouter nouvel utilisateur
        st.subheader("➕ Ajouter utilisateur")
        with st.form("add_user"):
            new_user = st.text_input("Nom utilisateur")
            new_pass = st.text_input("Mot de passe", type="password")
            confirm_pass = st.text_input("Confirmer", type="password")
            
            if st.form_submit_button("Ajouter"):
                if not new_user or not new_pass:
                    show_alert("Champs obligatoires", "error")
                elif new_pass != confirm_pass:
                    show_alert("Mots de passe différents", "error")
                elif new_user in users:
                    show_alert("Utilisateur existe", "error")
                else:
                    try:
                        users[new_user] = hash_password(new_pass)
                        with open(FILES["users"], "w", encoding="utf-8") as f:
                            json.dump(users, f)
                        show_alert(f"✅ Utilisateur {new_user} créé!")
                        logger.info(f"Nouvel utilisateur: {new_user}")
                        st.rerun()
                    except Exception as e:
                        logger.error(f"Erreur création user: {e}")
                        show_alert(f"Erreur création: {e}", "error")
    
    with tab3:
        st.header("Configuration système")
        
        # Info fichiers
        st.subheader("📁 Fichiers données")
        for name, filename in FILES.items():
            exists = os.path.exists(filename)
            size = f"{os.path.getsize(filename) / 1024:.1f} KB" if exists else "0 KB"
            status = "✅" if exists else "❌"
            st.write(f"**{name.capitalize()}**: {filename} {status} ({size})")
        
        # Actions système
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("💾 Backup complet"):
                try:
                    backup_dir = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    os.makedirs(backup_dir, exist_ok=True)
                    
                    import shutil
                    for filename in FILES.values():
                        if os.path.exists(filename):
                            shutil.copy2(filename, backup_dir)
                    
                    show_alert(f"✅ Backup: {backup_dir}")
                    logger.info(f"Backup: {backup_dir}")
                except Exception as e:
                    logger.error(f"Erreur backup: {e}")
                    show_alert(f"Erreur backup: {e}", "error")
        
        with col2:
            if st.button("🧹 Nettoyer logs"):
                try:
                    if os.path.exists("migrations.log"):
                        os.remove("migrations.log")
                    show_alert("✅ Logs nettoyés")
                    logger.info("Logs nettoyés")
                except Exception as e:
                    logger.error(f"Erreur nettoyage: {e}")
                    show_alert(f"Erreur nettoyage: {e}", "error")
        
        # Info système
        st.subheader("ℹ️ Informations")
        st.info(f"""
        **Streamlit**: {st.__version__}  
        **Utilisateur**: {st.session_state.username}  
        **Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """)

# Footer
st.markdown("---")
st.markdown("**IIBA Cameroun CRM** v2.0 - Développé avec ❤️ pour la Business Analysis | ✅ Migration Excel Corrigée")
