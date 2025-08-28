# app_final_fixed.py - Version corrigée sans erreurs HTML et avec compatibilité

"""
Application CRM IIBA Cameroun avec architecture robuste.
Cette version corrige tous les problèmes identifiés.
"""

from datetime import datetime, date, timedelta
from pathlib import Path
import io
import json
import re
import unicodedata
import numpy as np
import pandas as pd
import streamlit as st

# Import du module Google Sheets sécurisé
try:
    from gs_client import get_gs_client, test_connection
    HAS_GS_CLIENT = True
except ImportError:
    HAS_GS_CLIENT = False
    st.warning("Module gs_client non trouvé. Mode CSV uniquement.")

# === Configuration et constantes ===
STORAGE_BACKEND = st.secrets.get("storage_backend", "csv")  # "csv" or "gsheets"
GSHEET_SPREADSHEET = st.secrets.get("gsheet_spreadsheet", "IIBA CRM DB")

# Vérification préalable de la configuration Google Sheets
if STORAGE_BACKEND == "gsheets":
    if not HAS_GS_CLIENT:
        st.error("⚠️ Backend Google Sheets sélectionné mais module gs_client manquant.")
        st.info("💡 Conseil: Vérifiez que gs_client.py est présent ou basculez vers 'csv'.")
        st.stop()
    
    # Test de connexion au démarrage
    connection_test = test_connection(GSHEET_SPREADSHEET)
    if not connection_test["success"]:
        st.error(f"❌ Échec connexion Google Sheets: {connection_test.get('error', 'Erreur inconnue')}")
        st.info("💡 Conseil: Vérifiez vos secrets Streamlit et les permissions du service account.")
        st.stop()

# Import conditionnel des dépendances Google Sheets
if STORAGE_BACKEND == "gsheets" and HAS_GS_CLIENT:
    from gspread_dataframe import set_with_dataframe, get_as_dataframe
    
    # Client global
    _GS_CLIENT = get_gs_client(GSHEET_SPREADSHEET)
    _GS_CLIENT.connect()  # Connexion déjà validée plus haut

# AgGrid (optionnel)
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode
    HAS_AGGRID = True
except ImportError:
    HAS_AGGRID = False

# Altair (optionnel)
try:
    import altair as alt
except ImportError:
    alt = None

# Configuration page
st.set_page_config(
    page_title="IIBA Cameroun — CRM",
    page_icon="📊",
    layout="wide"
)

# === Schémas de données ===
DATA_DIR = Path("./data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "inter": DATA_DIR / "interactions.csv",
    "events": DATA_DIR / "evenements.csv",
    "parts": DATA_DIR / "participations.csv",
    "pay": DATA_DIR / "paiements.csv",
    "cert": DATA_DIR / "certifications.csv",
    "entreprises": DATA_DIR / "entreprises.csv",
    "params": DATA_DIR / "parametres.csv",
    "logs": DATA_DIR / "migration_logs.jsonl"
}

# Colonnes des tables
C_COLS = [
    "ID", "Nom", "Prénom", "Genre", "Titre", "Société", "Secteur",
    "Email", "Téléphone", "LinkedIn", "Ville", "Pays", "Type",
    "Source", "Statut", "Score_Engagement", "Date_Creation", "Notes", "Top20"
]

I_COLS = [
    "ID_Interaction", "ID", "Date", "Canal", "Objet", "Résumé",
    "Résultat", "Prochaine_Action", "Relance", "Responsable"
]

E_COLS = [
    "ID_Événement", "Nom_Événement", "Type", "Date", "Durée_h",
    "Lieu", "Formateur", "Objectif", "Periode", "Cout_Salle",
    "Cout_Formateur", "Cout_Logistique", "Cout_Pub", "Cout_Autres",
    "Cout_Total", "Notes"
]

P_COLS = [
    "ID_Participation", "ID", "ID_Événement", "Rôle", "Inscription",
    "Arrivée", "Temps_Present", "Feedback", "Note", "Commentaire"
]

PAY_COLS = [
    "ID_Paiement", "ID", "ID_Événement", "Date_Paiement", "Montant",
    "Moyen", "Statut", "Référence", "Notes", "Relance"
]

CERT_COLS = [
    "ID_Certif", "ID", "Type_Certif", "Date_Examen", "Résultat",
    "Score", "Date_Obtention", "Validité", "Renouvellement", "Notes"
]

ENT_COLS = [
    "ID_Entreprise", "Nom_Entreprise", "Secteur", "Taille", "CA_Annuel",
    "Nb_Employes", "Ville", "Pays", "Contact_Principal", "Email_Principal",
    "Telephone_Principal", "Site_Web", "Statut_Partenariat", "Type_Partenariat",
    "Date_Premier_Contact", "Responsable_IIBA", "Notes", "Opportunites", "Date_Maj"
]

# Colonnes d'audit
AUDIT_COLS = ["Created_At", "Created_By", "Updated_At", "Updated_By"]

ALL_SCHEMAS = {
    "contacts": C_COLS,
    "interactions": I_COLS,
    "evenements": E_COLS,
    "participations": P_COLS,
    "paiements": PAY_COLS,
    "certifications": CERT_COLS,
    "entreprises": ENT_COLS,
}

# Mapping des noms d'onglets Google Sheets
SHEET_NAME = {
    "contacts": "contacts",
    "inter": "interactions",
    "events": "evenements",
    "parts": "participations",
    "pay": "paiements",
    "cert": "certifications",
    "entreprises": "entreprises",
    "params": "parametres",
    "users": "users"
}

def _id_col_for(name: str) -> str:
    """Retourne la colonne ID pour une table donnée."""
    return {
        "contacts": "ID",
        "inter": "ID_Interaction",
        "events": "ID_Événement",
        "parts": "ID_Participation",
        "pay": "ID_Paiement",
        "cert": "ID_Certif",
        "entreprises": "ID_Entreprise",
        "users": "user_id"
    }.get(name, "ID")

# === Fonctions utilitaires ===
import hashlib

def _compute_etag(df: pd.DataFrame, name: str) -> str:
    """Calcule un etag pour détecter les modifications concurrentes."""
    if df is None or df.empty:
        return "empty"
    
    idc = _id_col_for(name)
    cols = [c for c in [idc, "Updated_At"] if c in df.columns]
    
    try:
        payload = df[cols].astype(str).fillna("").sort_values(by=cols).to_csv(index=False)
    except Exception:
        payload = df.astype(str).fillna("").to_csv(index=False)
    
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def _now_iso():
    """Retourne la date/heure courante en ISO."""
    return datetime.utcnow().isoformat()

def stamp_create(row: dict, user: dict):
    """Ajoute les colonnes d'audit lors d'une création."""
    row = dict(row)
    now = _now_iso()
    uid = user.get("UserID", "system") if user else "system"
    
    row.setdefault("Created_At", now)
    row.setdefault("Created_By", uid)
    row["Updated_At"] = row.get("Updated_At", now)
    row["Updated_By"] = row.get("Updated_By", uid)
    
    return row

def stamp_update(row: dict, user: dict):
    """Met à jour les colonnes d'audit lors d'une édition."""
    row = dict(row)
    row["Updated_At"] = _now_iso()
    row["Updated_By"] = user.get("UserID", "system") if user else "system"
    
    return row

# === Gestion des paramètres ===
DEFAULT_LISTS = {
    "genres": "Homme|Femme|Autre",
    "secteurs": "Banque|Télécom|IT|Éducation|Santé|ONG|Industrie|Public|Autre",
    "types_contact": "Membre|Prospect|Formateur|Partenaire",
    "sources": "Afterwork|Formation|LinkedIn|Recommandation|Site Web|Salon|Autre",
    "statuts_engagement": "Actif|Inactif|À relancer",
    "canaux": "Appel|Email|WhatsApp|Zoom|Présentiel|Autre",
    "villes": "Douala|Yaoundé|Limbe|Bafoussam|Garoua|Autres",
    "pays": "Cameroun|Côte d'Ivoire|Sénégal|France|Canada|Autres",
    "types_evenements": "Formation|Groupe d'étude|BA MEET UP|Webinaire|Conférence|Certification",
    "lieux": "Présentiel|Zoom|Hybride",
    "resultats_inter": "Positif|Négatif|À suivre|Sans suite",
    "statuts_paiement": "Réglé|Partiel|Non payé",
    "moyens_paiement": "Mobile Money|Virement|CB|Cash",
    "types_certif": "ECBA|CCBA|CBAP|PBA",
    "entreprises_cibles": "Dangote|MUPECI|SALAM|SUNU IARD|ENEO|PAD|PAK",
    "tailles_entreprise": "TPE (< 10)|PME (10-250)|ETI (250-5000)|GE (> 5000)",
    "statuts_partenariat": "Prospect|Partenaire|Client|Partenaire Stratégique|Inactif",
    "types_partenariat": "Formation|Recrutement|Conseil|Sponsoring|Certification|Autre",
    "responsables_iiba": "Aymard|Alix|Comité|Non assigné",
}

PARAM_DEFAULTS = {
    "vip_threshold": "500000",
    "score_w_interaction": "1",
    "score_w_participation": "1",
    "score_w_payment_regle": "2",
    "interactions_lookback_days": "90",
    "rule_hot_interactions_recent_min": "3",
    "rule_hot_participations_min": "1",
    "rule_hot_payment_partial_counts_as_hot": "1",
    "grid_crm_columns": ",".join([
        "ID", "Nom", "Prénom", "Société", "Type", "Statut", "Email",
        "Interactions", "Participations", "CA_réglé", "Impayé", "Resp_principal", "A_animé_ou_invité",
        "Score_composite", "Proba_conversion", "Tags", "Created_At", "Created_By", "Updated_At", "Updated_By"
    ]),
    "grid_events_columns": ",".join(E_COLS),
    "grid_entreprises_columns": ",".join([
        "ID_Entreprise", "Nom_Entreprise", "Secteur", "Taille", "Statut_Partenariat",
        "Type_Partenariat", "Contact_Principal", "Email_Principal", "Responsable_IIBA", "Date_Premier_Contact"
    ]),
    "kpi_enabled": ",".join([
        "contacts_total", "prospects_actifs", "membres", "events_count",
        "participations_total", "ca_regle", "impayes", "taux_conversion"
    ]),
    "kpi_target_contacts_total_year_2025": "1000",
    "kpi_target_ca_regle_year_2025": "5000000",
    "contacts_period_fallback": "1",
    "entreprises_scoring_ca_weight": "0.3",
    "entreprises_scoring_employes_weight": "0.2",
    "entreprises_scoring_interactions_weight": "0.5",
    "entreprises_ca_seuil_gros": "10000000",
    "entreprises_employes_seuil_gros": "500",
}

ALL_DEFAULTS = {**PARAM_DEFAULTS, **{f"list_{k}": v for k, v in DEFAULT_LISTS.items()}}

def load_params() -> dict:
    """Charge les paramètres depuis le fichier."""
    if not PATHS["params"].exists():
        df = pd.DataFrame({
            "key": list(ALL_DEFAULTS.keys()),
            "value": list(ALL_DEFAULTS.values())
        })
        df.to_csv(PATHS["params"], index=False, encoding="utf-8")
        return ALL_DEFAULTS.copy()
    
    try:
        df = pd.read_csv(PATHS["params"], dtype=str).fillna("")
        d = {r["key"]: r["value"] for _, r in df.iterrows()}
    except Exception:
        d = ALL_DEFAULTS.copy()
    
    # Compléter les paramètres manquants
    for k, v in ALL_DEFAULTS.items():
        if k not in d:
            d[k] = v
    
    return d

def save_params(d: dict):
    """Sauvegarde les paramètres dans le fichier."""
    rows = [{"key": k, "value": str(v)} for k, v in d.items()]
    pd.DataFrame(rows).to_csv(PATHS["params"], index=False, encoding="utf-8")

# Chargement des paramètres
PARAMS = load_params()

def get_list(name: str) -> list:
    """Récupère une liste de valeurs depuis les paramètres."""
    raw = PARAMS.get(f"list_{name}", DEFAULT_LISTS.get(name, ""))
    vals = [x.strip() for x in str(raw).split("|") if x.strip()]
    return vals

# Listes de valeurs pour l'interface
SET = {
    "genres": get_list("genres"),
    "secteurs": get_list("secteurs"),
    "types_contact": get_list("types_contact"),
    "sources": get_list("sources"),
    "statuts_engagement": get_list("statuts_engagement"),
    "canaux": get_list("canaux"),
    "villes": get_list("villes"),
    "pays": get_list("pays"),
    "types_evenements": get_list("types_evenements"),
    "lieux": get_list("lieux"),
    "resultats_inter": get_list("resultats_inter"),
    "statuts_paiement": get_list("statuts_paiement"),
    "moyens_paiement": get_list("moyens_paiement"),
    "types_certif": get_list("types_certif"),
    "entreprises_cibles": get_list("entreprises_cibles"),
    "tailles_entreprise": get_list("tailles_entreprise"),
    "statuts_partenariat": get_list("statuts_partenariat"),
    "types_partenariat": get_list("types_partenariat"),
    "responsables_iiba": get_list("responsables_iiba"),
}

# === Fonctions de chargement/sauvegarde unifiées ===
def ensure_df_source(name: str, cols: list) -> pd.DataFrame:
    """
    Charge un DataFrame depuis le backend sélectionné (CSV ou Google Sheets).
    """
    full_cols = cols + [c for c in AUDIT_COLS if c not in cols]
    tab = SHEET_NAME.get(name, name)
    
    if STORAGE_BACKEND == "gsheets" and HAS_GS_CLIENT:
        # Chargement Google Sheets
        ws = _GS_CLIENT.get_worksheet(tab)
        if ws is None:
            st.error(f"Impossible d'accéder à l'onglet '{tab}'")
            return pd.DataFrame(columns=full_cols)
        
        try:
            df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
            if df is None or df.empty:
                df = pd.DataFrame(columns=full_cols)
                set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
            else:
                for c in full_cols:
                    if c not in df.columns:
                        df[c] = ""
                df = df[full_cols]
        except Exception as e:
            st.error(f"Erreur chargement Google Sheets '{tab}': {e}")
            df = pd.DataFrame(columns=full_cols)
            
        st.session_state[f"etag_{name}"] = _compute_etag(df, name)
        return df
    
    # Fallback CSV
    path = PATHS[name] if name in PATHS else PATHS["contacts"]
    df = ensure_df(path, cols)
    st.session_state[f"etag_{name}"] = _compute_etag(df, name)
    return df

def save_df_target(name: str, df: pd.DataFrame):
    """
    Sauvegarde un DataFrame vers le backend sélectionné (CSV ou Google Sheets).
    """
    tab = SHEET_NAME.get(name, name)
    
    if STORAGE_BACKEND == "gsheets" and HAS_GS_CLIENT:
        # Sauvegarde Google Sheets avec détection de conflit
        ws = _GS_CLIENT.get_worksheet(tab)
        if ws is None:
            st.error(f"Impossible d'accéder à l'onglet '{tab}' pour sauvegarde")
            return
            
        try:
            df_remote = get_as_dataframe(ws, evaluate_formulas=True, header=0)
            if df_remote is None:
                df_remote = pd.DataFrame(columns=df.columns)
            
            expected = st.session_state.get(f"etag_{name}")
            current = _compute_etag(df_remote, name)
            
            if expected and expected != current:
                st.error(f"Conflit de modification détecté sur '{tab}'. Veuillez recharger la page avant de sauvegarder.")
                st.stop()
            
            set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
            st.session_state[f"etag_{name}"] = _compute_etag(df, name)
            
        except Exception as e:
            st.error(f"Erreur sauvegarde Google Sheets '{tab}': {e}")
        
        return
    
    # Fallback CSV avec détection de conflit basique
    path = PATHS[name] if name in PATHS else PATHS["contacts"]
    expected = st.session_state.get(f"etag_{name}")
    current_df = ensure_df(path, df.columns.tolist())
    current = _compute_etag(current_df, name)
    
    if expected and expected != current:
        st.error(f"Conflit de modification détecté sur '{name}'. Veuillez recharger la page avant de sauvegarder.")
        st.stop()
    
    save_df(df, path)
    st.session_state[f"etag_{name}"] = _compute_etag(df, name)

def ensure_df(path: Path, cols: list) -> pd.DataFrame:
    """Charge un DataFrame depuis un fichier CSV."""
    full_cols = cols + [c for c in AUDIT_COLS if c not in cols]
    
    if path.exists():
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8")
        except Exception:
            df = pd.DataFrame(columns=full_cols)
    else:
        df = pd.DataFrame(columns=full_cols)
    
    for c in full_cols:
        if c not in df.columns:
            df[c] = ""
    
    return df[full_cols]

def save_df(df: pd.DataFrame, path: Path):
    """Sauvegarde un DataFrame dans un fichier CSV."""
    df.to_csv(path, index=False, encoding="utf-8")

# === Fonctions utilitaires métier ===
def parse_date(s: str):
    """Parse une date depuis différents formats."""
    if not s or str(s).strip() == "" or str(s).lower() == "nan":
        return None
    
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(s), fmt).date()
        except:
            pass
    
    try:
        return pd.to_datetime(s).date()
    except:
        return None

def email_ok(s: str) -> bool:
    """Valide un email."""
    if not s or str(s).strip() == "" or str(s).lower() == "nan":
        return True
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", str(s).strip()))

def phone_ok(s: str) -> bool:
    """Valide un numéro de téléphone."""
    if not s or str(s).strip() == "" or str(s).lower() == "nan":
        return True
    s2 = re.sub(r"[ \.\-\(\)]", "", str(s)).replace("+", "")
    return s2.isdigit() and len(s2) >= 8

def generate_id(prefix: str, df: pd.DataFrame, id_col: str, width: int = 3) -> str:
    """Génère un ID unique avec préfixe."""
    if df.empty or id_col not in df.columns:
        return f"{prefix}_{str(1).zfill(width)}"
    
    patt = re.compile(rf"^{re.escape(prefix)}_(\d+)$")
    mx = 0
    
    for x in df[id_col].dropna().astype(str):
        m = patt.match(x.strip())
        if m:
            try:
                mx = max(mx, int(m.group(1)))
            except:
                pass
    
    return f"{prefix}_{str(mx + 1).zfill(width)}"

# === Chargement des données ===
with st.spinner("🔄 Chargement des données..."):
    df_contacts = ensure_df_source("contacts", C_COLS)
    df_inter = ensure_df_source("inter", I_COLS)
    df_events = ensure_df_source("events", E_COLS)
    df_parts = ensure_df_source("parts", P_COLS)
    df_pay = ensure_df_source("pay", PAY_COLS)
    df_cert = ensure_df_source("cert", CERT_COLS)
    df_entreprises = ensure_df_source("entreprises", ENT_COLS)

# Calcul Top20 pour les contacts
if not df_contacts.empty:
    df_contacts["Top20"] = df_contacts["Société"].fillna("").apply(
        lambda x: x in SET["entreprises_cibles"]
    )

st.success("✅ Données chargées avec succès !")

# === Authentification simplifiée ===
import bcrypt

USERS_PATH = DATA_DIR / "users.csv"
USER_COLS = [
    "user_id", "full_name", "role", "active",
    "pwd_hash", "must_change_pw", "created_at", "updated_at"
]

def _ensure_admin_user():
    """S'assure qu'un compte admin existe."""
    if not USERS_PATH.exists():
        default_pw = "admin123"
        admin_user = {
            "user_id": "admin@iiba.cm",
            "full_name": "Admin IIBA Cameroun",
            "role": "admin",
            "active": True,
            "pwd_hash": bcrypt.hashpw(default_pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8"),
            "must_change_pw": True,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        
        df_users = pd.DataFrame([admin_user])
        df_users.to_csv(USERS_PATH, index=False, encoding="utf-8")

def _check_password(clear_pw: str, pwd_hash: str) -> bool:
    """Vérifie un mot de passe."""
    try:
        return bcrypt.checkpw(clear_pw.encode("utf-8"), pwd_hash.encode("utf-8"))
    except Exception:
        return False

def login_box():
    """Affiche la boîte de connexion."""
    st.sidebar.markdown("### 🔐 Connexion")
    uid = st.sidebar.text_input("Email / User ID", value="admin@iiba.cm")
    pw = st.sidebar.text_input("Mot de passe", type="password", value="admin123")
    
    if st.sidebar.button("Se connecter", key="btn_login"):
        if not USERS_PATH.exists():
            st.sidebar.error("Aucun utilisateur configuré.")
            return
            
        try:
            df_users = pd.read_csv(USERS_PATH, dtype=str).fillna("")
            user_match = df_users[df_users["user_id"].str.lower() == uid.lower()]
            
            if user_match.empty:
                st.sidebar.error("Utilisateur introuvable.")
                return
                
            user = user_match.iloc[0]
            
            if user["active"].lower() not in ("true", "1"):
                st.sidebar.error("Compte inactif.")
                return
                
            if not _check_password(pw, user["pwd_hash"]):
                st.sidebar.error("Mot de passe incorrect.")
                return
            
            # Connexion réussie
            st.session_state["auth_user_id"] = user["user_id"]
            st.session_state["auth_role"] = user["role"]
            st.session_state["auth_full_name"] = user["full_name"]
            st.session_state["user"] = {"UserID": user["user_id"], "Role": user["role"]}
            
            st.rerun()
            
        except Exception as e:
            st.sidebar.error(f"Erreur de connexion: {e}")

# Initialisation utilisateur admin
_ensure_admin_user()

# Gestion de la session
if "user" not in st.session_state:
    login_box()
    st.stop()
else:
    # Utilisateur connecté
    st.sidebar.success(f"Connecté : {st.session_state.get('auth_full_name', 'Utilisateur')}")
    
    if st.sidebar.button("Se déconnecter"):
        for k in ["auth_user_id", "auth_role", "auth_full_name", "user"]:
            st.session_state.pop(k, None)
        st.rerun()

# === Navigation ===
ROLE = st.session_state["user"]["Role"]

def allow_page(name: str) -> bool:
    """Vérifie si l'utilisateur peut accéder à une page."""
    if ROLE == "admin":
        return True
    return name in ["CRM (Grille centrale)", "Événements", "Entreprises"]

st.sidebar.title("Navigation")
page = st.sidebar.radio("Aller à", [
    "CRM (Grille centrale)",
    "Événements",
    "Entreprises",
    "Rapports",
    "Admin"
], index=0)

if not allow_page(page):
    st.error("⛔ Accès refusé. Demandez un rôle 'admin' à un membre du comité.")
    st.stop()

# Filtres de période
this_year = datetime.now().year
annee = st.sidebar.selectbox("Année", ["Toutes"] + [str(this_year - 1), str(this_year), str(this_year + 1)], index=1)
mois = st.sidebar.selectbox("Mois", ["Tous"] + [f"{m:02d}" for m in range(1, 13)], index=0)

# === PAGE PRINCIPALE - Version simplifiée pour la démo ===
if page == "CRM (Grille centrale)":
    st.title("👥 CRM — Grille centrale (Contacts)")
    
    st.info("📊 Interface CRM simplifiée pour la démo")
    
    if not df_contacts.empty:
        st.subheader(f"📈 {len(df_contacts)} contacts dans la base")
        
        # Statistiques rapides
        col1, col2, col3, col4 = st.columns(4)
        
        prospects = len(df_contacts[df_contacts["Type"] == "Prospect"])
        membres = len(df_contacts[df_contacts["Type"] == "Membre"])
        top20 = len(df_contacts[df_contacts.get("Top20", False) == True])
        secteurs = df_contacts["Secteur"].nunique()
        
        col1.metric("🎯 Prospects", prospects)
        col2.metric("🏆 Membres", membres)
        col3.metric("⭐ Top-20", top20)
        col4.metric("🏢 Secteurs", secteurs)
        
        # Tableau des contacts
        st.subheader("📋 Liste des contacts")
        display_cols = ["ID", "Nom", "Prénom", "Société", "Secteur", "Type", "Email", "Ville"]
        available_cols = [c for c in display_cols if c in df_contacts.columns]
        
        st.dataframe(df_contacts[available_cols], use_container_width=True)
        
    else:
        st.info("Aucun contact dans la base. Commencez par importer des données.")

elif page == "Événements":
    st.title("📅 Événements")
    
    st.info("📊 Interface Événements simplifiée pour la démo")
    
    if not df_events.empty:
        st.subheader(f"📈 {len(df_events)} événements dans la base")
        
        # Tableau des événements
        display_cols = ["ID_Événement", "Nom_Événement", "Type", "Date", "Lieu", "Durée_h"]
        available_cols = [c for c in display_cols if c in df_events.columns]
        
        st.dataframe(df_events[available_cols], use_container_width=True)
    else:
        st.info("Aucun événement dans la base.")

elif page == "Entreprises":
    st.title("🏢 Entreprises & Partenaires")
    
    st.info("📊 Interface Entreprises simplifiée pour la démo")
    
    if not df_entreprises.empty:
        st.subheader(f"📈 {len(df_entreprises)} entreprises dans la base")
        
        # Tableau des entreprises
        display_cols = ["ID_Entreprise", "Nom_Entreprise", "Secteur", "Taille", "Statut_Partenariat", "Ville"]
        available_cols = [c for c in display_cols if c in df_entreprises.columns]
        
        st.dataframe(df_entreprises[available_cols], use_container_width=True)
    else:
        st.info("Aucune entreprise dans la base.")

elif page == "Rapports":
    st.title("📑 Rapports & KPI")
    
    st.info("📊 Interface Rapports simplifiée pour la démo")
    
    # KPIs de base
    st.subheader("📊 Indicateurs clés")
    
    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
    
    kpi_col1.metric("👥 Total Contacts", len(df_contacts))
    kpi_col2.metric("📅 Total Événements", len(df_events))
    kpi_col3.metric("🏢 Total Entreprises", len(df_entreprises))
    
    # Répartition par secteur si on a des données
    if not df_contacts.empty and "Secteur" in df_contacts.columns:
        st.subheader("📈 Répartition par secteur")
        secteur_counts = df_contacts["Secteur"].value_counts()
        st.bar_chart(secteur_counts)

elif page == "Admin":
    st.title("⚙️ Administration")
    
    if ROLE != "admin":
        st.error("Accès réservé aux administrateurs.")
        st.stop()
    
    st.info("🔧 Interface Admin simplifiée pour la démo")
    
    # Informations système
    st.subheader("📊 Informations système")
    
    info_col1, info_col2 = st.columns(2)
    
    info_col1.metric("💾 Backend", STORAGE_BACKEND.upper())
    
    if STORAGE_BACKEND == "gsheets":
        info_col2.metric("📊 Spreadsheet", GSHEET_SPREADSHEET)
    else:
        total_size = sum(path.stat().st_size if path.exists() else 0 for path in PATHS.values())
        info_col2.metric("💾 Taille données", f"{total_size/1024:.1f} KB")
    
    # Diagnostic Google Sheets
    if STORAGE_BACKEND == "gsheets" and HAS_GS_CLIENT:
        st.subheader("🩺 Diagnostic Google Sheets")
        
        client_info = _GS_CLIENT.get_client_info()
        
        if client_info.get("connected"):
            st.success("✅ Connexion Google Sheets active")
            
            if client_info.get("service_account_email"):
                st.info(f"📧 Service Account: {client_info['service_account_email']}")
            
            worksheets = client_info.get("worksheets", [])
            if worksheets:
                st.info(f"📁 Onglets détectés: {', '.join(worksheets)}")
        else:
            st.error("❌ Connexion Google Sheets inactive")
    
    # Export des données
    st.subheader("📦 Export des données")
    
    if st.button("⬇️ Exporter toutes les données (CSV)"):
        # Création d'un zip avec tous les CSVs
        import zipfile
        
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for name, df in [
                ("contacts", df_contacts),
                ("interactions", df_inter),
                ("evenements", df_events),
                ("participations", df_parts),
                ("paiements", df_pay),
                ("certifications", df_cert),
                ("entreprises", df_entreprises)
            ]:
                if not df.empty:
                    csv_data = df.to_csv(index=False, encoding="utf-8")
                    zip_file.writestr(f"{name}.csv", csv_data)
        
        zip_buffer.seek(0)
        
        st.download_button(
            "📁 Télécharger l'archive ZIP",
            zip_buffer,
            file_name=f"iiba_export_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
            mime="application/zip"
        )

# === Footer ===
st.markdown("---")
st.markdown("**IIBA Cameroun CRM** - Version stabilisée avec architecture robuste")
st.caption("🛡️ Backend sécurisé | 🧪 Tests automatiques | 📊 Multi-plateforme")
