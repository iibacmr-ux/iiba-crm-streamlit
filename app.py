# Streamlit CRM IIBA Cameroun - app.py complet avec page Entreprises

from datetime import datetime, date, timedelta
from pathlib import Path
import io
import json
import re
import unicodedata
import numpy as np
import pandas as pd
import streamlit as st

# AgGrid
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode
    HAS_AGGRID = True
except Exception:
    HAS_AGGRID = False

# Graphiques Altair
try:
    import altair as alt
except Exception:
    alt = None

import openpyxl

st.set_page_config(page_title="IIBA Cameroun — CRM", page_icon="📊", layout="wide")

# ----------- Paths et schémas ----------------
if "DATA_DIR" not in globals() or DATA_DIR is None:
    DATA_DIR = Path("./data")
DATA_DIR = Path(DATA_DIR)
DATA_DIR.mkdir(parents=True, exist_ok=True)

PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "inter": DATA_DIR / "interactions.csv",
    "events": DATA_DIR / "evenements.csv",
    "parts": DATA_DIR / "participations.csv",
    "pay": DATA_DIR / "paiements.csv",
    "cert": DATA_DIR / "certifications.csv",
    "entreprises": DATA_DIR / "entreprises.csv",  # NOUVEAU
    "params": DATA_DIR / "parametres.csv",
    "logs": DATA_DIR / "migration_logs.jsonl"
}

C_COLS = ["ID","Nom","Prénom","Genre","Titre","Société","Secteur","Email","Téléphone","LinkedIn",
          "Ville","Pays","Type","Source","Statut","Score_Engagement","Date_Creation","Notes","Top20"]
I_COLS = ["ID_Interaction","ID","Date","Canal","Objet","Résumé","Résultat","Prochaine_Action","Relance","Responsable"]
E_COLS = ["ID_Événement","Nom_Événement","Type","Date","Durée_h","Lieu","Formateur","Objectif","Periode",
          "Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total","Notes"]
P_COLS = ["ID_Participation","ID","ID_Événement","Rôle","Inscription","Arrivée","Temps_Present","Feedback","Note","Commentaire"]
PAY_COLS = ["ID_Paiement","ID","ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence","Notes","Relance"]
CERT_COLS = ["ID_Certif","ID","Type_Certif","Date_Examen","Résultat","Score","Date_Obtention","Validité","Renouvellement","Notes"]

# NOUVEAU: Schéma pour les entreprises
ENT_COLS = ["ID_Entreprise","Nom_Entreprise","Secteur","Taille","CA_Annuel","Nb_Employes","Ville","Pays",
           "Contact_Principal","Email_Principal","Telephone_Principal","Site_Web","Statut_Partenariat",
           "Type_Partenariat","Date_Premier_Contact","Responsable_IIBA","Notes","Opportunites","Date_Maj"]

# === AUDIT / META ===
AUDIT_COLS = ["Created_At", "Created_By", "Updated_At", "Updated_By"]

ALL_SCHEMAS = {
    "contacts": C_COLS, "interactions": I_COLS, "evenements": E_COLS,
    "participations": P_COLS, "paiements": PAY_COLS, "certifications": CERT_COLS,
    "entreprises": ENT_COLS,  # NOUVEAU
}

DEFAULT_LISTS = {
    "genres":"Homme|Femme|Autre",
    "secteurs":"Banque|Télécom|IT|Éducation|Santé|ONG|Industrie|Public|Autre",
    "types_contact":"Membre|Prospect|Formateur|Partenaire",
    "sources":"Afterwork|Formation|LinkedIn|Recommandation|Site Web|Salon|Autre",
    "statuts_engagement":"Actif|Inactif|À relancer",
    "canaux":"Appel|Email|WhatsApp|Zoom|Présentiel|Autre",
    "villes":"Douala|Yaoundé|Limbe|Bafoussam|Garoua|Autres",
    "pays":"Cameroun|Côte d'Ivoire|Sénégal|France|Canada|Autres",
    "types_evenements":"Formation|Groupe d'étude|BA MEET UP|Webinaire|Conférence|Certification",
    "lieux":"Présentiel|Zoom|Hybride",
    "resultats_inter":"Positif|Négatif|À suivre|Sans suite",
    "statuts_paiement":"Réglé|Partiel|Non payé",
    "moyens_paiement":"Mobile Money|Virement|CB|Cash",
    "types_certif":"ECBA|CCBA|CBAP|PBA",
    "entreprises_cibles":"Dangote|MUPECI|SALAM|SUNU IARD|ENEO|PAD|PAK",
    # NOUVEAU: Listes pour entreprises
    "tailles_entreprise":"TPE (< 10)|PME (10-250)|ETI (250-5000)|GE (> 5000)",
    "statuts_partenariat":"Prospect|Partenaire|Client|Partenaire Stratégique|Inactif",
    "types_partenariat":"Formation|Recrutement|Conseil|Sponsoring|Certification|Autre",
    "responsables_iiba":"Aymard|Alix|Comité|Non assigné",
}

PARAM_DEFAULTS = {
    "vip_threshold":"500000",
    "score_w_interaction":"1",
    "score_w_participation":"1",
    "score_w_payment_regle":"2",
    "interactions_lookback_days":"90",
    "rule_hot_interactions_recent_min":"3",
    "rule_hot_participations_min":"1",
    "rule_hot_payment_partial_counts_as_hot":"1",
    "grid_crm_columns": ",".join([
        "ID","Nom","Prénom","Société","Type","Statut","Email",
        "Interactions","Participations","CA_réglé","Impayé","Resp_principal","A_animé_ou_invité",
        "Score_composite","Proba_conversion","Tags","Created_At", "Created_By", "Updated_At", "Updated_By"
    ]),
    "grid_events_columns": ",".join(E_COLS),
    "grid_entreprises_columns": ",".join([
        "ID_Entreprise","Nom_Entreprise","Secteur","Taille","Statut_Partenariat",
        "Type_Partenariat","Contact_Principal","Email_Principal","Responsable_IIBA","Date_Premier_Contact"
    ]),
    "kpi_enabled": ",".join([
        "contacts_total","prospects_actifs","membres","events_count",
        "participations_total","ca_regle","impayes","taux_conversion"
    ]),
    "kpi_target_contacts_total_year_2025":"1000",
    "kpi_target_ca_regle_year_2025":"5000000",
    "contacts_period_fallback": "1",
    # NOUVEAU: Paramètres entreprises
    "entreprises_scoring_ca_weight":"0.3",
    "entreprises_scoring_employes_weight":"0.2",
    "entreprises_scoring_interactions_weight":"0.5",
    "entreprises_ca_seuil_gros":"10000000",
    "entreprises_employes_seuil_gros":"500",
}

ALL_DEFAULTS = {**PARAM_DEFAULTS, **{f"list_{k}":v for k,v in DEFAULT_LISTS.items()}}

# === AUDIT / META ===
def _now_iso():
    from datetime import datetime
    return datetime.utcnow().isoformat()

def stamp_create(row: dict, user: dict):
    """Ajoute/initialise les colonnes d'audit lors d'une création."""
    row = dict(row)
    now = _now_iso()
    uid = user.get("UserID", "system") if user else "system"
    row.setdefault("Created_At", now)
    row.setdefault("Created_By", uid)
    row["Updated_At"] = row.get("Updated_At", now)
    row["Updated_By"] = row.get("Updated_By", uid)
    return row

def stamp_update(row: dict, user: dict):
    """Met à jour Updated_* lors d'une édition."""
    row = dict(row)
    row["Updated_At"] = _now_iso()
    row["Updated_By"] = user.get("UserID", "system") if user else "system"
    return row

def load_params()->dict:
    if not PATHS["params"].exists():
        df = pd.DataFrame({"key":list(ALL_DEFAULTS.keys()), "value":list(ALL_DEFAULTS.values())})
        df.to_csv(PATHS["params"], index=False, encoding="utf-8")
        return ALL_DEFAULTS.copy()
    try:
        df = pd.read_csv(PATHS["params"], dtype=str).fillna("")
        d = {r["key"]: r["value"] for _,r in df.iterrows()}
    except Exception:
        d = ALL_DEFAULTS.copy()
    for k,v in ALL_DEFAULTS.items():
        if k not in d: d[k]=v
    return d

def save_params(d:dict):
    rows = [{"key":k,"value":str(v)} for k,v in d.items()]
    pd.DataFrame(rows).to_csv(PATHS["params"], index=False, encoding="utf-8")

PARAMS = load_params()

def get_list(name:str)->list:
    raw = PARAMS.get(f"list_{name}", DEFAULT_LISTS.get(name,""))
    vals = [x.strip() for x in str(raw).split("|") if x.strip()]
    return vals

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
    # NOUVEAU: Listes pour entreprises
    "tailles_entreprise": get_list("tailles_entreprise"),
    "statuts_partenariat": get_list("statuts_partenariat"),
    "types_partenariat": get_list("types_partenariat"),
    "responsables_iiba": get_list("responsables_iiba"),
}

# Utils for dataframe loading/saving
def ensure_df(path:Path, cols:list)->pd.DataFrame:
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

def save_df(df:pd.DataFrame, path:Path):
    df.to_csv(path, index=False, encoding="utf-8")

def parse_date(s:str):
    if not s or str(s).strip()=="" or str(s).lower()=="nan":
        return None
    for fmt in ("%Y-%m-%d","%d/%m/%Y","%Y/%m/%d"):
        try:
            return datetime.strptime(str(s), fmt).date()
        except:
            pass
    try:
        return pd.to_datetime(s).date()
    except:
        return None

def email_ok(s:str)->bool:
    if not s or str(s).strip()=="" or str(s).lower()=="nan":
        return True
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", str(s).strip()))

def phone_ok(s:str)->bool:
    if not s or str(s).strip()=="" or str(s).lower()=="nan":
        return True
    s2 = re.sub(r"[ \.\-\(\)]","",str(s)).replace("+","")
    return s2.isdigit() and len(s2)>=8

def generate_id(prefix:str, df:pd.DataFrame, id_col:str, width:int=3)->str:
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
    return f"{prefix}_{str(mx+1).zfill(width)}"

def log_event(kind:str, payload:dict):
    rec = {"ts": datetime.now().isoformat(), "kind": kind, **payload}
    with PATHS["logs"].open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

# Load data
df_contacts = ensure_df(PATHS["contacts"], C_COLS)
df_inter    = ensure_df(PATHS["inter"], I_COLS)
df_events   = ensure_df(PATHS["events"], E_COLS)
df_parts    = ensure_df(PATHS["parts"], P_COLS)
df_pay      = ensure_df(PATHS["pay"], PAY_COLS)
df_cert     = ensure_df(PATHS["cert"], CERT_COLS)
df_entreprises = ensure_df(PATHS["entreprises"], ENT_COLS)  # NOUVEAU

if not df_contacts.empty:
    df_contacts["Top20"] = df_contacts["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])

# === AUTH MINIMAL ===
import bcrypt

USERS_PATH = DATA_DIR / "users.csv"
USER_COLS = ["user_id", "full_name", "role", "active", "pwd_hash", "must_change_pw", "created_at", "updated_at"]

def _normalize_users_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    alias = {
        "userid": "user_id", "email": "user_id", "login": "user_id",
        "nom": "full_name", "prenom": "full_name",
        "role": "role", "active": "active",
        "passwordhash": "pwd_hash", "pwdhash": "pwd_hash",
        "mustchangepw": "must_change_pw",
        "created_at": "created_at", "updated_at": "updated_at",
    }
    for old, new in alias.items():
        if old in df.columns and new not in df.columns:
            df[new] = df[old]
    for c in USER_COLS:
        if c not in df.columns:
            df[c] = "" if c not in ("active", "must_change_pw") else False
    df["active"] = df["active"].astype(str).str.lower().isin(["1","true","yes","y","on"])
    df["must_change_pw"] = df["must_change_pw"].astype(str).str.lower().isin(["1","true","yes","y","on"])
    return df[USER_COLS].copy()

def _ensure_users_df() -> pd.DataFrame:
    if not USERS_PATH.exists():
        dfu = pd.DataFrame(columns=USER_COLS)
        default_pw = "admin123"
        row = {
            "user_id": "admin@iiba.cm",
            "full_name": "Admin IIBA Cameroun",
            "role": "admin",
            "active": True,
            "pwd_hash": bcrypt.hashpw(default_pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8"),
            "must_change_pw": True,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        dfu = pd.concat([dfu, pd.DataFrame([row])], ignore_index=True)
        dfu.to_csv(USERS_PATH, index=False, encoding="utf-8")
        return dfu
    try:
        raw = pd.read_csv(USERS_PATH, dtype=str).fillna("")
    except Exception:
        raw = pd.DataFrame(columns=USER_COLS)
    dfu = _normalize_users_df(raw)
    if not (dfu["user_id"] == "admin@iiba.cm").any():
        default_pw = "admin123"
        row = {
            "user_id": "admin@iiba.cm",
            "full_name": "Admin IIBA Cameroun",
            "role": "admin",
            "active": True,
            "pwd_hash": bcrypt.hashpw(default_pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8"),
            "must_change_pw": True,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        dfu = pd.concat([dfu, pd.DataFrame([row])], ignore_index=True)
        dfu.to_csv(USERS_PATH, index=False, encoding="utf-8")
    return dfu

# --- PATCH: forcer l'activation de admin@iiba.cm au démarrage ---
def _force_activate_admin():
    dfu = _ensure_users_df()
    dfu = _normalize_users_df(dfu)

    m = dfu["user_id"].astype(str).str.strip().str.lower() == "admin@iiba.cm"  
    
    if m.any():
        # réactive + redonne le rôle admin
        st.sidebar.error(f"_force_activate_admin - if m.any(): user_id : {dfu.loc[m, "user_id"]}")  # print 
        dfu.loc[m, "active"] = True
        dfu.loc[m, "role"] = "admin"
        dfu.loc[m, "updated_at"] = datetime.now().isoformat(timespec="seconds")
        dfu.to_csv(USERS_PATH, index=False, encoding="utf-8")
    else:
        # si le compte n'existe pas, on le (ré)crée proprement
        st.sidebar.error(f"réactive + redonne le rôle admin - else : {m}") # print
        default_pw = "admin123"
        row = {
            "user_id": "admin@iiba.cm",
            "full_name": "Admin IIBA Cameroun",
            "role": "admin",
            "active": True,
            "pwd_hash": bcrypt.hashpw(default_pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8"),
            "must_change_pw": True,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        dfu = pd.concat([dfu, pd.DataFrame([row])], ignore_index=True)
        dfu = dfu[USER_COLS]  # garantir l'ordre/les colonnes
        dfu.to_csv(USERS_PATH, index=False, encoding="utf-8")

# appelez-le une fois au chargement (avant login_box())
_force_activate_admin()

def _check_password(clear_pw: str, pwd_hash: str) -> bool:
    try:
        return bcrypt.checkpw(clear_pw.encode("utf-8"), pwd_hash.encode("utf-8"))
    except Exception:
        return False

def _safe_rerun():
    import streamlit as _st
    if hasattr(_st, "rerun"):
        _st.rerun()
    elif hasattr(_st, "experimental_rerun"):
        _st.experimental_rerun()

def login_box():
    st.sidebar.markdown("### 🔐 Connexion")
    uid = st.sidebar.text_input("Email / User ID", value=st.session_state.get("last_uid",""))
    pw = st.sidebar.text_input("Mot de passe", type="password") 
    
    if st.sidebar.button("Se connecter", key="btn_login"):
        users_df = _ensure_users_df()
        users_df = _normalize_users_df(users_df)
        m = (users_df["user_id"].astype(str).str.strip().str.lower() == str(uid).strip().lower()) 
        
        st.sidebar.error(f"login_box: user_id : {users_df.loc[m, "user_id"]}")  # print 
        st.sidebar.error(f"login_box: user_id : {users_df.loc[m, "updated_at"]}")  # print 
        st.sidebar.error(f"login_box: user_id : {users_df.loc[m, "active"]}")  # print  
        st.sidebar.error(f"login_box: user_id : {users_df.loc[m, "role"]}")  # print  
        st.sidebar.error(f"login_box: user_id : {users_df.loc[m, "pwd_hash"]}")  # print pwd_hash.encode("utf-8")
        st.sidebar.error(f"login_box: user_id : {users_df.loc[m, "pwd_hash"].encode("utf-8")}")  # print pwd_hash.encode("utf-8")

        if not m.any():
            st.sidebar.error("Utilisateur introuvable.")
            return
        row = users_df[m].iloc[0]
        if not bool(row["active"]):
            st.sidebar.error("Compte inactif. Contactez un administrateur.")
            return
        if not _check_password(pw, row["pwd_hash"]): 
            st.sidebar.error("Mot de passe incorrect.")
            return

        st.session_state["auth_user_id"] = row["user_id"]
        st.session_state["auth_role"] = row["role"]
        st.session_state["auth_full_name"] = row["full_name"]
        st.session_state["last_uid"] = uid
        st.session_state["user"] = {"UserID": row["user_id"], "Role": row["role"]}

        if bool(row.get("must_change_pw", False)):
            st.session_state["force_change_pw"] = True
        else:
            st.session_state["force_change_pw"] = False
        _safe_rerun()

    if "auth_user_id" in st.session_state:
        st.sidebar.success(f"Connecté : {st.session_state['auth_full_name']} ({st.session_state['auth_role']})")
        if st.sidebar.button("Se déconnecter", key="btn_logout"):
            for k in ["auth_user_id","auth_role","auth_full_name","force_change_pw","user"]:
                st.session_state.pop(k, None)
            _safe_rerun()

if "user" not in st.session_state:
    login_box()
    st.stop()

ROLE = st.session_state["user"]["Role"]
def allow_page(name:str)->bool:
    if ROLE == "admin":
        return True
    return name in ["CRM (Grille centrale)","Événements","Entreprises"]  # NOUVEAU: ajout Entreprises

# Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Aller à", [
    "CRM (Grille centrale)",
    "Événements", 
    "Entreprises",  # NOUVEAU
    "Rapports",
    "Admin"
], index=0)

if not allow_page(page):
    st.error("⛔ Accès refusé. Demandez un rôle 'admin' à un membre du comité.")
    st.stop()

this_year = datetime.now().year
annee = st.sidebar.selectbox("Année", ["Toutes"]+[str(this_year-1),str(this_year),str(this_year+1)], index=1)
mois = st.sidebar.selectbox("Mois", ["Tous"]+[f"{m:02d}" for m in range(1,13)], index=0)

def aggregates_for_contacts(today=None):
    today = today or date.today()
    vip_thr = float(PARAMS.get("vip_threshold", "500000"))
    w_int = float(PARAMS.get("score_w_interaction", "1"))
    w_part = float(PARAMS.get("score_w_participation", "1"))
    w_pay = float(PARAMS.get("score_w_payment_regle", "2"))
    lookback = int(PARAMS.get("interactions_lookback_days", "90"))
    hot_int_min = int(PARAMS.get("rule_hot_interactions_recent_min", "3"))
    hot_part_min = int(PARAMS.get("rule_hot_participations_min", "1"))
    hot_partiel = PARAMS.get("rule_hot_payment_partial_counts_as_hot", "1") in ("1", "true", "True")

    inter_count = df_inter.groupby("ID")["ID_Interaction"].count() if not df_inter.empty else pd.Series(dtype=int)
    inter_dates = pd.to_datetime(df_inter["Date"], errors="coerce") if not df_inter.empty else pd.Series(dtype="datetime64[ns]")
    last_contact = df_inter.assign(_d=inter_dates).groupby("ID")["_d"].max() if not df_inter.empty else pd.Series(dtype="datetime64[ns]")
    recent_cut = today - timedelta(days=lookback)
    recent_inter = df_inter.assign(_d=inter_dates).loc[lambda d: d["_d"] >= pd.Timestamp(recent_cut)].groupby("ID")["ID_Interaction"].count() if not df_inter.empty else pd.Series(dtype=int)

    resp_max = pd.Series(dtype=str)
    if not df_inter.empty:
        tmp = df_inter.groupby(["ID","Responsable"])["ID_Interaction"].count().reset_index()
        idx = tmp.groupby("ID")["ID_Interaction"].idxmax()
        resp_max = tmp.loc[idx].set_index("ID")["Responsable"]
        
    parts_count = df_parts.groupby("ID")["ID_Participation"].count() if not df_parts.empty else pd.Series(dtype=int)
    has_anim = pd.Series(dtype=bool)
    if not df_parts.empty:
        has_anim = df_parts.assign(_anim=df_parts["Rôle"].isin(["Animateur","Invité"])).groupby("ID")["_anim"].any()

    pay_reg_count = pd.Series(dtype=int)
    if not df_pay.empty:
        pay = df_pay.copy()
        pay["Montant"] = pd.to_numeric(pay["Montant"], errors="coerce").fillna(0.0)
        total_pay = pay.groupby("ID")["Montant"].sum()
        pay_regle = pay[pay["Statut"]=="Réglé"].groupby("ID")["Montant"].sum()
        pay_impaye = pay[pay["Statut"]!="Réglé"].groupby("ID")["Montant"].sum()
        pay_reg_count = pay[pay["Statut"]=="Réglé"].groupby("ID")["Montant"].count()
        has_partiel = pay[pay["Statut"]=="Partiel"].groupby("ID")["Montant"].count()
    else:
        total_pay = pd.Series(dtype=float)
        pay_regle = pd.Series(dtype=float)
        pay_impaye = pd.Series(dtype=float)
        has_partiel = pd.Series(dtype=int)

    has_cert = pd.Series(dtype=bool)
    if not df_cert.empty:
        has_cert = df_cert[df_cert["Résultat"]=="Réussi"].groupby("ID")["ID_Certif"].count() > 0

    ag = pd.DataFrame(index=df_contacts["ID"])
    ag["Interactions"] = ag.index.map(inter_count).fillna(0).astype(int)
    ag["Interactions_recent"] = ag.index.map(recent_inter).fillna(0).astype(int)
    ag["Dernier_contact"] = ag.index.map(last_contact)
    ag["Dernier_contact"] = pd.to_datetime(ag["Dernier_contact"], errors="coerce")
    ag["Dernier_contact"] = ag["Dernier_contact"].dt.date
    ag["Resp_principal"] = ag.index.map(resp_max).fillna("")
    ag["Participations"] = ag.index.map(parts_count).fillna(0).astype(int)
    ag["A_animé_ou_invité"] = ag.index.map(has_anim).fillna(False)
    ag["CA_total"] = ag.index.map(total_pay).fillna(0.0)
    ag["CA_réglé"] = ag.index.map(pay_regle).fillna(0.0)
    ag["Impayé"] = ag.index.map(pay_impaye).fillna(0.0)
    ag["Paiements_regles_n"] = ag.index.map(pay_reg_count).fillna(0).astype(int)
    ag["A_certification"] = ag.index.map(has_cert).fillna(False)
    ag["Score_composite"] = (w_int * ag["Interactions"] + w_part * ag["Participations"] + w_pay * ag["Paiements_regles_n"]).round(2)

    def make_tags(row):
        tags=[]
        if row.name in set(df_contacts.loc[(df_contacts["Type"]=="Prospect") & (df_contacts["Top20"]==True), "ID"]):
            tags.append("Prospect Top-20")
        if row["Participations"] >= 3 and row.name in set(df_contacts[df_contacts["Type"]=="Prospect"]["ID"]) and row["CA_réglé"] <= 0:
            tags.append("Régulier-non-converti")
        if row["A_animé_ou_invité"] or row["Participations"] >= 4:
            tags.append("Futur formateur")
        if row["A_certification"]:
            tags.append("Ambassadeur (certifié)")
        if row["CA_réglé"] >= vip_thr:
            tags.append("VIP (CA élevé)")
        return ", ".join(tags)

    ag["Tags"] = ag.apply(make_tags, axis=1)

    def proba(row):
        if row.name in set(df_contacts[df_contacts["Type"]=="Membre"]["ID"]):
            return "Converti"
        chaud = (row["Interactions_recent"] >= hot_int_min and row["Participations"] >= hot_part_min)
        if hot_partiel and row["Impayé"] > 0 and row["CA_réglé"] == 0:
            chaud = True
        tiede = (row["Interactions_recent"] >= 1 or row["Participations"] >= 1)
        if chaud:
            return "Chaud"
        if tiede:
            return "Tiède"
        return "Froid"

    ag["Proba_conversion"] = ag.apply(proba, axis=1)
    return ag.reset_index(names="ID")

# CRM Grille centrale (CODE EXISTANT CONSERVÉ)
if page == "CRM (Grille centrale)":
    st.title("👥 CRM — Grille centrale (Contacts)")
    colf1, colf2, colf3, colf4 = st.columns([2,1,1,1])
    q = colf1.text_input("Recherche (nom, société, email)…","")
    page_size = colf2.selectbox("Taille de page", [20,50,100,200], index=0)
    type_filtre = colf3.selectbox("Type", ["Tous"] + SET["types_contact"])
    top20_only = colf4.checkbox("Top-20 uniquement", value=False)

    dfc = df_contacts.copy()
    ag = aggregates_for_contacts()
    dfc = dfc.merge(ag, on="ID", how="left")

    if q:
        qs = q.lower()
        dfc = dfc[dfc.apply(lambda r: qs in str(r["Nom"]).lower() or qs in str(r["Prénom"]).lower()
                          or qs in str(r["Société"]).lower() or qs in str(r["Email"]).lower(), axis=1)]
    if type_filtre != "Tous":
        dfc = dfc[dfc["Type"] == type_filtre]
    if top20_only:
        dfc = dfc[dfc["Top20"] == True]

    def parse_cols(s, defaults):
        cols = [c.strip() for c in str(s).split(",") if c.strip()]
        valid = [c for c in cols if c in dfc.columns]
        return valid if valid else defaults

    default_cols = [
        "ID","Nom","Prénom","Société","Type","Statut","Email",
        "Interactions","Participations","CA_réglé","Impayé","Resp_principal","A_animé_ou_invité",
        "Score_composite","Proba_conversion","Tags"
    ]
    default_cols += [c for c in AUDIT_COLS if c in dfc.columns]
    table_cols = parse_cols(PARAMS.get("grid_crm_columns", ""), default_cols)    

    def _label_contact(row):
        return f"{row['ID']} — {row['Prénom']} {row['Nom']} — {row['Société']}"
    options = [] if dfc.empty else dfc.apply(_label_contact, axis=1).tolist()
    id_map = {} if dfc.empty else dict(zip(options, dfc["ID"]))

    colsel, _ = st.columns([3,1])
    sel_label = colsel.selectbox("Contact sélectionné (sélecteur maître)", [""] + options, index=0, key="select_contact_label")
    if sel_label:
        st.session_state["selected_contact_id"] = id_map[sel_label]

    if HAS_AGGRID and not dfc.empty:
        dfc_show = dfc[table_cols].copy()
        proba_style = JsCode("""
            function(params) {
              const v = params.value;
              let color = null;
              if (v === 'Chaud') color = '#10B981';
              else if (v === 'Tiède') color = '#F59E0B';
              else if (v === 'Froid') color = '#EF4444';
              else if (v === 'Converti') color = '#6366F1';
              if (color){
                return { color: 'white', 'font-weight':'600', 'text-align':'center', 'border-radius':'12px', 'background-color': color };
              }
              return {};
            }
        """)
        gob = GridOptionsBuilder.from_dataframe(dfc_show)
        gob.configure_default_column(filter=True, sortable=True, resizable=True)
        gob.configure_selection("single", use_checkbox=True)
        gob.configure_pagination(paginationAutoPageSize=False, paginationPageSize=page_size)
        gob.configure_side_bar()
        if "Proba_conversion" in dfc_show.columns:
            gob.configure_column("Proba_conversion", cellStyle=proba_style)
        grid = AgGrid(
            dfc_show, gridOptions=gob.build(), height=520,
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            key="crm_grid", allow_unsafe_jscode=True
        )
        if grid and grid.get("selected_rows"):
            new_sel = grid["selected_rows"][0].get("ID")
            if new_sel:
                st.session_state["selected_contact_id"] = new_sel
    else:
        st.info("Installez `streamlit-aggrid` pour filtres & pagination avancés.")
        st.dataframe(dfc[table_cols], use_container_width=True)

    st.markdown("---")
    cL, cR = st.columns([1,2])

    with cL:
        st.subheader("Fiche Contact")
        sel_id = st.session_state.get("selected_contact_id", None)
        if sel_id:
            c = df_contacts[df_contacts["ID"] == sel_id]
            if not c.empty:
                d = c.iloc[0].to_dict()
                
                a1, a2 = st.columns(2)
                if a1.button("➕ Nouveau contact"):
                    st.session_state["selected_contact_id"] = None
                if a2.button("🧬 Dupliquer ce contact", disabled=not bool(sel_id)):
                    if sel_id:
                        src = df_contacts[df_contacts["ID"] == sel_id]
                        if not src.empty:
                            clone = src.iloc[0].to_dict()
                            new_id = generate_id("CNT", df_contacts, "ID")
                            clone["ID"] = new_id
                            globals()["df_contacts"] = pd.concat([df_contacts, pd.DataFrame([clone])], ignore_index=True)
                            save_df(df_contacts, PATHS["contacts"])
                            st.session_state["selected_contact_id"] = new_id
                            st.success(f"Contact dupliqué sous l'ID {new_id}.")
                            
                with st.form("edit_contact"):
                    st.text_input("ID", value=d["ID"], disabled=True)
                    n1, n2 = st.columns(2)
                    nom = n1.text_input("Nom", d.get("Nom",""))
                    prenom = n2.text_input("Prénom", d.get("Prénom",""))
                    g1,g2 = st.columns(2)
                    genre = g1.selectbox("Genre", SET["genres"], index=SET["genres"].index(d.get("Genre","Homme")) if d.get("Genre","Homme") in SET["genres"] else 0)
                    titre = g2.text_input("Titre / Position", d.get("Titre",""))
                    s1,s2 = st.columns(2)
                    societe = s1.text_input("Société", d.get("Société",""))
                    secteur = s2.selectbox("Secteur", SET["secteurs"], index=SET["secteurs"].index(d.get("Secteur","Autre")) if d.get("Secteur","Autre") in SET["secteurs"] else len(SET["secteurs"])-1)
                    e1,e2,e3 = st.columns(3)
                    email = e1.text_input("Email", d.get("Email",""))
                    tel = e2.text_input("Téléphone", d.get("Téléphone",""))
                    linkedin = e3.text_input("LinkedIn", d.get("LinkedIn",""))
                    l1,l2,l3 = st.columns(3)
                    ville = l1.selectbox("Ville", SET["villes"], index=SET["villes"].index(d.get("Ville","Autres")) if d.get("Ville","Autres") in SET["villes"] else len(SET["villes"])-1)
                    pays = l2.selectbox("Pays", SET["pays"], index=SET["pays"].index(d.get("Pays","Cameroun")) if d.get("Pays","Cameroun") in SET["pays"] else 0)
                    typec = l3.selectbox("Type", SET["types_contact"], index=SET["types_contact"].index(d.get("Type","Prospect")) if d.get("Type","Prospect") in SET["types_contact"] else 0)
                    s3,s4,s5 = st.columns(3)
                    source = s3.selectbox("Source", SET["sources"], index=SET["sources"].index(d.get("Source","LinkedIn")) if d.get("Source","LinkedIn") in SET["sources"] else 0)
                    statut = s4.selectbox("Statut", SET["statuts_engagement"], index=SET["statuts_engagement"].index(d.get("Statut","Actif")) if d.get("Statut","Actif") in SET["statuts_engagement"] else 0)
                    score = s5.number_input("Score IIBA", value=float(d.get("Score_Engagement") or 0), step=1.0)
                    dc = st.date_input("Date de création", value=parse_date(d.get("Date_Creation")) or date.today())
                    notes = st.text_area("Notes", d.get("Notes",""))
                    top20 = st.checkbox("Top-20 entreprise", value=bool(str(d.get("Top20")).lower() in ["true","1","yes"]))
                    ok = st.form_submit_button("💾 Enregistrer le contact")
                    if ok:
                        if not str(nom).strip():
                            st.error("❌ Le nom du contact est obligatoire. Enregistrement annulé.")
                            st.stop()
                        if not email_ok(email):
                            st.error("Email invalide.")
                            st.stop()
                        if not phone_ok(tel):
                            st.error("Téléphone invalide.")
                            st.stop()
                        idx = df_contacts.index[df_contacts["ID"] == sel_id][0]
                        new_row = {"ID":sel_id,"Nom":nom,"Prénom":prenom,"Genre":genre,"Titre":titre,"Société":societe,"Secteur":secteur,
                                       "Email":email,"Téléphone":tel,"LinkedIn":linkedin,"Ville":ville,"Pays":pays,"Type":typec,"Source":source,
                                       "Statut":statut,"Score_Engagement":int(score),"Date_Creation":dc.isoformat(),"Notes":notes,"Top20":top20}
                        raw_existing = df_contacts.loc[idx].to_dict()
                        raw_existing.update(new_row)
                        raw_existing = stamp_update(raw_existing, st.session_state.get("user", {}))
                        df_contacts.loc[idx] = raw_existing
                        save_df(df_contacts, PATHS["contacts"])
                        st.success("Contact mis à jour.")
                st.markdown("---")
                with st.expander("➕ Ajouter ce contact à un **nouvel événement**"):
                    with st.form("quick_evt"):
                        c1,c2 = st.columns(2)
                        nom_ev = c1.text_input("Nom de l'événement")
                        type_ev = c2.selectbox("Type", SET["types_evenements"])
                        c3,c4 = st.columns(2)
                        date_ev = c3.date_input("Date", value=date.today())
                        lieu_ev = c4.selectbox("Lieu", SET["lieux"])
                        role = st.selectbox("Rôle du contact", ["Participant","Animateur","Invité"])
                        ok2 = st.form_submit_button("💾 Créer l'événement **et** inscrire ce contact")
                        if ok2:
                            new_eid = generate_id("EVT", df_events, "ID_Événement")
                            rowe = {"ID_Événement":new_eid,"Nom_Événement":nom_ev,"Type":type_ev,"Date":date_ev.isoformat(),
                                    "Durée_h":"2","Lieu":lieu_ev,"Formateur":"","Objectif":"","Periode":"",
                                    "Cout_Salle":0,"Cout_Formateur":0,"Cout_Logistique":0,"Cout_Pub":0,"Cout_Autres":0,"Cout_Total":0,"Notes":""}
                            globals()["df_events"] = pd.concat([df_events, pd.DataFrame([rowe])], ignore_index=True)
                            save_df(df_events, PATHS["events"])
                            new_pid = generate_id("PAR", df_parts, "ID_Participation")
                            rowp = {"ID_Participation":new_pid,"ID":sel_id,"ID_Événement":new_eid,"Rôle":role,
                                    "Inscription":"","Arrivée":"","Temps_Present":"","Feedback":"","Note":"","Commentaire":""}
                            globals()["df_parts"] = pd.concat([df_parts, pd.DataFrame([rowp])], ignore_index=True)
                            save_df(df_parts, PATHS["parts"])
                            st.success(f"Événement créé ({new_eid}) et contact inscrit ({new_pid}).")
            else:
                st.warning("ID introuvable (rafraîchissez la page).")
        else:
            st.info("Sélectionnez un contact via la grille ou le sélecteur maître.")
            
            if not st.session_state.get("selected_contact_id"):
                with st.expander("➕ Créer un nouveau contact"):
                    with st.form("create_contact"):
                        n1, n2 = st.columns(2)
                        nom_new = n1.text_input("Nom *", "")
                        prenom_new = n2.text_input("Prénom", "")
                        g1,g2 = st.columns(2)
                        genre_new = g1.selectbox("Genre", SET["genres"], index=0)
                        titre_new = g2.text_input("Titre / Position", "")
                        s1,s2 = st.columns(2)
                        societe_new = s1.text_input("Société", "")
                        secteur_new = s2.selectbox("Secteur", SET["secteurs"], index=len(SET["secteurs"])-1)
                        e1,e2,e3 = st.columns(3)
                        email_new = e1.text_input("Email", "")
                        tel_new = e2.text_input("Téléphone", "")
                        linkedin_new = e3.text_input("LinkedIn", "")
                        l1,l2,l3 = st.columns(3)
                        ville_new = l1.selectbox("Ville", SET["villes"], index=len(SET["villes"])-1)
                        pays_new = l2.selectbox("Pays", SET["pays"], index=0)
                        typec_new = l3.selectbox("Type", SET["types_contact"], index=0)
                        s3,s4,s5 = st.columns(3)
                        source_new = s3.selectbox("Source", SET["sources"], index=0)
                        statut_new = s4.selectbox("Statut", SET["statuts_engagement"], index=0)
                        score_new = s5.number_input("Score IIBA", value=0.0, step=1.0)
                        dc_new = st.date_input("Date de création", value=date.today())
                        notes_new = st.text_area("Notes", "")
                        top20_new = st.checkbox("Top-20 entreprise", value=False)
                        ok_new = st.form_submit_button("💾 Créer le contact")

                        if ok_new:
                            if not str(nom_new).strip():
                                st.error("❌ Le nom du contact est obligatoire. Création annulée.")
                                st.stop()
                            if not email_ok(email_new):
                                st.error("Email invalide.")
                                st.stop()
                            if not phone_ok(tel_new):
                                st.error("Téléphone invalide.")
                                st.stop()

                            new_id = generate_id("CNT", df_contacts, "ID")
                            new_row = {
                                "ID": new_id, "Nom": nom_new, "Prénom": prenom_new, "Genre": genre_new, "Titre": titre_new,
                                "Société": societe_new, "Secteur": secteur_new, "Email": email_new, "Téléphone": tel_new,
                                "LinkedIn": linkedin_new, "Ville": ville_new, "Pays": pays_new, "Type": typec_new,
                                "Source": source_new, "Statut": statut_new, "Score_Engagement": int(score_new),
                                "Date_Creation": dc_new.isoformat(), "Notes": notes_new, "Top20": top20_new
                            }
                            globals()["df_contacts"] = pd.concat([df_contacts, pd.DataFrame([new_row])], ignore_index=True)
                            save_df(df_contacts, PATHS["contacts"])
                            st.session_state["selected_contact_id"] = new_id
                            st.success(f"Contact créé ({new_id}).")
            
    with cR:
        st.subheader("Actions liées au contact sélectionné")
        sel_id = st.session_state.get("selected_contact_id")
        if not sel_id:
            st.info("Sélectionnez un contact pour créer une interaction, participation, paiement ou certification.")
        else:
            tabs = st.tabs(["➕ Interaction","➕ Participation","➕ Paiement","➕ Certification","📑 Vue 360°"])
            with tabs[0]:
                with st.form("add_inter"):
                    c1,c2,c3 = st.columns(3)
                    dti = c1.date_input("Date", value=date.today())
                    canal = c2.selectbox("Canal", SET["canaux"])
                    resp = c3.selectbox("Responsable", ["Aymard","Alix","Autre"])
                    obj = st.text_input("Objet")
                    resu = st.selectbox("Résultat", SET["resultats_inter"])
                    resume = st.text_area("Résumé")
                    add_rel = st.checkbox("Planifier une relance ?")
                    rel = st.date_input("Relance", value=date.today()) if add_rel else None
                    ok = st.form_submit_button("💾 Enregistrer l'interaction")
                    if ok:
                        nid = generate_id("INT", df_inter, "ID_Interaction")
                        row = {"ID_Interaction":nid,"ID":sel_id,"Date":dti.isoformat(),"Canal":canal,"Objet":obj,"Résumé":resume,
                               "Résultat":resu,"Prochaine_Action":"","Relance":rel.isoformat() if rel else "","Responsable":resp}
                        globals()["df_inter"] = pd.concat([df_inter, pd.DataFrame([row])], ignore_index=True)
                        save_df(df_inter, PATHS["inter"])
                        st.success(f"Interaction enregistrée ({nid}).")
            with tabs[1]:
                with st.form("add_part"):
                    if df_events.empty:
                        st.warning("Créez d'abord un événement.")
                    else:
                        ide = st.selectbox("Événement", df_events["ID_Événement"].tolist())
                        role = st.selectbox("Rôle", ["Participant","Animateur","Invité"])
                        fb = st.selectbox("Feedback", ["Très satisfait","Satisfait","Moyen","Insatisfait"])
                        note = st.number_input("Note (1-5)", min_value=1, max_value=5, value=5)
                        ok = st.form_submit_button("💾 Enregistrer la participation")
                        if ok:
                            nid = generate_id("PAR", df_parts, "ID_Participation")
                            row = {"ID_Participation":nid,"ID":sel_id,"ID_Événement":ide,"Rôle":role,"Inscription":"","Arrivée":"",
                                   "Temps_Present":"","Feedback":fb,"Note":str(note),"Commentaire":""}
                            globals()["df_parts"] = pd.concat([df_parts, pd.DataFrame([row])], ignore_index=True)
                            save_df(df_parts, PATHS["parts"])
                            st.success(f"Participation ajoutée ({nid}).")
            with tabs[2]:
                with st.form("add_pay"):
                    if df_events.empty:
                        st.warning("Créez d'abord un événement.")
                    else:
                        ide = st.selectbox("Événement", df_events["ID_Événement"].tolist())
                        dtp = st.date_input("Date paiement", value=date.today())
                        montant = st.number_input("Montant (FCFA)", min_value=0, step=1000)
                        moyen = st.selectbox("Moyen", SET["moyens_paiement"])
                        statut = st.selectbox("Statut", SET["statuts_paiement"])
                        ref = st.text_input("Référence")
                        ok = st.form_submit_button("💾 Enregistrer le paiement")
                        if ok:
                            nid = generate_id("PAY", df_pay, "ID_Paiement")
                            row = {"ID_Paiement":nid,"ID":sel_id,"ID_Événement":ide,"Date_Paiement":dtp.isoformat(),"Montant":str(montant),
                                   "Moyen":moyen,"Statut":statut,"Référence":ref,"Notes":"","Relance":""}
                            globals()["df_pay"] = pd.concat([df_pay, pd.DataFrame([row])], ignore_index=True)
                            save_df(df_pay, PATHS["pay"])
                            st.success(f"Paiement enregistré ({nid}).")
            with tabs[3]:
                with st.form("add_cert"):
                    tc = st.selectbox("Type Certification", SET["types_certif"])
                    dte = st.date_input("Date Examen", value=date.today())
                    res = st.selectbox("Résultat", ["Réussi","Échoué","En cours","Reporté"])
                    sc = st.number_input("Score", min_value=0, max_value=100, value=0)
                    has_dto = st.checkbox("Renseigner une date d'obtention ?")
                    dto = st.date_input("Date Obtention", value=date.today()) if has_dto else None
                    ok = st.form_submit_button("💾 Enregistrer la certification")
                    if ok:
                        nid = generate_id("CER", df_cert, "ID_Certif")
                        row = {"ID_Certif":nid,"ID":sel_id,"Type_Certif":tc,"Date_Examen":dte.isoformat(),"Résultat":res,"Score":str(sc),
                               "Date_Obtention":dto.isoformat() if dto else "","Validité":"","Renouvellement":"","Notes":""}
                        globals()["df_cert"] = pd.concat([df_cert, pd.DataFrame([row])], ignore_index=True)
                        save_df(df_cert, PATHS["cert"])
                        st.success(f"Certification ajoutée ({nid}).")
            with tabs[4]:
                st.markdown("#### Vue 360°")
                if not df_inter.empty:
                    st.write("**Interactions**")
                    st.dataframe(df_inter[df_inter["ID"]==sel_id][["Date","Canal","Objet","Résultat","Relance","Responsable"]], use_container_width=True)
                if not df_parts.empty:
                    st.write("**Participations**")
                    dfp = df_parts[df_parts["ID"]==sel_id].copy()
                    if not df_events.empty:
                        ev_names = df_events.set_index("ID_Événement")["Nom_Événement"]
                        dfp["Événement"] = dfp["ID_Événement"].map(ev_names)
                    st.dataframe(dfp[["Événement","Rôle","Feedback","Note"]], use_container_width=True)
                if not df_pay.empty:
                    st.write("**Paiements**")
                    st.dataframe(df_pay[df_pay["ID"]==sel_id][["ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence"]], use_container_width=True)
                if not df_cert.empty:
                    st.write("**Certifications**")
                    st.dataframe(df_cert[df_cert["ID"]==sel_id][["Type_Certif","Date_Examen","Résultat","Score","Date_Obtention"]], use_container_width=True)

# PAGE ÉVÉNEMENTS (CODE EXISTANT CONSERVÉ)
elif page == "Événements":
    st.title("📅 Événements")

    if "selected_event_id" not in st.session_state:
        st.session_state["selected_event_id"] = ""
    if "event_form_mode" not in st.session_state:
        st.session_state["event_form_mode"] = "create"

    def _label_event(row):
        dat = row.get("Date", "")
        nom = row.get("Nom_Événement", "")
        typ = row.get("Type", "")
        return f"{row['ID_Événement']} — {nom} — {typ} — {dat}"

    options = []
    if not df_events.empty:
        options = df_events.apply(_label_event, axis=1).tolist()
    id_map = dict(zip(options, df_events["ID_Événement"])) if options else {}

    sel_col, new_col = st.columns([3,1])
    cur_label = sel_col.selectbox(
        "Événement sélectionné (sélecteur maître)",
        ["— Aucun —"] + options,
        index=0,
        key="event_select_label"
    )
    if cur_label and cur_label != "— Aucun —":
        st.session_state["selected_event_id"] = id_map[cur_label]
        st.session_state["event_form_mode"] = "edit"
    else:
        st.session_state["selected_event_id"] = ""
        st.session_state["event_form_mode"] = "create"

    if new_col.button("➕ Nouveau", key="evt_new_btn"):
        st.session_state["selected_event_id"] = ""
        st.session_state["event_form_mode"] = "create"
        _safe_rerun()

    st.markdown("---")

    with st.expander("📝 Gérer un événement (pré-rempli si un événement est sélectionné)", expanded=True):
        mode = st.session_state["event_form_mode"]
        sel_eid = st.session_state["selected_event_id"]

        if mode == "edit" and sel_eid:
            src = df_events[df_events["ID_Événement"] == sel_eid]
            if src.empty:
                st.warning("ID sélectionné introuvable; passage en mode création.")
                mode = "create"
                st.session_state["event_form_mode"] = "create"
                sel_eid = ""
                row_init = {c: "" for c in E_COLS}
            else:
                row_init = src.iloc[0].to_dict()
        else:
            row_init = {c: "" for c in E_COLS}

        with st.form("event_form_main", clear_on_submit=False):
            id_dis = st.text_input("ID_Événement", value=row_init.get("ID_Événement", ""), disabled=True)

            c1, c2, c3 = st.columns(3)
            nom = c1.text_input("Nom de l'événement", value=row_init.get("Nom_Événement",""))
            typ = c2.selectbox("Type", SET["types_evenements"], index=SET["types_evenements"].index(row_init.get("Type","Formation")) if row_init.get("Type","Formation") in SET["types_evenements"] else 0)
            dat_val = parse_date(row_init.get("Date")) or date.today()
            dat = c3.date_input("Date", value=dat_val)

            c4, c5, c6 = st.columns(3)
            lieu = c4.selectbox("Lieu", SET["lieux"], index=SET["lieux"].index(row_init.get("Lieu","Présentiel")) if row_init.get("Lieu","Présentiel") in SET["lieux"] else 0)
            duree = c5.number_input("Durée (h)", min_value=0.0, step=0.5, value=float(row_init.get("Durée_h") or 2.0))
            formateur = c6.text_input("Formateur(s)", value=row_init.get("Formateur",""))

            obj = st.text_area("Objectif", value=row_init.get("Objectif",""))

            couts = st.columns(5)
            c_salle = couts[0].number_input("Coût salle", min_value=0.0, step=1000.0, value=float(row_init.get("Cout_Salle") or 0.0))
            c_form  = couts[1].number_input("Coût formateur", min_value=0.0, step=1000.0, value=float(row_init.get("Cout_Formateur") or 0.0))
            c_log   = couts[2].number_input("Coût logistique", min_value=0.0, step=1000.0, value=float(row_init.get("Cout_Logistique") or 0.0))
            c_pub   = couts[3].number_input("Coût pub", min_value=0.0, step=1000.0, value=float(row_init.get("Cout_Pub") or 0.0))
            c_aut   = couts[4].number_input("Autres coûts", min_value=0.0, step=1000.0, value=float(row_init.get("Cout_Autres") or 0.0))

            notes = st.text_area("Notes", value=row_init.get("Notes",""))

            cL, cM, cR = st.columns([1.2,1.2,2])
            btn_create = cL.form_submit_button("🆕 Créer l'événement", disabled=(mode=="edit"))
            btn_save   = cM.form_submit_button("💾 Enregistrer modifications", disabled=(mode!="edit"))

            if btn_create:
                if not nom.strip():
                    st.error("Le nom de l'événement est obligatoire.")
                    st.stop()
                new_id = generate_id("EVT", df_events, "ID_Événement")
                new_row = {
                    "ID_Événement": new_id, "Nom_Événement": nom, "Type": typ, "Date": dat.isoformat(),
                    "Durée_h": str(duree), "Lieu": lieu, "Formateur": formateur, "Objectif": obj, "Periode": "",
                    "Cout_Salle": c_salle, "Cout_Formateur": c_form, "Cout_Logistique": c_log,
                    "Cout_Pub": c_pub, "Cout_Autres": c_aut, "Cout_Total": 0, "Notes": notes
                }
                globals()["df_events"] = pd.concat([df_events, pd.DataFrame([new_row])], ignore_index=True)
                save_df(df_events, PATHS["events"])
                st.success(f"Événement créé ({new_id}).")
                st.session_state["selected_event_id"] = new_id
                st.session_state["event_form_mode"] = "edit"
                _safe_rerun()

            if btn_save:
                if not sel_eid:
                    st.error("Aucun événement sélectionné pour enregistrer des modifications.")
                    st.stop()
                if not nom.strip():
                    st.error("Le nom de l'événement est obligatoire.")
                    st.stop()
                idx = df_events.index[df_events["ID_Événement"] == sel_eid]
                if len(idx) == 0:
                    st.error("Événement introuvable (rafraîchissez).")
                    st.stop()
                rowe = {
                    "ID_Événement": sel_eid, "Nom_Événement": nom, "Type": typ, "Date": dat.isoformat(),
                    "Durée_h": str(duree), "Lieu": lieu, "Formateur": formateur, "Objectif": obj, "Periode": "",
                    "Cout_Salle": c_salle, "Cout_Formateur": c_form, "Cout_Logistique": c_log,
                    "Cout_Pub": c_pub, "Cout_Autres": c_aut, "Cout_Total": 0, "Notes": notes
                }
                df_events.loc[idx[0]] = rowe
                save_df(df_events, PATHS["events"])
                st.success(f"Événement {sel_eid} mis à jour.")

    st.markdown("---")

    col_dup, col_del, col_clear = st.columns([1,1,1])
    if col_dup.button("🧬 Dupliquer l'événement sélectionné", key="evt_dup_btn", disabled=(st.session_state["event_form_mode"]!="edit" or not st.session_state["selected_event_id"])):
        src_id = st.session_state["selected_event_id"]
        src = df_events[df_events["ID_Événement"] == src_id]
        if src.empty:
            st.error("Impossible de dupliquer: événement introuvable.")
        else:
            new_id = generate_id("EVT", df_events, "ID_Événement")
            clone = src.iloc[0].to_dict()
            clone["ID_Événement"] = new_id
            globals()["df_events"] = pd.concat([df_events, pd.DataFrame([clone])], ignore_index=True)
            save_df(df_events, PATHS["events"])
            st.success(f"Événement dupliqué sous l'ID {new_id}.")
            st.session_state["selected_event_id"] = new_id
            st.session_state["event_form_mode"] = "edit"
            _safe_rerun()

    with col_del:
        st.caption("Confirmation suppression")
        confirm_txt = st.text_input("Tapez SUPPRIME ou DELETE", value="", key="evt_del_confirm")
        if st.button("🗑️ Supprimer définitivement", key="evt_del_btn", disabled=(st.session_state["event_form_mode"]!="edit" or not st.session_state["selected_event_id"])):
            if confirm_txt.strip().upper() not in ("SUPPRIME", "DELETE"):
                st.error("Veuillez confirmer en saisissant SUPPRIME ou DELETE.")
            else:
                del_id = st.session_state["selected_event_id"]
                if not del_id:
                    st.error("Aucun événement sélectionné.")
                else:
                    globals()["df_events"] = df_events[df_events["ID_Événement"] != del_id]
                    save_df(df_events, PATHS["events"])
                    st.success(f"Événement {del_id} supprimé.")
                    st.session_state["selected_event_id"] = ""
                    st.session_state["event_form_mode"] = "create"
                    _safe_rerun()

    if col_clear.button("🧹 Vider la sélection", key="evt_clear_btn"):
        st.session_state["selected_event_id"] = ""
        st.session_state["event_form_mode"] = "create"
        _safe_rerun()

    st.markdown("---")

    st.subheader("📋 Liste des événements")
    filt = st.text_input("Filtre rapide (nom, type, lieu, notes…)", "", key="evt_filter")
    page_size_evt = st.selectbox("Taille de page", [20,50,100,200], index=0, key="pg_evt")

    evt_default_cols = E_COLS + [c for c in AUDIT_COLS if c in df_events.columns]
    df_show = df_events[evt_default_cols].copy()

    if filt:
        t = filt.lower()
        df_show = df_show[df_show.apply(lambda r: any(t in str(r[c]).lower() for c in ["Nom_Événement","Type","Lieu","Notes"]), axis=1)]

    if HAS_AGGRID:
        gb = GridOptionsBuilder.from_dataframe(df_show)
        gb.configure_default_column(filter=True, sortable=True, resizable=True, editable=True)
        for c in AUDIT_COLS:
            if c in df_show.columns:
                gb.configure_column(c, editable=False)
        gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=page_size_evt)
        gb.configure_selection("single", use_checkbox=True)
        go = gb.build()
        grid = AgGrid(df_show, gridOptions=go, height=520,
                      update_mode=GridUpdateMode.MODEL_CHANGED,
                      data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                      key="evt_grid", allow_unsafe_jscode=True)
        col_apply = st.columns([1])[0]
        if col_apply.button("💾 Appliquer les modifications (grille)", key="evt_apply_grid"):
            new_df = pd.DataFrame(grid["data"])
            for c in E_COLS:
                if c not in new_df.columns:
                    new_df[c] = ""
            globals()["df_events"] = new_df[E_COLS].copy()
            save_df(df_events, PATHS["events"])
            st.success("Modifications enregistrées depuis la grille.")
    else:
        st.dataframe(df_show, use_container_width=True)
        st.info("Installez `streamlit-aggrid` pour éditer/dupliquer directement dans la grille.")

# ===== NOUVELLE PAGE ENTREPRISES =====
elif page == "Entreprises":
    st.title("🏢 Entreprises & Partenaires")

    # Session state pour la sélection d'entreprise
    if "selected_entreprise_id" not in st.session_state:
        st.session_state["selected_entreprise_id"] = ""
    if "entreprise_form_mode" not in st.session_state:
        st.session_state["entreprise_form_mode"] = "create"

    # Sélecteur d'entreprise
    def _label_entreprise(row):
        nom = row.get("Nom_Entreprise", "")
        secteur = row.get("Secteur", "")
        statut = row.get("Statut_Partenariat", "")
        return f"{row['ID_Entreprise']} — {nom} — {secteur} — {statut}"

    options_ent = []
    if not df_entreprises.empty:
        options_ent = df_entreprises.apply(_label_entreprise, axis=1).tolist()
    id_map_ent = dict(zip(options_ent, df_entreprises["ID_Entreprise"])) if options_ent else {}

    sel_col_ent, new_col_ent = st.columns([3,1])
    cur_label_ent = sel_col_ent.selectbox(
        "Entreprise sélectionnée (sélecteur maître)",
        ["— Aucune —"] + options_ent,
        index=0,
        key="entreprise_select_label"
    )
    if cur_label_ent and cur_label_ent != "— Aucune —":
        st.session_state["selected_entreprise_id"] = id_map_ent[cur_label_ent]
        st.session_state["entreprise_form_mode"] = "edit"
    else:
        st.session_state["selected_entreprise_id"] = ""
        st.session_state["entreprise_form_mode"] = "create"

    if new_col_ent.button("➕ Nouvelle", key="ent_new_btn"):
        st.session_state["selected_entreprise_id"] = ""
        st.session_state["entreprise_form_mode"] = "create"
        _safe_rerun()

    st.markdown("---")

    # Formulaire de gestion d'entreprise
    with st.expander("📝 Gérer une entreprise (pré-rempli si une entreprise est sélectionnée)", expanded=True):
        mode_ent = st.session_state["entreprise_form_mode"]
        sel_entid = st.session_state["selected_entreprise_id"]

        # Pré-remplissage si édition
        if mode_ent == "edit" and sel_entid:
            src_ent = df_entreprises[df_entreprises["ID_Entreprise"] == sel_entid]
            if src_ent.empty:
                st.warning("ID sélectionné introuvable; passage en mode création.")
                mode_ent = "create"
                st.session_state["entreprise_form_mode"] = "create"
                sel_entid = ""
                row_init_ent = {c: "" for c in ENT_COLS}
            else:
                row_init_ent = src_ent.iloc[0].to_dict()
        else:
            row_init_ent = {c: "" for c in ENT_COLS}

        with st.form("entreprise_form_main", clear_on_submit=False):
            # ID grisé
            id_dis_ent = st.text_input("ID_Entreprise", value=row_init_ent.get("ID_Entreprise", ""), disabled=True)

            # Informations de base
            c1_ent, c2_ent, c3_ent = st.columns(3)
            nom_ent = c1_ent.text_input("Nom de l'entreprise", value=row_init_ent.get("Nom_Entreprise",""))
            secteur_ent = c2_ent.selectbox("Secteur", SET["secteurs"], 
                                          index=SET["secteurs"].index(row_init_ent.get("Secteur","Autre")) if row_init_ent.get("Secteur","Autre") in SET["secteurs"] else len(SET["secteurs"])-1)
            taille_ent = c3_ent.selectbox("Taille", SET["tailles_entreprise"],
                                         index=SET["tailles_entreprise"].index(row_init_ent.get("Taille","PME (10-250)")) if row_init_ent.get("Taille","PME (10-250)") in SET["tailles_entreprise"] else 1)

            # Données économiques
            c4_ent, c5_ent = st.columns(2)
            ca_annuel = c4_ent.number_input("CA Annuel (FCFA)", min_value=0, step=1000000, value=int(float(row_init_ent.get("CA_Annuel") or 0)))
            nb_employes = c5_ent.number_input("Nombre d'employés", min_value=0, step=10, value=int(float(row_init_ent.get("Nb_Employes") or 0)))

            # Localisation
            c6_ent, c7_ent = st.columns(2)
            ville_ent = c6_ent.selectbox("Ville", SET["villes"],
                                        index=SET["villes"].index(row_init_ent.get("Ville","Douala")) if row_init_ent.get("Ville","Douala") in SET["villes"] else 0)
            pays_ent = c7_ent.selectbox("Pays", SET["pays"],
                                       index=SET["pays"].index(row_init_ent.get("Pays","Cameroun")) if row_init_ent.get("Pays","Cameroun") in SET["pays"] else 0)

            # Contact principal
            st.subheader("Contact principal")
            c8_ent, c9_ent, c10_ent = st.columns(3)
            contact_principal = c8_ent.text_input("Nom du contact", value=row_init_ent.get("Contact_Principal",""))
            email_principal = c9_ent.text_input("Email", value=row_init_ent.get("Email_Principal",""))
            tel_principal = c10_ent.text_input("Téléphone", value=row_init_ent.get("Telephone_Principal",""))
            site_web = st.text_input("Site Web", value=row_init_ent.get("Site_Web",""))

            # Partenariat
            st.subheader("Partenariat")
            c11_ent, c12_ent, c13_ent = st.columns(3)
            statut_part = c11_ent.selectbox("Statut Partenariat", SET["statuts_partenariat"],
                                           index=SET["statuts_partenariat"].index(row_init_ent.get("Statut_Partenariat","Prospect")) if row_init_ent.get("Statut_Partenariat","Prospect") in SET["statuts_partenariat"] else 0)
            type_part = c12_ent.selectbox("Type Partenariat", SET["types_partenariat"],
                                         index=SET["types_partenariat"].index(row_init_ent.get("Type_Partenariat","Formation")) if row_init_ent.get("Type_Partenariat","Formation") in SET["types_partenariat"] else 0)
            resp_iiba = c13_ent.selectbox("Responsable IIBA", SET["responsables_iiba"],
                                         index=SET["responsables_iiba"].index(row_init_ent.get("Responsable_IIBA","Non assigné")) if row_init_ent.get("Responsable_IIBA","Non assigné") in SET["responsables_iiba"] else len(SET["responsables_iiba"])-1)

            # Dates
            c14_ent, c15_ent = st.columns(2)
            date_premier_contact = c14_ent.date_input("Date premier contact", 
                                                     value=parse_date(row_init_ent.get("Date_Premier_Contact")) or date.today())
            date_maj = c15_ent.date_input("Date mise à jour", value=date.today())

            # Notes et opportunités
            notes_ent = st.text_area("Notes", value=row_init_ent.get("Notes",""))
            opportunites = st.text_area("Opportunités", value=row_init_ent.get("Opportunites",""))

            # Boutons
            cL_ent, cM_ent, cR_ent = st.columns([1.2,1.2,2])
            btn_create_ent = cL_ent.form_submit_button("🆕 Créer l'entreprise", disabled=(mode_ent=="edit"))
            btn_save_ent = cM_ent.form_submit_button("💾 Enregistrer modifications", disabled=(mode_ent!="edit"))

            # Actions du formulaire
            if btn_create_ent:
                if not nom_ent.strip():
                    st.error("Le nom de l'entreprise est obligatoire.")
                    st.stop()
                if email_principal and not email_ok(email_principal):
                    st.error("Email principal invalide.")
                    st.stop()
                if tel_principal and not phone_ok(tel_principal):
                    st.error("Téléphone principal invalide.")
                    st.stop()

                new_id_ent = generate_id("ENT", df_entreprises, "ID_Entreprise")
                new_row_ent = {
                    "ID_Entreprise": new_id_ent, "Nom_Entreprise": nom_ent, "Secteur": secteur_ent, "Taille": taille_ent,
                    "CA_Annuel": ca_annuel, "Nb_Employes": nb_employes, "Ville": ville_ent, "Pays": pays_ent,
                    "Contact_Principal": contact_principal, "Email_Principal": email_principal, "Telephone_Principal": tel_principal,
                    "Site_Web": site_web, "Statut_Partenariat": statut_part, "Type_Partenariat": type_part,
                    "Date_Premier_Contact": date_premier_contact.isoformat(), "Responsable_IIBA": resp_iiba,
                    "Notes": notes_ent, "Opportunites": opportunites, "Date_Maj": date_maj.isoformat()
                }
                globals()["df_entreprises"] = pd.concat([df_entreprises, pd.DataFrame([new_row_ent])], ignore_index=True)
                save_df(df_entreprises, PATHS["entreprises"])
                st.success(f"Entreprise créée ({new_id_ent}).")
                st.session_state["selected_entreprise_id"] = new_id_ent
                st.session_state["entreprise_form_mode"] = "edit"
                _safe_rerun()

            if btn_save_ent:
                if not sel_entid:
                    st.error("Aucune entreprise sélectionnée pour enregistrer des modifications.")
                    st.stop()
                if not nom_ent.strip():
                    st.error("Le nom de l'entreprise est obligatoire.")
                    st.stop()
                if email_principal and not email_ok(email_principal):
                    st.error("Email principal invalide.")
                    st.stop()
                if tel_principal and not phone_ok(tel_principal):
                    st.error("Téléphone principal invalide.")
                    st.stop()

                idx_ent = df_entreprises.index[df_entreprises["ID_Entreprise"] == sel_entid]
                if len(idx_ent) == 0:
                    st.error("Entreprise introuvable (rafraîchissez).")
                    st.stop()
                rowe_ent = {
                    "ID_Entreprise": sel_entid, "Nom_Entreprise": nom_ent, "Secteur": secteur_ent, "Taille": taille_ent,
                    "CA_Annuel": ca_annuel, "Nb_Employes": nb_employes, "Ville": ville_ent, "Pays": pays_ent,
                    "Contact_Principal": contact_principal, "Email_Principal": email_principal, "Telephone_Principal": tel_principal,
                    "Site_Web": site_web, "Statut_Partenariat": statut_part, "Type_Partenariat": type_part,
                    "Date_Premier_Contact": date_premier_contact.isoformat(), "Responsable_IIBA": resp_iiba,
                    "Notes": notes_ent, "Opportunites": opportunites, "Date_Maj": date_maj.isoformat()
                }
                df_entreprises.loc[idx_ent[0]] = rowe_ent
                save_df(df_entreprises, PATHS["entreprises"])
                st.success(f"Entreprise {sel_entid} mise à jour.")

    st.markdown("---")

    # Actions avancées pour entreprises
    col_dup_ent, col_del_ent, col_clear_ent = st.columns([1,1,1])
    
    if col_dup_ent.button("🧬 Dupliquer l'entreprise sélectionnée", key="ent_dup_btn", 
                          disabled=(st.session_state["entreprise_form_mode"]!="edit" or not st.session_state["selected_entreprise_id"])):
        src_id_ent = st.session_state["selected_entreprise_id"]
        src_ent = df_entreprises[df_entreprises["ID_Entreprise"] == src_id_ent]
        if src_ent.empty:
            st.error("Impossible de dupliquer: entreprise introuvable.")
        else:
            new_id_ent = generate_id("ENT", df_entreprises, "ID_Entreprise")
            clone_ent = src_ent.iloc[0].to_dict()
            clone_ent["ID_Entreprise"] = new_id_ent
            clone_ent["Nom_Entreprise"] = f"{clone_ent['Nom_Entreprise']} (Copie)"
            globals()["df_entreprises"] = pd.concat([df_entreprises, pd.DataFrame([clone_ent])], ignore_index=True)
            save_df(df_entreprises, PATHS["entreprises"])
            st.success(f"Entreprise dupliquée sous l'ID {new_id_ent}.")
            st.session_state["selected_entreprise_id"] = new_id_ent
            st.session_state["entreprise_form_mode"] = "edit"
            _safe_rerun()

    with col_del_ent:
        st.caption("Confirmation suppression")
        confirm_txt_ent = st.text_input("Tapez SUPPRIME ou DELETE", value="", key="ent_del_confirm")
        if st.button("🗑️ Supprimer définitivement", key="ent_del_btn", 
                     disabled=(st.session_state["entreprise_form_mode"]!="edit" or not st.session_state["selected_entreprise_id"])):
            if confirm_txt_ent.strip().upper() not in ("SUPPRIME", "DELETE"):
                st.error("Veuillez confirmer en saisissant SUPPRIME ou DELETE.")
            else:
                del_id_ent = st.session_state["selected_entreprise_id"]
                if not del_id_ent:
                    st.error("Aucune entreprise sélectionnée.")
                else:
                    globals()["df_entreprises"] = df_entreprises[df_entreprises["ID_Entreprise"] != del_id_ent]
                    save_df(df_entreprises, PATHS["entreprises"])
                    st.success(f"Entreprise {del_id_ent} supprimée.")
                    st.session_state["selected_entreprise_id"] = ""
                    st.session_state["entreprise_form_mode"] = "create"
                    _safe_rerun()

    if col_clear_ent.button("🧹 Vider la sélection", key="ent_clear_btn"):
        st.session_state["selected_entreprise_id"] = ""
        st.session_state["entreprise_form_mode"] = "create"
        _safe_rerun()

    st.markdown("---")

    # Grille des entreprises
    st.subheader("📋 Liste des entreprises")
    filt_ent = st.text_input("Filtre rapide (nom, secteur, statut…)", "", key="ent_filter")
    page_size_ent = st.selectbox("Taille de page", [20,50,100,200], index=0, key="pg_ent")

    def parse_cols_ent(s, defaults):
        cols = [c.strip() for c in str(s).split(",") if c.strip()]
        valid = [c for c in cols if c in df_entreprises.columns]
        return valid if valid else defaults

    ent_default_cols = parse_cols_ent(PARAMS.get("grid_entreprises_columns", ""), [
        "ID_Entreprise","Nom_Entreprise","Secteur","Taille","Statut_Partenariat",
        "Type_Partenariat","Contact_Principal","Email_Principal","Responsable_IIBA","Date_Premier_Contact"
    ])
    ent_default_cols += [c for c in AUDIT_COLS if c in df_entreprises.columns]
    
    df_show_ent = df_entreprises[ent_default_cols].copy()

    if filt_ent:
        t_ent = filt_ent.lower()
        df_show_ent = df_show_ent[df_show_ent.apply(
            lambda r: any(t_ent in str(r[c]).lower() for c in ["Nom_Entreprise","Secteur","Statut_Partenariat","Notes"]), axis=1)]

    if HAS_AGGRID:
        gb_ent = GridOptionsBuilder.from_dataframe(df_show_ent)
        gb_ent.configure_default_column(filter=True, sortable=True, resizable=True, editable=True)
        for c in AUDIT_COLS:
            if c in df_show_ent.columns:
                gb_ent.configure_column(c, editable=False)
        gb_ent.configure_pagination(paginationAutoPageSize=False, paginationPageSize=page_size_ent)
        gb_ent.configure_selection("single", use_checkbox=True)
        go_ent = gb_ent.build()
        grid_ent = AgGrid(df_show_ent, gridOptions=go_ent, height=520,
                          update_mode=GridUpdateMode.MODEL_CHANGED,
                          data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                          key="ent_grid", allow_unsafe_jscode=True)
        col_apply_ent = st.columns([1])[0]
        if col_apply_ent.button("💾 Appliquer les modifications (grille)", key="ent_apply_grid"):
            new_df_ent = pd.DataFrame(grid_ent["data"])
            for c in ENT_COLS:
                if c not in new_df_ent.columns:
                    new_df_ent[c] = ""
            globals()["df_entreprises"] = new_df_ent[ENT_COLS].copy()
            save_df(df_entreprises, PATHS["entreprises"])
            st.success("Modifications enregistrées depuis la grille.")
    else:
        st.dataframe(df_show_ent, use_container_width=True)
        st.info("Installez `streamlit-aggrid` pour éditer directement dans la grille.")

    # Statistiques rapides
    st.markdown("---")
    st.subheader("📊 Statistiques des entreprises")
    
    if not df_entreprises.empty:
        col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
        
        total_entreprises = len(df_entreprises)
        partenaires_actifs = len(df_entreprises[df_entreprises["Statut_Partenariat"].isin(["Partenaire", "Client", "Partenaire Stratégique"])])
        prospects = len(df_entreprises[df_entreprises["Statut_Partenariat"] == "Prospect"])
        ca_total_ent = df_entreprises["CA_Annuel"].astype(str).str.replace("", "0").astype(float).sum()
        
        col_stat1.metric("🏢 Total Entreprises", total_entreprises)
        col_stat2.metric("🤝 Partenaires Actifs", partenaires_actifs)
        col_stat3.metric("🎯 Prospects", prospects)
        col_stat4.metric("💰 CA Cumulé", f"{ca_total_ent/1e9:.1f}B FCFA")
        
        # Graphiques si Altair disponible
        if alt:
            col_chart1, col_chart2 = st.columns(2)
            
            with col_chart1:
                statut_counts = df_entreprises["Statut_Partenariat"].value_counts().reset_index()
                statut_counts.columns = ["Statut", "Count"]
                chart_statut = alt.Chart(statut_counts).mark_bar().encode(
                    x=alt.X("Count:Q", title="Nombre"),
                    y=alt.Y("Statut:N", title="Statut Partenariat"),
                    color=alt.Color("Statut:N", legend=None)
                ).properties(height=250, title="Répartition par statut")
                st.altair_chart(chart_statut, use_container_width=True)
            
            with col_chart2:
                secteur_counts = df_entreprises["Secteur"].value_counts().reset_index()
                secteur_counts.columns = ["Secteur", "Count"]
                chart_secteur = alt.Chart(secteur_counts).mark_arc().encode(
                    theta=alt.Theta("Count:Q"),
                    color=alt.Color("Secteur:N"),
                    tooltip=["Secteur", "Count"]
                ).properties(height=250, title="Répartition par secteur")
                st.altair_chart(chart_secteur, use_container_width=True)
    else:
        st.info("Aucune entreprise enregistrée.")

# PAGE RAPPORTS (CODE EXISTANT CONSERVÉ - partie simplifiée)
elif page == "Rapports":
    st.title("📑 Rapports & KPI — IIBA Cameroun")
    
    # KPI de base
    total_contacts = len(df_contacts)
    prospects_actifs = len(df_contacts[(df_contacts.get("Type","")=="Prospect") & (df_contacts.get("Statut","")=="Actif")])
    membres = len(df_contacts[df_contacts.get("Type","")=="Membre"])
    events_count = len(df_events)
    parts_total = len(df_parts)
    entreprises_total = len(df_entreprises)  # NOUVEAU

    ca_regle, impayes = 0.0, 0.0
    if not df_pay.empty:
        df_pay_copy = df_pay.copy()
        df_pay_copy["Montant"] = pd.to_numeric(df_pay_copy["Montant"], errors='coerce').fillna(0)
        ca_regle = float(df_pay_copy[df_pay_copy["Statut"]=="Réglé"]["Montant"].sum())
        impayes = float(df_pay_copy[df_pay_copy["Statut"]!="Réglé"]["Montant"].sum())

    denom_prospects = max(1, len(df_contacts[df_contacts.get("Type","")=="Prospect"]))
    taux_conv = (membres / denom_prospects) * 100

    # Affichage KPI
    col_k1, col_k2, col_k3, col_k4 = st.columns(4)
    col_k1.metric("👥 Contacts", total_contacts)
    col_k2.metric("🧲 Prospects actifs", prospects_actifs)
    col_k3.metric("🏆 Membres", membres)
    col_k4.metric("🏢 Entreprises", entreprises_total)  # NOUVEAU

    col_k5, col_k6, col_k7, col_k8 = st.columns(4)
    col_k5.metric("📅 Événements", events_count)
    col_k6.metric("🎟 Participations", parts_total)
    col_k7.metric("💰 CA réglé", f"{int(ca_regle):,} FCFA".replace(",", " "))
    col_k8.metric("🔄 Taux conversion", f"{taux_conv:.1f}%")

    # Export Excel simple
    st.markdown("---")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df_contacts.to_excel(writer, sheet_name="Contacts", index=False)
        df_events.to_excel(writer, sheet_name="Événements", index=False)
        df_parts.to_excel(writer, sheet_name="Participations", index=False)
        df_pay.to_excel(writer, sheet_name="Paiements", index=False)
        df_cert.to_excel(writer, sheet_name="Certifications", index=False)
        df_entreprises.to_excel(writer, sheet_name="Entreprises", index=False)  # NOUVEAU
    st.download_button("⬇ Export Excel Complet", buf.getvalue(), "export_iiba_complet.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# PAGE ADMIN
elif page == "Admin":
    st.title("⚙️ Admin — Paramètres, Migration & Maintenance")

    # PARAMETRES LISTES DEROULANTES
    st.markdown("### Listes déroulantes (stockées dans parametres.csv)")
    with st.form("lists_form"):
        def show_line(name, label):
            raw = PARAMS.get(f"list_{name}", DEFAULT_LISTS.get(name, ""))
            return st.text_input(label, raw)
        
        genres = show_line("genres","Genres (séparés par |)")
        types_contact = show_line("types_contact","Types de contact (|)")
        statuts_engagement = show_line("statuts_engagement","Statuts d'engagement (|)")
        secteurs = show_line("secteurs","Secteurs (|)")
        pays = show_line("pays","Pays (|)")
        villes = show_line("villes","Villes (|)")
        sources = show_line("sources","Sources (|)")
        canaux = show_line("canaux","Canaux (|)")
        resultats_inter = show_line("resultats_inter","Résultats d'interaction (|)")
        types_evenements = show_line("types_evenements","Types d'événements (|)")
        lieux = show_line("lieux","Lieux (|)")
        statuts_paiement = show_line("statuts_paiement","Statuts paiement (|)")
        moyens_paiement = show_line("moyens_paiement","Moyens paiement (|)")
        types_certif = show_line("types_certif","Types certification (|)")
        entreprises_cibles = show_line("entreprises_cibles","Entreprises cibles (Top-20) (|)")
        
        # NOUVEAU: Listes pour entreprises
        st.markdown("#### Listes spécifiques aux entreprises")
        tailles_entreprise = show_line("tailles_entreprise","Tailles d'entreprise (|)")
        statuts_partenariat = show_line("statuts_partenariat","Statuts de partenariat (|)")
        types_partenariat = show_line("types_partenariat","Types de partenariat (|)")
        responsables_iiba = show_line("responsables_iiba","Responsables IIBA (|)")
        
        ok1 = st.form_submit_button("💾 Enregistrer les listes")
        if ok1:
            PARAMS.update({
                "list_genres": genres, "list_types_contact": types_contact, "list_statuts_engagement": statuts_engagement,
                "list_secteurs": secteurs, "list_pays": pays, "list_villes": villes, "list_sources": sources,
                "list_canaux": canaux, "list_resultats_inter": resultats_inter, "list_types_evenements": types_evenements,
                "list_lieux": lieux, "list_statuts_paiement": statuts_paiement, "list_moyens_paiement": moyens_paiement,
                "list_types_certif": types_certif, "list_entreprises_cibles": entreprises_cibles,
                # NOUVEAU
                "list_tailles_entreprise": tailles_entreprise, "list_statuts_partenariat": statuts_partenariat,
                "list_types_partenariat": types_partenariat, "list_responsables_iiba": responsables_iiba,
            })
            save_params(PARAMS)
            st.success("Listes enregistrées dans parametres.csv — rechargez la page si nécessaire.")

    # PARAMETRES SCORING ET AFFICHAGE
    st.markdown("### Règles de scoring & d'affichage")
    with st.form("rules_form"):
        c1,c2,c3,c4 = st.columns(4)
        vip_thr = c1.number_input("Seuil VIP (FCFA)", min_value=0.0, step=50000.0, value=float(PARAMS.get("vip_threshold","500000")))
        w_int = c2.number_input("Poids Interaction", min_value=0.0, step=0.5, value=float(PARAMS.get("score_w_interaction","1")))
        w_part = c3.number_input("Poids Participation", min_value=0.0, step=0.5, value=float(PARAMS.get("score_w_participation","1")))
        w_pay = c4.number_input("Poids Paiement réglé", min_value=0.0, step=0.5, value=float(PARAMS.get("score_w_payment_regle","2")))
        c5,c6,c7 = st.columns(3)
        lookback = c5.number_input("Fenêtre interactions récentes (jours)", min_value=1, step=1, value=int(PARAMS.get("interactions_lookback_days","90")))
        hot_int_min = c6.number_input("Interactions récentes min (chaud)", min_value=0, step=1, value=int(PARAMS.get("rule_hot_interactions_recent_min","3")))
        hot_part_min = c7.number_input("Participations min (chaud)", min_value=0, step=1, value=int(PARAMS.get("rule_hot_participations_min","1")))
        hot_partiel = st.checkbox("Paiement partiel = prospect chaud", value=PARAMS.get("rule_hot_payment_partial_counts_as_hot","1") in ("1","true","True"))

        st.write("**Colonnes des grilles (ordre, séparées par des virgules)**")
        grid_crm = st.text_input("CRM → Colonnes", PARAMS.get("grid_crm_columns",""))
        grid_events = st.text_input("Événements → Colonnes", PARAMS.get("grid_events_columns",""))
        grid_entreprises = st.text_input("Entreprises → Colonnes", PARAMS.get("grid_entreprises_columns",""))  # NOUVEAU

        # NOUVEAU: Paramètres spécifiques aux entreprises
        st.markdown("#### Paramètres de scoring des entreprises")
        c_ent1, c_ent2, c_ent3 = st.columns(3)
        ent_ca_weight = c_ent1.number_input("Poids CA", min_value=0.0, max_value=1.0, step=0.1, 
                                           value=float(PARAMS.get("entreprises_scoring_ca_weight","0.3")))
        ent_emp_weight = c_ent2.number_input("Poids Employés", min_value=0.0, max_value=1.0, step=0.1,
                                            value=float(PARAMS.get("entreprises_scoring_employes_weight","0.2")))
        ent_int_weight = c_ent3.number_input("Poids Interactions", min_value=0.0, max_value=1.0, step=0.1,
                                            value=float(PARAMS.get("entreprises_scoring_interactions_weight","0.5")))
        
        c_ent4, c_ent5 = st.columns(2)
        ent_ca_seuil = c_ent4.number_input("Seuil CA gros client (FCFA)", min_value=0, step=1000000,
                                          value=int(float(PARAMS.get("entreprises_ca_seuil_gros","10000000"))))
        ent_emp_seuil = c_ent5.number_input("Seuil employés gros client", min_value=0, step=50,
                                           value=int(float(PARAMS.get("entreprises_employes_seuil_gros","500"))))

        st.write("**KPI visibles (séparés par des virgules)**")
        kpi_enabled = st.text_input("KPI activés", PARAMS.get("kpi_enabled",""))

        ok2 = st.form_submit_button("💾 Enregistrer les paramètres")
        if ok2:
            PARAMS.update({
                "vip_threshold": str(vip_thr),
                "score_w_interaction": str(w_int),
                "score_w_participation": str(w_part),
                "score_w_payment_regle": str(w_pay),
                "interactions_lookback_days": str(int(lookback)),
                "rule_hot_interactions_recent_min": str(int(hot_int_min)),
                "rule_hot_participations_min": str(int(hot_part_min)),
                "rule_hot_payment_partial_counts_as_hot": "1" if hot_partiel else "0",
                "grid_crm_columns": grid_crm,
                "grid_events_columns": grid_events,
                "grid_entreprises_columns": grid_entreprises,  # NOUVEAU
                "kpi_enabled": kpi_enabled,
                # NOUVEAU: Paramètres entreprises
                "entreprises_scoring_ca_weight": str(ent_ca_weight),
                "entreprises_scoring_employes_weight": str(ent_emp_weight),
                "entreprises_scoring_interactions_weight": str(ent_int_weight),
                "entreprises_ca_seuil_gros": str(ent_ca_seuil),
                "entreprises_employes_seuil_gros": str(ent_emp_seuil),
            })
            save_params(PARAMS)
            st.success("Paramètres enregistrés.")

    # IMPORT/EXPORT SIMPLIFIÉ EXCEL AVEC ONGLETS
    st.markdown("---")
    st.header("📦 Import/Export Excel Multi-onglets")
    
    # Export avec nouveau onglet entreprises
    st.subheader("📤 Export")
    buf_export = io.BytesIO()
    with pd.ExcelWriter(buf_export, engine="openpyxl") as writer:
        df_contacts.to_excel(writer, sheet_name="contacts", index=False)
        df_inter.to_excel(writer, sheet_name="interactions", index=False)
        df_events.to_excel(writer, sheet_name="evenements", index=False)
        df_parts.to_excel(writer, sheet_name="participations", index=False)
        df_pay.to_excel(writer, sheet_name="paiements", index=False)
        df_cert.to_excel(writer, sheet_name="certifications", index=False)
        df_entreprises.to_excel(writer, sheet_name="entreprises", index=False)  # NOUVEAU

    st.download_button(
        "⬇️ Exporter toutes les données Excel",
        buf_export.getvalue(),
        file_name=f"IIBA_export_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # Import avec gestion de l'onglet entreprises
    st.subheader("📥 Import")
    fichier_import = st.file_uploader(
        "Fichier Excel (.xlsx) avec onglets: contacts, interactions, evenements, participations, paiements, certifications, entreprises",
        type=["xlsx"], key="xlsx_import_admin"
    )

    if st.button("📥 Importer Excel") and fichier_import is not None:
        try:
            xls = pd.ExcelFile(fichier_import)
            
            # Mapping onglets -> schémas
            sheet_mappings = {
                "contacts": (C_COLS, "CNT"),
                "interactions": (I_COLS, "INT"),
                "evenements": (E_COLS, "EVT"),
                "participations": (P_COLS, "PAR"),
                "paiements": (PAY_COLS, "PAY"),
                "certifications": (CERT_COLS, "CER"),
                "entreprises": (ENT_COLS, "ENT"),  # NOUVEAU
            }
            
            results = {}
            
            for sheet_name, (cols, prefix) in sheet_mappings.items():
                if sheet_name in xls.sheet_names:
                    df_imported = pd.read_excel(xls, sheet_name=sheet_name, dtype=str).fillna("")
                    
                    # Assurer la présence de toutes les colonnes
                    for c in cols:
                        if c not in df_imported.columns:
                            df_imported[c] = ""
                    df_imported = df_imported[cols]
                    
                    # Charger la base existante
                    path_key = {
                        "contacts": "contacts", "interactions": "inter", "evenements": "events",
                        "participations": "parts", "paiements": "pay", "certifications": "cert",
                        "entreprises": "entreprises"  # NOUVEAU
                    }[sheet_name]
                    
                    df_base = ensure_df(PATHS[path_key], cols)
                    
                    # Gérer les IDs
                    id_col = cols[0]
                    existing_ids = set(df_base[id_col].astype(str).tolist())
                    
                    new_rows = []
                    next_num = 1
                    if existing_ids:
                        patt = re.compile(rf"^{prefix}_(\d+)$")
                        max_num = 0
                        for vid in existing_ids:
                            m = patt.match(str(vid))
                            if m:
                                try:
                                    max_num = max(max_num, int(m.group(1)))
                                except:
                                    pass
                        next_num = max_num + 1
                    
                    for _, row in df_imported.iterrows():
                        rid = str(row[id_col]).strip()
                        if (not rid) or rid.lower() == "nan" or rid in existing_ids:
                            rid = f"{prefix}_{str(next_num).zfill(3)}"
                            next_num += 1
                        
                        r = row.to_dict()
                        r[id_col] = rid
                        new_rows.append(r)
                    
                    if new_rows:
                        df_final = pd.concat([df_base, pd.DataFrame(new_rows, columns=cols)], ignore_index=True)
                        save_df(df_final, PATHS[path_key])
                        
                        # Mettre à jour les variables globales
                        global_var_name = {
                            "contacts": "df_contacts", "interactions": "df_inter", "evenements": "df_events",
                            "participations": "df_parts", "paiements": "df_pay", "certifications": "df_cert",
                            "entreprises": "df_entreprises"  # NOUVEAU
                        }[sheet_name]
                        globals()[global_var_name] = df_final
                        
                        results[sheet_name] = len(new_rows)
                    else:
                        results[sheet_name] = 0
                else:
                    results[sheet_name] = f"Onglet manquant"
            
            st.success("Import terminé!")
            st.json(results)
            
        except Exception as e:
            st.error(f"Erreur lors de l'import: {e}")

    # TÉLÉCHARGEMENT ET IMPORT INDIVIDUEL DES CSV
    st.markdown("---")
    st.header("📁 Gestion individuelle des fichiers CSV")
    
    # Téléchargement individuel
    st.subheader("📥 Télécharger les fichiers CSV individuellement")
    
    csv_files = {
        "Contacts": (df_contacts, "contacts.csv"),
        "Interactions": (df_inter, "interactions.csv"),
        "Événements": (df_events, "evenements.csv"),
        "Participations": (df_parts, "participations.csv"),
        "Paiements": (df_pay, "paiements.csv"),
        "Certifications": (df_cert, "certifications.csv"),
        "Entreprises": (df_entreprises, "entreprises.csv"),  # NOUVEAU
        "Paramètres": (pd.read_csv(PATHS["params"]) if PATHS["params"].exists() else pd.DataFrame(), "parametres.csv")
    }
    
    cols_download = st.columns(4)
    for i, (name, (df, filename)) in enumerate(csv_files.items()):
        col = cols_download[i % 4]
        csv_data = df.to_csv(index=False, encoding="utf-8")
        col.download_button(
            f"⬇️ {name}",
            csv_data,
            filename,
            mime="text/csv",
            key=f"dl_{filename}"
        )
    
    # Import individuel
    st.subheader("📤 Importer/Remplacer un fichier CSV individuel")
    
    csv_type = st.selectbox("Type de fichier à importer", [
        "contacts", "interactions", "evenements", "participations", 
        "paiements", "certifications", "entreprises", "parametres"  # NOUVEAU
    ])
    
    uploaded_csv = st.file_uploader(f"Fichier CSV pour {csv_type}", type=["csv"], key=f"upload_{csv_type}")
    
    replace_mode = st.radio("Mode d'import", ["Ajouter aux données existantes", "Remplacer complètement"], key="csv_import_mode")
    
    if st.button("📥 Importer ce CSV") and uploaded_csv is not None:
        try:
            df_uploaded = pd.read_csv(uploaded_csv, dtype=str, encoding="utf-8").fillna("")
            
            if csv_type == "parametres":
                # Cas spécial pour les paramètres
                if "key" in df_uploaded.columns and "value" in df_uploaded.columns:
                    df_uploaded.to_csv(PATHS["params"], index=False, encoding="utf-8")
                    st.success("Fichier paramètres importé avec succès!")
                    st.info("Rechargez la page pour voir les changements.")
                else:
                    st.error("Le fichier paramètres doit contenir les colonnes 'key' et 'value'.")
            else:
                # Import normal des données
                schema_mapping = {
                    "contacts": (C_COLS, "contacts", "CNT"),
                    "interactions": (I_COLS, "inter", "INT"),
                    "evenements": (E_COLS, "events", "EVT"),
                    "participations": (P_COLS, "parts", "PAR"),
                    "paiements": (PAY_COLS, "pay", "PAY"),
                    "certifications": (CERT_COLS, "cert", "CER"),
                    "entreprises": (ENT_COLS, "entreprises", "ENT"),  # NOUVEAU
                }
                
                if csv_type in schema_mapping:
                    cols, path_key, prefix = schema_mapping[csv_type]
                    
                    # Assurer la présence de toutes les colonnes
                    for c in cols:
                        if c not in df_uploaded.columns:
                            df_uploaded[c] = ""
                    df_uploaded = df_uploaded[cols]
                    
                    if replace_mode == "Remplacer complètement":
                        # Remplacer complètement
                        save_df(df_uploaded, PATHS[path_key])
                        global_var = f"df_{path_key}"
                        globals()[global_var] = df_uploaded
                        st.success(f"Données {csv_type} remplacées complètement! ({len(df_uploaded)} lignes)")
                    else:
                        # Ajouter aux données existantes
                        df_base = ensure_df(PATHS[path_key], cols)
                        id_col = cols[0]
                        existing_ids = set(df_base[id_col].astype(str).tolist())
                        
                        new_rows = []
                        next_num = 1
                        if existing_ids:
                            patt = re.compile(rf"^{prefix}_(\d+)$")
                            max_num = 0
                            for vid in existing_ids:
                                m = patt.match(str(vid))
                                if m:
                                    try:
                                        max_num = max(max_num, int(m.group(1)))
                                    except:
                                        pass
                            next_num = max_num + 1
                        
                        for _, row in df_uploaded.iterrows():
                            rid = str(row[id_col]).strip()
                            if (not rid) or rid.lower() == "nan" or rid in existing_ids:
                                rid = f"{prefix}_{str(next_num).zfill(3)}"
                                next_num += 1
                            
                            r = row.to_dict()
                            r[id_col] = rid
                            new_rows.append(r)
                        
                        if new_rows:
                            df_final = pd.concat([df_base, pd.DataFrame(new_rows, columns=cols)], ignore_index=True)
                            save_df(df_final, PATHS[path_key])
                            global_var = f"df_{path_key}"
                            globals()[global_var] = df_final
                            st.success(f"Ajouté {len(new_rows)} nouvelles lignes à {csv_type}!")
                        else:
                            st.info("Aucune nouvelle ligne à ajouter.")
                else:
                    st.error(f"Type de fichier non reconnu: {csv_type}")
                
        except Exception as e:
            st.error(f"Erreur lors de l'import: {e}")

    # MAINTENANCE (simplifié)
    st.markdown("---")
    st.header("🔧 Maintenance")
    
    col_reset, col_info = st.columns(2)
    
    with col_reset:
        st.subheader("🗑️ Réinitialisation")
        if st.button("⚠️ RESET COMPLET (tous les fichiers)", type="secondary"):
            try:
                for path in PATHS.values():
                    if path.exists():
                        path.unlink()
                st.success("✅ Tous les fichiers supprimés! Rechargez la page.")
            except Exception as e:
                st.error(f"Erreur: {e}")
    
    with col_info:
        st.subheader("📊 Informations")
        total_size = sum(path.stat().st_size if path.exists() else 0 for path in PATHS.values())
        st.metric("💾 Taille totale des données", f"{total_size/1024:.1f} KB")
        
        file_counts = {
            "Contacts": len(df_contacts),
            "Interactions": len(df_inter),
            "Événements": len(df_events),
            "Participations": len(df_parts),
            "Paiements": len(df_pay),
            "Certifications": len(df_cert),
            "Entreprises": len(df_entreprises),  # NOUVEAU
        }
        
        for name, count in file_counts.items():
            st.write(f"• {name}: {count} lignes")

    # GESTION DES UTILISATEURS (simplifié)
    st.markdown("---")
    st.header("👤 Gestion des utilisateurs")
    
    current_user = st.session_state.get("user", {})
    if current_user.get("Role") != "admin":
        st.warning("Accès réservé aux administrateurs.")
    else:
        try:
            import bcrypt
            
            def _save_users_df(dfu: pd.DataFrame):
                out = dfu.copy()
                out["active"] = out["active"].map(lambda x: "1" if bool(x) else "0")
                out.to_csv(USERS_PATH, index=False, encoding="utf-8")

            def _hash_password(pw: str) -> str:
                return bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

            df_users = _ensure_users_df()
            
            tab_create, tab_list = st.tabs(["➕ Créer utilisateur", "📋 Liste"])
            
            with tab_create:
                with st.form("create_user_simple"):
                    col1, col2 = st.columns(2)
                    new_user_id = col1.text_input("Email/Login", placeholder="prenom.nom@iiba.cm")
                    full_name = col2.text_input("Nom complet", placeholder="Prénom NOM")
                    role = st.selectbox("Rôle", ["admin","standard"], index=1)
                    pw_plain = st.text_input("Mot de passe", type="password")
                    
                    if st.form_submit_button("Créer"):
                        if new_user_id and full_name and pw_plain and len(pw_plain) >= 6:
                            if new_user_id not in df_users["user_id"].tolist():
                                row = {
                                    "user_id": new_user_id,
                                    "full_name": full_name,
                                    "role": role,
                                    "active": True,
                                    "pwd_hash": _hash_password(pw_plain),
                                    "created_at": datetime.now().isoformat(),
                                    "updated_at": datetime.now().isoformat(),
                                }
                                df_users = pd.concat([df_users, pd.DataFrame([row])], ignore_index=True)
                                _save_users_df(df_users)
                                st.success(f"Utilisateur '{new_user_id}' créé.")
                                _safe_rerun()
                            else:
                                st.error("Cet email existe déjà.")
                        else:
                            st.error("Tous les champs sont requis (mot de passe >= 6 caractères).")
            
            with tab_list:
                if not df_users.empty:
                    show_users = df_users[["user_id","full_name","role","active"]].copy()
                    show_users["active"] = show_users["active"].map(lambda x: "✅" if x else "❌")
                    st.dataframe(show_users, use_container_width=True)
                else:
                    st.info("Aucun utilisateur.")
                    
        except ImportError:
            st.error("Module 'bcrypt' requis. Installez avec: pip install bcrypt")
