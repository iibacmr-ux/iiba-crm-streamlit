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

st.set_page_config(page_title="IIBA Cameroun â€” CRM", page_icon="ðŸ“Š", layout="wide")

# ----------- Paths et schÃ©mas ----------------
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

C_COLS = ["ID","Nom","PrÃ©nom","Genre","Titre","SociÃ©tÃ©","Secteur","Email","TÃ©lÃ©phone","LinkedIn",
          "Ville","Pays","Type","Source","Statut","Score_Engagement","Date_Creation","Notes","Top20"]
I_COLS = ["ID_Interaction","ID","Date","Canal","Objet","RÃ©sumÃ©","RÃ©sultat","Prochaine_Action","Relance","Responsable"]
E_COLS = ["ID_Ã‰vÃ©nement","Nom_Ã‰vÃ©nement","Type","Date","DurÃ©e_h","Lieu","Formateur","Objectif","Periode",
          "Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total","Notes"]
P_COLS = ["ID_Participation","ID","ID_Ã‰vÃ©nement","RÃ´le","Inscription","ArrivÃ©e","Temps_Present","Feedback","Note","Commentaire"]
PAY_COLS = ["ID_Paiement","ID","ID_Ã‰vÃ©nement","Date_Paiement","Montant","Moyen","Statut","RÃ©fÃ©rence","Notes","Relance"]
CERT_COLS = ["ID_Certif","ID","Type_Certif","Date_Examen","RÃ©sultat","Score","Date_Obtention","ValiditÃ©","Renouvellement","Notes"]

# NOUVEAU: SchÃ©ma pour les entreprises
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
    "secteurs":"Banque|TÃ©lÃ©com|IT|Ã‰ducation|SantÃ©|ONG|Industrie|Public|Autre",
    "types_contact":"Membre|Prospect|Formateur|Partenaire",
    "sources":"Afterwork|Formation|LinkedIn|Recommandation|Site Web|Salon|Autre",
    "statuts_engagement":"Actif|Inactif|Ã€ relancer",
    "canaux":"Appel|Email|WhatsApp|Zoom|PrÃ©sentiel|Autre",
    "villes":"Douala|YaoundÃ©|Limbe|Bafoussam|Garoua|Autres",
    "pays":"Cameroun|CÃ´te d'Ivoire|SÃ©nÃ©gal|France|Canada|Autres",
    "types_evenements":"Formation|Groupe d'Ã©tude|BA MEET UP|Webinaire|ConfÃ©rence|Certification",
    "lieux":"PrÃ©sentiel|Zoom|Hybride",
    "resultats_inter":"Positif|NÃ©gatif|Ã€ suivre|Sans suite",
    "statuts_paiement":"RÃ©glÃ©|Partiel|Non payÃ©",
    "moyens_paiement":"Mobile Money|Virement|CB|Cash",
    "types_certif":"ECBA|CCBA|CBAP|PBA",
    "entreprises_cibles":"Dangote|MUPECI|SALAM|SUNU IARD|ENEO|PAD|PAK",
    # NOUVEAU: Listes pour entreprises
    "tailles_entreprise":"TPE (< 10)|PME (10-250)|ETI (250-5000)|GE (> 5000)",
    "statuts_partenariat":"Prospect|Partenaire|Client|Partenaire StratÃ©gique|Inactif",
    "types_partenariat":"Formation|Recrutement|Conseil|Sponsoring|Certification|Autre",
    "responsables_iiba":"Aymard|Alix|ComitÃ©|Non assignÃ©",
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
        "ID","Nom","PrÃ©nom","SociÃ©tÃ©","Type","Statut","Email",
        "Interactions","Participations","CA_rÃ©glÃ©","ImpayÃ©","Resp_principal","A_animÃ©_ou_invitÃ©",
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
    # NOUVEAU: ParamÃ¨tres entreprises
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
    """Ajoute/initialise les colonnes d'audit lors d'une crÃ©ation."""
    row = dict(row)
    now = _now_iso()
    uid = user.get("UserID", "system") if user else "system"
    row.setdefault("Created_At", now)
    row.setdefault("Created_By", uid)
    row["Updated_At"] = row.get("Updated_At", now)
    row["Updated_By"] = row.get("Updated_By", uid)
    return row

def stamp_update(row: dict, user: dict):
    """Met Ã  jour Updated_* lors d'une Ã©dition."""
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
    df_contacts["Top20"] = df_contacts["SociÃ©tÃ©"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])

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

# --- PATCH: forcer l'activation de admin@iiba.cm au dÃ©marrage ---
def _force_activate_admin():
    dfu = _ensure_users_df()
    dfu = _normalize_users_df(dfu)

    m = dfu["user_id"].astype(str).str.strip().str.lower() == "admin@iiba.cm"  
    
    if m.any():
        # rÃ©active + redonne le rÃ´le admin
        # st.sidebar.error(f"_force_activate_admin - if m.any(): user_id : {dfu.loc[m, "user_id"]}")  # print 
        dfu.loc[m, "active"] = True
        dfu.loc[m, "role"] = "admin"
        dfu.loc[m, "updated_at"] = datetime.now().isoformat(timespec="seconds")
        dfu.to_csv(USERS_PATH, index=False, encoding="utf-8")
    else:
        # si le compte n'existe pas, on le (rÃ©)crÃ©e proprement
        st.sidebar.error(f"rÃ©active + redonne le rÃ´le admin - else : {m}") # print
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
    st.sidebar.markdown("### ðŸ” Connexion")
    uid = st.sidebar.text_input("Email / User ID", value=st.session_state.get("last_uid",""))
    pw = st.sidebar.text_input("Mot de passe", type="password") 
    
    if st.sidebar.button("Se connecter", key="btn_login"):
        users_df = _ensure_users_df()
        users_df = _normalize_users_df(users_df)
        m = (users_df["user_id"].astype(str).str.strip().str.lower() == str(uid).strip().lower()) 
        
        # st.sidebar.error(f"login_box: user_id : {users_df.loc[m, "user_id"]}")  # print 
        # st.sidebar.error(f"login_box: user_id : {users_df.loc[m, "updated_at"]}")  # print 
        # st.sidebar.error(f"login_box: user_id : {users_df.loc[m, "active"]}")  # print  
        # st.sidebar.error(f"login_box: user_id : {users_df.loc[m, "role"]}")  # print  
        # st.sidebar.error(f"login_box: user_id : {users_df.loc[m, "pwd_hash"]}")  # print pwd_hash.encode("utf-8")
        # st.sidebar.error(f"login_box: user_id : {users_df.loc[m, "pwd_hash"].encode("utf-8")}")  # print pwd_hash.encode("utf-8")

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
        st.sidebar.success(f"ConnectÃ© : {st.session_state['auth_full_name']} ({st.session_state['auth_role']})")
        if st.sidebar.button("Se dÃ©connecter", key="btn_logout"):
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
    return name in ["CRM (Grille centrale)","Ã‰vÃ©nements","Entreprises"]  # NOUVEAU: ajout Entreprises

# Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Aller Ã ", [
    "CRM (Grille centrale)",
    "Ã‰vÃ©nements", 
    "Entreprises",  # NOUVEAU
    "Rapports",
    "Admin"
], index=0)

if not allow_page(page):
    st.error("â›” AccÃ¨s refusÃ©. Demandez un rÃ´le 'admin' Ã  un membre du comitÃ©.")
    st.stop()

this_year = datetime.now().year
annee = st.sidebar.selectbox("AnnÃ©e", ["Toutes"]+[str(this_year-1),str(this_year),str(this_year+1)], index=1)
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
        has_anim = df_parts.assign(_anim=df_parts["RÃ´le"].isin(["Animateur","InvitÃ©"])).groupby("ID")["_anim"].any()

    pay_reg_count = pd.Series(dtype=int)
    if not df_pay.empty:
        pay = df_pay.copy()
        pay["Montant"] = pd.to_numeric(pay["Montant"], errors="coerce").fillna(0.0)
        total_pay = pay.groupby("ID")["Montant"].sum()
        pay_regle = pay[pay["Statut"]=="RÃ©glÃ©"].groupby("ID")["Montant"].sum()
        pay_impaye = pay[pay["Statut"]!="RÃ©glÃ©"].groupby("ID")["Montant"].sum()
        pay_reg_count = pay[pay["Statut"]=="RÃ©glÃ©"].groupby("ID")["Montant"].count()
        has_partiel = pay[pay["Statut"]=="Partiel"].groupby("ID")["Montant"].count()
    else:
        total_pay = pd.Series(dtype=float)
        pay_regle = pd.Series(dtype=float)
        pay_impaye = pd.Series(dtype=float)
        has_partiel = pd.Series(dtype=int)

    has_cert = pd.Series(dtype=bool)
    if not df_cert.empty:
        has_cert = df_cert[df_cert["RÃ©sultat"]=="RÃ©ussi"].groupby("ID")["ID_Certif"].count() > 0

    ag = pd.DataFrame(index=df_contacts["ID"])
    ag["Interactions"] = ag.index.map(inter_count).fillna(0).astype(int)
    ag["Interactions_recent"] = ag.index.map(recent_inter).fillna(0).astype(int)
    ag["Dernier_contact"] = ag.index.map(last_contact)
    ag["Dernier_contact"] = pd.to_datetime(ag["Dernier_contact"], errors="coerce")
    ag["Dernier_contact"] = ag["Dernier_contact"].dt.date
    ag["Resp_principal"] = ag.index.map(resp_max).fillna("")
    ag["Participations"] = ag.index.map(parts_count).fillna(0).astype(int)
    ag["A_animÃ©_ou_invitÃ©"] = ag.index.map(has_anim).fillna(False)
    ag["CA_total"] = ag.index.map(total_pay).fillna(0.0)
    ag["CA_rÃ©glÃ©"] = ag.index.map(pay_regle).fillna(0.0)
    ag["ImpayÃ©"] = ag.index.map(pay_impaye).fillna(0.0)
    ag["Paiements_regles_n"] = ag.index.map(pay_reg_count).fillna(0).astype(int)
    ag["A_certification"] = ag.index.map(has_cert).fillna(False)
    ag["Score_composite"] = (w_int * ag["Interactions"] + w_part * ag["Participations"] + w_pay * ag["Paiements_regles_n"]).round(2)

    def make_tags(row):
        tags=[]
        if row.name in set(df_contacts.loc[(df_contacts["Type"]=="Prospect") & (df_contacts["Top20"]==True), "ID"]):
            tags.append("Prospect Top-20")
        if row["Participations"] >= 3 and row.name in set(df_contacts[df_contacts["Type"]=="Prospect"]["ID"]) and row["CA_rÃ©glÃ©"] <= 0:
            tags.append("RÃ©gulier-non-converti")
        if row["A_animÃ©_ou_invitÃ©"] or row["Participations"] >= 4:
            tags.append("Futur formateur")
        if row["A_certification"]:
            tags.append("Ambassadeur (certifiÃ©)")
        if row["CA_rÃ©glÃ©"] >= vip_thr:
            tags.append("VIP (CA Ã©levÃ©)")
        return ", ".join(tags)

    ag["Tags"] = ag.apply(make_tags, axis=1)

    def proba(row):
        if row.name in set(df_contacts[df_contacts["Type"]=="Membre"]["ID"]):
            return "Converti"
        chaud = (row["Interactions_recent"] >= hot_int_min and row["Participations"] >= hot_part_min)
        if hot_partiel and row["ImpayÃ©"] > 0 and row["CA_rÃ©glÃ©"] == 0:
            chaud = True
        tiede = (row["Interactions_recent"] >= 1 or row["Participations"] >= 1)
        if chaud:
            return "Chaud"
        if tiede:
            return "TiÃ¨de"
        return "Froid"

    ag["Proba_conversion"] = ag.apply(proba, axis=1)
    return ag.reset_index(names="ID")

# CRM Grille centrale (CODE EXISTANT CONSERVÃ‰)
if page == "CRM (Grille centrale)":
    st.title("ðŸ‘¥ CRM â€” Grille centrale (Contacts)")
    colf1, colf2, colf3, colf4 = st.columns([2,1,1,1])
    q = colf1.text_input("Recherche (nom, sociÃ©tÃ©, email)â€¦","")
    page_size = colf2.selectbox("Taille de page", [20,50,100,200], index=0)
    type_filtre = colf3.selectbox("Type", ["Tous"] + SET["types_contact"])
    top20_only = colf4.checkbox("Top-20 uniquement", value=False)

    dfc = df_contacts.copy()
    ag = aggregates_for_contacts()
    dfc = dfc.merge(ag, on="ID", how="left")

    if q:
        qs = q.lower()
        dfc = dfc[dfc.apply(lambda r: qs in str(r["Nom"]).lower() or qs in str(r["PrÃ©nom"]).lower()
                          or qs in str(r["SociÃ©tÃ©"]).lower() or qs in str(r["Email"]).lower(), axis=1)]
    if type_filtre != "Tous":
        dfc = dfc[dfc["Type"] == type_filtre]
    if top20_only:
        dfc = dfc[dfc["Top20"] == True]

    def parse_cols(s, defaults):
        cols = [c.strip() for c in str(s).split(",") if c.strip()]
        valid = [c for c in cols if c in dfc.columns]
        return valid if valid else defaults

    default_cols = [
        "ID","Nom","PrÃ©nom","SociÃ©tÃ©","Type","Statut","Email",
        "Interactions","Participations","CA_rÃ©glÃ©","ImpayÃ©","Resp_principal","A_animÃ©_ou_invitÃ©",
        "Score_composite","Proba_conversion","Tags"
    ]
    default_cols += [c for c in AUDIT_COLS if c in dfc.columns]
    table_cols = parse_cols(PARAMS.get("grid_crm_columns", ""), default_cols)    

    def _label_contact(row):
        return f"{row['ID']} â€” {row['PrÃ©nom']} {row['Nom']} â€” {row['SociÃ©tÃ©']}"
    options = [] if dfc.empty else dfc.apply(_label_contact, axis=1).tolist()
    id_map = {} if dfc.empty else dict(zip(options, dfc["ID"]))

    colsel, _ = st.columns([3,1])
    sel_label = colsel.selectbox("Contact sÃ©lectionnÃ© (sÃ©lecteur maÃ®tre)", [""] + options, index=0, key="select_contact_label")
    if sel_label:
        st.session_state["selected_contact_id"] = id_map[sel_label]

    if HAS_AGGRID and not dfc.empty:
        dfc_show = dfc[table_cols].copy()
        proba_style = JsCode("""
            function(params) {
              const v = params.value;
              let color = null;
              if (v === 'Chaud') color = '#10B981';
              else if (v === 'TiÃ¨de') color = '#F59E0B';
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
        st.info("Installez `streamlit-aggrid` pour filtres & pagination avancÃ©s.")
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
                if a1.button("âž• Nouveau contact"):
                    st.session_state["selected_contact_id"] = None
                if a2.button("ðŸ§¬ Dupliquer ce contact", disabled=not bool(sel_id)):
                    if sel_id:
                        src = df_contacts[df_contacts["ID"] == sel_id]
                        if not src.empty:
                            clone = src.iloc[0].to_dict()
                            new_id = generate_id("CNT", df_contacts, "ID")
                            clone["ID"] = new_id
                            globals()["df_contacts"] = pd.concat([df_contacts, pd.DataFrame([clone])], ignore_index=True)
                            save_df(df_contacts, PATHS["contacts"])
                            st.session_state["selected_contact_id"] = new_id
                            st.success(f"Contact dupliquÃ© sous l'ID {new_id}.")
                            
                with st.form("edit_contact"):
                    st.text_input("ID", value=d["ID"], disabled=True)
                    n1, n2 = st.columns(2)
                    nom = n1.text_input("Nom", d.get("Nom",""))
                    prenom = n2.text_input("PrÃ©nom", d.get("PrÃ©nom",""))
                    g1,g2 = st.columns(2)
                    genre = g1.selectbox("Genre", SET["genres"], index=SET["genres"].index(d.get("Genre","Homme")) if d.get("Genre","Homme") in SET["genres"] else 0)
                    titre = g2.text_input("Titre / Position", d.get("Titre",""))
                    s1,s2 = st.columns(2)
                    societe = s1.text_input("SociÃ©tÃ©", d.get("SociÃ©tÃ©",""))
                    secteur = s2.selectbox("Secteur", SET["secteurs"], index=SET["secteurs"].index(d.get("Secteur","Autre")) if d.get("Secteur","Autre") in SET["secteurs"] else len(SET["secteurs"])-1)
                    e1,e2,e3 = st.columns(3)
                    email = e1.text_input("Email", d.get("Email",""))
                    tel = e2.text_input("TÃ©lÃ©phone", d.get("TÃ©lÃ©phone",""))
                    linkedin = e3.text_input("LinkedIn", d.get("LinkedIn",""))
                    l1,l2,l3 = st.columns(3)
                    ville = l1.selectbox("Ville", SET["villes"], index=SET["villes"].index(d.get("Ville","Autres")) if d.get("Ville","Autres") in SET["villes"] else len(SET["villes"])-1)
                    pays = l2.selectbox("Pays", SET["pays"], index=SET["pays"].index(d.get("Pays","Cameroun")) if d.get("Pays","Cameroun") in SET["pays"] else 0)
                    typec = l3.selectbox("Type", SET["types_contact"], index=SET["types_contact"].index(d.get("Type","Prospect")) if d.get("Type","Prospect") in SET["types_contact"] else 0)
                    s3,s4,s5 = st.columns(3)
                    source = s3.selectbox("Source", SET["sources"], index=SET["sources"].index(d.get("Source","LinkedIn")) if d.get("Source","LinkedIn") in SET["sources"] else 0)
                    statut = s4.selectbox("Statut", SET["statuts_engagement"], index=SET["statuts_engagement"].index(d.get("Statut","Actif")) if d.get("Statut","Actif") in SET["statuts_engagement"] else 0)
                    score = s5.number_input("Score IIBA", value=float(d.get("Score_Engagement") or 0), step=1.0)
                    dc = st.date_input("Date de crÃ©ation", value=parse_date(d.get("Date_Creation")) or date.today())
                    notes = st.text_area("Notes", d.get("Notes",""))
                    top20 = st.checkbox("Top-20 entreprise", value=bool(str(d.get("Top20")).lower() in ["true","1","yes"]))
                    ok = st.form_submit_button("ðŸ’¾ Enregistrer le contact")
                    if ok:
                        if not str(nom).strip():
                            st.error("âŒ Le nom du contact est obligatoire. Enregistrement annulÃ©.")
                            st.stop()
                        if not email_ok(email):
                            st.error("Email invalide.")
                            st.stop()
                        if not phone_ok(tel):
                            st.error("TÃ©lÃ©phone invalide.")
                            st.stop()
                        idx = df_contacts.index[df_contacts["ID"] == sel_id][0]
                        new_row = {"ID":sel_id,"Nom":nom,"PrÃ©nom":prenom,"Genre":genre,"Titre":titre,"SociÃ©tÃ©":societe,"Secteur":secteur,
                                       "Email":email,"TÃ©lÃ©phone":tel,"LinkedIn":linkedin,"Ville":ville,"Pays":pays,"Type":typec,"Source":source,
                                       "Statut":statut,"Score_Engagement":int(score),"Date_Creation":dc.isoformat(),"Notes":notes,"Top20":top20}
                        raw_existing = df_contacts.loc[idx].to_dict()
                        raw_existing.update(new_row)
                        raw_existing = stamp_update(raw_existing, st.session_state.get("user", {}))
                        df_contacts.loc[idx] = raw_existing
                        save_df(df_contacts, PATHS["contacts"])
                        st.success("Contact mis Ã  jour.")
                st.markdown("---")
                with st.expander("âž• Ajouter ce contact Ã  un **nouvel Ã©vÃ©nement**"):
                    with st.form("quick_evt"):
                        c1,c2 = st.columns(2)
                        nom_ev = c1.text_input("Nom de l'Ã©vÃ©nement")
                        type_ev = c2.selectbox("Type", SET["types_evenements"])
                        c3,c4 = st.columns(2)
                        date_ev = c3.date_input("Date", value=date.today())
                        lieu_ev = c4.selectbox("Lieu", SET["lieux"])
                        role = st.selectbox("RÃ´le du contact", ["Participant","Animateur","InvitÃ©"])
                        ok2 = st.form_submit_button("ðŸ’¾ CrÃ©er l'Ã©vÃ©nement **et** inscrire ce contact")
                        if ok2:
                            new_eid = generate_id("EVT", df_events, "ID_Ã‰vÃ©nement")
                            rowe = {"ID_Ã‰vÃ©nement":new_eid,"Nom_Ã‰vÃ©nement":nom_ev,"Type":type_ev,"Date":date_ev.isoformat(),
                                    "DurÃ©e_h":"2","Lieu":lieu_ev,"Formateur":"","Objectif":"","Periode":"",
                                    "Cout_Salle":0,"Cout_Formateur":0,"Cout_Logistique":0,"Cout_Pub":0,"Cout_Autres":0,"Cout_Total":0,"Notes":""}
                            globals()["df_events"] = pd.concat([df_events, pd.DataFrame([rowe])], ignore_index=True)
                            save_df(df_events, PATHS["events"])
                            new_pid = generate_id("PAR", df_parts, "ID_Participation")
                            rowp = {"ID_Participation":new_pid,"ID":sel_id,"ID_Ã‰vÃ©nement":new_eid,"RÃ´le":role,
                                    "Inscription":"","ArrivÃ©e":"","Temps_Present":"","Feedback":"","Note":"","Commentaire":""}
                            globals()["df_parts"] = pd.concat([df_parts, pd.DataFrame([rowp])], ignore_index=True)
                            save_df(df_parts, PATHS["parts"])
                            st.success(f"Ã‰vÃ©nement crÃ©Ã© ({new_eid}) et contact inscrit ({new_pid}).")
            else:
                st.warning("ID introuvable (rafraÃ®chissez la page).")
        else:
            st.info("SÃ©lectionnez un contact via la grille ou le sÃ©lecteur maÃ®tre.")
            
            if not st.session_state.get("selected_contact_id"):
                with st.expander("âž• CrÃ©er un nouveau contact"):
                    with st.form("create_contact"):
                        n1, n2 = st.columns(2)
                        nom_new = n1.text_input("Nom *", "")
                        prenom_new = n2.text_input("PrÃ©nom", "")
                        g1,g2 = st.columns(2)
                        genre_new = g1.selectbox("Genre", SET["genres"], index=0)
                        titre_new = g2.text_input("Titre / Position", "")
                        s1,s2 = st.columns(2)
                        societe_new = s1.text_input("SociÃ©tÃ©", "")
                        secteur_new = s2.selectbox("Secteur", SET["secteurs"], index=len(SET["secteurs"])-1)
                        e1,e2,e3 = st.columns(3)
                        email_new = e1.text_input("Email", "")
                        tel_new = e2.text_input("TÃ©lÃ©phone", "")
                        linkedin_new = e3.text_input("LinkedIn", "")
                        l1,l2,l3 = st.columns(3)
                        ville_new = l1.selectbox("Ville", SET["villes"], index=len(SET["villes"])-1)
                        pays_new = l2.selectbox("Pays", SET["pays"], index=0)
                        typec_new = l3.selectbox("Type", SET["types_contact"], index=0)
                        s3,s4,s5 = st.columns(3)
                        source_new = s3.selectbox("Source", SET["sources"], index=0)
                        statut_new = s4.selectbox("Statut", SET["statuts_engagement"], index=0)
                        score_new = s5.number_input("Score IIBA", value=0.0, step=1.0)
                        dc_new = st.date_input("Date de crÃ©ation", value=date.today())
                        notes_new = st.text_area("Notes", "")
                        top20_new = st.checkbox("Top-20 entreprise", value=False)
                        ok_new = st.form_submit_button("ðŸ’¾ CrÃ©er le contact")

                        if ok_new:
                            if not str(nom_new).strip():
                                st.error("âŒ Le nom du contact est obligatoire. CrÃ©ation annulÃ©e.")
                                st.stop()
                            if not email_ok(email_new):
                                st.error("Email invalide.")
                                st.stop()
                            if not phone_ok(tel_new):
                                st.error("TÃ©lÃ©phone invalide.")
                                st.stop()

                            new_id = generate_id("CNT", df_contacts, "ID")
                            new_row = {
                                "ID": new_id, "Nom": nom_new, "PrÃ©nom": prenom_new, "Genre": genre_new, "Titre": titre_new,
                                "SociÃ©tÃ©": societe_new, "Secteur": secteur_new, "Email": email_new, "TÃ©lÃ©phone": tel_new,
                                "LinkedIn": linkedin_new, "Ville": ville_new, "Pays": pays_new, "Type": typec_new,
                                "Source": source_new, "Statut": statut_new, "Score_Engagement": int(score_new),
                                "Date_Creation": dc_new.isoformat(), "Notes": notes_new, "Top20": top20_new
                            }
                            globals()["df_contacts"] = pd.concat([df_contacts, pd.DataFrame([new_row])], ignore_index=True)
                            save_df(df_contacts, PATHS["contacts"])
                            st.session_state["selected_contact_id"] = new_id
                            st.success(f"Contact crÃ©Ã© ({new_id}).")
            
    with cR:
        st.subheader("Actions liÃ©es au contact sÃ©lectionnÃ©")
        sel_id = st.session_state.get("selected_contact_id")
        if not sel_id:
            st.info("SÃ©lectionnez un contact pour crÃ©er une interaction, participation, paiement ou certification.")
        else:
            tabs = st.tabs(["âž• Interaction","âž• Participation","âž• Paiement","âž• Certification","ðŸ“‘ Vue 360Â°"])
            with tabs[0]:
                with st.form("add_inter"):
                    c1,c2,c3 = st.columns(3)
                    dti = c1.date_input("Date", value=date.today())
                    canal = c2.selectbox("Canal", SET["canaux"])
                    resp = c3.selectbox("Responsable", ["Aymard","Alix","Autre"])
                    obj = st.text_input("Objet")
                    resu = st.selectbox("RÃ©sultat", SET["resultats_inter"])
                    resume = st.text_area("RÃ©sumÃ©")
                    add_rel = st.checkbox("Planifier une relance ?")
                    rel = st.date_input("Relance", value=date.today()) if add_rel else None
                    ok = st.form_submit_button("ðŸ’¾ Enregistrer l'interaction")
                    if ok:
                        nid = generate_id("INT", df_inter, "ID_Interaction")
                        row = {"ID_Interaction":nid,"ID":sel_id,"Date":dti.isoformat(),"Canal":canal,"Objet":obj,"RÃ©sumÃ©":resume,
                               "RÃ©sultat":resu,"Prochaine_Action":"","Relance":rel.isoformat() if rel else "","Responsable":resp}
                        globals()["df_inter"] = pd.concat([df_inter, pd.DataFrame([row])], ignore_index=True)
                        save_df(df_inter, PATHS["inter"])
                        st.success(f"Interaction enregistrÃ©e ({nid}).")
            with tabs[1]:
                with st.form("add_part"):
                    if df_events.empty:
                        st.warning("CrÃ©ez d'abord un Ã©vÃ©nement.")
                    else:
                        ide = st.selectbox("Ã‰vÃ©nement", df_events["ID_Ã‰vÃ©nement"].tolist())
                        role = st.selectbox("RÃ´le", ["Participant","Animateur","InvitÃ©"])
                        fb = st.selectbox("Feedback", ["TrÃ¨s satisfait","Satisfait","Moyen","Insatisfait"])
                        note = st.number_input("Note (1-5)", min_value=1, max_value=5, value=5)
                        ok = st.form_submit_button("ðŸ’¾ Enregistrer la participation")
                        if ok:
                            nid = generate_id("PAR", df_parts, "ID_Participation")
                            row = {"ID_Participation":nid,"ID":sel_id,"ID_Ã‰vÃ©nement":ide,"RÃ´le":role,"Inscription":"","ArrivÃ©e":"",
                                   "Temps_Present":"","Feedback":fb,"Note":str(note),"Commentaire":""}
                            globals()["df_parts"] = pd.concat([df_parts, pd.DataFrame([row])], ignore_index=True)
                            save_df(df_parts, PATHS["parts"])
                            st.success(f"Participation ajoutÃ©e ({nid}).")
            with tabs[2]:
                with st.form("add_pay"):
                    if df_events.empty:
                        st.warning("CrÃ©ez d'abord un Ã©vÃ©nement.")
                    else:
                        ide = st.selectbox("Ã‰vÃ©nement", df_events["ID_Ã‰vÃ©nement"].tolist())
                        dtp = st.date_input("Date paiement", value=date.today())
                        montant = st.number_input("Montant (FCFA)", min_value=0, step=1000)
                        moyen = st.selectbox("Moyen", SET["moyens_paiement"])
                        statut = st.selectbox("Statut", SET["statuts_paiement"])
                        ref = st.text_input("RÃ©fÃ©rence")
                        ok = st.form_submit_button("ðŸ’¾ Enregistrer le paiement")
                        if ok:
                            nid = generate_id("PAY", df_pay, "ID_Paiement")
                            row = {"ID_Paiement":nid,"ID":sel_id,"ID_Ã‰vÃ©nement":ide,"Date_Paiement":dtp.isoformat(),"Montant":str(montant),
                                   "Moyen":moyen,"Statut":statut,"RÃ©fÃ©rence":ref,"Notes":"","Relance":""}
                            globals()["df_pay"] = pd.concat([df_pay, pd.DataFrame([row])], ignore_index=True)
                            save_df(df_pay, PATHS["pay"])
                            st.success(f"Paiement enregistrÃ© ({nid}).")
            with tabs[3]:
                with st.form("add_cert"):
                    tc = st.selectbox("Type Certification", SET["types_certif"])
                    dte = st.date_input("Date Examen", value=date.today())
                    res = st.selectbox("RÃ©sultat", ["RÃ©ussi","Ã‰chouÃ©","En cours","ReportÃ©"])
                    sc = st.number_input("Score", min_value=0, max_value=100, value=0)
                    has_dto = st.checkbox("Renseigner une date d'obtention ?")
                    dto = st.date_input("Date Obtention", value=date.today()) if has_dto else None
                    ok = st.form_submit_button("ðŸ’¾ Enregistrer la certification")
                    if ok:
                        nid = generate_id("CER", df_cert, "ID_Certif")
                        row = {"ID_Certif":nid,"ID":sel_id,"Type_Certif":tc,"Date_Examen":dte.isoformat(),"RÃ©sultat":res,"Score":str(sc),
                               "Date_Obtention":dto.isoformat() if dto else "","ValiditÃ©":"","Renouvellement":"","Notes":""}
                        globals()["df_cert"] = pd.concat([df_cert, pd.DataFrame([row])], ignore_index=True)
                        save_df(df_cert, PATHS["cert"])
                        st.success(f"Certification ajoutÃ©e ({nid}).")
            with tabs[4]:
                st.markdown("#### Vue 360Â°")
                if not df_inter.empty:
                    st.write("**Interactions**")
                    st.dataframe(df_inter[df_inter["ID"]==sel_id][["Date","Canal","Objet","RÃ©sultat","Relance","Responsable"]], use_container_width=True)
                if not df_parts.empty:
                    st.write("**Participations**")
                    dfp = df_parts[df_parts["ID"]==sel_id].copy()
                    if not df_events.empty:
                        ev_names = df_events.set_index("ID_Ã‰vÃ©nement")["Nom_Ã‰vÃ©nement"]
                        dfp["Ã‰vÃ©nement"] = dfp["ID_Ã‰vÃ©nement"].map(ev_names)
                    st.dataframe(dfp[["Ã‰vÃ©nement","RÃ´le","Feedback","Note"]], use_container_width=True)
                if not df_pay.empty:
                    st.write("**Paiements**")
                    st.dataframe(df_pay[df_pay["ID"]==sel_id][["ID_Ã‰vÃ©nement","Date_Paiement","Montant","Moyen","Statut","RÃ©fÃ©rence"]], use_container_width=True)
                if not df_cert.empty:
                    st.write("**Certifications**")
                    st.dataframe(df_cert[df_cert["ID"]==sel_id][["Type_Certif","Date_Examen","RÃ©sultat","Score","Date_Obtention"]], use_container_width=True)

# PAGE Ã‰VÃ‰NEMENTS (CODE EXISTANT CONSERVÃ‰)
elif page == "Ã‰vÃ©nements":
    st.title("ðŸ“… Ã‰vÃ©nements")

    if "selected_event_id" not in st.session_state:
        st.session_state["selected_event_id"] = ""
    if "event_form_mode" not in st.session_state:
        st.session_state["event_form_mode"] = "create"

    def _label_event(row):
        dat = row.get("Date", "")
        nom = row.get("Nom_Ã‰vÃ©nement", "")
        typ = row.get("Type", "")
        return f"{row['ID_Ã‰vÃ©nement']} â€” {nom} â€” {typ} â€” {dat}"

    options = []
    if not df_events.empty:
        options = df_events.apply(_label_event, axis=1).tolist()
    id_map = dict(zip(options, df_events["ID_Ã‰vÃ©nement"])) if options else {}

    sel_col, new_col = st.columns([3,1])
    cur_label = sel_col.selectbox(
        "Ã‰vÃ©nement sÃ©lectionnÃ© (sÃ©lecteur maÃ®tre)",
        ["â€” Aucun â€”"] + options,
        index=0,
        key="event_select_label"
    )
    if cur_label and cur_label != "â€” Aucun â€”":
        st.session_state["selected_event_id"] = id_map[cur_label]
        st.session_state["event_form_mode"] = "edit"
    else:
        st.session_state["selected_event_id"] = ""
        st.session_state["event_form_mode"] = "create"

    if new_col.button("âž• Nouveau", key="evt_new_btn"):
        st.session_state["selected_event_id"] = ""
        st.session_state["event_form_mode"] = "create"
        _safe_rerun()

    st.markdown("---")

    with st.expander("ðŸ“ GÃ©rer un Ã©vÃ©nement (prÃ©-rempli si un Ã©vÃ©nement est sÃ©lectionnÃ©)", expanded=True):
        mode = st.session_state["event_form_mode"]
        sel_eid = st.session_state["selected_event_id"]

        if mode == "edit" and sel_eid:
            src = df_events[df_events["ID_Ã‰vÃ©nement"] == sel_eid]
            if src.empty:
                st.warning("ID sÃ©lectionnÃ© introuvable; passage en mode crÃ©ation.")
                mode = "create"
                st.session_state["event_form_mode"] = "create"
                sel_eid = ""
                row_init = {c: "" for c in E_COLS}
            else:
                row_init = src.iloc[0].to_dict()
        else:
            row_init = {c: "" for c in E_COLS}

        with st.form("event_form_main", clear_on_submit=False):
            id_dis = st.text_input("ID_Ã‰vÃ©nement", value=row_init.get("ID_Ã‰vÃ©nement", ""), disabled=True)

            c1, c2, c3 = st.columns(3)
            nom = c1.text_input("Nom de l'Ã©vÃ©nement", value=row_init.get("Nom_Ã‰vÃ©nement",""))
            typ = c2.selectbox("Type", SET["types_evenements"], index=SET["types_evenements"].index(row_init.get("Type","Formation")) if row_init.get("Type","Formation") in SET["types_evenements"] else 0)
            dat_val = parse_date(row_init.get("Date")) or date.today()
            dat = c3.date_input("Date", value=dat_val)

            c4, c5, c6 = st.columns(3)
            lieu = c4.selectbox("Lieu", SET["lieux"], index=SET["lieux"].index(row_init.get("Lieu","PrÃ©sentiel")) if row_init.get("Lieu","PrÃ©sentiel") in SET["lieux"] else 0)
            duree = c5.number_input("DurÃ©e (h)", min_value=0.0, step=0.5, value=float(row_init.get("DurÃ©e_h") or 2.0))
            formateur = c6.text_input("Formateur(s)", value=row_init.get("Formateur",""))

            obj = st.text_area("Objectif", value=row_init.get("Objectif",""))

            couts = st.columns(5)
            c_salle = couts[0].number_input("CoÃ»t salle", min_value=0.0, step=1000.0, value=float(row_init.get("Cout_Salle") or 0.0))
            c_form  = couts[1].number_input("CoÃ»t formateur", min_value=0.0, step=1000.0, value=float(row_init.get("Cout_Formateur") or 0.0))
            c_log   = couts[2].number_input("CoÃ»t logistique", min_value=0.0, step=1000.0, value=float(row_init.get("Cout_Logistique") or 0.0))
            c_pub   = couts[3].number_input("CoÃ»t pub", min_value=0.0, step=1000.0, value=float(row_init.get("Cout_Pub") or 0.0))
            c_aut   = couts[4].number_input("Autres coÃ»ts", min_value=0.0, step=1000.0, value=float(row_init.get("Cout_Autres") or 0.0))

            notes = st.text_area("Notes", value=row_init.get("Notes",""))

            cL, cM, cR = st.columns([1.2,1.2,2])
            btn_create = cL.form_submit_button("ðŸ†• CrÃ©er l'Ã©vÃ©nement", disabled=(mode=="edit"))
            btn_save   = cM.form_submit_button("ðŸ’¾ Enregistrer modifications", disabled=(mode!="edit"))

            if btn_create:
                if not nom.strip():
                    st.error("Le nom de l'Ã©vÃ©nement est obligatoire.")
                    st.stop()
                new_id = generate_id("EVT", df_events, "ID_Ã‰vÃ©nement")
                new_row = {
                    "ID_Ã‰vÃ©nement": new_id, "Nom_Ã‰vÃ©nement": nom, "Type": typ, "Date": dat.isoformat(),
                    "DurÃ©e_h": str(duree), "Lieu": lieu, "Formateur": formateur, "Objectif": obj, "Periode": "",
                    "Cout_Salle": c_salle, "Cout_Formateur": c_form, "Cout_Logistique": c_log,
                    "Cout_Pub": c_pub, "Cout_Autres": c_aut, "Cout_Total": 0, "Notes": notes
                }
                globals()["df_events"] = pd.concat([df_events, pd.DataFrame([new_row])], ignore_index=True)
                save_df(df_events, PATHS["events"])
                st.success(f"Ã‰vÃ©nement crÃ©Ã© ({new_id}).")
                st.session_state["selected_event_id"] = new_id
                st.session_state["event_form_mode"] = "edit"
                _safe_rerun()

            if btn_save:
                if not sel_eid:
                    st.error("Aucun Ã©vÃ©nement sÃ©lectionnÃ© pour enregistrer des modifications.")
                    st.stop()
                if not nom.strip():
                    st.error("Le nom de l'Ã©vÃ©nement est obligatoire.")
                    st.stop()
                idx = df_events.index[df_events["ID_Ã‰vÃ©nement"] == sel_eid]
                if len(idx) == 0:
                    st.error("Ã‰vÃ©nement introuvable (rafraÃ®chissez).")
                    st.stop()
                rowe = {
                    "ID_Ã‰vÃ©nement": sel_eid, "Nom_Ã‰vÃ©nement": nom, "Type": typ, "Date": dat.isoformat(),
                    "DurÃ©e_h": str(duree), "Lieu": lieu, "Formateur": formateur, "Objectif": obj, "Periode": "",
                    "Cout_Salle": c_salle, "Cout_Formateur": c_form, "Cout_Logistique": c_log,
                    "Cout_Pub": c_pub, "Cout_Autres": c_aut, "Cout_Total": 0, "Notes": notes
                }
                df_events.loc[idx[0]] = rowe
                save_df(df_events, PATHS["events"])
                st.success(f"Ã‰vÃ©nement {sel_eid} mis Ã  jour.")

    st.markdown("---")

    col_dup, col_del, col_clear = st.columns([1,1,1])
    if col_dup.button("ðŸ§¬ Dupliquer l'Ã©vÃ©nement sÃ©lectionnÃ©", key="evt_dup_btn", disabled=(st.session_state["event_form_mode"]!="edit" or not st.session_state["selected_event_id"])):
        src_id = st.session_state["selected_event_id"]
        src = df_events[df_events["ID_Ã‰vÃ©nement"] == src_id]
        if src.empty:
            st.error("Impossible de dupliquer: Ã©vÃ©nement introuvable.")
        else:
            new_id = generate_id("EVT", df_events, "ID_Ã‰vÃ©nement")
            clone = src.iloc[0].to_dict()
            clone["ID_Ã‰vÃ©nement"] = new_id
            globals()["df_events"] = pd.concat([df_events, pd.DataFrame([clone])], ignore_index=True)
            save_df(df_events, PATHS["events"])
            st.success(f"Ã‰vÃ©nement dupliquÃ© sous l'ID {new_id}.")
            st.session_state["selected_event_id"] = new_id
            st.session_state["event_form_mode"] = "edit"
            _safe_rerun()

    with col_del:
        st.caption("Confirmation suppression")
        confirm_txt = st.text_input("Tapez SUPPRIME ou DELETE", value="", key="evt_del_confirm")
        if st.button("ðŸ—‘ï¸ Supprimer dÃ©finitivement", key="evt_del_btn", disabled=(st.session_state["event_form_mode"]!="edit" or not st.session_state["selected_event_id"])):
            if confirm_txt.strip().upper() not in ("SUPPRIME", "DELETE"):
                st.error("Veuillez confirmer en saisissant SUPPRIME ou DELETE.")
            else:
                del_id = st.session_state["selected_event_id"]
                if not del_id:
                    st.error("Aucun Ã©vÃ©nement sÃ©lectionnÃ©.")
                else:
                    globals()["df_events"] = df_events[df_events["ID_Ã‰vÃ©nement"] != del_id]
                    save_df(df_events, PATHS["events"])
                    st.success(f"Ã‰vÃ©nement {del_id} supprimÃ©.")
                    st.session_state["selected_event_id"] = ""
                    st.session_state["event_form_mode"] = "create"
                    _safe_rerun()

    if col_clear.button("ðŸ§¹ Vider la sÃ©lection", key="evt_clear_btn"):
        st.session_state["selected_event_id"] = ""
        st.session_state["event_form_mode"] = "create"
        _safe_rerun()

    st.markdown("---")

    st.subheader("ðŸ“‹ Liste des Ã©vÃ©nements")
    filt = st.text_input("Filtre rapide (nom, type, lieu, notesâ€¦)", "", key="evt_filter")
    page_size_evt = st.selectbox("Taille de page", [20,50,100,200], index=0, key="pg_evt")

    evt_default_cols = E_COLS + [c for c in AUDIT_COLS if c in df_events.columns]
    df_show = df_events[evt_default_cols].copy()

    if filt:
        t = filt.lower()
        df_show = df_show[df_show.apply(lambda r: any(t in str(r[c]).lower() for c in ["Nom_Ã‰vÃ©nement","Type","Lieu","Notes"]), axis=1)]

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
        if col_apply.button("ðŸ’¾ Appliquer les modifications (grille)", key="evt_apply_grid"):
            new_df = pd.DataFrame(grid["data"])
            for c in E_COLS:
                if c not in new_df.columns:
                    new_df[c] = ""
            globals()["df_events"] = new_df[E_COLS].copy()
            save_df(df_events, PATHS["events"])
            st.success("Modifications enregistrÃ©es depuis la grille.")
    else:
        st.dataframe(df_show, use_container_width=True)
        st.info("Installez `streamlit-aggrid` pour Ã©diter/dupliquer directement dans la grille.")

# ===== NOUVELLE PAGE ENTREPRISES =====
elif page == "Entreprises":
    st.title("ðŸ¢ Entreprises & Partenaires")

    # Session state pour la sÃ©lection d'entreprise
    if "selected_entreprise_id" not in st.session_state:
        st.session_state["selected_entreprise_id"] = ""
    if "entreprise_form_mode" not in st.session_state:
        st.session_state["entreprise_form_mode"] = "create"

    # SÃ©lecteur d'entreprise
    def _label_entreprise(row):
        nom = row.get("Nom_Entreprise", "")
        secteur = row.get("Secteur", "")
        statut = row.get("Statut_Partenariat", "")
        return f"{row['ID_Entreprise']} â€” {nom} â€” {secteur} â€” {statut}"

    options_ent = []
    if not df_entreprises.empty:
        options_ent = df_entreprises.apply(_label_entreprise, axis=1).tolist()
    id_map_ent = dict(zip(options_ent, df_entreprises["ID_Entreprise"])) if options_ent else {}

    sel_col_ent, new_col_ent = st.columns([3,1])
    cur_label_ent = sel_col_ent.selectbox(
        "Entreprise sÃ©lectionnÃ©e (sÃ©lecteur maÃ®tre)",
        ["â€” Aucune â€”"] + options_ent,
        index=0,
        key="entreprise_select_label"
    )
    if cur_label_ent and cur_label_ent != "â€” Aucune â€”":
        st.session_state["selected_entreprise_id"] = id_map_ent[cur_label_ent]
        st.session_state["entreprise_form_mode"] = "edit"
    else:
        st.session_state["selected_entreprise_id"] = ""
        st.session_state["entreprise_form_mode"] = "create"

    if new_col_ent.button("âž• Nouvelle", key="ent_new_btn"):
        st.session_state["selected_entreprise_id"] = ""
        st.session_state["entreprise_form_mode"] = "create"
        _safe_rerun()

    st.markdown("---")

    # Formulaire de gestion d'entreprise
    with st.expander("ðŸ“ GÃ©rer une entreprise (prÃ©-rempli si une entreprise est sÃ©lectionnÃ©e)", expanded=True):
        mode_ent = st.session_state["entreprise_form_mode"]
        sel_entid = st.session_state["selected_entreprise_id"]

        # PrÃ©-remplissage si Ã©dition
        if mode_ent == "edit" and sel_entid:
            src_ent = df_entreprises[df_entreprises["ID_Entreprise"] == sel_entid]
            if src_ent.empty:
                st.warning("ID sÃ©lectionnÃ© introuvable; passage en mode crÃ©ation.")
                mode_ent = "create"
                st.session_state["entreprise_form_mode"] = "create"
                sel_entid = ""
                row_init_ent = {c: "" for c in ENT_COLS}
            else:
                row_init_ent = src_ent.iloc[0].to_dict()
        else:
            row_init_ent = {c: "" for c in ENT_COLS}

        with st.form("entreprise_form_main", clear_on_submit=False):
            # ID grisÃ©
            id_dis_ent = st.text_input("ID_Entreprise", value=row_init_ent.get("ID_Entreprise", ""), disabled=True)

            # Informations de base
            c1_ent, c2_ent, c3_ent = st.columns(3)
            nom_ent = c1_ent.text_input("Nom de l'entreprise", value=row_init_ent.get("Nom_Entreprise",""))
            secteur_ent = c2_ent.selectbox("Secteur", SET["secteurs"], 
                                          index=SET["secteurs"].index(row_init_ent.get("Secteur","Autre")) if row_init_ent.get("Secteur","Autre") in SET["secteurs"] else len(SET["secteurs"])-1)
            taille_ent = c3_ent.selectbox("Taille", SET["tailles_entreprise"],
                                         index=SET["tailles_entreprise"].index(row_init_ent.get("Taille","PME (10-250)")) if row_init_ent.get("Taille","PME (10-250)") in SET["tailles_entreprise"] else 1)

            # DonnÃ©es Ã©conomiques
            c4_ent, c5_ent = st.columns(2)
            ca_annuel = c4_ent.number_input("CA Annuel (FCFA)", min_value=0, step=1000000, value=int(float(row_init_ent.get("CA_Annuel") or 0)))
            nb_employes = c5_ent.number_input("Nombre d'employÃ©s", min_value=0, step=10, value=int(float(row_init_ent.get("Nb_Employes") or 0)))

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
            tel_principal = c10_ent.text_input("TÃ©lÃ©phone", value=row_init_ent.get("Telephone_Principal",""))
            site_web = st.text_input("Site Web", value=row_init_ent.get("Site_Web",""))

            # Partenariat
            st.subheader("Partenariat")
            c11_ent, c12_ent, c13_ent = st.columns(3)
            statut_part = c11_ent.selectbox("Statut Partenariat", SET["statuts_partenariat"],
                                           index=SET["statuts_partenariat"].index(row_init_ent.get("Statut_Partenariat","Prospect")) if row_init_ent.get("Statut_Partenariat","Prospect") in SET["statuts_partenariat"] else 0)
            type_part = c12_ent.selectbox("Type Partenariat", SET["types_partenariat"],
                                         index=SET["types_partenariat"].index(row_init_ent.get("Type_Partenariat","Formation")) if row_init_ent.get("Type_Partenariat","Formation") in SET["types_partenariat"] else 0)
            resp_iiba = c13_ent.selectbox("Responsable IIBA", SET["responsables_iiba"],
                                         index=SET["responsables_iiba"].index(row_init_ent.get("Responsable_IIBA","Non assignÃ©")) if row_init_ent.get("Responsable_IIBA","Non assignÃ©") in SET["responsables_iiba"] else len(SET["responsables_iiba"])-1)

            # Dates
            c14_ent, c15_ent = st.columns(2)
            date_premier_contact = c14_ent.date_input("Date premier contact", 
                                                     value=parse_date(row_init_ent.get("Date_Premier_Contact")) or date.today())
            date_maj = c15_ent.date_input("Date mise Ã  jour", value=date.today())

            # Notes et opportunitÃ©s
            notes_ent = st.text_area("Notes", value=row_init_ent.get("Notes",""))
            opportunites = st.text_area("OpportunitÃ©s", value=row_init_ent.get("Opportunites",""))

            # Boutons
            cL_ent, cM_ent, cR_ent = st.columns([1.2,1.2,2])
            btn_create_ent = cL_ent.form_submit_button("ðŸ†• CrÃ©er l'entreprise", disabled=(mode_ent=="edit"))
            btn_save_ent = cM_ent.form_submit_button("ðŸ’¾ Enregistrer modifications", disabled=(mode_ent!="edit"))

            # Actions du formulaire
            if btn_create_ent:
                if not nom_ent.strip():
                    st.error("Le nom de l'entreprise est obligatoire.")
                    st.stop()
                if email_principal and not email_ok(email_principal):
                    st.error("Email principal invalide.")
                    st.stop()
                if tel_principal and not phone_ok(tel_principal):
                    st.error("TÃ©lÃ©phone principal invalide.")
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
                st.success(f"Entreprise crÃ©Ã©e ({new_id_ent}).")
                st.session_state["selected_entreprise_id"] = new_id_ent
                st.session_state["entreprise_form_mode"] = "edit"
                _safe_rerun()

            if btn_save_ent:
                if not sel_entid:
                    st.error("Aucune entreprise sÃ©lectionnÃ©e pour enregistrer des modifications.")
                    st.stop()
                if not nom_ent.strip():
                    st.error("Le nom de l'entreprise est obligatoire.")
                    st.stop()
                if email_principal and not email_ok(email_principal):
                    st.error("Email principal invalide.")
                    st.stop()
                if tel_principal and not phone_ok(tel_principal):
                    st.error("TÃ©lÃ©phone principal invalide.")
                    st.stop()

                idx_ent = df_entreprises.index[df_entreprises["ID_Entreprise"] == sel_entid]
                if len(idx_ent) == 0:
                    st.error("Entreprise introuvable (rafraÃ®chissez).")
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
                st.success(f"Entreprise {sel_entid} mise Ã  jour.")

    st.markdown("---")

    # Actions avancÃ©es pour entreprises
    col_dup_ent, col_del_ent, col_clear_ent = st.columns([1,1,1])
    
    if col_dup_ent.button("ðŸ§¬ Dupliquer l'entreprise sÃ©lectionnÃ©e", key="ent_dup_btn", 
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
            st.success(f"Entreprise dupliquÃ©e sous l'ID {new_id_ent}.")
            st.session_state["selected_entreprise_id"] = new_id_ent
            st.session_state["entreprise_form_mode"] = "edit"
            _safe_rerun()

    with col_del_ent:
        st.caption("Confirmation suppression")
        confirm_txt_ent = st.text_input("Tapez SUPPRIME ou DELETE", value="", key="ent_del_confirm")
        if st.button("ðŸ—‘ï¸ Supprimer dÃ©finitivement", key="ent_del_btn", 
                     disabled=(st.session_state["entreprise_form_mode"]!="edit" or not st.session_state["selected_entreprise_id"])):
            if confirm_txt_ent.strip().upper() not in ("SUPPRIME", "DELETE"):
                st.error("Veuillez confirmer en saisissant SUPPRIME ou DELETE.")
            else:
                del_id_ent = st.session_state["selected_entreprise_id"]
                if not del_id_ent:
                    st.error("Aucune entreprise sÃ©lectionnÃ©e.")
                else:
                    globals()["df_entreprises"] = df_entreprises[df_entreprises["ID_Entreprise"] != del_id_ent]
                    save_df(df_entreprises, PATHS["entreprises"])
                    st.success(f"Entreprise {del_id_ent} supprimÃ©e.")
                    st.session_state["selected_entreprise_id"] = ""
                    st.session_state["entreprise_form_mode"] = "create"
                    _safe_rerun()

    if col_clear_ent.button("ðŸ§¹ Vider la sÃ©lection", key="ent_clear_btn"):
        st.session_state["selected_entreprise_id"] = ""
        st.session_state["entreprise_form_mode"] = "create"
        _safe_rerun()

    st.markdown("---")

    # Grille des entreprises
    st.subheader("ðŸ“‹ Liste des entreprises")
    filt_ent = st.text_input("Filtre rapide (nom, secteur, statutâ€¦)", "", key="ent_filter")
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
        if col_apply_ent.button("ðŸ’¾ Appliquer les modifications (grille)", key="ent_apply_grid"):
            new_df_ent = pd.DataFrame(grid_ent["data"])
            for c in ENT_COLS:
                if c not in new_df_ent.columns:
                    new_df_ent[c] = ""
            globals()["df_entreprises"] = new_df_ent[ENT_COLS].copy()
            save_df(df_entreprises, PATHS["entreprises"])
            st.success("Modifications enregistrÃ©es depuis la grille.")
    else:
        st.dataframe(df_show_ent, use_container_width=True)
        st.info("Installez `streamlit-aggrid` pour Ã©diter directement dans la grille.")

    # Statistiques rapides
    st.markdown("---")
    st.subheader("ðŸ“Š Statistiques des entreprises")
    
    if not df_entreprises.empty:
        col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
        
        total_entreprises = len(df_entreprises)
        partenaires_actifs = len(df_entreprises[df_entreprises["Statut_Partenariat"].isin(["Partenaire", "Client", "Partenaire StratÃ©gique"])])
        prospects = len(df_entreprises[df_entreprises["Statut_Partenariat"] == "Prospect"])
        ca_total_ent = df_entreprises["CA_Annuel"].astype(str).str.replace("", "0").astype(float).sum()
        
        col_stat1.metric("ðŸ¢ Total Entreprises", total_entreprises)
        col_stat2.metric("ðŸ¤ Partenaires Actifs", partenaires_actifs)
        col_stat3.metric("ðŸŽ¯ Prospects", prospects)
        col_stat4.metric("ðŸ’° CA CumulÃ©", f"{ca_total_ent/1e9:.1f}B FCFA")
        
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
                ).properties(height=250, title="RÃ©partition par statut")
                st.altair_chart(chart_statut, use_container_width=True)
            
            with col_chart2:
                secteur_counts = df_entreprises["Secteur"].value_counts().reset_index()
                secteur_counts.columns = ["Secteur", "Count"]
                chart_secteur = alt.Chart(secteur_counts).mark_arc().encode(
                    theta=alt.Theta("Count:Q"),
                    color=alt.Color("Secteur:N"),
                    tooltip=["Secteur", "Count"]
                ).properties(height=250, title="RÃ©partition par secteur")
                st.altair_chart(chart_secteur, use_container_width=True)
    else:
        st.info("Aucune entreprise enregistrÃ©e.")

# PAGE RAPPORTS (CODE EXISTANT CONSERVÃ‰ - partie simplifiÃ©e)


elif page == "Rapports":
    st.title("ðŸ“‘ Rapports & KPI â€” IIBA Cameroun")

    # ---------- Helpers gÃ©nÃ©riques ----------
    def _safe_parse_series(s: pd.Series) -> pd.Series:
        return s.map(lambda x: parse_date(x) if pd.notna(x) and str(x).strip() != "" else None)

    def _build_mask_from_dates(d: pd.Series, year_sel: str, month_sel: str) -> pd.Series:
        mask = pd.Series(True, index=d.index)
        if year_sel != "Toutes":
            y = int(year_sel)
            mask = mask & d.map(lambda x: isinstance(x, (datetime, date)) and x.year == y)
        if month_sel != "Tous":
            m = int(month_sel)
            mask = mask & d.map(lambda x: isinstance(x, (datetime, date)) and x.month == m)
        return mask.fillna(False)

    # ---------- Filtrage des tables principales ----------
    def filtered_tables_for_period(year_sel: str, month_sel: str):
        # Ã‰vÃ©nements
        if df_events.empty:
            dfe2 = df_events.copy()
        else:
            ev_dates = _safe_parse_series(df_events["Date"])
            mask_e = _build_mask_from_dates(ev_dates, year_sel, month_sel)
            dfe2 = df_events[mask_e].copy()

        # Participations (via date d'Ã©vÃ©nement)
        if df_parts.empty:
            dfp2 = df_parts.copy()
        else:
            dfp2 = df_parts.copy()
            if not df_events.empty:
                ev_dates_map = df_events.set_index("ID_Ã‰vÃ©nement")["Date"].map(parse_date)
                dfp2["_d_evt"] = dfp2["ID_Ã‰vÃ©nement"].map(ev_dates_map)
                mask_p = _build_mask_from_dates(dfp2["_d_evt"], year_sel, month_sel)
                dfp2 = dfp2[mask_p].copy()
            else:
                dfp2 = dfp2.iloc[0:0].copy()

        # Paiements
        if df_pay.empty:
            dfpay2 = df_pay.copy()
        else:
            pay_dates = _safe_parse_series(df_pay["Date_Paiement"])
            mask_pay = _build_mask_from_dates(pay_dates, year_sel, month_sel)
            dfpay2 = df_pay[mask_pay].copy()

        # Certifications
        if df_cert.empty:
            dfcert2 = df_cert.copy()
        else:
            obt = _safe_parse_series(df_cert["Date_Obtention"]) if "Date_Obtention" in df_cert.columns else pd.Series([None]*len(df_cert), index=df_cert.index)
            exa = _safe_parse_series(df_cert["Date_Examen"])    if "Date_Examen"    in df_cert.columns    else pd.Series([None]*len(df_cert), index=df_cert.index)
            mask_c = _build_mask_from_dates(obt, year_sel, month_sel) | _build_mask_from_dates(exa, year_sel, month_sel)
            dfcert2 = df_cert[mask_c.fillna(False)].copy()

        return dfe2, dfp2, dfpay2, dfcert2

    # ---------- Filtrage des CONTACTS par pÃ©riode ----------
    # Logique : on prend Date_Creation si dispo. Sinon, on essaie de dÃ©duire une "date de rÃ©fÃ©rence"
    # depuis la 1re interaction, 1re participation (date d'Ã©vÃ©nement) ou 1er paiement.
    def filtered_contacts_for_period(
        year_sel: str,
        month_sel: str,
        dfe_all: pd.DataFrame,   # events (toutes lignes, pas filtrÃ©es)
        dfi_all: pd.DataFrame,   # interactions (toutes)
        dfp_all: pd.DataFrame,   # participations (toutes)
        dfpay_all: pd.DataFrame  # paiements (toutes)
    ) -> pd.DataFrame:
        """
        Filtre les CONTACTS par pÃ©riode.
        Logique configurable via PARAMS["contacts_period_fallback"]:
          - OFF/0: ne filtre que sur Date_Creation (contact inclus si Date_Creation âˆˆ pÃ©riode)
          - ON/1 (par dÃ©faut): utilise Date_Creation, sinon retombe sur la 1re activitÃ© dÃ©tectÃ©e
            (1re interaction, 1re participation via date d'Ã©vÃ©nement, 1er paiement).
        """

        base = df_contacts.copy()
        if base.empty or "ID" not in base.columns:
            return base  # rien Ã  filtrer

        # Normalisation ID en str (Ã©vite les merges/map sur types hÃ©tÃ©rogÃ¨nes)
        base["ID"] = base["ID"].astype(str).str.strip()

        # Parse Date_Creation -> sÃ©rie de dates (ou None)
        if "Date_Creation" in base.columns:
            base["_dc"] = _safe_parse_series(base["Date_Creation"])
        else:
            base["_dc"] = pd.Series([None] * len(base), index=base.index)

        # ParamÃ¨tre fallback (Admin -> ParamÃ¨tres)
        use_fallback = str(PARAMS.get("contacts_period_fallback", "on")).lower() in ("on", "1", "true", "vrai", "yes")

        # ========== MODE STRICT (fallback OFF) ==========
        if not use_fallback:
            mask = _build_mask_from_dates(base["_dc"], year_sel, month_sel)
            return base[mask].drop(columns=["_dc"], errors="ignore")

        # ========== MODE FALLBACK (ON) ==========
        # 1) 1re Interaction
        if not dfi_all.empty and "Date" in dfi_all.columns and "ID" in dfi_all.columns:
            dfi = dfi_all.copy()
            dfi["ID"] = dfi["ID"].astype(str).str.strip()
            dfi["_di"] = pd.to_datetime(_safe_parse_series(dfi["Date"]), errors="coerce") 
            first_inter = dfi.groupby("ID")["_di"].min()
        else:
            first_inter = pd.Series(dtype=object)

        # 2) 1re Participation (via date d'Ã©vÃ©nement)
        if (not dfp_all.empty and "ID" in dfp_all.columns and "ID_Ã‰vÃ©nement" in dfp_all.columns
            and not dfe_all.empty and "ID_Ã‰vÃ©nement" in dfe_all.columns and "Date" in dfe_all.columns):
            dfp = dfp_all.copy()
            dfp = dfp[dfp["ID_Ã‰vÃ©nement"].notna()]  # Ã©vite les NaN dans le mapping
            dfp["ID"] = dfp["ID"].astype(str).str.strip()

            ev_dates = dfe_all.copy()
            ev_dates["_de"] = _safe_parse_series(ev_dates["Date"])           # objets date/None
            ev_map = ev_dates.set_index("ID_Ã‰vÃ©nement")["_de"]

            dfp["_de"] = dfp["ID_Ã‰vÃ©nement"].map(ev_map)
            dfp["_de"] = pd.to_datetime(dfp["_de"], errors="coerce")         # -> datetime64[ns]
            first_part = dfp.groupby("ID")["_de"].min()
        else:
            first_part = pd.Series(dtype="datetime64[ns]")

        # 3) 1er Paiement
        if not dfpay_all.empty and "Date_Paiement" in dfpay_all.columns and "ID" in dfpay_all.columns:
            dfpay = dfpay_all.copy()
            dfpay["ID"] = dfpay["ID"].astype(str).str.strip()
            dfpay["_dp"] = pd.to_datetime(_safe_parse_series(dfpay["Date_Paiement"]), errors="coerce")
            first_pay = dfpay.groupby("ID")["_dp"].min()
        else:
            first_pay = pd.Series(dtype=object)

        # Choisir la 1re date valide parmi: Date_Creation, 1re interaction, 1re participation, 1er paiement
        def _first_valid_date(dc, fi, fp, fpay):
            cands = []
            for v in (dc, fi, fp, fpay):
                if isinstance(v, pd.Timestamp):
                    v = v.to_pydatetime()
                if isinstance(v, datetime):
                    cands.append(v.date())
                elif isinstance(v, date):
                    cands.append(v)
            return min(cands) if cands else None

        # Construire un dict ID -> date de rÃ©fÃ©rence
        ref_dates = {}
        ids = base["ID"].tolist()
        # set pour lecture rapide
        set_ids = set(ids)

        # accÃ¨s direct aux sÃ©ries pour perf
        s_dc = base.set_index("ID")["_dc"] if "ID" in base.columns else pd.Series(dtype=object)

        for cid in ids:
            dc   = s_dc.get(cid, None) if not s_dc.empty else None
            fi   = first_inter.get(cid, None) if not first_inter.empty else None
            fp   = first_part.get(cid, None)  if not first_part.empty else None
            fpay = first_pay.get(cid, None)   if not first_pay.empty else None
            ref_dates[cid] = _first_valid_date(dc, fi, fp, fpay)

        base["_ref"] = base["ID"].map(ref_dates)

        # Filtrage final par pÃ©riode
        mask = _build_mask_from_dates(base["_ref"], year_sel, month_sel)
        return base[mask].drop(columns=["_dc", "_ref"], errors="ignore")



    # ---------- AgrÃ©gats pÃ©riode (version locale, basÃ©e sur les tables filtrÃ©es) ----------
    def aggregates_for_contacts_period(contacts: pd.DataFrame,
                                       dfi: pd.DataFrame, dfp: pd.DataFrame,
                                       dfpay: pd.DataFrame, dfcert: pd.DataFrame) -> pd.DataFrame:
        if contacts.empty:
            return pd.DataFrame({"ID": [], "Interactions": [], "Interactions_recent": [], "Dernier_contact": [],
                                 "Resp_principal": [], "Participations": [], "A_animÃ©_ou_invitÃ©": [],
                                 "CA_total": [], "CA_rÃ©glÃ©": [], "ImpayÃ©": [], "Paiements_regles_n": [],
                                 "A_certification": [], "Score_composite": [], "Tags": [], "Proba_conversion": []})

        # Params scoring (identiques Ã  aggregates_for_contacts)
        vip_thr = float(PARAMS.get("vip_threshold", "500000"))
        w_int = float(PARAMS.get("score_w_interaction", "1"))
        w_part = float(PARAMS.get("score_w_participation", "1"))
        w_pay = float(PARAMS.get("score_w_payment_regle", "2"))
        lookback = int(PARAMS.get("interactions_lookback_days", "90"))
        hot_int_min = int(PARAMS.get("rule_hot_interactions_recent_min", "3"))
        hot_part_min = int(PARAMS.get("rule_hot_participations_min", "1"))
        hot_partiel = PARAMS.get("rule_hot_payment_partial_counts_as_hot", "1") in ("1", "true", "True")

        today = date.today()
        recent_cut_ts = pd.Timestamp(today - timedelta(days=lookback))

        # ---------------- Interactions ----------------
        if not dfi.empty:
            dfi = dfi.copy()
            # â¬…ï¸ Convertir proprement en datetime64[ns]
            dfi["_d"] = pd.to_datetime(dfi["Date"], errors="coerce")
            # Compteurs
            inter_count = dfi.groupby("ID")["ID_Interaction"].count()
            last_contact = dfi.groupby("ID")["_d"].max()
            recent_inter = (
                dfi.loc[dfi["_d"] >= recent_cut_ts]
                   .groupby("ID")["ID_Interaction"].count()
            )
            # Responsable le plus actif
            tmp = dfi.groupby(["ID", "Responsable"])["ID_Interaction"].count().reset_index()
            if not tmp.empty:
                idx = tmp.groupby("ID")["ID_Interaction"].idxmax()
                resp_max = tmp.loc[idx].set_index("ID")["Responsable"]
            else:
                resp_max = pd.Series(dtype=str)
        else:
            inter_count = pd.Series(dtype=int)
            last_contact = pd.Series(dtype="datetime64[ns]")
            recent_inter = pd.Series(dtype=int)
            resp_max = pd.Series(dtype=str)

        # ---------------- Participations ----------------
        if not dfp.empty:
            parts_count = dfp.groupby("ID")["ID_Participation"].count()
            has_anim = (
                dfp.assign(_anim=dfp["RÃ´le"].isin(["Animateur", "InvitÃ©"]))
                   .groupby("ID")["_anim"].any()
            )
        else:
            parts_count = pd.Series(dtype=int)
            has_anim = pd.Series(dtype=bool)

        # ---------------- Paiements ----------------
        if not dfpay.empty:
            pay = dfpay.copy()
            pay["Montant"] = pd.to_numeric(pay["Montant"], errors="coerce").fillna(0.0)
            total_pay = pay.groupby("ID")["Montant"].sum()
            pay_regle = pay[pay["Statut"] == "RÃ©glÃ©"].groupby("ID")["Montant"].sum()
            pay_impaye = pay[pay["Statut"] != "RÃ©glÃ©"].groupby("ID")["Montant"].sum()
            pay_reg_count = pay[pay["Statut"] == "RÃ©glÃ©"].groupby("ID")["Montant"].count()
        else:
            total_pay = pd.Series(dtype=float)
            pay_regle = pd.Series(dtype=float)
            pay_impaye = pd.Series(dtype=float)
            pay_reg_count = pd.Series(dtype=int)

        # ---------------- Certifications ----------------
        if not dfcert.empty:
            has_cert = dfcert[dfcert["RÃ©sultat"] == "RÃ©ussi"].groupby("ID")["ID_Certif"].count() > 0
        else:
            has_cert = pd.Series(dtype=bool)

        # ---------------- Assemblage ----------------
        ag = pd.DataFrame(index=contacts["ID"])
        ag["Interactions"] = ag.index.map(inter_count).fillna(0).astype(int)
        ag["Interactions_recent"] = ag.index.map(recent_inter).fillna(0).astype(int)

        # Dernier contact en date â†’ date pure
        # (1) map via Series to ensure we get a pandas Series, not an Index/ndarray
        lc = ag.index.to_series().map(last_contact)

        # (2) force to datetime, coerce bad values to NaT
        lc = pd.to_datetime(lc, errors="coerce")

        # (3) safely extract the date
        ag["Dernier_contact"] = lc.dt.date
            
        ag["Resp_principal"] = ag.index.map(resp_max).fillna("")
        ag["Participations"] = ag.index.map(parts_count).fillna(0).astype(int)
        ag["A_animÃ©_ou_invitÃ©"] = ag.index.map(has_anim).fillna(False)
        ag["CA_total"] = ag.index.map(total_pay).fillna(0.0)
        ag["CA_rÃ©glÃ©"] = ag.index.map(pay_regle).fillna(0.0)
        ag["ImpayÃ©"] = ag.index.map(pay_impaye).fillna(0.0)
        ag["Paiements_regles_n"] = ag.index.map(pay_reg_count).fillna(0).astype(int)

        ag["A_certification"] = ag.index.map(has_cert).fillna(False)
        ag["Score_composite"] = (w_int * ag["Interactions"] +
                                 w_part * ag["Participations"] +
                                 w_pay * ag["Paiements_regles_n"]).round(2)

        def make_tags(row):
            tags = []
            if row.name in set(contacts.loc[contacts.get("Top20", False) == True, "ID"]):
                tags.append("Prospect Top-20")
            if row["Participations"] >= 3 and row["CA_rÃ©glÃ©"] <= 0:
                tags.append("RÃ©gulier-non-converti")
            if row["A_animÃ©_ou_invitÃ©"] or row["Participations"] >= 4:
                tags.append("Futur formateur")
            if row["A_certification"]:
                tags.append("Ambassadeur (certifiÃ©)")
            if row["CA_rÃ©glÃ©"] >= vip_thr:
                tags.append("VIP (CA Ã©levÃ©)")
            return ", ".join(tags)

        ag["Tags"] = ag.apply(make_tags, axis=1)

        def proba(row):
            if row.name in set(contacts[contacts.get("Type", "") == "Membre"]["ID"]):
                return "Converti"
            chaud = (row["Interactions_recent"] >= hot_int_min and row["Participations"] >= hot_part_min)
            if hot_partiel and row["ImpayÃ©"] > 0 and row["CA_rÃ©glÃ©"] == 0:
                chaud = True
            tiede = (row["Interactions_recent"] >= 1 or row["Participations"] >= 1)
            if chaud:
                return "Chaud"
            if tiede:
                return "TiÃ¨de"
            return "Froid"

        ag["Proba_conversion"] = ag.apply(proba, axis=1)
        return ag.reset_index(names="ID")


    # ---------- Finance Ã©vÃ©nements (identique) ----------
    def event_financials(dfe2, dfpay2):
        rec_by_evt = pd.Series(dtype=float)
        if not dfpay2.empty:
            r = dfpay2[dfpay2["Statut"]=="RÃ©glÃ©"].copy()
            r["Montant"] = pd.to_numeric(r["Montant"], errors='coerce').fillna(0)
            rec_by_evt = r.groupby("ID_Ã‰vÃ©nement")["Montant"].sum()
        ev = dfe2 if not dfe2.empty else df_events.copy()
        if ev.empty:
            return pd.DataFrame(columns=["ID_Ã‰vÃ©nement", "Nom_Ã‰vÃ©nement", "Type", "Date", "CoÃ»t_Total", "Recette", "BÃ©nÃ©fice"])
        for c in ["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total"]:
            ev[c] = pd.to_numeric(ev[c], errors='coerce').fillna(0)
        ev["Cout_Total"] = ev["Cout_Total"].where(ev["Cout_Total"]>0, ev[["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres"]].sum(axis=1))
        ev = ev.set_index("ID_Ã‰vÃ©nement")
        rep = pd.DataFrame({
            "Nom_Ã‰vÃ©nement": ev["Nom_Ã‰vÃ©nement"],
            "Type": ev["Type"],
            "Date": ev["Date"],
            "CoÃ»t_Total": ev["Cout_Total"]
        })
        rep["Recette"] = rec_by_evt.reindex(rep.index, fill_value=0)
        rep["BÃ©nÃ©fice"] = rep["Recette"] - rep["CoÃ»t_Total"]
        return rep.reset_index()

    # === Filtrages pÃ©riode ===
    dfe2, dfp2, dfpay2, dfcert2 = filtered_tables_for_period(annee, mois)
    dfc2 = filtered_contacts_for_period(annee, mois, df_events, df_inter, df_parts, df_pay)

    # === KPI de base (sur pÃ©riode) === 
    total_contacts = len(dfc2)

    prospects_actifs = len(dfc2[(dfc2.get("Type","")=="Prospect") & (dfc2.get("Statut","")=="Actif")])
    membres = len(dfc2[dfc2.get("Type","")=="Membre"])
    events_count = len(dfe2)
    parts_total = len(dfp2)

    ca_regle, impayes = 0.0, 0.0
    if not dfpay2.empty:
        dfpay2["Montant"] = pd.to_numeric(dfpay2["Montant"], errors='coerce').fillna(0)
        ca_regle = float(dfpay2[dfpay2["Statut"]=="RÃ©glÃ©"]["Montant"].sum())
        impayes = float(dfpay2[dfpay2["Statut"]!="RÃ©glÃ©"]["Montant"].sum())

    denom_prospects = max(1, len(dfc2[dfc2.get("Type","")=="Prospect"]))
    taux_conv = (membres / denom_prospects) * 100

    # --- Interactions filtrÃ©es pour la pÃ©riode (pour KPI Engagement) ---
    if not df_inter.empty:
        di = _safe_parse_series(df_inter["Date"])
        mask_i = _build_mask_from_dates(di, annee, mois)
        dfi2 = df_inter[mask_i].copy()
    else:
        dfi2 = df_inter.copy()

    # --- KPI Engagement (au moins 1 interaction OU 1 participation dans la pÃ©riode) ---
    ids_contacts_periode = set(dfc2.get("ID", pd.Series([], dtype=str)).astype(str))
    ids_inter = set(dfi2.get("ID", pd.Series([], dtype=str)).astype(str)) if not dfi2.empty else set()
    ids_parts = set(dfp2.get("ID", pd.Series([], dtype=str)).astype(str)) if not dfp2.empty else set()
    ids_engaged = (ids_inter | ids_parts) & ids_contacts_periode
    engagement_n = len(ids_engaged)
    engagement_rate = (engagement_n / max(1, len(ids_contacts_periode))) * 100

    # --- Dictionnaire KPI (inclut alias 'taux_conversion') ---
    kpis = {
        "contacts_total":        ("ðŸ‘¥ Contacts (crÃ©Ã©s / pÃ©riode)", total_contacts),
        "prospects_actifs":      ("ðŸ§² Prospects actifs (pÃ©riode)", prospects_actifs),
        "membres":               ("ðŸ† Membres (pÃ©riode)", membres),
        "events_count":          ("ðŸ“… Ã‰vÃ©nements (pÃ©riode)", events_count),
        "participations_total":  ("ðŸŽŸ Participations (pÃ©riode)", parts_total),
        "ca_regle":              ("ðŸ’° CA rÃ©glÃ© (pÃ©riode)", f"{int(ca_regle):,} FCFA".replace(",", " ")),
        "impayes":               ("âŒ ImpayÃ©s (pÃ©riode)", f"{int(impayes):,} FCFA".replace(",", " ")),
        "taux_conv":             ("ðŸ”„ Taux conversion (pÃ©riode)", f"{taux_conv:.1f}%"),
        # Nouveau KPI Engagement
        "engagement":            ("ðŸ™Œ Engagement (pÃ©riode)", f"{engagement_rate:.1f}%"),
    }

    # Alias pour compatibilitÃ© avec Admin ("taux_conversion")
    aliases = {
        "taux_conversion": "taux_conv",
    }

    # Liste des KPI activÃ©s (depuis PARAMS), en appliquant les alias
    enabled_raw = [x.strip() for x in PARAMS.get("kpi_enabled","").split(",") if x.strip()]
    enabled_keys = []
    for k in (enabled_raw or list(kpis.keys())):
        enabled_keys.append(aliases.get(k, k))  # remap si alias, sinon identique

    # Ne garder que ceux rÃ©ellement disponibles
    enabled = [k for k in enabled_keys if k in kpis]

    # --- Affichage sur 2 lignes (4 colonnes max par ligne) ---
    ncols = 4
    rows = [enabled[i:i+ncols] for i in range(0, len(enabled), ncols)]
    for row in rows:
        cols = st.columns(len(row))
        for col, k in zip(cols, row):
            label, value = kpis[k]
            col.metric(label, value)    
            
    # --- Finance Ã©vÃ©nementielle (pÃ©riode) ---
    ev_fin = event_financials(dfe2, dfpay2)

    # --- Graphe CA/CoÃ»t/BÃ©nÃ©fice par Ã©vÃ©nement ---
    if alt and not ev_fin.empty:
        chart1 = alt.Chart(
            ev_fin.melt(id_vars=["Nom_Ã‰vÃ©nement"], value_vars=["Recette","CoÃ»t_Total","BÃ©nÃ©fice"])
        ).mark_bar().encode(
            x=alt.X("Nom_Ã‰vÃ©nement:N", sort="-y", title="Ã‰vÃ©nement"),
            y=alt.Y('value:Q', title='Montant (FCFA)'),
            color=alt.Color('variable:N', title='Indicateur'),
            tooltip=['Nom_Ã‰vÃ©nement', 'variable', 'value']
        ).properties(height=300, title='CA vs CoÃ»t vs BÃ©nÃ©fice (pÃ©riode)')
        st.altair_chart(chart1, use_container_width=True)

    # --- Participants par mois (via date d'Ã©vÃ©nement liÃ©e) ---
    if not dfp2.empty and "_d_evt" in dfp2.columns:
        _m = pd.to_datetime(dfp2["_d_evt"], errors="coerce")
        dfp2["_mois"] = _m.dt.to_period("M").astype(str)
        agg = dfp2.dropna(subset=["_mois"]).groupby("_mois")["ID_Participation"].count().reset_index()
        if alt and not agg.empty:
            chart2 = alt.Chart(agg).mark_line(point=True).encode(
                x=alt.X('_mois:N', title='Mois'),
                y=alt.Y('ID_Participation:Q', title='Participations')
            ).properties(height=250, title="Participants par mois (pÃ©riode)")
            st.altair_chart(chart2, use_container_width=True)

    # --- Satisfaction moyenne par type dâ€™Ã©vÃ©nement (pÃ©riode) ---
    if not dfp2.empty and not df_events.empty:
        type_map = df_events.set_index('ID_Ã‰vÃ©nement')["Type"]
        dfp2 = dfp2.copy()
        dfp2["Type"] = dfp2["ID_Ã‰vÃ©nement"].map(type_map)
        if "Note" in dfp2.columns:
            dfp2["Note"] = pd.to_numeric(dfp2["Note"], errors='coerce')
        agg_satis = dfp2.dropna(subset=["Type","Note"]).groupby('Type')["Note"].mean().reset_index()
        if alt and not agg_satis.empty:
            chart3 = alt.Chart(agg_satis).mark_bar().encode(
                x=alt.X('Type:N', title="Type d'Ã©vÃ©nement"),
                y=alt.Y('Note:Q', title="Note moyenne"),
                tooltip=['Type', 'Note']
            ).properties(height=250, title="Satisfaction par type (pÃ©riode)")
            st.altair_chart(chart3, use_container_width=True)

    # --- Objectifs vs RÃ©el (libellÃ©s + pÃ©riode) ---
    st.header("ðŸŽ¯ Objectifs vs RÃ©el (pÃ©riode)")
    def get_target(k):
        try:
            return float(PARAMS.get(k, "0"))
        except:
            return 0.0
    y = datetime.now().year
    df_targets = pd.DataFrame([
        ("Contacts crÃ©Ã©s",                get_target(f'kpi_target_contacts_total_year_{y}'), total_contacts),
        ("Participations enregistrÃ©es",   get_target(f'kpi_target_participations_total_year_{y}'), parts_total),
        ("CA rÃ©glÃ© (FCFA)",               get_target(f'kpi_target_ca_regle_year_{y}'), ca_regle),
    ], columns=['Indicateur','Objectif','RÃ©el'])
    df_targets['Ã‰cart'] = df_targets['RÃ©el'] - df_targets['Objectif']
    st.dataframe(df_targets, use_container_width=True)

    # --- Export Excel du rapport de base (pÃ©riode) ---
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        dfc2.to_excel(writer, sheet_name="Contacts(pÃ©riode)", index=False)
        dfe2.to_excel(writer, sheet_name="Ã‰vÃ©nements(pÃ©riode)", index=False)
        dfp2.to_excel(writer, sheet_name="Participations(pÃ©riode)", index=False)
        dfpay2.to_excel(writer, sheet_name="Paiements(pÃ©riode)", index=False)
        dfcert2.to_excel(writer, sheet_name="Certifications(pÃ©riode)", index=False)
        ev_fin.to_excel(writer, sheet_name="Finance(pÃ©riode)", index=False)
    st.download_button("â¬‡ Export Rapport Excel (pÃ©riode)", buf.getvalue(), "rapport_iiba_cameroon_periode.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    st.markdown("---")
    st.header("ðŸ“Š Rapports AvancÃ©s & Analyse StratÃ©gique (pÃ©riode)")

    # === DonnÃ©es enrichies (pÃ©riode) ===
    # Construire aggrÃ©gats sur la pÃ©riode uniquement pour les IDs prÃ©sents dans dfc2
    ids_period = pd.Index([])
    if not dfc2.empty and "ID" in dfc2.columns:
        ids_period = dfc2["ID"].astype(str).str.strip()

    sub_inter = df_inter[df_inter.get("ID", "").astype(str).isin(ids_period)] if not df_inter.empty else df_inter
    sub_parts = df_parts[df_parts.get("ID", "").astype(str).isin(ids_period)] if not df_parts.empty else df_parts
    sub_pay  = df_pay [df_pay .get("ID", "").astype(str).isin(ids_period)] if not df_pay.empty  else df_pay
    sub_cert = df_cert[df_cert.get("ID","").astype(str).isin(ids_period)]   if not df_cert.empty else df_cert

    ag_period = aggregates_for_contacts_period(
        dfc2.copy(),  # contacts (pÃ©riode)
        sub_inter.copy(),
        sub_parts.copy(),
        sub_pay.copy(),
        sub_cert.copy()
    )

    # --- Normalisation des clÃ©s de jointure "ID" ---
    def _normalize_id_col(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if "ID" not in df.columns:
            df["ID"] = ""
        # .astype(str) avant .fillna, puis strip
        df["ID"] = df["ID"].astype(str).str.strip()
        # Quelques "nan" littÃ©raux peuvent rester aprÃ¨s astype(str)
        df["ID"] = df["ID"].replace({"nan": "", "None": "", "NaT": ""})
        return df

    dfc2 = _normalize_id_col(dfc2)
    ag_period = _normalize_id_col(ag_period)

    # Sâ€™assurer quâ€™on nâ€™a quâ€™une ligne par ID cÃ´tÃ© aggrÃ©gats
    if not ag_period.empty:
        ag_period = ag_period.drop_duplicates(subset=["ID"])

    # Si ag_period est vide, garantir au moins la colonne "ID" pour Ã©viter le ValueError
    if ag_period.empty and "ID" not in ag_period.columns:
        ag_period = pd.DataFrame({"ID": []})

    # --- Jointure sÃ»re ---
    dfc_enriched = dfc2.merge(ag_period, on="ID", how="left", validate="one_to_one")
    # Normaliser le type au besoin
    if "Score_Engagement" in dfc_enriched.columns:
        dfc_enriched["Score_Engagement"] = pd.to_numeric(dfc_enriched["Score_Engagement"], errors="coerce").fillna(0)

    total_ba = len(dfc_enriched)
    certifies = len(dfc_enriched[dfc_enriched.get("A_certification", False) == True])
    taux_certif = (certifies / total_ba * 100) if total_ba > 0 else 0
    secteur_counts = dfc_enriched["Secteur"].value_counts(dropna=True)
    top_secteurs = secteur_counts.head(4)
    diversite_sectorielle = int(secteur_counts.shape[0])

    def estimate_salary(row):
        base_salary = {
            "Banque": 800000, "TÃ©lÃ©com": 750000, "IT": 700000,
            "Ã‰ducation": 500000, "SantÃ©": 600000, "ONG": 450000,
            "Industrie": 650000, "Public": 550000, "Autre": 500000
        }
        multiplier = 1.3 if row.get("A_certification", False) else 1.0
        return base_salary.get(row.get("Secteur","Autre"), 500000) * multiplier
    if total_ba > 0:
        dfc_enriched["Salaire_Estime"] = dfc_enriched.apply(estimate_salary, axis=1)
        salaire_moyen = int(dfc_enriched["Salaire_Estime"].mean())
    else:
        salaire_moyen = 0

    taux_participation = float(dfc_enriched.get("Participations", pd.Series(dtype=float)).mean() or 0)
    ca_total = float(dfc_enriched.get("CA_rÃ©glÃ©", pd.Series(dtype=float)).sum() or 0)
    prospects_chauds = len(dfc_enriched[dfc_enriched.get("Proba_conversion","") == "Chaud"])

    # Onglets avancÃ©s
    tab_exec, tab_profil, tab_swot, tab_bsc = st.tabs([
        "ðŸŽ¯ Executive Summary",
        "ðŸ‘¤ Profil BA Camerounais",
        "âš–ï¸ SWOT Analysis",
        "ðŸ“ˆ Balanced Scorecard"
    ])

    with tab_exec:
        st.subheader("ðŸ“‹ SynthÃ¨se ExÃ©cutive â€” pÃ©riode")
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("ðŸ‘¥ Total BA", total_ba)
        c2.metric("ðŸŽ“ CertifiÃ©s", f"{taux_certif:.1f}%")
        c3.metric("ðŸ’° Salaire Moyen", f"{salaire_moyen:,} FCFA")
        c4.metric("ðŸ¢ Secteurs", diversite_sectorielle)

        st.subheader("ðŸ† Top Ã‰vÃ©nements (bÃ©nÃ©fice)")
        ev_fin_period = event_financials(dfe2, dfpay2)
        if not ev_fin_period.empty:
            top_events = ev_fin_period.nlargest(5, "BÃ©nÃ©fice")[["Nom_Ã‰vÃ©nement", "Recette", "CoÃ»t_Total", "BÃ©nÃ©fice"]]
            st.dataframe(top_events, use_container_width=True)
        else:
            st.info("Pas de donnÃ©es financiÃ¨res d'Ã©vÃ©nements sur la pÃ©riode.")

        st.subheader("ðŸŽ¯ Segmentation (pÃ©riode)")
        segments = dfc_enriched["Proba_conversion"].value_counts()
        col_s1, col_s2 = st.columns(2)
        with col_s1:
            if total_ba > 0 and not segments.empty:
                for segment, count in segments.items():
                    pct = (count / total_ba * 100)
                    st.write(f"â€¢ {segment}: {count} ({pct:.1f}%)")
            else:
                st.write("Aucune donnÃ©e de segmentation.")
        with col_s2:
            if alt and not segments.empty:
                chart_data = pd.DataFrame({'Segment': segments.index, 'Count': segments.values})
                pie_chart = alt.Chart(chart_data).mark_arc().encode(
                    theta=alt.Theta(field="Count", type="quantitative"),
                    color=alt.Color(field="Segment", type="nominal"),
                    tooltip=['Segment', 'Count']
                ).properties(width=220, height=220)
                st.altair_chart(pie_chart, use_container_width=True)

    with tab_profil:
        st.subheader("ðŸ‘¤ Profil Type â€” pÃ©riode")
        col_demo1, col_demo2 = st.columns(2)
        with col_demo1:
            st.write("**ðŸ“Š RÃ©partition par Genre**")
            genre_counts = dfc_enriched["Genre"].value_counts()
            if total_ba > 0 and not genre_counts.empty:
                for genre, count in genre_counts.items():
                    pct = (count / total_ba * 100)
                    st.write(f"â€¢ {genre}: {count} ({pct:.1f}%)")
            else:
                st.write("Aucune donnÃ©e de genre.")

            st.write("**ðŸ™ï¸ Top Villes**")
            ville_counts = dfc_enriched["Ville"].value_counts().head(5)
            if total_ba > 0 and not ville_counts.empty:
                for ville, count in ville_counts.items():
                    pct = (count / total_ba * 100)
                    st.write(f"â€¢ {ville}: {count} ({pct:.1f}%)")
            else:
                st.write("Aucune donnÃ©e de ville.")

        with col_demo2:
            st.write("**ðŸ¢ Secteurs dominants**")
            if total_ba > 0 and not top_secteurs.empty:
                for secteur, count in top_secteurs.items():
                    pct = (count / total_ba * 100)
                    st.write(f"â€¢ {secteur}: {count} ({pct:.1f}%)")
            else:
                st.write("Aucune donnÃ©e de secteur.")

            st.write("**ðŸ’¼ Types de profils**")
            type_counts = dfc_enriched["Type"].value_counts()
            if total_ba > 0 and not type_counts.empty:
                for typ, count in type_counts.items():
                    pct = (count / total_ba * 100)
                    st.write(f"â€¢ {typ}: {count} ({pct:.1f}%)")
            else:
                st.write("Aucune donnÃ©e de type de profil.")

        st.subheader("ðŸ“ˆ Engagement par Secteur (pÃ©riode)")
        if not dfc_enriched.empty:
            engagement_secteur = dfc_enriched.groupby("Secteur").agg({
                "Score_composite": "mean",
                "Participations": "mean",
                "CA_rÃ©glÃ©": "sum"
            }).round(2)
            engagement_secteur.columns = ["Score Moyen", "Participations Moy", "CA Total"]
            st.dataframe(engagement_secteur, use_container_width=True)
        else:
            engagement_secteur = pd.DataFrame()

        st.subheader("ðŸŒ Comparaison Standards Internationaux (pÃ©riode)")
        ba_experience_ratio = (len(dfc_enriched[dfc_enriched.get("Score_Engagement", 0) >= 50]) / total_ba * 100) if total_ba > 0 else 0
        formation_continue = (len(dfc_enriched[dfc_enriched.get("Participations", 0) >= 2]) / total_ba * 100) if total_ba > 0 else 0
        kpi_standards = pd.DataFrame({
            "KPI": [
                "Taux de certification",
                "Formation continue",
                "ExpÃ©rience mÃ©tier",
                "DiversitÃ© sectorielle",
                "Engagement communautaire"
            ],
            "Cameroun": [f"{taux_certif:.1f}%", f"{formation_continue:.1f}%", f"{ba_experience_ratio:.1f}%",
                        f"{diversite_sectorielle} secteurs", f"{dfc_enriched.get('Participations', pd.Series(dtype=float)).mean():.1f} events/BA"],
            "Standard IIBA": ["25-35%", "60-70%", "70-80%", "8-10 secteurs", "2-3 events/an"]
        })
        st.dataframe(kpi_standards, use_container_width=True)

    with tab_swot:
        st.subheader("âš–ï¸ Analyse SWOT â€” pÃ©riode")
        col_sw, col_ot = st.columns(2)

        with col_sw:
            st.markdown("### ðŸ’ª **FORCES**")
            st.write(f"""
            â€¢ **DiversitÃ© sectorielle**: {diversite_sectorielle} secteurs reprÃ©sentÃ©s  
            â€¢ **Engagement communautaire**: {taux_participation:.1f} participations moy./BA  
            â€¢ **Base financiÃ¨re**: {ca_total:,.0f} FCFA de revenus  
            â€¢ **Pipeline prospects**: {prospects_chauds} prospects chauds  
            â€¢ **Croissance digitale**: Adoption d'outils en ligne  
            """)

            st.markdown("### âš ï¸ **FAIBLESSES**")
            st.write(f"""
            â€¢ **Taux de certification**: {taux_certif:.1f}% (vs 30% standard)  
            â€¢ **Concentration gÃ©ographique**: Focus Douala/YaoundÃ©  
            â€¢ **Formations avancÃ©es limitÃ©es**  
            â€¢ **Standardisation des pratiques Ã  renforcer**  
            â€¢ **VisibilitÃ© internationale faible**  
            """)

        with col_ot:
            st.markdown("### ðŸš€ **OPPORTUNITÃ‰S**")
            st.write("""
            â€¢ Transformation digitale : demande croissante BA  
            â€¢ Partenariats entreprises : Top-20 identifiÃ©es  
            â€¢ Certification IIBA : programme de dÃ©veloppement  
            â€¢ Expansion rÃ©gionale : Afrique Centrale  
            â€¢ Formations spÃ©cialisÃ©es : IA, Data, Agile  
            """)

            st.markdown("### â›” **MENACES**")
            st.write("""
            â€¢ Concurrence de consultants internationaux  
            â€¢ Fuite des cerveaux vers l'Ã©tranger  
            â€¢ Ã‰conomie incertaine (budgets formation)  
            â€¢ Manque de reconnaissance du mÃ©tier BA  
            â€¢ Technologie Ã©voluant rapidement  
            """)

        st.subheader("ðŸŽ¯ Plan d'Actions StratÃ©giques")
        actions_df = pd.DataFrame({
            "Axe": ["Formation", "Certification", "Partenariats", "Expansion", "Communication"],
            "Action": [
                "DÃ©velopper programme formation continue",
                "Accompagner vers certifications IIBA",
                "Formaliser accords entreprises Top-20",
                "Ouvrir antennes rÃ©gionales",
                "Renforcer visibilitÃ© et marketing"
            ],
            "PrioritÃ©": ["Ã‰levÃ©e", "Ã‰levÃ©e", "Moyenne", "Faible", "Moyenne"],
            "Ã‰chÃ©ance": ["6 mois", "12 mois", "9 mois", "24 mois", "Continu"]
        })
        st.dataframe(actions_df, use_container_width=True)

    with tab_bsc:
        st.subheader("ðŸ“ˆ Balanced Scorecard â€” pÃ©riode")
        tab_fin, tab_client, tab_proc, tab_app = st.tabs(["ðŸ’° FinanciÃ¨re", "ðŸ‘¥ Client", "âš™ï¸ Processus", "ðŸ“š Apprentissage"])

        with tab_fin:
            col_f1, col_f2, col_f3 = st.columns(3)
            ev_fin_period = event_financials(dfe2, dfpay2)
            if not ev_fin_period.empty and ev_fin_period["Recette"].sum() > 0:
                marge_benefice = (ev_fin_period["BÃ©nÃ©fice"].sum() / ev_fin_period["Recette"].sum() * 100)
            else:
                marge_benefice = 0.0
            col_f1.metric("ðŸ’µ CA Total (pÃ©riode)", f"{ca_total:,.0f} FCFA")
            col_f2.metric("ðŸ“ˆ Croissance CA", "â€”", help="Ã€ calculer si historique disponible")
            col_f3.metric("ðŸ“Š Marge BÃ©nÃ©fice", f"{marge_benefice:.1f}%")

            fin_data = pd.DataFrame({
                "Indicateur": ["Revenus formations", "Revenus certifications", "Revenus Ã©vÃ©nements", "CoÃ»ts opÃ©rationnels"],
                "RÃ©el": [f"{ca_total*0.6:.0f}", f"{ca_total*0.2:.0f}", f"{ca_total*0.2:.0f}", f"{ev_fin_period['CoÃ»t_Total'].sum() if not ev_fin_period.empty else 0:.0f}"],
                "Objectif": ["3M", "1M", "1M", "3.5M"],
                "Ã‰cart": ["Ã€ calculer", "Ã€ calculer", "Ã€ calculer", "Ã€ calculer"]
            })
            st.dataframe(fin_data, use_container_width=True)

        with tab_client:
            col_c1, col_c2, col_c3 = st.columns(3)
            satisfaction_moy = float(dfc_enriched[dfc_enriched.get("A_certification", False) == True].get("Score_Engagement", pd.Series(dtype=float)).mean() or 0)
            denom_ret = len(dfc_enriched[dfc_enriched.get("Type","").isin(["Membre", "Prospect"])])
            retention = (len(dfc_enriched[dfc_enriched.get("Type","") == "Membre"]) / denom_ret * 100) if denom_ret > 0 else 0
            col_c1.metric("ðŸ˜Š Satisfaction", f"{satisfaction_moy:.1f}/100", help="Score engagement (certifiÃ©s)")
            col_c2.metric("ðŸ”„ RÃ©tention", f"{retention:.1f}%")
            col_c3.metric("ðŸ“ˆ NPS EstimÃ©", "65")

            client_data = pd.DataFrame({
                "Segment": ["Prospects Chauds", "Prospects TiÃ¨des", "Prospects Froids", "Membres Actifs"],
                "Nombre": [
                    len(dfc_enriched[dfc_enriched.get("Proba_conversion","") == "Chaud"]),
                    len(dfc_enriched[dfc_enriched.get("Proba_conversion","") == "TiÃ¨de"]),
                    len(dfc_enriched[dfc_enriched.get("Proba_conversion","") == "Froid"]),
                    len(dfc_enriched[dfc_enriched.get("Type","") == "Membre"])
                ],
            })
            client_data["% Total"] = (client_data["Nombre"] / max(1, client_data["Nombre"].sum()) * 100).round(1)
            st.dataframe(client_data, use_container_width=True)

        with tab_proc:
            col_p1, col_p2, col_p3 = st.columns(3)
            denom_prosp = len(dfc_enriched[dfc_enriched.get("Type","") == "Prospect"])
            efficacite_conv = (prospects_chauds / denom_prosp * 100) if denom_prosp > 0 else 0
            temps_reponse = 2.5  # placeholder
            col_p1.metric("âš¡ EfficacitÃ© Conversion", f"{efficacite_conv:.1f}%")
            col_p2.metric("â±ï¸ Temps RÃ©ponse", f"{temps_reponse} jours")
            col_p3.metric("ðŸŽ¯ Taux Participation", f"{taux_participation:.1f}")

            proc_data = pd.DataFrame({
                "Processus": ["Acquisition prospects", "Conversion membres", "DÃ©livrance formations", "Suivi post-formation"],
                "Performance": ["75%", f"{retention:.1f}%", "90%", "60%"],
                "Objectif": ["80%", "25%", "95%", "75%"],
                "Actions": ["AmÃ©liorer ciblage", "Renforcer follow-up", "Optimiser contenu", "SystÃ©matiser enquÃªtes"]
            })
            st.dataframe(proc_data, use_container_width=True)

        with tab_app:
            col_a1, col_a2, col_a3 = st.columns(3)
            col_a1.metric("ðŸŽ“ Taux Certification", f"{taux_certif:.1f}%")
            col_a2.metric("ðŸ“– Formation Continue", f"{(len(dfc_enriched[dfc_enriched.get('Participations',0) >= 2]) / max(1,total_ba) * 100):.1f}%")
            col_a3.metric("ðŸ”„ Innovation", "3 projets", help="Nouveaux programmes/an")

            comp_data = pd.DataFrame({
                "CompÃ©tence": ["Business Analysis", "AgilitÃ©", "Data Analysis", "Digital Transformation", "Leadership"],
                "Niveau Actuel": [65, 45, 35, 40, 55],
                "Objectif 2025": [80, 65, 60, 70, 70],
                "Gap": [15, 20, 25, 30, 15]
            })
            st.dataframe(comp_data, use_container_width=True)

    # --- Export Markdown consolidÃ© (pÃ©riode) ---
    st.markdown("---")
    col_export1, col_export2 = st.columns(2)

    with col_export1:
        if st.button("ðŸ“„ GÃ©nÃ©rer Rapport Markdown Complet (pÃ©riode)"):
            try:
                ev_fin_period = event_financials(dfe2, dfpay2)
                if not ev_fin_period.empty and ev_fin_period["Recette"].sum() > 0:
                    marge_benefice = (ev_fin_period["BÃ©nÃ©fice"].sum() / ev_fin_period["Recette"].sum() * 100)
                else:
                    marge_benefice = 0.0
                genre_counts_md = dfc_enriched["Genre"].value_counts()

                rapport_md = f"""
# Rapport StratÃ©gique IIBA Cameroun â€” {datetime.now().year} (pÃ©riode sÃ©lectionnÃ©e)

## Executive Summary
- **Total BA**: {total_ba}
- **Taux Certification**: {taux_certif:.1f}%
- **CA RÃ©alisÃ© (pÃ©riode)**: {ca_total:,.0f} FCFA
- **Secteurs (pÃ©riode)**: {diversite_sectorielle}

## Profil Type BA Camerounais (pÃ©riode)
- RÃ©partition par genre: {dict(genre_counts_md)}
- Secteurs dominants: {dict(top_secteurs)}

## SWOT (pÃ©riode)
- Forces: diversitÃ© sectorielle, engagement, pipeline, base financiÃ¨re
- OpportunitÃ©s: partenariats Top-20, certif IIBA, expansion rÃ©gionale, IA/Data/Agile
- Menaces: concurrence, fuite des cerveaux, budgets formation, rythme techno

## Balanced Scorecard (pÃ©riode)
- CA: {ca_total:,.0f} FCFA â€” Marge: {marge_benefice:.1f}%
- Satisfaction: {float(dfc_enriched[dfc_enriched.get("A_certification", False) == True].get("Score_Engagement", pd.Series(dtype=float)).mean() or 0):.1f}/100
- RÃ©tention: {((len(dfc_enriched[dfc_enriched.get("Type","") == "Membre"]) / max(1,len(dfc_enriched[dfc_enriched.get("Type","").isin(["Membre","Prospect"])])))*100):.1f}%

_GÃ©nÃ©rÃ© le {datetime.now().strftime('%Y-%m-%d %H:%M')}_"""
                st.download_button(
                    "â¬‡ï¸ TÃ©lÃ©charger Rapport.md",
                    rapport_md,
                    file_name=f"Rapport_IIBA_Cameroun_periode_{datetime.now().strftime('%Y%m%d')}.md",
                    mime="text/markdown"
                )
            except Exception as e:
                st.error(f"Erreur gÃ©nÃ©ration Markdown : {e}")

    with col_export2:
        # Export Excel des analyses avancÃ©es (pÃ©riode)
        buf_adv = io.BytesIO()
        with pd.ExcelWriter(buf_adv, engine="openpyxl") as writer:
            dfc_enriched.to_excel(writer, sheet_name="Contacts_Enrichis(pÃ©riode)", index=False)
            try:
                engagement_secteur.to_excel(writer, sheet_name="Engagement_Secteur", index=False)
            except Exception:
                pd.DataFrame().to_excel(writer, sheet_name="Engagement_Secteur", index=False)
            try:
                kpi_standards.to_excel(writer, sheet_name="KPI_Standards", index=False)
            except Exception:
                pd.DataFrame().to_excel(writer, sheet_name="KPI_Standards", index=False)
            try:
                actions_df.to_excel(writer, sheet_name="Plan_Actions", index=False)
            except Exception:
                pd.DataFrame().to_excel(writer, sheet_name="Plan_Actions", index=False)

        st.download_button(
            "ðŸ“Š Export Analyses Excel (pÃ©riode)",
            buf_adv.getvalue(),
            file_name=f"Analyses_IIBA_periode_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )



# PAGE ADMIN
elif page == "Admin":
    st.title("âš™ï¸ Admin â€” ParamÃ¨tres, Migration & Maintenance")

    # PARAMETRES LISTES DEROULANTES
    st.markdown("### Listes dÃ©roulantes (stockÃ©es dans parametres.csv)")
    with st.form("lists_form"):
        def show_line(name, label):
            raw = PARAMS.get(f"list_{name}", DEFAULT_LISTS.get(name, ""))
            return st.text_input(label, raw)
        
        genres = show_line("genres","Genres (sÃ©parÃ©s par |)")
        types_contact = show_line("types_contact","Types de contact (|)")
        statuts_engagement = show_line("statuts_engagement","Statuts d'engagement (|)")
        secteurs = show_line("secteurs","Secteurs (|)")
        pays = show_line("pays","Pays (|)")
        villes = show_line("villes","Villes (|)")
        sources = show_line("sources","Sources (|)")
        canaux = show_line("canaux","Canaux (|)")
        resultats_inter = show_line("resultats_inter","RÃ©sultats d'interaction (|)")
        types_evenements = show_line("types_evenements","Types d'Ã©vÃ©nements (|)")
        lieux = show_line("lieux","Lieux (|)")
        statuts_paiement = show_line("statuts_paiement","Statuts paiement (|)")
        moyens_paiement = show_line("moyens_paiement","Moyens paiement (|)")
        types_certif = show_line("types_certif","Types certification (|)")
        entreprises_cibles = show_line("entreprises_cibles","Entreprises cibles (Top-20) (|)")
        
        # NOUVEAU: Listes pour entreprises
        st.markdown("#### Listes spÃ©cifiques aux entreprises")
        tailles_entreprise = show_line("tailles_entreprise","Tailles d'entreprise (|)")
        statuts_partenariat = show_line("statuts_partenariat","Statuts de partenariat (|)")
        types_partenariat = show_line("types_partenariat","Types de partenariat (|)")
        responsables_iiba = show_line("responsables_iiba","Responsables IIBA (|)")
        
        ok1 = st.form_submit_button("ðŸ’¾ Enregistrer les listes")
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
            st.success("Listes enregistrÃ©es dans parametres.csv â€” rechargez la page si nÃ©cessaire.")

    # PARAMETRES SCORING ET AFFICHAGE
    st.markdown("### RÃ¨gles de scoring & d'affichage")
    with st.form("rules_form"):
        c1,c2,c3,c4 = st.columns(4)
        vip_thr = c1.number_input("Seuil VIP (FCFA)", min_value=0.0, step=50000.0, value=float(PARAMS.get("vip_threshold","500000")))
        w_int = c2.number_input("Poids Interaction", min_value=0.0, step=0.5, value=float(PARAMS.get("score_w_interaction","1")))
        w_part = c3.number_input("Poids Participation", min_value=0.0, step=0.5, value=float(PARAMS.get("score_w_participation","1")))
        w_pay = c4.number_input("Poids Paiement rÃ©glÃ©", min_value=0.0, step=0.5, value=float(PARAMS.get("score_w_payment_regle","2")))
        c5,c6,c7 = st.columns(3)
        lookback = c5.number_input("FenÃªtre interactions rÃ©centes (jours)", min_value=1, step=1, value=int(PARAMS.get("interactions_lookback_days","90")))
        hot_int_min = c6.number_input("Interactions rÃ©centes min (chaud)", min_value=0, step=1, value=int(PARAMS.get("rule_hot_interactions_recent_min","3")))
        hot_part_min = c7.number_input("Participations min (chaud)", min_value=0, step=1, value=int(PARAMS.get("rule_hot_participations_min","1")))
        hot_partiel = st.checkbox("Paiement partiel = prospect chaud", value=PARAMS.get("rule_hot_payment_partial_counts_as_hot","1") in ("1","true","True"))

        st.write("**Colonnes des grilles (ordre, sÃ©parÃ©es par des virgules)**")
        grid_crm = st.text_input("CRM â†’ Colonnes", PARAMS.get("grid_crm_columns",""))
        grid_events = st.text_input("Ã‰vÃ©nements â†’ Colonnes", PARAMS.get("grid_events_columns",""))
        grid_entreprises = st.text_input("Entreprises â†’ Colonnes", PARAMS.get("grid_entreprises_columns",""))  # NOUVEAU

        # NOUVEAU: ParamÃ¨tres spÃ©cifiques aux entreprises
        st.markdown("#### ParamÃ¨tres de scoring des entreprises")
        c_ent1, c_ent2, c_ent3 = st.columns(3)
        ent_ca_weight = c_ent1.number_input("Poids CA", min_value=0.0, max_value=1.0, step=0.1, 
                                           value=float(PARAMS.get("entreprises_scoring_ca_weight","0.3")))
        ent_emp_weight = c_ent2.number_input("Poids EmployÃ©s", min_value=0.0, max_value=1.0, step=0.1,
                                            value=float(PARAMS.get("entreprises_scoring_employes_weight","0.2")))
        ent_int_weight = c_ent3.number_input("Poids Interactions", min_value=0.0, max_value=1.0, step=0.1,
                                            value=float(PARAMS.get("entreprises_scoring_interactions_weight","0.5")))
        
        c_ent4, c_ent5 = st.columns(2)
        ent_ca_seuil = c_ent4.number_input("Seuil CA gros client (FCFA)", min_value=0, step=1000000,
                                          value=int(float(PARAMS.get("entreprises_ca_seuil_gros","10000000"))))
        ent_emp_seuil = c_ent5.number_input("Seuil employÃ©s gros client", min_value=0, step=50,
                                           value=int(float(PARAMS.get("entreprises_employes_seuil_gros","500"))))

        st.write("**KPI visibles (sÃ©parÃ©s par des virgules)**")
        kpi_enabled = st.text_input("KPI activÃ©s", PARAMS.get("kpi_enabled",""))

        ok2 = st.form_submit_button("ðŸ’¾ Enregistrer les paramÃ¨tres")
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
                # NOUVEAU: ParamÃ¨tres entreprises
                "entreprises_scoring_ca_weight": str(ent_ca_weight),
                "entreprises_scoring_employes_weight": str(ent_emp_weight),
                "entreprises_scoring_interactions_weight": str(ent_int_weight),
                "entreprises_ca_seuil_gros": str(ent_ca_seuil),
                "entreprises_employes_seuil_gros": str(ent_emp_seuil),
            })
            save_params(PARAMS)
            st.success("ParamÃ¨tres enregistrÃ©s.")

    # IMPORT/EXPORT SIMPLIFIÃ‰ EXCEL AVEC ONGLETS
    st.markdown("---")
    st.header("ðŸ“¦ Import/Export Excel Multi-onglets")
    
    # Export avec nouveau onglet entreprises
    st.subheader("ðŸ“¤ Export")
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
        "â¬‡ï¸ Exporter toutes les donnÃ©es Excel",
        buf_export.getvalue(),
        file_name=f"IIBA_export_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # Import avec gestion de l'onglet entreprises
    st.subheader("ðŸ“¥ Import")
    fichier_import = st.file_uploader(
        "Fichier Excel (.xlsx) avec onglets: contacts, interactions, evenements, participations, paiements, certifications, entreprises",
        type=["xlsx"], key="xlsx_import_admin"
    )

    if st.button("ðŸ“¥ Importer Excel") and fichier_import is not None:
        try:
            xls = pd.ExcelFile(fichier_import)
            
            # Mapping onglets -> schÃ©mas
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
                    
                    # Assurer la prÃ©sence de toutes les colonnes
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
                    
                    # GÃ©rer les IDs
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
                        
                        # Mettre Ã  jour les variables globales
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
            
            st.success("Import terminÃ©!")
            st.json(results)
            
        except Exception as e:
            st.error(f"Erreur lors de l'import: {e}")

    # TÃ‰LÃ‰CHARGEMENT ET IMPORT INDIVIDUEL DES CSV
    st.markdown("---")
    st.header("ðŸ“ Gestion individuelle des fichiers CSV")
    
    # TÃ©lÃ©chargement individuel
    st.subheader("ðŸ“¥ TÃ©lÃ©charger les fichiers CSV individuellement")
    
    csv_files = {
        "Contacts": (df_contacts, "contacts.csv"),
        "Interactions": (df_inter, "interactions.csv"),
        "Ã‰vÃ©nements": (df_events, "evenements.csv"),
        "Participations": (df_parts, "participations.csv"),
        "Paiements": (df_pay, "paiements.csv"),
        "Certifications": (df_cert, "certifications.csv"),
        "Entreprises": (df_entreprises, "entreprises.csv"),  # NOUVEAU
        "ParamÃ¨tres": (pd.read_csv(PATHS["params"]) if PATHS["params"].exists() else pd.DataFrame(), "parametres.csv")
    }
    
    cols_download = st.columns(4)
    for i, (name, (df, filename)) in enumerate(csv_files.items()):
        col = cols_download[i % 4]
        csv_data = df.to_csv(index=False, encoding="utf-8")
        col.download_button(
            f"â¬‡ï¸ {name}",
            csv_data,
            filename,
            mime="text/csv",
            key=f"dl_{filename}"
        )
    
    # Import individuel
    st.subheader("ðŸ“¤ Importer/Remplacer un fichier CSV individuel")
    
    csv_type = st.selectbox("Type de fichier Ã  importer", [
        "contacts", "interactions", "evenements", "participations", 
        "paiements", "certifications", "entreprises", "parametres"  # NOUVEAU
    ])
    
    uploaded_csv = st.file_uploader(f"Fichier CSV pour {csv_type}", type=["csv"], key=f"upload_{csv_type}")
    
    replace_mode = st.radio("Mode d'import", ["Ajouter aux donnÃ©es existantes", "Remplacer complÃ¨tement"], key="csv_import_mode")
    
    if st.button("ðŸ“¥ Importer ce CSV") and uploaded_csv is not None:
        try:
            df_uploaded = pd.read_csv(uploaded_csv, dtype=str, encoding="utf-8").fillna("")
            
            if csv_type == "parametres":
                # Cas spÃ©cial pour les paramÃ¨tres
                if "key" in df_uploaded.columns and "value" in df_uploaded.columns:
                    df_uploaded.to_csv(PATHS["params"], index=False, encoding="utf-8")
                    st.success("Fichier paramÃ¨tres importÃ© avec succÃ¨s!")
                    st.info("Rechargez la page pour voir les changements.")
                else:
                    st.error("Le fichier paramÃ¨tres doit contenir les colonnes 'key' et 'value'.")
            else:
                # Import normal des donnÃ©es
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
                    
                    # Assurer la prÃ©sence de toutes les colonnes
                    for c in cols:
                        if c not in df_uploaded.columns:
                            df_uploaded[c] = ""
                    df_uploaded = df_uploaded[cols]
                    
                    if replace_mode == "Remplacer complÃ¨tement":
                        # Remplacer complÃ¨tement
                        save_df(df_uploaded, PATHS[path_key])
                        global_var = f"df_{path_key}"
                        globals()[global_var] = df_uploaded
                        st.success(f"DonnÃ©es {csv_type} remplacÃ©es complÃ¨tement! ({len(df_uploaded)} lignes)")
                    else:
                        # Ajouter aux donnÃ©es existantes
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
                            st.success(f"AjoutÃ© {len(new_rows)} nouvelles lignes Ã  {csv_type}!")
                        else:
                            st.info("Aucune nouvelle ligne Ã  ajouter.")
                else:
                    st.error(f"Type de fichier non reconnu: {csv_type}")
                
        except Exception as e:
            st.error(f"Erreur lors de l'import: {e}")

    # MAINTENANCE (simplifiÃ©)
    st.markdown("---")
    st.header("ðŸ”§ Maintenance")
    
    col_reset, col_info = st.columns(2)
    
    with col_reset:
        st.subheader("ðŸ—‘ï¸ RÃ©initialisation")
        if st.button("âš ï¸ RESET COMPLET (tous les fichiers)", type="secondary"):
            try:
                for path in PATHS.values():
                    if path.exists():
                        path.unlink()
                st.success("âœ… Tous les fichiers supprimÃ©s! Rechargez la page.")
            except Exception as e:
                st.error(f"Erreur: {e}")
    
    with col_info:
        st.subheader("ðŸ“Š Informations")
        total_size = sum(path.stat().st_size if path.exists() else 0 for path in PATHS.values())
        st.metric("ðŸ’¾ Taille totale des donnÃ©es", f"{total_size/1024:.1f} KB")
        
        file_counts = {
            "Contacts": len(df_contacts),
            "Interactions": len(df_inter),
            "Ã‰vÃ©nements": len(df_events),
            "Participations": len(df_parts),
            "Paiements": len(df_pay),
            "Certifications": len(df_cert),
            "Entreprises": len(df_entreprises),  # NOUVEAU
        }
        
        for name, count in file_counts.items():
            st.write(f"â€¢ {name}: {count} lignes")

    # GESTION DES UTILISATEURS (simplifiÃ©)
    st.markdown("---")
    st.header("ðŸ‘¤ Gestion des utilisateurs")
    
    current_user = st.session_state.get("user", {})
    if current_user.get("Role") != "admin":
        st.warning("AccÃ¨s rÃ©servÃ© aux administrateurs.")
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
            
            tab_create, tab_list = st.tabs(["âž• CrÃ©er utilisateur", "ðŸ“‹ Liste"])
            
            with tab_create:
                with st.form("create_user_simple"):
                    col1, col2 = st.columns(2)
                    new_user_id = col1.text_input("Email/Login", placeholder="prenom.nom@iiba.cm")
                    full_name = col2.text_input("Nom complet", placeholder="PrÃ©nom NOM")
                    role = st.selectbox("RÃ´le", ["admin","standard"], index=1)
                    pw_plain = st.text_input("Mot de passe", type="password")
                    
                    if st.form_submit_button("CrÃ©er"):
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
                                st.success(f"Utilisateur '{new_user_id}' crÃ©Ã©.")
                                _safe_rerun()
                            else:
                                st.error("Cet email existe dÃ©jÃ .")
                        else:
                            st.error("Tous les champs sont requis (mot de passe >= 6 caractÃ¨res).")
            
            with tab_list:
                if not df_users.empty:
                    show_users = df_users[["user_id","full_name","role","active"]].copy()
                    show_users["active"] = show_users["active"].map(lambda x: "âœ…" if x else "âŒ")
                    st.dataframe(show_users, use_container_width=True)
                else:
                    st.info("Aucun utilisateur.")
                    
        except ImportError:
            st.error("Module 'bcrypt' requis. Installez avec: pip install bcrypt")
