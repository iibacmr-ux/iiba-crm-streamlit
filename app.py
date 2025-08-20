# -*- coding: utf-8 -*-
"""
IIBA Cameroun — CRM Streamlit (monofichier)
Extension :
- Colonne **Score_composite** (Interactions + Participations + 2*PaiementsRéglés)
- **Tags** automatiques : "Futur formateur", "Régulier-non-converti", "Prospect Top-20", "Ambassadeur (certifié)", "VIP (CA élevé)"
- **Probabilité de conversion** : Froid / Tiède / Chaud selon règles métiers
Le reste des fonctionnalités (CRM, Événements CRUD + duplication, Rapports Altair, Admin/Migration/Reset/Purge) est conservé.
"""

from datetime import datetime, date, timedelta
from pathlib import Path
import io, json, re

import numpy as np
import pandas as pd
import streamlit as st

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
    HAS_AGGRID = True
except Exception:
    HAS_AGGRID = False

try:
    import altair as alt
except Exception:
    alt = None

st.set_page_config(page_title="IIBA Cameroun — CRM", page_icon="📊", layout="wide")

# ---------------------- FICHIERS & SCHÉMAS ----------------------
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "inter": DATA_DIR / "interactions.csv",
    "events": DATA_DIR / "evenements.csv",
    "parts": DATA_DIR / "participations.csv",
    "pay": DATA_DIR / "paiements.csv",
    "cert": DATA_DIR / "certifications.csv",
    "settings": DATA_DIR / "settings.json",
    "logs": DATA_DIR / "migration_logs.jsonl",
}

C_COLS = ["ID","Nom","Prénom","Genre","Titre","Société","Secteur","Email","Téléphone","LinkedIn",
          "Ville","Pays","Type","Source","Statut","Score_Engagement","Date_Creation","Notes","Top20"]
I_COLS = ["ID_Interaction","ID","Date","Canal","Objet","Résumé","Résultat","Prochaine_Action","Relance","Responsable"]
E_COLS = ["ID_Événement","Nom_Événement","Type","Date","Durée_h","Lieu","Formateur","Objectif","Periode",
          "Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total","Notes"]
P_COLS = ["ID_Participation","ID","ID_Événement","Rôle","Inscription","Arrivée","Temps_Present","Feedback","Note","Commentaire"]
PAY_COLS = ["ID_Paiement","ID","ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence","Notes","Relance"]
CERT_COLS = ["ID_Certif","ID","Type_Certif","Date_Examen","Résultat","Score","Date_Obtention","Validité","Renouvellement","Notes"]

ALL_SCHEMAS = {
    "contacts": C_COLS, "interactions": I_COLS, "evenements": E_COLS,
    "participations": P_COLS, "paiements": PAY_COLS, "certifications": CERT_COLS,
}
TABLE_ID_COL = {"contacts":"ID","interactions":"ID_Interaction","evenements":"ID_Événement",
                "participations":"ID_Participation","paiements":"ID_Paiement","certifications":"ID_Certif"}

DEFAULT = {
    "genres":["Homme","Femme","Autre"],
    "secteurs":["Banque","Télécom","IT","Éducation","Santé","ONG","Industrie","Public","Autre"],
    "types_contact":["Membre","Prospect","Formateur","Partenaire"],
    "sources":["Afterwork","Formation","LinkedIn","Recommandation","Site Web","Salon","Autre"],
    "statuts_engagement":["Actif","Inactif","À relancer"],
    "canaux":["Appel","Email","WhatsApp","Zoom","Présentiel","Autre"],
    "villes":["Douala","Yaoundé","Limbe","Bafoussam","Garoua","Autres"],
    "pays":["Cameroun","Côte d'Ivoire","Sénégal","France","Canada","Autres"],
    "types_evenements":["Formation","Groupe d'étude","BA MEET UP","Webinaire","Conférence","Certification"],
    "lieux":["Présentiel","Zoom","Hybride"],
    "resultats_inter":["Positif","Négatif","À suivre","Sans suite"],
    "statuts_paiement":["Réglé","Partiel","Non payé"],
    "moyens_paiement":["Mobile Money","Virement","CB","Cash"],
    "types_certif":["ECBA","CCBA","CBAP","PBA"],
    "entreprises_cibles":["Dangote","MUPECI","SALAM","SUNU IARD","ENEO","PAD","PAK"],
}

def load_settings():
    if PATHS["settings"].exists():
        try: d = json.loads(PATHS["settings"].read_text(encoding="utf-8"))
        except Exception: d = DEFAULT.copy()
    else: d = DEFAULT.copy()
    for k,v in DEFAULT.items():
        if k not in d or not isinstance(d[k],list): d[k]=v
    return d

def save_settings(d:dict):
    PATHS["settings"].write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

SET = load_settings()

# ---------------------- OUTILS ----------------------
def ensure_df(path:Path, cols:list)->pd.DataFrame:
    if path.exists():
        try: df = pd.read_csv(path, dtype=str, encoding="utf-8")
        except Exception: df = pd.DataFrame(columns=cols)
    else: df = pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns: df[c]=""
    return df[cols]

def save_df(df:pd.DataFrame, path:Path): df.to_csv(path, index=False, encoding="utf-8")

def parse_date(s:str):
    if not s or str(s).strip()=="" or str(s).lower()=="nan": return None
    for fmt in ("%Y-%m-%d","%d/%m/%Y","%Y/%m/%d"):
        try: return datetime.strptime(str(s), fmt).date()
        except: pass
    try: return pd.to_datetime(s).date()
    except: return None

def email_ok(s:str)->bool:
    if not s or str(s).strip()=="" or str(s).lower()=="nan": return True
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", str(s).strip()))

def phone_ok(s:str)->bool:
    if not s or str(s).strip()=="" or str(s).lower()=="nan": return True
    s2 = re.sub(r"[ \.\-\(\)]","",str(s)).replace("+","")
    return s2.isdigit() and len(s2)>=8

def generate_id(prefix:str, df:pd.DataFrame, id_col:str, width:int=3)->str:
    if df.empty or id_col not in df.columns: return f"{prefix}_{str(1).zfill(width)}"
    patt = re.compile(rf"^{re.escape(prefix)}_(\d+)$"); mx=0
    for x in df[id_col].dropna().astype(str):
        m=patt.match(x.strip()); 
        if m:
            try: mx=max(mx,int(m.group(1)))
            except: pass
    return f"{prefix}_{str(mx+1).zfill(width)}"

def log_event(kind:str, payload:dict):
    rec={"ts":datetime.now().isoformat(),"kind":kind,**payload}
    with PATHS["logs"].open("a", encoding="utf-8") as f: f.write(json.dumps(rec, ensure_ascii=False)+"\n")

# ---------------------- CHARGEMENT ----------------------
df_contacts = ensure_df(PATHS["contacts"], C_COLS)
df_inter    = ensure_df(PATHS["inter"], I_COLS)
df_events   = ensure_df(PATHS["events"], E_COLS)
df_parts    = ensure_df(PATHS["parts"], P_COLS)
df_pay      = ensure_df(PATHS["pay"], PAY_COLS)
df_cert     = ensure_df(PATHS["cert"], CERT_COLS)
if not df_contacts.empty:
    df_contacts["Top20"] = df_contacts["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])

# ---------------------- AGRÉGATS CONTACTS ----------------------
def aggregates_for_contacts(today=None):
    """Calcule agrégats + score + tags + probabilité de conversion."""
    today = today or date.today()
    # Interactions
    inter_count = df_inter.groupby("ID")["ID_Interaction"].count() if not df_inter.empty else pd.Series(dtype=int)
    inter_dates = pd.to_datetime(df_inter["Date"], errors="coerce") if not df_inter.empty else pd.Series(dtype="datetime64[ns]")
    last_contact = (df_inter.assign(_d=inter_dates).groupby("ID")["_d"].max()) if not df_inter.empty else pd.Series(dtype="datetime64[ns]")
    recent_cut = today - timedelta(days=90)
    recent_inter = (df_inter.assign(_d=inter_dates).loc[lambda d: d["_d"]>=pd.Timestamp(recent_cut)].groupby("ID")["ID_Interaction"].count()) if not df_inter.empty else pd.Series(dtype=int)
    # Responsable principal
    resp_max = pd.Series(dtype=str)
    if not df_inter.empty:
        tmp = df_inter.groupby(["ID","Responsable"])["ID_Interaction"].count().reset_index()
        idx = tmp.groupby("ID")["ID_Interaction"].idxmax()
        resp_max = tmp.loc[idx].set_index("ID")["Responsable"]
    # Participations
    parts_count = df_parts.groupby("ID")["ID_Participation"].count() if not df_parts.empty else pd.Series(dtype=int)
    has_anim = pd.Series(dtype=bool)
    if not df_parts.empty:
        has_anim = df_parts.assign(_anim=df_parts["Rôle"].isin(["Animateur","Invité"])).groupby("ID")["_anim"].any()
    # Paiements
    pay_reg_count = pd.Series(dtype=int)
    if not df_pay.empty:
        pay = df_pay.copy()
        pay["Montant"] = pd.to_numeric(pay["Montant"], errors="coerce").fillna(0.0)
        total_pay = pay.groupby("ID")["Montant"].sum()
        pay_regle = pay[pay["Statut"]=="Réglé"].groupby("ID")["Montant"].sum()
        pay_impaye = pay[pay["Statut"]!="Réglé"].groupby("ID")["Montant"].sum()
        pay_reg_count = pay[pay["Statut"]=="Réglé"].groupby("ID")["ID_Paiement"].count() if "ID_Paiement" in pay.columns else pay[pay["Statut"]=="Réglé"].groupby("ID")["Montant"].count()
        has_partiel = pay[pay["Statut"]=="Partiel"].groupby("ID")["Montant"].count()
    else:
        total_pay = pd.Series(dtype=float); pay_regle = pd.Series(dtype=float); pay_impaye = pd.Series(dtype=float)
        has_partiel = pd.Series(dtype=int)
    # Certifications (ambassadeurs)
    has_cert = pd.Series(dtype=bool)
    if not df_cert.empty:
        has_cert = df_cert[df_cert["Résultat"]=="Réussi"].groupby("ID")["ID_Certif"].count()>0

    # Assemble
    ag = pd.DataFrame(index=df_contacts["ID"])
    ag["Interactions"] = ag.index.map(inter_count).fillna(0).astype(int)
    ag["Interactions_90j"] = ag.index.map(recent_inter).fillna(0).astype(int)
    ag["Dernier_contact"] = ag.index.map(last_contact).dt.date
    ag["Resp_principal"] = ag.index.map(resp_max).fillna("")
    ag["Participations"] = ag.index.map(parts_count).fillna(0).astype(int)
    ag["A_animé_ou_invité"] = ag.index.map(has_anim).fillna(False)
    ag["CA_total"] = ag.index.map(total_pay).fillna(0.0)
    ag["CA_réglé"] = ag.index.map(pay_regle).fillna(0.0)
    ag["Impayé"] = ag.index.map(pay_impaye).fillna(0.0)
    ag["Paiements_regles_n"] = ag.index.map(pay_reg_count).fillna(0).astype(int)
    ag["A_certification"] = ag.index.map(has_cert).fillna(False)

    # Score composite
    ag["Score_composite"] = ag["Interactions"] + ag["Participations"] + 2*ag["Paiements_regles_n"]

    # Tags automatiques
    def make_tags(row):
        tags=[]
        # Prospect Top-20
        if row["ID"] in set(df_contacts[df_contacts["Type"]=="Prospect"].query("Top20==True")["ID"]):
            tags.append("Prospect Top-20")
        # Régulier non converti
        if row["Participations"]>=3 and row["ID"] in set(df_contacts[df_contacts["Type"]=="Prospect"]["ID"]) and row["CA_réglé"]<=0:
            tags.append("Régulier-non-converti")
        # Futur formateur
        if row["A_animé_ou_invité"] or row["Participations"]>=4:
            tags.append("Futur formateur")
        # Ambassadeur
        if row["A_certification"]:
            tags.append("Ambassadeur (certifié)")
        # VIP CA
        if row["CA_réglé"]>=500000:
            tags.append("VIP (CA élevé)")
        return ", ".join(tags)
    ag["Tags"] = ag.apply(make_tags, axis=1)

    # Probabilité de conversion (règles simples)
    def proba(row):
        if row["ID"] in set(df_contacts[df_contacts["Type"]=="Membre"]["ID"]):
            return "Converti"
        chaud = (row["Interactions_90j"]>=3 and row["Participations"]>=1) or (row["Impayé"]>0 and row["CA_réglé"]==0)
        tiede = (row["Interactions_90j"]>=1 or row["Participations"]>=1)
        if chaud: return "Chaud"
        if tiede: return "Tiède"
        return "Froid"
    ag["Proba_conversion"] = ag.apply(proba, axis=1)
    return ag.reset_index(names="ID")

# ---------------------- PAGES (CRM/Événements/Rapports/Admin)
# NOTE : Pour la brièveté ici, on garde la structure et les fonctions CRUD/migration/rapports
# de la version précédente, mais la grille CRM doit utiliser les colonnes ajoutées :
# ["Score_composite","Tags","Proba_conversion"] en plus des agrégats existants.
# ------------------------------------------------------------------

# (Pour ne pas dupliquer ~1000 lignes, on laisse le reste inchangé.)
# Vous pouvez remplacer dans votre app.py précédent :
# - la fonction aggregates_for_contacts() par celle-ci
# - et dans la page CRM, étendre `table_cols` avec :
#   "Score_composite","Proba_conversion","Tags"

