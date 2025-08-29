# pages/_shared.py
from __future__ import annotations
from pathlib import Path
from datetime import datetime, date, timedelta
import hashlib
import pandas as pd
import streamlit as st

# === Import helpers from project (fallbacks if absent) ===
try:
    from storage_backend import (
        AUDIT_COLS, SHEET_NAME, compute_etag, ensure_df_source, save_df_target
    )
except Exception:
    # Minimal fallbacks for local-only previews
    AUDIT_COLS = ["Created_At","Created_By","Updated_At","Updated_By"]
    SHEET_NAME = {
        "contacts":"contacts","inter":"interactions","events":"evenements","parts":"participations",
        "pay":"paiements","cert":"certifications","entreprises":"entreprises","params":"parametres","users":"users"
    }
    import pandas as _pd
    def compute_etag(df: _pd.DataFrame, name: str) -> str:
        if df is None or df.empty:
            return "empty"
        try:
            payload = df.astype(str).fillna("").to_csv(index=False)
        except Exception:
            payload = ""
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    def ensure_df_source(name: str, cols: list, paths: dict = None, ws_func=None) -> _pd.DataFrame:
        full_cols = list(dict.fromkeys(cols + AUDIT_COLS))
        path = (Path("data") / f"{name}.csv")
        path.parent.mkdir(exist_ok=True)
        if path.exists():
            try:
                df = _pd.read_csv(path, dtype=str).fillna("")
            except Exception:
                df = _pd.DataFrame(columns=full_cols)
        else:
            df = _pd.DataFrame(columns=full_cols)
            df.to_csv(path, index=False, encoding="utf-8")
        for c in full_cols:
            if c not in df.columns: df[c] = ""
        return df[full_cols]
    def save_df_target(name: str, df: _pd.DataFrame, paths: dict = None, ws_func=None):
        path = (Path("data") / f"{name}.csv")
        path.parent.mkdir(exist_ok=True)
        df.to_csv(path, index=False, encoding="utf-8")

# === Column schemas (supersets; pages pick what they need) ===
C_COLS = [
    "ID","Nom","Prénom","Email","Téléphone","Genre",
    "Société","Fonction","Secteur","Pays","Ville","Type","Statut",
    "Top20","Notes"
] + AUDIT_COLS
ENT_COLS = [
    "ID_Entreprise","Nom_Entreprise","Secteur","Adresse","Pays","Ville",
    "Contact_Principal_ID","CA_Annuel","Nb_Employés","Statut_Partenariat","Notes"
] + AUDIT_COLS
E_COLS = [
    "ID_Événement","Nom_Événement","Type","Date","Lieu",
    "Capacité","Coût_Total","Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres",
    "Statut","Description"
] + AUDIT_COLS
INTER_COLS = [
    "ID_Interaction","ID","Date","Canal","Objet","Résultat","Relance","Responsable","Notes"
] + AUDIT_COLS
PART_COLS = [
    "ID_Participation","ID","ID_Événement","Rôle","Feedback","Note"
] + AUDIT_COLS
PAY_COLS = [
    "ID_Paiement","ID","ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence"
] + AUDIT_COLS
CERT_COLS = [
    "ID_Certif","ID","Type_Certif","Date_Examen","Résultat","Score","Date_Obtention","Validité","Renouvellement","Notes"
] + AUDIT_COLS

# === Backend opener (CSV by default; GSheets if configured) ===
def _get_backend():
    backend = st.secrets.get("storage_backend","csv")
    DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
    PATHS = {
        "users": DATA_DIR/"users.csv",
        "contacts": DATA_DIR/"contacts.csv",
        "inter": DATA_DIR/"interactions.csv",
        "events": DATA_DIR/"evenements.csv",
        "parts": DATA_DIR/"participations.csv",
        "pay": DATA_DIR/"paiements.csv",
        "cert": DATA_DIR/"certifications.csv",
        "entreprises": DATA_DIR/"entreprises.csv",
        "params": DATA_DIR/"parametres.csv",
    }
    WS_FUNC = None
    if backend == "gsheets":
        try:
            from gs_client import read_service_account_secret, get_gspread_client, make_ws_func
            info = read_service_account_secret()
            GC = get_gspread_client(info)
            WS_FUNC = make_ws_func(GC)
        except Exception as e:
            st.warning(f"Back-end Google Sheets non initialisé: {e}")
            WS_FUNC = None
    return backend, PATHS, WS_FUNC

# === Utils ===
def parse_date(x):
    from datetime import datetime as _dt
    if x is None: return None
    s = str(x).strip()
    if not s: return None
    for fmt in ("%Y-%m-%d","%d/%m/%Y","%Y/%m/%d","%d-%m-%Y","%Y-%m-%d %H:%M:%S"):
        try: return _dt.strptime(s, fmt).date()
        except Exception: pass
    try:
        return pd.to_datetime(s, errors="coerce").date()
    except Exception:
        return None

def to_int_safe(x, default=0):
    try:
        if x in (None,"","nan","NaN"): return default
        return int(float(str(x).replace(" ","").replace(",",".")))
    except Exception:
        return default

def generate_id(prefix: str, df: pd.DataFrame, col: str) -> str:
    exist = pd.to_numeric(
        df[col].astype(str).str.replace(prefix,"", regex=False),
        errors="coerce"
    ).fillna(0).astype(int)
    nxt = (exist.max() + 1) if len(exist) else 1
    return f"{prefix}{nxt:05d}"

def get_sets_and_params(df_params: pd.DataFrame):
    """
    Convertit 'parametres' en deux structures:
      - PARAMS: dict(key -> value str)
      - SET: dict(key -> list[str]) pour listes utilisables en dropdown
    Format attendu coté CSV/Sheets: colonnes ['cle','val'] ou ['key','value'].
    """
    PARAMS, SET = {}, {}
    if df_params is not None and not df_params.empty:
        cols = [c.lower() for c in df_params.columns]
        if "cle" in cols and "val" in cols:
            key_col, val_col = "cle","val"
        elif "key" in cols and "value" in cols:
            key_col, val_col = "key","value"
        else:
            # fallback: 1ère = key, 2ème = value
            key_col, val_col = df_params.columns[:2].tolist()
        tmp = df_params.rename(columns={key_col:key_col.lower(), val_col:val_col.lower()})
        for _, r in tmp.iterrows():
            k = str(r[key_col.lower()]).strip()
            v = str(r[val_col.lower()]).strip()
            PARAMS[k] = v
            # fabriquer SET si valeur contient des virgules
            if "," in v:
                SET[k] = [s.strip() for s in v.split(",") if s.strip()]
    # valeurs par défaut pour listes clés si manquantes
    SET.setdefault("types_contact", ["Prospect","Membre","Partenaire","Autre"])
    SET.setdefault("statuts_contact", ["Actif","Inactif","Perdu"])
    SET.setdefault("fonctions", ["BA","DA","PM","Étudiant","Autre"])
    SET.setdefault("secteurs", ["Banque","Télécom","IT","Éducation","Santé","ONG","Industrie","Public","Autre"])
    SET.setdefault("pays", ["Cameroun","Côte d'Ivoire","Suisse","France","Autre"])
    SET.setdefault("villes", ["Douala","Yaoundé","Abidjan","Genève","Autre"])
    SET.setdefault("types_evt", ["Formation","Meetup","Webinar","Certification","Autre"])
    SET.setdefault("roles_evt", ["Participant","Animateur","Invité"])
    SET.setdefault("moyens_paiement", ["Mobile Money","Virement","Cash","Carte"])
    SET.setdefault("statuts_paiement", ["Réglé","Partiel","En attente","Annulé"])
    SET.setdefault("types_certif", ["ECBA","CCBA","CBAP","AAC","CBDA","CPOA"])
    return PARAMS, SET

def load_all_tables():
    backend, PATHS, WS_FUNC = _get_backend()
    # Ensure each table with its columns
    df_contacts = ensure_df_source("contacts", C_COLS, PATHS, WS_FUNC)
    df_inter    = ensure_df_source("inter",    INTER_COLS, PATHS, WS_FUNC)
    df_events   = ensure_df_source("events",   E_COLS, PATHS, WS_FUNC)
    df_parts    = ensure_df_source("parts",    PART_COLS, PATHS, WS_FUNC)
    df_pay      = ensure_df_source("pay",      PAY_COLS, PATHS, WS_FUNC)
    df_cert     = ensure_df_source("cert",     CERT_COLS, PATHS, WS_FUNC)
    df_ent      = ensure_df_source("entreprises", ENT_COLS, PATHS, WS_FUNC)
    df_params   = ensure_df_source("params",   ["cle","val"] + AUDIT_COLS, PATHS, WS_FUNC)

    # normalize types
    for df in (df_contacts, df_inter, df_events, df_parts, df_pay, df_cert, df_ent):
        for c in df.columns:
            df[c] = df[c].astype(str).fillna("")
    PARAMS, SET = get_sets_and_params(df_params)

    return {
        "backend": backend, "PATHS": PATHS, "WS_FUNC": WS_FUNC,
        "contacts": df_contacts, "inter": df_inter, "events": df_events,
        "parts": df_parts, "pay": df_pay, "cert": df_cert, "entreprises": df_ent,
        "params": df_params, "PARAMS": PARAMS, "SET": SET
    }

def aggregates_for_contacts(dfs: dict) -> pd.DataFrame:
    """Retourne les agrégats (Interactions, Participations, CA, etc.) par ID de contact."""
    df_inter = dfs["inter"]; df_parts = dfs["parts"]; df_pay = dfs["pay"]; df_cert = dfs["cert"]
    ids = dfs["contacts"]["ID"].astype(str).str.strip().tolist()
    ag = pd.DataFrame(index=ids)

    # Interactions
    if not df_inter.empty:
        inter_count = df_inter.groupby("ID")["ID_Interaction"].count()
        last_contact = df_inter.groupby("ID")["Date"].max()
        ag["Interactions"] = ag.index.to_series().map(inter_count).fillna(0).astype(int)
        ag["Dernier_contact"] = ag.index.to_series().map(last_contact).fillna("")
    else:
        ag["Interactions"] = 0
        ag["Dernier_contact"] = ""

    # Participations
    if not df_parts.empty:
        parts_count = df_parts.groupby("ID")["ID_Participation"].count()
        ag["Participations"] = ag.index.to_series().map(parts_count).fillna(0).astype(int)
    else:
        ag["Participations"] = 0

    # Paiements
    if not df_pay.empty:
        p = df_pay.copy()
        p["Montant"] = pd.to_numeric(p["Montant"], errors="coerce").fillna(0.0)
        total_pay = p.groupby("ID")["Montant"].sum()
        pay_regle = p[p["Statut"]=="Réglé"].groupby("ID")["Montant"].sum()
        impaye = p[p["Statut"]!="Réglé"].groupby("ID")["Montant"].sum()
        ag["CA_réglé"] = ag.index.to_series().map(pay_regle).fillna(0.0)
        ag["Impayé"] = ag.index.to_series().map(impaye).fillna(0.0)
    else:
        ag["CA_réglé"] = 0.0
        ag["Impayé"] = 0.0

    # Certifications
    if not df_cert.empty:
        cert_success = (df_cert["Résultat"]=="Réussi").groupby(df_cert["ID"]).sum()
        ag["A_certification"] = ag.index.to_series().map(cert_success).fillna(False).astype(bool)
    else:
        ag["A_certification"] = False

    # Scores simples
    ag["Score_composite"] = (ag["Interactions"] + ag["Participations"] + (ag["CA_réglé"]>0).astype(int)*2).astype(float)
    def _proba(row):
        if row["CA_réglé"]>0: return "Converti"
        if row["Interactions"]>=3 and row["Participations"]>=1: return "Chaud"
        if row["Interactions"]>=1 or row["Participations"]>=1: return "Tiède"
        return "Froid"
    ag["Proba_conversion"] = ag.apply(_proba, axis=1)
    ag["Tags"] = ag.apply(lambda r: ", ".join([
        "Ambassadeur (certifié)" if r["A_certification"] else "",
        "VIP" if r["CA_réglé"]>=500000 else ""
    ]).replace(", ,",", ").strip(", "), axis=1)

    return ag.reset_index(names="ID")
