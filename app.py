# IIBA Cameroun — CRM (version complète monofichier)
# -----------------------------------------------------------------------------
# Application Streamlit auto-contenue avec backend Google Sheets (ou CSV fallback).
# - Helpers Google Sheets (lecture secrets, client, diagnostics) inlinés.
# - Backend de stockage (ensure/save + etag anti-collisions) inliné.
# - Pages : Tableau de bord, Contacts, Entreprises, Interactions, Événements,
#           Participations, Paiements, Certifications, Paramètres, Admin/Export.
# - Sécurité : contrôle d'etag pour éviter les collisions simultanées.
# - Horodatage anti-collisions pour IDs (timestamp + suffixe aléatoire).
# - Statistiques et outils (recherche, déduplication simple, export).
# -----------------------------------------------------------------------------

import streamlit as st
import pandas as pd
from pathlib import Path as _Path
from datetime import datetime, date
import random
import hashlib as _hashlib
import io

st.set_page_config(page_title="IIBA Cameroun — CRM", page_icon="📊", layout="wide")

# ============================================================================
# Utils — conversions robustes, ID, horodatage
# ============================================================================
def to_float_safe(x, default=0.0):
    """Convertir vers float robuste (espaces, 'nan', etc.)."""
    try:
        if x is None:
            return float(default)
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).replace(" ", "").replace("\u202f", "").strip()
        if s == "" or s.lower() in ("nan", "none", "null"):
            return float(default)
        return float(s)
    except Exception:
        return float(default)

def to_int_safe(x, default=0):
    """Convertir vers int robuste (en s'appuyant sur to_float_safe)."""
    try:
        v = to_float_safe(x, default=default)
        return int(v) if v == v else int(default)
    except Exception:
        return int(default)

def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def gen_id(prefix: str) -> str:
    """ID horodaté + suffixe aléatoire (anti-collision simple côté client)."""
    return f"{prefix}{datetime.now().strftime('%Y%m%d%H%M%S')}{random.randint(100,999)}"


# ============================================================================
# Helpers Google Sheets (secrets + client + diagnostics)
# ============================================================================
import json as _json
import ast as _ast

try:
    from google.oauth2.service_account import Credentials as _Credentials
    import gspread as _gspread
    from gspread.exceptions import APIError, SpreadsheetNotFound
    from gspread_dataframe import set_with_dataframe as _set_with_dataframe, get_as_dataframe as _get_as_dataframe
except Exception:
    _Credentials = None
    _gspread = None
    APIError = Exception
    SpreadsheetNotFound = Exception

    def _set_with_dataframe(*args, **kwargs):  # type: ignore
        raise RuntimeError("gspread indisponible (backend gsheets non utilisable)")

    def _get_as_dataframe(*args, **kwargs):  # type: ignore
        raise RuntimeError("gspread indisponible (backend gsheets non utilisable)")

def _as_mapping(obj):
    try:
        from collections.abc import Mapping as _Mapping
    except Exception:
        _Mapping = dict
    if isinstance(obj, _Mapping):
        return obj
    if hasattr(obj, "keys") and hasattr(obj, "__getitem__"):
        return obj
    return None

def _parse_secret_value(val):
    m = _as_mapping(val)
    if m is not None:
        try:
            return dict(m)
        except Exception:
            try:
                return {k: m[k] for k in m.keys()}
            except Exception:
                return None
    if isinstance(val, str):
        s = val.strip()
        try:
            return _json.loads(s)
        except Exception:
            try:
                d = _ast.literal_eval(s)
                if isinstance(d, dict):
                    return d
            except Exception:
                pass
    return None

def _normalize_private_key(info: dict) -> dict:
    pk = info.get("private_key")
    # Cas courant: clé collée avec \n littéraux → on remet de vrais sauts de ligne
    if isinstance(pk, str) and "\\n" in pk and "\n" not in pk:
        info["private_key"] = pk.replace("\\n", "\n")
    return info

def read_service_account_secret(secret_key: str = "google_service_account", secrets=None) -> dict:
    if secrets is None:
        secrets = st.secrets
    try:
        keys = list(secrets.keys())
    except Exception:
        keys = []
    if secret_key not in keys:
        raise ValueError(f"Clé '{secret_key}' absente dans Secrets. Clés disponibles: {', '.join(keys) or 'aucune'}.")
    raw = secrets[secret_key]
    info = _parse_secret_value(raw)
    if not isinstance(info, dict):
        raise ValueError(
            "Le secret 'google_service_account' n'est pas un dictionnaire exploitable. "
            "Utilisez soit: 1) un bloc JSON entre triples guillemets, soit 2) une table TOML [google_service_account]."
        )
    info = _normalize_private_key(info)
    required = ["type", "project_id", "private_key_id", "private_key", "client_email", "client_id", "token_uri"]
    missing = [k for k in required if k not in info or not info[k]]
    if missing:
        raise ValueError("Champs manquants dans le secret: " + ", ".join(missing))
    return info

def get_gspread_client(info: dict = None):
    if info is None:
        info = read_service_account_secret()
    if _Credentials is None or _gspread is None:
        raise RuntimeError("google-auth/gspread indisponibles dans l'environnement.")
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = _Credentials.from_service_account_info(info, scopes=scopes)
    return _gspread.authorize(creds)

def show_diagnostics_sidebar(spreadsheet_name: str, sheet_map: dict):
    with st.sidebar.expander("🩺 Diagnostics (Google Sheets)", expanded=False):
        st.caption("Vérification détaillée de la configuration et accès Google Sheets.")
        backend = st.secrets.get("storage_backend", "csv")
        st.write(f"**Backend** : `{backend}`")
        st.write(f"**Spreadsheet (nom)** : `{spreadsheet_name}`")
        ss_id = st.secrets.get("gsheet_spreadsheet_id", "").strip() if "gsheet_spreadsheet_id" in st.secrets else ""
        st.write(f"**Spreadsheet (ID)** : `{ss_id or '—'}`")

        st.markdown("**1) Analyse du secret `google_service_account`**")
        try:
            try:
                all_keys = list(st.secrets.keys())
            except Exception:
                all_keys = []
            st.write(f"- Clés en Secrets : {', '.join(all_keys) or '—'}")
            raw = st.secrets.get("google_service_account", None)
            st.write(f"- Type brut: `{type(raw).__name__}`")
            if raw is not None:
                preview = str(raw).replace("private_key", "private_key(…masqué…)")
                st.code(preview[:900] + (" …" if len(preview) > 900 else ""), language="text")
            info = read_service_account_secret()
            pk = info.get("private_key", "")
            st.success("Secret parsé ✅")
            st.write(f"- `private_key` longueur: {len(pk) if isinstance(pk, str) else '—'}")
        except Exception as e:
            st.error(f"Parsing secret: {e}")
            return

        st.markdown("---")
        st.markdown("**2) Connexion Google Sheets & onglets**")
        try:
            gc = get_gspread_client(info)
            if ss_id:
                try:
                    sh = gc.open_by_key(ss_id)
                except APIError as e:
                    msg = str(e).lower()
                    if "not supported for this document" in msg or "operation is not supported" in msg:
                        st.warning("L’ID fourni n’est pas un Google Sheet natif. Fallback par titre si disponible.")
                        sh = gc.open(spreadsheet_name)
                    else:
                        raise
            else:
                sh = gc.open(spreadsheet_name)

            ws_list = [w.title for w in sh.worksheets()]
            st.success(f"Connexion OK. **{len(ws_list)}** onglet(s).")
            st.write("**Onglets détectés** :", ", ".join(ws_list) or "—")
            required_tabs = list({v for v in sheet_map.values()})
            missing = [t for t in required_tabs if t not in ws_list]
            if missing:
                st.warning("Onglets manquants : " + ", ".join(missing))
                if st.button("🛠️ Créer les onglets manquants"):
                    for t in missing:
                        try:
                            sh.add_worksheet(title=t, rows=2, cols=50)
                        except Exception as _e:
                            st.error(f"Impossible de créer '{t}' : {_e}")
                    st.experimental_rerun()
            else:
                st.info("Toutes les tables attendues existent.")
        except Exception as e:
            st.error(f"Connexion échouée: {e}")

# ============================================================================
# Backend de stockage (mapping, etag, ensure/save)
# ============================================================================
AUDIT_COLS = ["Created_At", "Created_By", "Updated_At", "Updated_By"]

SHEET_NAME = {
    "contacts": "contacts",
    "inter": "interactions",
    "events": "evenements",
    "parts": "participations",
    "pay": "paiements",
    "cert": "certifications",
    "entreprises": "entreprises",
    "params": "parametres",
    "users": "users",
}

def _id_col_for(name: str) -> str:
    return {
        "contacts": "ID",
        "inter": "ID_Interaction",
        "events": "ID_Événement",
        "parts": "ID_Participation",
        "pay": "ID_Paiement",
        "cert": "ID_Certif",
        "entreprises": "ID_Entreprise",
        "users": "user_id",
    }.get(name, "ID")

def _compute_etag(df: pd.DataFrame, name: str) -> str:
    if df is None or df.empty:
        return "empty"
    idc = _id_col_for(name)
    cols = [c for c in [idc, "Updated_At"] if c in df.columns]
    try:
        payload = df[cols].astype(str).fillna("").sort_values(by=cols).to_csv(index=False)
    except Exception:
        payload = df.astype(str).fillna("").to_csv(index=False)
    return _hashlib.sha256(payload.encode("utf-8")).hexdigest()

def ensure_df_source(name: str, cols: list, paths: dict = None, ws_func=None) -> pd.DataFrame:
    full_cols = cols + [c for c in AUDIT_COLS if c not in cols]
    backend = st.secrets.get("storage_backend", "csv")
    st.session_state.setdefault(f"etag_{name}", "empty")

    if backend == "gsheets":
        if ws_func is None:
            raise RuntimeError("ws_func requis pour backend gsheets")
        tab = SHEET_NAME.get(name, name)
        ws = ws_func(tab)
        df = _get_as_dataframe(ws, evaluate_formulas=True, header=0)
        if df is None or df.empty:
            df = pd.DataFrame(columns=full_cols)
            _set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
        else:
            for c in full_cols:
                if c not in df.columns:
                    df[c] = ""
            df = df[full_cols]
        st.session_state[f"etag_{name}"] = _compute_etag(df, name)
        return df

    # CSV fallback
    if paths is None or name not in paths:
        raise RuntimeError("PATHS manquant pour CSV backend")
    path = paths[name]
    if not path.exists():
        df = pd.DataFrame(columns=full_cols)
        df.to_csv(path, index=False, encoding="utf-8")
    else:
        try:
            df = pd.read_csv(path, dtype=str).fillna("")
        except Exception:
            df = pd.DataFrame(columns=full_cols)
    for c in full_cols:
        if c not in df.columns: df[c] = ""
    df = df[full_cols]
    st.session_state[f"etag_{name}"] = _compute_etag(df, name)
    return df

def save_df_target(name: str, df: pd.DataFrame, paths: dict = None, ws_func=None):
    backend = st.secrets.get("storage_backend", "csv")

    if backend == "gsheets":
        if ws_func is None:
            raise RuntimeError("ws_func requis pour backend gsheets")
        tab = SHEET_NAME.get(name, name)
        ws = ws_func(tab)
        df_remote = _get_as_dataframe(ws, evaluate_formulas=True, header=0)
        if df_remote is None:
            df_remote = pd.DataFrame(columns=df.columns)
        expected = st.session_state.get(f"etag_{name}")
        current = _compute_etag(df_remote, name)
        if expected and expected != current:
            st.error(f"Conflit de modification détecté sur '{tab}'. Veuillez recharger la page.")
            st.stop()
        _set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
        st.session_state[f"etag_{name}"] = _compute_etag(df, name)
        return

    # CSV fallback
    if paths is None or name not in paths:
        raise RuntimeError("PATHS manquant pour CSV backend")
    path = paths[name]
    try:
        cur = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        cur = pd.DataFrame(columns=df.columns)
    expected = st.session_state.get(f"etag_{name}")
    current = _compute_etag(cur, name)
    if expected and expected != current:
        st.error(f"Conflit de modification détecté sur '{name}'. Veuillez recharger la page.")
        st.stop()
    df.to_csv(path, index=False, encoding="utf-8")
    st.session_state[f"etag_{name}"] = _compute_etag(df, name)

# ============================================================================
# Configuration backend + initialisation Google Sheets + ws(name)
# ============================================================================
STORAGE_BACKEND = st.secrets.get("storage_backend", "csv")
GSHEET_SPREADSHEET = st.secrets.get("gsheet_spreadsheet", "IIBA CRM DB")
GSHEET_SPREADSHEET_ID = st.secrets.get("gsheet_spreadsheet_id", "").strip() if "gsheet_spreadsheet_id" in st.secrets else ""

GC = None
if STORAGE_BACKEND == "gsheets":
    try:
        info = read_service_account_secret()
        GC = get_gspread_client(info)
    except Exception as e:
        st.error(f"Initialisation Google Sheets échouée : {e}")
        st.stop()

def ws(name: str):
    """
    Retourne un worksheet Google Sheets intitulé `name`.
    1) Tente open_by_key(GSHEET_SPREADSHEET_ID).
    2) Si APIError 400 'not supported for this document' → fallback open(GSHEET_SPREADSHEET).
    """
    if GC is None:
        st.error("Client Google Sheets non initialisé.")
        st.stop()

    def _open_by_title_with_notice():
        try:
            sh2 = GC.open(GSHEET_SPREADSHEET)
            st.info(
                f"Ouverture par **titre** '{GSHEET_SPREADSHEET}' (fallback). "
                "Vérifiez que `gsheet_spreadsheet_id` pointe bien vers un **Google Sheet** natif."
            )
            return sh2
        except Exception as e2:
            st.error(f"Échec de l’ouverture par titre '{GSHEET_SPREADSHEET}' : {e2}")
            st.stop()

    try:
        if GSHEET_SPREADSHEET_ID:
            try:
                sh = GC.open_by_key(GSHEET_SPREADSHEET_ID)
            except APIError as e:
                msg = str(e).lower()
                if "not supported for this document" in msg or "operation is not supported" in msg:
                    st.warning(
                        "L’ID fourni n’est **pas** un Google Sheet natif (Excel, raccourci, dossier, etc.). "
                        "Fallback par **titre**."
                    )
                    sh = _open_by_title_with_notice()
                else:
                    raise
        else:
            sh = GC.open(GSHEET_SPREADSHEET)
    except SpreadsheetNotFound:
        st.error(
            f"Spreadsheet introuvable. Vérifiez le nom `{GSHEET_SPREADSHEET}` ou renseignez `gsheet_spreadsheet_id` "
            "(ID visible dans l’URL `/spreadsheets/d/<ID>/edit`)."
        )
        st.stop()
    except APIError as e:
        st.error(f"Google APIError lors de l'ouverture du spreadsheet : {e}")
        st.info("Causes fréquentes : 1) Fichier non partagé en **Editor** avec le Service Account ; "
                "2) Nom erroné ; 3) L’ID ne correspond pas à un Google Sheet natif.")
        st.info("Solution : fournissez `gsheet_spreadsheet_id` d’un **Google Sheet** (pas un Excel/upload), ou convertissez le fichier en Google Sheets.")
        st.stop()
    except Exception as e:
        st.error(f"Impossible d'ouvrir le spreadsheet : {e}")
        st.stop()

    # Retourne l'onglet existant ou le crée
    try:
        return sh.worksheet(name)
    except Exception:
        try:
            return sh.add_worksheet(title=name, rows=2, cols=50)
        except Exception as e:
            st.error(f"Impossible de créer l'onglet '{name}' : {e}")
            st.stop()

# ============================================================================
# Schémas colonnes & chemins CSV (fallback local)
# ============================================================================
C_COLS = ["ID","Nom","Prénom","Société","Email","Téléphone","Created_At","Created_By","Updated_At","Updated_By"]
E_COLS = ["ID_Événement","Nom_Événement","Type","Date","Lieu","Created_At","Created_By","Updated_At","Updated_By"]
ENT_COLS = [
    "ID_Entreprise","Nom_Entreprise","Secteur","CA_Annuel","Nb_Employes",
    "Contact_Principal","Email_Principal","Telephone_Principal","Site_Web",
    "Created_At","Created_By","Updated_At","Updated_By"
]
INTER_COLS = [
    "ID_Interaction","Date","Type","Canal","Sujet","Description","ID","ID_Entreprise",
    "Created_At","Created_By","Updated_At","Updated_By"
]
PARTS_COLS = ["ID_Participation","ID_Événement","ID","Présence","Role","Created_At","Created_By","Updated_At","Updated_By"]
PAY_COLS   = ["ID_Paiement","ID","Montant","Devise","Date","Moyen","Objet","Created_At","Created_By","Updated_At","Updated_By"]
CERT_COLS  = ["ID_Certif","ID","Type","Date","Score","Statut","Created_At","Created_By","Updated_At","Updated_By"]
PARAMS_COLS= ["Param","Valeur","Description","Updated_At","Updated_By"]
USERS_COLS = ["user_id","display_name","email","role","active","Created_At","Created_By","Updated_At","Updated_By"]

DATA_DIR = _Path("./data"); DATA_DIR.mkdir(exist_ok=True)
PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "inter": DATA_DIR / "interactions.csv",
    "events": DATA_DIR / "evenements.csv",
    "parts": DATA_DIR / "participations.csv",
    "pay": DATA_DIR / "paiements.csv",
    "cert": DATA_DIR / "certifications.csv",
    "entreprises": DATA_DIR / "entreprises.csv",
    "params": DATA_DIR / "parametres.csv",
    "users": DATA_DIR / "users.csv",
}

# ============================================================================
# Barre latérale : navigation + diagnostics
# ============================================================================
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Aller à",
    [
        "📊 Tableau de bord",
        "👥 Contacts",
        "🏢 Entreprises",
        "💬 Interactions",
        "🗓️ Événements",
        "🧾 Participations",
        "💸 Paiements",
        "🎓 Certifications",
        "⚙️ Paramètres",
        "🛠️ Admin / Import-Export",
    ],
    index=0
)

# Panneau diagnostics (en dehors de la radio ci-dessus)
show_diagnostics_sidebar(GSHEET_SPREADSHEET, SHEET_NAME)

# ============================================================================
# Chargements de données (après initialisation GS & ws)
# ============================================================================
df_contacts     = ensure_df_source("contacts",     C_COLS,      PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
df_events       = ensure_df_source("events",       E_COLS,      PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
df_entreprises  = ensure_df_source("entreprises",  ENT_COLS,    PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
df_inter        = ensure_df_source("inter",        INTER_COLS,  PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
df_parts        = ensure_df_source("parts",        PARTS_COLS,  PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
df_pay          = ensure_df_source("pay",          PAY_COLS,    PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
df_cert         = ensure_df_source("cert",         CERT_COLS,   PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
df_params       = ensure_df_source("params",       PARAMS_COLS, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
df_users        = ensure_df_source("users",        USERS_COLS,  PATHS, ws if STORAGE_BACKEND=="gsheets" else None)

# ============================================================================
# Outils UI communs
# ============================================================================
def _df_download_button(df: pd.DataFrame, label: str, filename: str):
    if df is None:
        return
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=label,
        data=csv,
        file_name=filename,
        mime="text/csv",
        use_container_width=True
    )

def _export_xlsx_bytes(dfs: dict):
    """Exporter plusieurs DataFrames dans un seul classeur Excel (xlsx en mémoire)."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for name, d in dfs.items():
            d.to_excel(writer, sheet_name=name[:31], index=False)
    return output.getvalue()

def _combo_label_contact(row):
    return f"{row.get('ID','')} — {row.get('Nom','')} {row.get('Prénom','')} — {row.get('Société','')}"

def _combo_label_entreprise(row):
    return f"{row.get('ID_Entreprise','')} — {row.get('Nom_Entreprise','')}"

def _combo_label_event(row):
    d = row.get("Date","")
    return f"{row.get('ID_Événement','')} — {row.get('Nom_Événement','')} — {d}"

# ============================================================================
# PAGE — Tableau de bord
# ============================================================================
if page == "📊 Tableau de bord":
    st.subheader("Aperçu général")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Contacts", len(df_contacts))
    m2.metric("Entreprises", len(df_entreprises))
    m3.metric("Événements", len(df_events))
    try:
        ca_total_ent = pd.to_numeric(df_entreprises["CA_Annuel"], errors="coerce").fillna(0).sum()
    except Exception:
        ca_total_ent = 0
    m4.metric("CA Annuel total (FCFA)", f"{int(ca_total_ent):,}".replace(",", " "))

    st.markdown("### Activité récente")
    c1, c2 = st.columns(2)
    c1.markdown("**Dernières interactions**")
    if not df_inter.empty:
        c1.dataframe(df_inter.sort_values("Updated_At", ascending=False).head(10).fillna(""), use_container_width=True, height=250)
    else:
        c1.info("Aucune interaction.")

    c2.markdown("**Prochains événements**")
    if not df_events.empty:
        _ev = df_events.copy()
        _ev["Date_parsed"] = pd.to_datetime(_ev["Date"], errors="coerce")
        _ev = _ev.sort_values("Date_parsed").drop(columns=["Date_parsed"])
        c2.dataframe(_ev.head(10).fillna(""), use_container_width=True, height=250)
    else:
        c2.info("Aucun événement.")

    st.markdown("### Export rapide")
    _df_download_button(df_contacts, "⬇️ Export Contacts (CSV)", "contacts.csv")
    _df_download_button(df_entreprises, "⬇️ Export Entreprises (CSV)", "entreprises.csv")
    _df_download_button(df_events, "⬇️ Export Événements (CSV)", "evenements.csv")

# ============================================================================
# PAGE — Contacts
# ============================================================================
elif page == "👥 Contacts":
    st.subheader("Gestion des contacts")

    # Filtres
    fc1, fc2, fc3 = st.columns([1,1,1])
    filtre_nom = fc1.text_input("Nom (contient)", "")
    filtre_soc = fc2.text_input("Société (contient)", "")
    filtre_mail= fc3.text_input("Email (contient)", "")

    _dfc = df_contacts.copy().fillna("")
    if filtre_nom:
        _dfc = _dfc[_dfc["Nom"].str.contains(filtre_nom, case=False, na=False)]
    if filtre_soc:
        _dfc = _dfc[_dfc["Société"].str.contains(filtre_soc, case=False, na=False)]
    if filtre_mail:
        _dfc = _dfc[_dfc["Email"].str.contains(filtre_mail, case=False, na=False)]

    st.markdown("### Liste")
    st.dataframe(_dfc, use_container_width=True, height=350)

    st.markdown("### Créer / éditer un contact")
    # Sélecteur existant
    opts = ["— Nouveau contact —"]
    lab_map = {}
    for _, r in df_contacts.fillna("").iterrows():
        lab = _combo_label_contact(r)
        opts.append(lab)
        lab_map[lab] = r["ID"]
    choice = st.selectbox("Sélection", opts, index=0)
    is_new = (choice == "— Nouveau contact —")
    if not is_new:
        cid = lab_map[choice]
        init = df_contacts[df_contacts["ID"].astype(str)==str(cid)].iloc[0].to_dict()
    else:
        init = {c:"" for c in C_COLS}

    with st.form("contact_form", clear_on_submit=False):
        c1, c2, c3 = st.columns([1,1,1])
        nom = c1.text_input("Nom", value=init.get("Nom",""))
        prenom = c2.text_input("Prénom", value=init.get("Prénom",""))
        soc = c3.text_input("Société", value=init.get("Société",""))

        c4, c5 = st.columns([1,1])
        email = c4.text_input("Email", value=init.get("Email",""))
        tel   = c5.text_input("Téléphone", value=init.get("Téléphone",""))

        b1, b2 = st.columns([1,1])
        submit = b1.form_submit_button("💾 Enregistrer")
        delete = (not is_new) and b2.form_submit_button("🗑️ Supprimer")

    if submit:
        if not nom:
            st.warning("Veuillez renseigner au minimum le nom.")
        else:
            if is_new:
                new = {c:"" for c in C_COLS}
                new.update({
                    "ID": gen_id("CTC"),
                    "Nom": nom,
                    "Prénom": prenom,
                    "Société": soc,
                    "Email": email,
                    "Téléphone": tel,
                    "Created_At": _now_str(),
                    "Created_By": "system",
                    "Updated_At": _now_str(),
                    "Updated_By": "system",
                })
                df_contacts = pd.concat([df_contacts, pd.DataFrame([new])], ignore_index=True)
                save_df_target("contacts", df_contacts, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                st.success(f"Contact créé : {nom} {prenom}")
            else:
                idx = df_contacts.index[df_contacts["ID"].astype(str)==str(init["ID"])].tolist()
                if idx:
                    i = idx[0]
                    df_contacts.at[i,"Nom"] = nom
                    df_contacts.at[i,"Prénom"] = prenom
                    df_contacts.at[i,"Société"] = soc
                    df_contacts.at[i,"Email"] = email
                    df_contacts.at[i,"Téléphone"] = tel
                    df_contacts.at[i,"Updated_At"] = _now_str()
                    df_contacts.at[i,"Updated_By"] = "system"
                    save_df_target("contacts", df_contacts, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                    st.success(f"Contact mis à jour : {nom} {prenom}")

    if (not is_new) and delete:
        ixs = df_contacts.index[df_contacts["ID"].astype(str)==str(init["ID"])].tolist()
        if ixs:
            df_contacts = df_contacts.drop(ixs).reset_index(drop=True)
            save_df_target("contacts", df_contacts, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
            st.success("Contact supprimé.")

    st.markdown("### Export")
    _df_download_button(df_contacts, "⬇️ CSV — Contacts", "contacts.csv")

# ============================================================================
# PAGE — Entreprises
# ============================================================================
elif page == "🏢 Entreprises":
    st.subheader("Gestion des entreprises")

    # Filtres
    fc1, fc2 = st.columns([1,1])
    f_nom = fc1.text_input("Nom d’entreprise (contient)", "")
    f_secteur = fc2.text_input("Secteur (contient)", "")

    _dfe = df_entreprises.copy().fillna("")
    if f_nom:
        _dfe = _dfe[_dfe["Nom_Entreprise"].str.contains(f_nom, case=False, na=False)]
    if f_secteur:
        _dfe = _dfe[_dfe["Secteur"].str.contains(f_secteur, case=False, na=False)]

    st.markdown("### Liste")
    st.dataframe(_dfe, use_container_width=True, height=350)

    # Sélecteur
    opts = ["— Nouvelle entreprise —"]
    lab_map = {}
    for _, r in df_entreprises.fillna("").iterrows():
        lab = _combo_label_entreprise(r)
        opts.append(lab)
        lab_map[lab] = r["ID_Entreprise"]
    choix = st.selectbox("Sélection", opts, index=0)
    is_new = (choix == "— Nouvelle entreprise —")
    if not is_new:
        ent_id = lab_map[choix]
        row_init_ent = df_entreprises[df_entreprises["ID_Entreprise"].astype(str) == str(ent_id)].iloc[0].to_dict()
    else:
        row_init_ent = {c:"" for c in ENT_COLS}

    with st.form("ent_form", clear_on_submit=False):
        c1_ent, c2_ent = st.columns([2,1])
        nom_ent = c1_ent.text_input("Nom de l’entreprise", value=row_init_ent.get("Nom_Entreprise",""))
        secteur = c2_ent.text_input("Secteur", value=row_init_ent.get("Secteur",""))

        c3_ent, c4_ent = st.columns([1,1])
        ca_annuel = c3_ent.number_input("CA Annuel (FCFA)", min_value=0, step=1_000_000, value=to_int_safe(row_init_ent.get("CA_Annuel"), 0))
        nb_emp = c4_ent.number_input("Nombre d'employés", min_value=0, step=10, value=to_int_safe(row_init_ent.get("Nb_Employes"), 0))

        # Contact principal depuis la base contacts
        st.markdown("#### Contact principal")
        _opts_cp = []
        _idmap_cp = {}
        if not df_contacts.empty:
            _tmp = df_contacts[["ID","Nom","Prénom","Société","Email","Téléphone"]].fillna("")
            for _, r_ in _tmp.iterrows():
                lab = f"{r_['ID']} - {r_['Nom']} {r_['Prénom']} - {r_['Société']}"
                _opts_cp.append(lab)
                _idmap_cp[lab] = (r_["ID"], r_["Email"], r_["Téléphone"])
        cur_cp_id = str(row_init_ent.get("Contact_Principal","") or "")
        cur_label = "— Aucun —"
        if cur_cp_id and not df_contacts.empty and cur_cp_id in df_contacts["ID"].astype(str).values:
            for lab in _opts_cp:
                if lab.startswith(f"{cur_cp_id} -"):
                    cur_label = lab
                    break
        sel_label = st.selectbox("Sélectionner le contact principal (ID - Nom Prénom - Entreprise)",
                                 ["— Aucun —"] + _opts_cp,
                                 index=(["— Aucun —"] + _opts_cp).index(cur_label) if cur_label in (["— Aucun —"] + _opts_cp) else 0)
        if sel_label and sel_label != "— Aucun —":
            _cp_id, _cp_email, _cp_tel = _idmap_cp[sel_label]
            contact_principal = _cp_id
            email_principal = _cp_email
            tel_principal = _cp_tel
        else:
            contact_principal = ""
            email_principal = ""
            tel_principal = ""

        site_web = st.text_input("Site Web", value=row_init_ent.get("Site_Web",""))

        col_save, col_del = st.columns([1,1])
        submitted = col_save.form_submit_button("💾 Enregistrer")
        do_delete = (not is_new) and col_del.form_submit_button("🗑️ Supprimer")

    if submitted:
        if not nom_ent:
            st.warning("Veuillez renseigner le nom de l’entreprise.")
        else:
            if is_new:
                new_id = gen_id("ENT")
                new = {c:"" for c in ENT_COLS}
                new.update({
                    "ID_Entreprise": new_id,
                    "Nom_Entreprise": nom_ent,
                    "Secteur": secteur,
                    "CA_Annuel": ca_annuel,
                    "Nb_Employes": nb_emp,
                    "Contact_Principal": contact_principal,
                    "Email_Principal": email_principal,
                    "Telephone_Principal": tel_principal,
                    "Site_Web": site_web,
                    "Created_At": _now_str(),
                    "Created_By": "system",
                    "Updated_At": _now_str(),
                    "Updated_By": "system",
                })
                df_entreprises = pd.concat([df_entreprises, pd.DataFrame([new])], ignore_index=True)
                save_df_target("entreprises", df_entreprises, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                st.success(f"Entreprise créée : {nom_ent} (ID {new_id})")
            else:
                idx = df_entreprises.index[df_entreprises["ID_Entreprise"].astype(str) == str(row_init_ent["ID_Entreprise"])].tolist()
                if idx:
                    i = idx[0]
                    df_entreprises.at[i,"Nom_Entreprise"]      = nom_ent
                    df_entreprises.at[i,"Secteur"]             = secteur
                    df_entreprises.at[i,"CA_Annuel"]           = ca_annuel
                    df_entreprises.at[i,"Nb_Employes"]         = nb_emp
                    df_entreprises.at[i,"Contact_Principal"]   = contact_principal
                    df_entreprises.at[i,"Email_Principal"]     = email_principal
                    df_entreprises.at[i,"Telephone_Principal"] = tel_principal
                    df_entreprises.at[i,"Site_Web"]            = site_web
                    df_entreprises.at[i,"Updated_At"]          = _now_str()
                    df_entreprises.at[i,"Updated_By"]          = "system"
                    save_df_target("entreprises", df_entreprises, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                    st.success(f"Entreprise mise à jour : {nom_ent}")

    if (not is_new) and do_delete:
        ixs = df_entreprises.index[df_entreprises["ID_Entreprise"].astype(str) == str(row_init_ent["ID_Entreprise"])].tolist()
        if ixs:
            df_entreprises = df_entreprises.drop(ixs).reset_index(drop=True)
            save_df_target("entreprises", df_entreprises, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
            st.success(f"Entreprise supprimée : {row_init_ent.get('Nom_Entreprise','')}")

    # Statistiques
    st.markdown("### Statistiques des entreprises")
    try:
        ca_total_ent = pd.to_numeric(df_entreprises["CA_Annuel"], errors="coerce").fillna(0).sum()
    except Exception:
        ca_total_ent = 0
    st.write(f"**CA total (FCFA)** : {int(ca_total_ent):,}".replace(",", " "))

    st.markdown("### Export")
    _df_download_button(df_entreprises, "⬇️ CSV — Entreprises", "entreprises.csv")

# ============================================================================
# PAGE — Interactions
# ============================================================================
elif page == "💬 Interactions":
    st.subheader("Journal des interactions")

    # Filtres
    fi1, fi2, fi3 = st.columns([1,1,1])
    f_contact = fi1.text_input("Filtrer par ID Contact (exact)", "")
    f_ent     = fi2.text_input("Filtrer par ID Entreprise (exact)", "")
    f_type    = fi3.text_input("Type (contient)", "")

    _dfi = df_inter.copy().fillna("")
    if f_contact:
        _dfi = _dfi[_dfi["ID"].astype(str) == f_contact]
    if f_ent:
        _dfi = _dfi[_dfi["ID_Entreprise"].astype(str) == f_ent]
    if f_type:
        _dfi = _dfi[_dfi["Type"].str.contains(f_type, case=False, na=False)]

    st.dataframe(_dfi.sort_values("Date", ascending=False), use_container_width=True, height=350)

    st.markdown("### Créer / éditer une interaction")
    # Sélection d'une interaction existante
    opts = ["— Nouvelle interaction —"] + [f"{r['ID_Interaction']} — {r['Sujet']}" for _, r in df_inter.fillna("").iterrows()]
    choice = st.selectbox("Sélection", opts, index=0)
    is_new = (choice == "— Nouvelle interaction —")
    if not is_new:
        iid = choice.split(" — ",1)[0]
        init = df_inter[df_inter["ID_Interaction"].astype(str)==iid].iloc[0].to_dict()
    else:
        init = {c:"" for c in INTER_COLS}
        init["Date"] = date.today().isoformat()

    # Choix contact
    _opts_ctc = ["— Aucun —"]
    _map_ctc = {}
    for _, r in df_contacts.fillna("").iterrows():
        lab = _combo_label_contact(r)
        _opts_ctc.append(lab)
        _map_ctc[lab] = r["ID"]
    # Choix entreprise
    _opts_ent = ["— Aucun —"]
    _map_ent = {}
    for _, r in df_entreprises.fillna("").iterrows():
        lab = _combo_label_entreprise(r)
        _opts_ent.append(lab)
        _map_ent[lab] = r["ID_Entreprise"]

    with st.form("inter_form", clear_on_submit=False):
        c1, c2, c3 = st.columns([1,1,1])
        dte  = c1.date_input("Date", value=pd.to_datetime(init.get("Date",""), errors="coerce") if init.get("Date") else date.today())
        typ  = c2.text_input("Type", value=init.get("Type",""))
        canal= c3.selectbox("Canal", ["Email","Téléphone","Réunion","WhatsApp","Autre"], index= ["Email","Téléphone","Réunion","WhatsApp","Autre"].index(init.get("Canal","Autre")) if init.get("Canal") in ["Email","Téléphone","Réunion","WhatsApp","Autre"] else 4)

        c4, c5 = st.columns([1,1])
        sujet = c4.text_input("Sujet", value=init.get("Sujet",""))
        desc  = c5.text_area("Description", value=init.get("Description",""), height=100)

        c6, c7 = st.columns([1,1])
        # Pré-sélection contact
        init_ctc_label = "— Aucun —"
        if init.get("ID"):
            for lab in _opts_ctc:
                if lab.startswith(str(init["ID"])):
                    init_ctc_label = lab; break
        sel_ctc = c6.selectbox("Contact", _opts_ctc, index= _opts_ctc.index(init_ctc_label) if init_ctc_label in _opts_ctc else 0)

        # Pré-sélection entreprise
        init_ent_label = "— Aucun —"
        if init.get("ID_Entreprise"):
            for lab in _opts_ent:
                if lab.startswith(str(init["ID_Entreprise"])):
                    init_ent_label = lab; break
        sel_ent = c7.selectbox("Entreprise", _opts_ent, index= _opts_ent.index(init_ent_label) if init_ent_label in _opts_ent else 0)

        b1, b2 = st.columns([1,1])
        submit = b1.form_submit_button("💾 Enregistrer")
        delete = (not is_new) and b2.form_submit_button("🗑️ Supprimer")

    if submit:
        cid = _map_ctc.get(sel_ctc, "") if sel_ctc != "— Aucun —" else ""
        entid = _map_ent.get(sel_ent, "") if sel_ent != "— Aucun —" else ""
        if is_new:
            new = {c:"" for c in INTER_COLS}
            new.update({
                "ID_Interaction": gen_id("INT"),
                "Date": dte.isoformat() if dte else "",
                "Type": typ, "Canal": canal, "Sujet": sujet, "Description": desc,
                "ID": cid, "ID_Entreprise": entid,
                "Created_At": _now_str(), "Created_By": "system", "Updated_At": _now_str(), "Updated_By": "system",
            })
            df_inter = pd.concat([df_inter, pd.DataFrame([new])], ignore_index=True)
            save_df_target("inter", df_inter, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
            st.success(f"Interaction créée ({new['ID_Interaction']}).")
        else:
            idx = df_inter.index[df_inter["ID_Interaction"].astype(str)==str(init["ID_Interaction"])].tolist()
            if idx:
                i = idx[0]
                df_inter.at[i,"Date"] = dte.isoformat() if dte else ""
                df_inter.at[i,"Type"] = typ
                df_inter.at[i,"Canal"]= canal
                df_inter.at[i,"Sujet"]= sujet
                df_inter.at[i,"Description"]= desc
                df_inter.at[i,"ID"] = cid
                df_inter.at[i,"ID_Entreprise"] = entid
                df_inter.at[i,"Updated_At"] = _now_str()
                df_inter.at[i,"Updated_By"] = "system"
                save_df_target("inter", df_inter, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                st.success("Interaction mise à jour.")

    if (not is_new) and delete:
        ixs = df_inter.index[df_inter["ID_Interaction"].astype(str)==str(init["ID_Interaction"])].tolist()
        if ixs:
            df_inter = df_inter.drop(ixs).reset_index(drop=True)
            save_df_target("inter", df_inter, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
            st.success("Interaction supprimée.")

# ============================================================================
# PAGE — Événements
# ============================================================================
elif page == "🗓️ Événements":
    st.subheader("Gestion des événements")

    fe1, fe2 = st.columns([1,1])
    f_evt = fe1.text_input("Nom d’événement (contient)", "")
    f_lieu= fe2.text_input("Lieu (contient)", "")

    _e = df_events.copy().fillna("")
    if f_evt:
        _e = _e[_e["Nom_Événement"].str.contains(f_evt, case=False, na=False)]
    if f_lieu:
        _e = _e[_e["Lieu"].str.contains(f_lieu, case=False, na=False)]

    st.dataframe(_e, use_container_width=True, height=350)

    # Sélection / creation
    opts = ["— Nouvel événement —"] + [f"{r['ID_Événement']} — {r['Nom_Événement']}" for _, r in df_events.fillna("").iterrows()]
    choice = st.selectbox("Sélection", opts, index=0)
    is_new = (choice == "— Nouvel événement —")
    if not is_new:
        evid = choice.split(" — ",1)[0]
        init = df_events[df_events["ID_Événement"].astype(str)==evid].iloc[0].to_dict()
    else:
        init = {c:"" for c in E_COLS}
        init["Date"] = date.today().isoformat()

    with st.form("evt_form", clear_on_submit=False):
        c1, c2 = st.columns([2,1])
        nom_evt = c1.text_input("Nom de l’événement", value=init.get("Nom_Événement",""))
        type_evt= c2.selectbox("Type", ["Conférence","Atelier","Webinaire","Réunion","Autre"],
                               index = ["Conférence","Atelier","Webinaire","Réunion","Autre"].index(init.get("Type","Autre")) if init.get("Type") in ["Conférence","Atelier","Webinaire","Réunion","Autre"] else 4)
        c3, c4 = st.columns([1,1])
        date_evt = c3.date_input("Date", value=pd.to_datetime(init.get("Date",""), errors="coerce") if init.get("Date") else date.today())
        lieu_evt = c4.text_input("Lieu", value=init.get("Lieu",""))

        b1, b2 = st.columns([1,1])
        submit = b1.form_submit_button("💾 Enregistrer")
        delete = (not is_new) and b2.form_submit_button("🗑️ Supprimer")

    if submit:
        if not nom_evt:
            st.warning("Veuillez saisir un nom d’événement.")
        else:
            if is_new:
                new = {c:"" for c in E_COLS}
                new.update({
                    "ID_Événement": gen_id("EVT"),
                    "Nom_Événement": nom_evt,
                    "Type": type_evt,
                    "Date": date_evt.isoformat() if date_evt else "",
                    "Lieu": lieu_evt,
                    "Created_At": _now_str(),
                    "Created_By": "system",
                    "Updated_At": _now_str(),
                    "Updated_By": "system",
                })
                df_events = pd.concat([df_events, pd.DataFrame([new])], ignore_index=True)
                save_df_target("events", df_events, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                st.success(f"Événement créé : {nom_evt} (ID {new['ID_Événement']})")
            else:
                idx = df_events.index[df_events["ID_Événement"].astype(str)==str(init["ID_Événement"])].tolist()
                if idx:
                    i = idx[0]
                    df_events.at[i,"Nom_Événement"] = nom_evt
                    df_events.at[i,"Type"] = type_evt
                    df_events.at[i,"Date"] = date_evt.isoformat() if date_evt else ""
                    df_events.at[i,"Lieu"] = lieu_evt
                    df_events.at[i,"Updated_At"] = _now_str()
                    df_events.at[i,"Updated_By"] = "system"
                    save_df_target("events", df_events, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                    st.success(f"Événement mis à jour : {nom_evt}")

    if (not is_new) and delete:
        ixs = df_events.index[df_events["ID_Événement"].astype(str)==str(init["ID_Événement"])].tolist()
        if ixs:
            df_events = df_events.drop(ixs).reset_index(drop=True)
            save_df_target("events", df_events, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
            st.success("Événement supprimé.")

# ============================================================================
# PAGE — Participations
# ============================================================================
elif page == "🧾 Participations":
    st.subheader("Participations aux événements")

    _parts = df_parts.copy().fillna("")
    st.dataframe(_parts, use_container_width=True, height=350)

    # Combo Events
    _opts_evt = ["— Aucun —"]; _map_evt = {}
    for _, r in df_events.fillna("").iterrows():
        lab = _combo_label_event(r)
        _opts_evt.append(lab); _map_evt[lab] = r["ID_Événement"]
    # Combo Contacts
    _opts_ctc = ["— Aucun —"]; _map_ctc = {}
    for _, r in df_contacts.fillna("").iterrows():
        lab = _combo_label_contact(r)
        _opts_ctc.append(lab); _map_ctc[lab] = r["ID"]

    # Sélection participation existante
    opts = ["— Nouvelle participation —"] + [f"{r['ID_Participation']} — {r['ID_Événement']} / {r['ID']}" for _, r in df_parts.fillna("").iterrows()]
    choice = st.selectbox("Sélection", opts, index=0)
    is_new = (choice == "— Nouvelle participation —")
    if not is_new:
        pid = choice.split(" — ",1)[0]
        init = df_parts[df_parts["ID_Participation"].astype(str)==pid].iloc[0].to_dict()
    else:
        init = {c:"" for c in PARTS_COLS}
        init["Présence"] = "Inconnu"

    with st.form("parts_form", clear_on_submit=False):
        c1, c2 = st.columns([1,1])
        # Event
        evt_label = "— Aucun —"
        if init.get("ID_Événement"):
            for lab in _opts_evt:
                if lab.startswith(str(init["ID_Événement"])):
                    evt_label = lab; break
        sel_evt = c1.selectbox("Événement", _opts_evt, index= _opts_evt.index(evt_label) if evt_label in _opts_evt else 0)

        # Contact
        ctc_label = "— Aucun —"
        if init.get("ID"):
            for lab in _opts_ctc:
                if lab.startswith(str(init["ID"])):
                    ctc_label = lab; break
        sel_ctc = c2.selectbox("Contact", _opts_ctc, index= _opts_ctc.index(ctc_label) if ctc_label in _opts_ctc else 0)

        c3, c4 = st.columns([1,1])
        presence = c3.selectbox("Présence", ["Présent","Absent","Inconnu"], index= ["Présent","Absent","Inconnu"].index(init.get("Présence","Inconnu")) if init.get("Présence") in ["Présent","Absent","Inconnu"] else 2)
        role     = c4.text_input("Rôle (ex: Orateur, Participant…)", value=init.get("Role",""))

        b1, b2 = st.columns([1,1])
        submit = b1.form_submit_button("💾 Enregistrer")
        delete = (not is_new) and b2.form_submit_button("🗑️ Supprimer")

    if submit:
        evid = _map_evt.get(sel_evt, "") if sel_evt!="— Aucun —" else ""
        cid  = _map_ctc.get(sel_ctc, "") if sel_ctc!="— Aucun —" else ""
        if not evid or not cid:
            st.warning("Sélectionnez un événement et un contact.")
        else:
            if is_new:
                new = {c:"" for c in PARTS_COLS}
                new.update({
                    "ID_Participation": gen_id("PAR"),
                    "ID_Événement": evid, "ID": cid,
                    "Présence": presence, "Role": role,
                    "Created_At": _now_str(),"Created_By": "system",
                    "Updated_At": _now_str(),"Updated_By": "system",
                })
                df_parts = pd.concat([df_parts, pd.DataFrame([new])], ignore_index=True)
                save_df_target("parts", df_parts, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                st.success("Participation ajoutée.")
            else:
                idx = df_parts.index[df_parts["ID_Participation"].astype(str)==str(init["ID_Participation"])].tolist()
                if idx:
                    i = idx[0]
                    df_parts.at[i,"ID_Événement"] = evid
                    df_parts.at[i,"ID"] = cid
                    df_parts.at[i,"Présence"] = presence
                    df_parts.at[i,"Role"] = role
                    df_parts.at[i,"Updated_At"] = _now_str()
                    df_parts.at[i,"Updated_By"] = "system"
                    save_df_target("parts", df_parts, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                    st.success("Participation mise à jour.")

    if (not is_new) and delete:
        ixs = df_parts.index[df_parts["ID_Participation"].astype(str)==str(init["ID_Participation"])].tolist()
        if ixs:
            df_parts = df_parts.drop(ixs).reset_index(drop=True)
            save_df_target("parts", df_parts, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
            st.success("Participation supprimée.")

# ============================================================================
# PAGE — Paiements
# ============================================================================
elif page == "💸 Paiements":
    st.subheader("Suivi des paiements")

    _pay = df_pay.copy().fillna("")
    st.dataframe(_pay, use_container_width=True, height=350)

    # Combos
    _opts_ctc = ["— Aucun —"]; _map_ctc = {}
    for _, r in df_contacts.fillna("").iterrows():
        lab = _combo_label_contact(r); _opts_ctc.append(lab); _map_ctc[lab] = r["ID"]

    # Sélection paiement
    opts = ["— Nouveau paiement —"] + [f"{r['ID_Paiement']} — {r['ID']} — {r['Montant']} {r['Devise']}" for _, r in df_pay.fillna("").iterrows()]
    choice = st.selectbox("Sélection", opts, index=0)
    is_new = (choice == "— Nouveau paiement —")
    if not is_new:
        pid = choice.split(" — ",1)[0]
        init = df_pay[df_pay["ID_Paiement"].astype(str)==pid].iloc[0].to_dict()
    else:
        init = {c:"" for c in PAY_COLS}
        init["Date"] = date.today().isoformat()
        init["Devise"] = "FCFA"
        init["Montant"] = 0

    with st.form("pay_form", clear_on_submit=False):
        c1, c2 = st.columns([1,1])
        # Contact
        ctc_label = "— Aucun —"
        if init.get("ID"):
            for lab in _opts_ctc:
                if lab.startswith(str(init["ID"])):
                    ctc_label = lab; break
        sel_ctc = c1.selectbox("Contact", _opts_ctc, index=_opts_ctc.index(ctc_label) if ctc_label in _opts_ctc else 0)

        # Montant / Devise
        m = to_int_safe(init.get("Montant"), 0)
        montant = c2.number_input("Montant", min_value=0, step=1000, value=m)

        c3, c4, c5 = st.columns([1,1,1])
        devise = c3.selectbox("Devise", ["FCFA","EUR","USD"], index= ["FCFA","EUR","USD"].index(init.get("Devise","FCFA")) if init.get("Devise") in ["FCFA","EUR","USD"] else 0)
        dte    = c4.date_input("Date", value=pd.to_datetime(init.get("Date",""), errors="coerce") if init.get("Date") else date.today())
        moyen  = c5.selectbox("Moyen", ["Espèces","Virement","Carte","Chèque","Mobile Money","Autre"],
                              index= ["Espèces","Virement","Carte","Chèque","Mobile Money","Autre"].index(init.get("Moyen","Autre")) if init.get("Moyen") in ["Espèces","Virement","Carte","Chèque","Mobile Money","Autre"] else 5)
        obj    = st.text_input("Objet", value=init.get("Objet",""))

        b1, b2 = st.columns([1,1])
        submit = b1.form_submit_button("💾 Enregistrer")
        delete = (not is_new) and b2.form_submit_button("🗑️ Supprimer")

    if submit:
        cid = _map_ctc.get(sel_ctc, "") if sel_ctc!="— Aucun —" else ""
        if not cid:
            st.warning("Sélectionnez un contact.")
        else:
            if is_new:
                new = {c:"" for c in PAY_COLS}
                new.update({
                    "ID_Paiement": gen_id("PAY"),
                    "ID": cid,
                    "Montant": montant,
                    "Devise": devise,
                    "Date": dte.isoformat() if dte else "",
                    "Moyen": moyen,
                    "Objet": obj,
                    "Created_At": _now_str(),"Created_By": "system",
                    "Updated_At": _now_str(),"Updated_By": "system",
                })
                df_pay = pd.concat([df_pay, pd.DataFrame([new])], ignore_index=True)
                save_df_target("pay", df_pay, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                st.success("Paiement ajouté.")
            else:
                idx = df_pay.index[df_pay["ID_Paiement"].astype(str)==str(init["ID_Paiement"])].tolist()
                if idx:
                    i = idx[0]
                    df_pay.at[i,"ID"] = cid
                    df_pay.at[i,"Montant"] = montant
                    df_pay.at[i,"Devise"] = devise
                    df_pay.at[i,"Date"] = dte.isoformat() if dte else ""
                    df_pay.at[i,"Moyen"] = moyen
                    df_pay.at[i,"Objet"] = obj
                    df_pay.at[i,"Updated_At"] = _now_str()
                    df_pay.at[i,"Updated_By"] = "system"
                    save_df_target("pay", df_pay, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                    st.success("Paiement mis à jour.")

    if (not is_new) and delete:
        ixs = df_pay.index[df_pay["ID_Paiement"].astype(str)==str(init["ID_Paiement"])].tolist()
        if ixs:
            df_pay = df_pay.drop(ixs).reset_index(drop=True)
            save_df_target("pay", df_pay, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
            st.success("Paiement supprimé.")

# ============================================================================
# PAGE — Certifications
# ============================================================================
elif page == "🎓 Certifications":
    st.subheader("Certifications des membres")

    _c = df_cert.copy().fillna("")
    st.dataframe(_c, use_container_width=True, height=350)

    # Combo Contacts
    _opts_ctc = ["— Aucun —"]; _map_ctc = {}
    for _, r in df_contacts.fillna("").iterrows():
        lab = _combo_label_contact(r); _opts_ctc.append(lab); _map_ctc[lab] = r["ID"]

    # Sélection
    opts = ["— Nouvelle certification —"] + [f"{r['ID_Certif']} — {r['ID']} — {r['Type']}" for _, r in df_cert.fillna("").iterrows()]
    choice = st.selectbox("Sélection", opts, index=0)
    is_new = (choice == "— Nouvelle certification —")
    if not is_new:
        cid = choice.split(" — ",1)[0]
        init = df_cert[df_cert["ID_Certif"].astype(str)==cid].iloc[0].to_dict()
    else:
        init = {c:"" for c in CERT_COLS}
        init["Date"] = date.today().isoformat()
        init["Statut"] = "En cours"

    with st.form("cert_form", clear_on_submit=False):
        c1, c2 = st.columns([1,1])
        # Contact
        ctc_label = "— Aucun —"
        if init.get("ID"):
            for lab in _opts_ctc:
                if lab.startswith(str(init["ID"])):
                    ctc_label = lab; break
        sel_ctc = c1.selectbox("Contact", _opts_ctc, index=_opts_ctc.index(ctc_label) if ctc_label in _opts_ctc else 0)

        typ = c2.selectbox("Type", ["CBAP","CCBA","ECBA","AAC","CBDA","CCA"],
                           index= ["CBAP","CCBA","ECBA","AAC","CBDA","CCA"].index(init.get("Type","ECBA")) if init.get("Type") in ["CBAP","CCBA","ECBA","AAC","CBDA","CCA"] else 2)

        c3, c4, c5 = st.columns([1,1,1])
        dte   = c3.date_input("Date", value=pd.to_datetime(init.get("Date",""), errors="coerce") if init.get("Date") else date.today())
        score = c4.number_input("Score", min_value=0, max_value=100, step=1, value=to_int_safe(init.get("Score"), 0))
        statut= c5.selectbox("Statut", ["En cours","Réussi","Échoué"],
                             index= ["En cours","Réussi","Échoué"].index(init.get("Statut","En cours")) if init.get("Statut") in ["En cours","Réussi","Échoué"] else 0)

        b1, b2 = st.columns([1,1])
        submit = b1.form_submit_button("💾 Enregistrer")
        delete = (not is_new) and b2.form_submit_button("🗑️ Supprimer")

    if submit:
        cid = _map_ctc.get(sel_ctc, "") if sel_ctc!="— Aucun —" else ""
        if not cid:
            st.warning("Sélectionnez un contact.")
        else:
            if is_new:
                new = {c:"" for c in CERT_COLS}
                new.update({
                    "ID_Certif": gen_id("CER"),
                    "ID": cid, "Type": typ,
                    "Date": dte.isoformat() if dte else "",
                    "Score": score, "Statut": statut,
                    "Created_At": _now_str(),"Created_By": "system",
                    "Updated_At": _now_str(),"Updated_By": "system",
                })
                df_cert = pd.concat([df_cert, pd.DataFrame([new])], ignore_index=True)
                save_df_target("cert", df_cert, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                st.success("Certification ajoutée.")
            else:
                idx = df_cert.index[df_cert["ID_Certif"].astype(str)==str(init["ID_Certif"])].tolist()
                if idx:
                    i = idx[0]
                    df_cert.at[i,"ID"] = cid
                    df_cert.at[i,"Type"] = typ
                    df_cert.at[i,"Date"] = dte.isoformat() if dte else ""
                    df_cert.at[i,"Score"] = score
                    df_cert.at[i,"Statut"] = statut
                    df_cert.at[i,"Updated_At"] = _now_str()
                    df_cert.at[i,"Updated_By"] = "system"
                    save_df_target("cert", df_cert, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                    st.success("Certification mise à jour.")

    if (not is_new) and delete:
        ixs = df_cert.index[df_cert["ID_Certif"].astype(str)==str(init["ID_Certif"])].tolist()
        if ixs:
            df_cert = df_cert.drop(ixs).reset_index(drop=True)
            save_df_target("cert", df_cert, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
            st.success("Certification supprimée.")

# ============================================================================
# PAGE — Paramètres
# ============================================================================
elif page == "⚙️ Paramètres":
    st.subheader("Paramètres généraux")

    st.info("Cette section vous permet d’enregistrer des paramètres clés (chaînes simples).")

    # Edition simple via data_editor
    data_edit = st.data_editor(df_params, use_container_width=True, height=300, num_rows="dynamic")
    b = st.button("💾 Enregistrer les paramètres")
    if b:
        # Nettoyage colonnes minimales
        for c in PARAMS_COLS:
            if c not in data_edit.columns:
                data_edit[c] = ""
        data_edit["Updated_At"] = _now_str()
        data_edit["Updated_By"] = "system"
        data_edit = data_edit[PARAMS_COLS]
        df_params = data_edit.copy()
        save_df_target("params", df_params, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
        st.success("Paramètres enregistrés.")

    st.markdown("### Export")
    _df_download_button(df_params, "⬇️ CSV — Paramètres", "parametres.csv")

# ============================================================================
# PAGE — Admin / Import-Export
# ============================================================================
elif page == "🛠️ Admin / Import-Export":
    st.subheader("Administration, imports & exports")

    st.markdown("### Exports groupés")
    xlsx = _export_xlsx_bytes({
        "contacts": df_contacts,
        "entreprises": df_entreprises,
        "evenements": df_events,
        "interactions": df_inter,
        "participations": df_parts,
        "paiements": df_pay,
        "certifications": df_cert,
        "parametres": df_params,
        "users": df_users,
    })
    st.download_button("⬇️ Export XLSX (toutes tables)", data=xlsx, file_name="iiba_crm_export.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       use_container_width=True)

    st.markdown("---")
    st.markdown("### Outils de maintenance")
    c1, c2 = st.columns([1,1])
    if c1.button("🧹 Dédupliquer les contacts (Nom+Prénom+Email)"):
        key_cols = ["Nom","Prénom","Email"]
        if all(k in df_contacts.columns for k in key_cols):
            before = len(df_contacts)
            df_contacts = df_contacts.sort_values("Updated_At", ascending=False).drop_duplicates(key_cols, keep="first")
            save_df_target("contacts", df_contacts, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
            st.success(f"Déduplication terminée: {before} → {len(df_contacts)}")
        else:
            st.warning("Colonnes manquantes pour la déduplication.")

    if c2.button("🧹 Dédupliquer les entreprises (Nom_Entreprise)"):
        if "Nom_Entreprise" in df_entreprises.columns:
            before = len(df_entreprises)
            df_entreprises = df_entreprises.sort_values("Updated_At", ascending=False).drop_duplicates(["Nom_Entreprise"], keep="first")
            save_df_target("entreprises", df_entreprises, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
            st.success(f"Déduplication terminée: {before} → {len(df_entreprises)}")
        else:
            st.warning("Colonne Nom_Entreprise manquante.")

    st.markdown("---")
    st.markdown("### Import CSV (remplacement)")
    st.caption("⚠️ Remplace intégralement la table sélectionnée par le CSV fourni (entêtes requis).")
    tbl = st.selectbox("Table cible", list(SHEET_NAME.keys()))
    file = st.file_uploader("Choisir un CSV", type=["csv"])
    if st.button("📥 Importer CSV dans la table sélectionnée", disabled=(file is None)):
        if file is None:
            st.warning("Aucun fichier fourni.")
        else:
            try:
                df_new = pd.read_csv(file).fillna("")
                # S'assurer des colonnes audit
                for c in AUDIT_COLS:
                    if c not in df_new.columns:
                        df_new[c] = ""
                save_df_target(tbl, df_new, PATHS, ws if STORAGE_BACKEND=="gsheets" else None)
                st.success(f"Import terminé pour '{tbl}' ({len(df_new)} lignes).")
            except Exception as e:
                st.error(f"Échec import: {e}")
