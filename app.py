import streamlit as st
import pandas as pd
from pathlib import Path as _Path
from datetime import datetime
import random
import hashlib as _hashlib

st.set_page_config(page_title="IIBA Cameroun — CRM", page_icon="📊", layout="wide")

# ===========================================================
# === Helpers NUMÉRIQUES (robustes pour colonnes vides)   ===
# ===========================================================
def to_float_safe(x, default=0.0):
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
    try:
        v = to_float_safe(x, default=default)
        return int(v) if v == v else int(default)
    except Exception:
        return int(default)

# ===========================================================
# === Helpers Google Sheets (secret + client + diagnostics)===
# ===========================================================
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
            sh = gc.open_by_key(ss_id) if ss_id else gc.open(spreadsheet_name)
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

# ===========================================================
# === Helpers stockage (mapping, etag, ensure/save)      ===
# ===========================================================
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
        if c not in df.columns:
            df[c] = ""
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

# ===========================================================
# === Config backend + initialisation GS + ws(name)       ===
# ===========================================================
STORAGE_BACKEND = st.secrets.get("storage_backend", "csv")
GSHEET_SPREADSHEET = st.secrets.get("gsheet_spreadsheet", "IIBA CRM DB")
GSHEET_SPREADSHEET_ID = (
    st.secrets.get("gsheet_spreadsheet_id", "").strip() if "gsheet_spreadsheet_id" in st.secrets else ""
)

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
    Utilise `GSHEET_SPREADSHEET_ID` si fourni (open_by_key), sinon `GSHEET_SPREADSHEET` (open par titre).
    """
    if GC is None:
        st.error("Client Google Sheets non initialisé.")
        st.stop()
    try:
        if GSHEET_SPREADSHEET_ID:
            sh = GC.open_by_key(GSHEET_SPREADSHEET_ID)
        else:
            sh = GC.open(GSHEET_SPREADSHEET)
    except SpreadsheetNotFound:
        st.error(
            f"Spreadsheet introuvable. Vérifiez le nom `{GSHEET_SPREADSHEET}` ou renseignez `gsheet_spreadsheet_id` (ID dans l’URL)."
        )
        st.stop()
    except APIError as e:
        st.error(f"Google APIError lors de l'ouverture du spreadsheet : {e}")
        st.info(
            "Causes fréquentes : 1) Fichier non partagé en **Editor** avec le Service Account ; "
            "2) Nom erroné ; 3) Recherche Drive par titre bloquée."
        )
        st.info("Solution recommandée : fournissez `gsheet_spreadsheet_id` pour ouvrir par ID.")
        st.stop()
    except Exception as e:
        st.error(f"Impossible d'ouvrir le spreadsheet : {e}")
        st.stop()
    try:
        return sh.worksheet(name)
    except Exception:
        try:
            return sh.add_worksheet(title=name, rows=2, cols=50)
        except Exception as e:
            st.error(f"Impossible de créer l'onglet '{name}' : {e}")
            st.stop()

# ===========================================================
# === Fonctions utilitaires CRM                          ===
# ===========================================================
def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def gen_id(prefix: str) -> str:
    # horodatage + suffixe aléatoire (anti-collisions)
    return f"{prefix}{datetime.now().strftime('%Y%m%d%H%M%S')}{random.randint(100, 999)}"

# ===========================================================
# === Schémas colonnes & chemins CSV (fallback local)    ===
# ===========================================================
C_COLS = [
    "ID", "Nom", "Prénom", "Société", "Email", "Téléphone",
    "Created_At", "Created_By", "Updated_At", "Updated_By"
]
E_COLS = [
    "ID_Événement", "Nom_Événement", "Type", "Date", "Lieu",
    "Created_At", "Created_By", "Updated_At", "Updated_By"
]
ENT_COLS = [
    "ID_Entreprise", "Nom_Entreprise", "Secteur", "CA_Annuel", "Nb_Employes",
    "Contact_Principal", "Email_Principal", "Telephone_Principal", "Site_Web",
    "Created_At", "Created_By", "Updated_At", "Updated_By"
]

DATA_DIR = _Path("./data")
DATA_DIR.mkdir(exist_ok=True)
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

# ===========================================================
# === Navigation & Diagnostics                           ===
# ===========================================================
st.sidebar.title("Navigation")
page = st.sidebar.radio("Aller à", ["CRM (Grille centrale)", "Événements", "Entreprises"], index=0)

# Panneau diagnostics : impérativement en dehors de la liste ci-dessus
show_diagnostics_sidebar(GSHEET_SPREADSHEET, SHEET_NAME)

# ===========================================================
# === Chargements de données                             ===
# ===========================================================
df_contacts = ensure_df_source("contacts", C_COLS, PATHS, ws if STORAGE_BACKEND == "gsheets" else None)
df_events = ensure_df_source("events", E_COLS, PATHS, ws if STORAGE_BACKEND == "gsheets" else None)
df_entreprises = ensure_df_source("entreprises", ENT_COLS, PATHS, ws if STORAGE_BACKEND == "gsheets" else None)

# ===========================================================
# === Pages                                              ===
# ===========================================================
if page == "CRM (Grille centrale)":
    st.subheader("Aperçu général")
    c1, c2, c3 = st.columns(3)
    c1.metric("Contacts", len(df_contacts))
    c2.metric("Événements", len(df_events))
    try:
        ca_total_ent = pd.to_numeric(df_entreprises["CA_Annuel"], errors="coerce").fillna(0).sum()
    except Exception:
        ca_total_ent = 0
    c3.metric("CA Annuel total (FCFA)", f"{int(ca_total_ent):,}".replace(",", " "))

    st.markdown("### Aperçu des tables")
    st.dataframe(df_contacts.fillna(""), use_container_width=True, height=200)
    st.dataframe(df_events.fillna(""), use_container_width=True, height=200)
    st.dataframe(df_entreprises.fillna(""), use_container_width=True, height=200)

elif page == "Événements":
    st.subheader("Gestion des événements")
    with st.form("evt_form", clear_on_submit=True):
        f1, f2 = st.columns([2, 1])
        nom_evt = f1.text_input("Nom de l’événement")
        type_evt = f2.selectbox("Type", ["Conférence", "Atelier", "Webinaire", "Réunion", "Autre"])
        f3, f4 = st.columns([1, 1])
        date_evt = f3.date_input("Date")
        lieu_evt = f4.text_input("Lieu")
        submitted = st.form_submit_button("➕ Créer l’événement")
        if submitted:
            if not nom_evt:
                st.warning("Veuillez saisir un nom d’événement.")
            else:
                new = {c: "" for c in E_COLS}
                new["ID_Événement"] = gen_id("EVT")
                new["Nom_Événement"] = nom_evt
                new["Type"] = type_evt
                new["Date"] = date_evt.isoformat() if date_evt else ""
                new["Lieu"] = lieu_evt
                new["Created_At"] = _now_str()
                new["Created_By"] = "system"
                new["Updated_At"] = _now_str()
                new["Updated_By"] = "system"
                df_events = pd.concat([df_events, pd.DataFrame([new])], ignore_index=True)
                save_df_target("events", df_events, PATHS, ws if STORAGE_BACKEND == "gsheets" else None)
                st.success(f"Événement créé : {new['Nom_Événement']} (ID {new['ID_Événement']})")

    st.markdown("### Liste des événements")
    st.dataframe(df_events.fillna(""), use_container_width=True, height=400)

elif page == "Entreprises":
    st.subheader("Gestion des entreprises")

    # Sélecteur d'entreprise existante ou création
    labels = []
    for _, r in df_entreprises.fillna("").iterrows():
        labels.append(f"{r.get('ID_Entreprise','')} — {r.get('Nom_Entreprise','')}")
    choix = st.selectbox("Sélectionner une entreprise", ["— Nouvelle entreprise —"] + labels, index=0)
    is_new = (choix == "— Nouvelle entreprise —")

    if not is_new:
        sel_id = choix.split(" — ", 1)[0]
        row_init_ent = df_entreprises[df_entreprises["ID_Entreprise"].astype(str) == sel_id].iloc[0].to_dict()
    else:
        row_init_ent = {c: "" for c in ENT_COLS}

    with st.form("ent_form", clear_on_submit=False):
        c1_ent, c2_ent = st.columns([2, 1])
        nom_ent = c1_ent.text_input("Nom de l’entreprise", value=row_init_ent.get("Nom_Entreprise", ""))
        secteur = c2_ent.text_input("Secteur", value=row_init_ent.get("Secteur", ""))

        c3_ent, c4_ent = st.columns([1, 1])
        ca_annuel = c3_ent.number_input(
            "CA Annuel (FCFA)", min_value=0, step=1_000_000, value=to_int_safe(row_init_ent.get("CA_Annuel"), 0)
        )
        nb_emp = c4_ent.number_input(
            "Nombre d'employés", min_value=0, step=10, value=to_int_safe(row_init_ent.get("Nb_Employes"), 0)
        )

        # Contact principal à partir des contacts existants
        st.markdown("#### Contact principal")
        _opts_cp = []
        _idmap_cp = {}
        if not df_contacts.empty:
            _tmp = df_contacts[["ID", "Nom", "Prénom", "Société", "Email", "Téléphone"]].fillna("")
            for _, r_ in _tmp.iterrows():
                lab = f"{r_['ID']} - {r_['Nom']} {r_['Prénom']} - {r_['Société']}"
                _opts_cp.append(lab)
                _idmap_cp[lab] = (r_["ID"], r_["Email"], r_["Téléphone"])
        cur_cp_id = str(row_init_ent.get("Contact_Principal", "") or "")
        cur_label = "— Aucun —"
        if cur_cp_id and not df_contacts.empty and cur_cp_id in df_contacts["ID"].astype(str).values:
            for lab in _opts_cp:
                if lab.startswith(f"{cur_cp_id} -"):
                    cur_label = lab
                    break
        sel_label = st.selectbox(
            "Sélectionner le contact principal (ID - Nom Prénom - Entreprise)",
            ["— Aucun —"] + _opts_cp,
            index=(["— Aucun —"] + _opts_cp).index(cur_label) if cur_label in (["— Aucun —"] + _opts_cp) else 0,
        )
        if sel_label and sel_label != "— Aucun —":
            _cp_id, _cp_email, _cp_tel = _idmap_cp[sel_label]
            contact_principal = _cp_id
            email_principal = _cp_email
            tel_principal = _cp_tel
        else:
            contact_principal = ""
            email_principal = ""
            tel_principal = ""

        site_web = st.text_input("Site Web", value=row_init_ent.get("Site_Web", ""))

        col_save, col_del = st.columns([1, 1])
        submitted = col_save.form_submit_button("💾 Enregistrer")
        do_delete = (not is_new) and col_del.form_submit_button("🗑️ Supprimer")

        if submitted:
            if not nom_ent:
                st.warning("Veuillez renseigner le nom de l’entreprise.")
            else:
                if is_new:
                    new_id = gen_id("ENT")
                    new = {c: "" for c in ENT_COLS}
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
                    save_df_target("entreprises", df_entreprises, PATHS, ws if STORAGE_BACKEND == "gsheets" else None)
                    st.success(f"Entreprise créée : {nom_ent} (ID {new_id})")
                else:
                    # mise à jour
                    idx = df_entreprises.index[
                        df_entreprises["ID_Entreprise"].astype(str) == row_init_ent["ID_Entreprise"]
                    ].tolist()
                    if idx:
                        i = idx[0]
                        df_entreprises.at[i, "Nom_Entreprise"] = nom_ent
                        df_entreprises.at[i, "Secteur"] = secteur
                        df_entreprises.at[i, "CA_Annuel"] = ca_annuel
                        df_entreprises.at[i, "Nb_Employes"] = nb_emp
                        df_entreprises.at[i, "Contact_Principal"] = contact_principal
                        df_entreprises.at[i, "Email_Principal"] = email_principal
                        df_entreprises.at[i, "Telephone_Principal"] = tel_principal
                        df_entreprises.at[i, "Site_Web"] = site_web
                        df_entreprises.at[i, "Updated_At"] = _now_str()
                        df_entreprises.at[i, "Updated_By"] = "system"
                        save_df_target("entreprises", df_entreprises, PATHS, ws if STORAGE_BACKEND == "gsheets" else None)
                        st.success(f"Entreprise mise à jour : {nom_ent}")

        if do_delete:
            ixs = df_entreprises.index[
                df_entreprises["ID_Entreprise"].astype(str) == row_init_ent["ID_Entreprise"]
            ].tolist()
            if ixs:
                df_entreprises = df_entreprises.drop(ixs).reset_index(drop=True)
                save_df_target("entreprises", df_entreprises, PATHS, ws if STORAGE_BACKEND == "gsheets" else None)
                st.success(f"Entreprise supprimée : {row_init_ent.get('Nom_Entreprise', '')}")

    # Statistiques des entreprises
    st.markdown("### Statistiques des entreprises")
    try:
        ca_total_ent = pd.to_numeric(df_entreprises["CA_Annuel"], errors="coerce").fillna(0).sum()
    except Exception:
        ca_total_ent = 0
    st.write(f"**CA total (FCFA)** : {int(ca_total_ent):,}".replace(",", " "))

    st.markdown("### Liste des entreprises")
    st.dataframe(df_entreprises.fillna(""), use_container_width=True, height=400)
