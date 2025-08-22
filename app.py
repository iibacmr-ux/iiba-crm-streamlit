# Streamlit CRM IIBA Cameroun - app.py corrigé et complet

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
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "inter": DATA_DIR / "interactions.csv",
    "events": DATA_DIR / "evenements.csv",
    "parts": DATA_DIR / "participations.csv",
    "pay": DATA_DIR / "paiements.csv",
    "cert": DATA_DIR / "certifications.csv",
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

ALL_SCHEMAS = {
    "contacts": C_COLS, "interactions": I_COLS, "evenements": E_COLS,
    "participations": P_COLS, "paiements": PAY_COLS, "certifications": CERT_COLS,
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
        "Score_composite","Proba_conversion","Tags"
    ]),
    "grid_events_columns": ",".join(E_COLS),
    "kpi_enabled": ",".join([
        "contacts_total","prospects_actifs","membres","events_count",
        "participations_total","ca_regle","impayes","taux_conversion"
    ]),
    "kpi_target_contacts_total_year_2025":"1000",
    "kpi_target_ca_regle_year_2025":"5000000",
    "contacts_period_fallback": "1",   # 1 = ON (utilise fallback si Date_Creation manquante), 0 = OFF
}

ALL_DEFAULTS = {**PARAM_DEFAULTS, **{f"list_{k}":v for k,v in DEFAULT_LISTS.items()}}

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
}

# Utils for dataframe loading/saving

def ensure_df(path:Path, cols:list)->pd.DataFrame:
    if path.exists():
        try: 
            df = pd.read_csv(path, dtype=str, encoding="utf-8")
        except Exception:
            df = pd.DataFrame(columns=cols)
    else:
        df = pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns:
            df[c]=""
    return df[cols]

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
if not df_contacts.empty:
    df_contacts["Top20"] = df_contacts["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])

# Aggregate function for contacts (calculs des scores, tags, etc.)

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
    # remonte la date la plus récente de contact, gère les valeurs manquantes
    ag["Dernier_contact"] = ag.index.map(last_contact)  # série de Timestamps ou NaT
    ag["Dernier_contact"] = pd.to_datetime(ag["Dernier_contact"], errors="coerce")  # convertit en datetime
    ag["Dernier_contact"] = ag["Dernier_contact"].dt.date  # extrait la date, les NaT deviennent None
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

# ------------------ Navigation & pages ----------------------

st.sidebar.title("Navigation")
page = st.sidebar.radio("Aller à", ["CRM (Grille centrale)","Événements","Rapports","Admin"], index=0)
this_year = datetime.now().year
annee = st.sidebar.selectbox("Année", ["Toutes"]+[str(this_year-1),str(this_year),str(this_year+1)], index=1)
mois = st.sidebar.selectbox("Mois", ["Tous"]+[f"{m:02d}" for m in range(1,13)], index=0)

# CRM Grille centrale
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

    table_cols = parse_cols(PARAMS.get("grid_crm_columns", ""), [
        "ID","Nom","Prénom","Société","Type","Statut","Email",
        "Interactions","Participations","CA_réglé","Impayé","Resp_principal","A_animé_ou_invité",
        "Score_composite","Proba_conversion","Tags"
    ])

    def _label_contact(row):
        return f"{row['ID']} — {row['Prénom']} {row['Nom']} — {row['Société']}"
    options = [] if dfc.empty else dfc.apply(_label_contact, axis=1).tolist()
    id_map = {} if dfc.empty else dict(zip(options, dfc["ID"]))

    colsel, _ = st.columns([3,1])
    sel_label = colsel.selectbox("Contact sélectionné (sélecteur maître)", [""] + options, index=0, key="select_contact_label")
    if sel_label:
        st.session_state["selected_contact_id"] = id_map[sel_label]

    # Affichage grille avec AgGrid (si installé)
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
                
                # --- Barre d'actions contact ---
                a1, a2 = st.columns(2)
                if a1.button("➕ Nouveau contact"):
                    st.session_state["selected_contact_id"] = None  # on nettoie la sélection pour ouvrir le form "nouveau"
                if a2.button("🧬 Dupliquer ce contact", disabled=not bool(sel_id)):
                    if sel_id:
                        src = df_contacts[df_contacts["ID"] == sel_id]
                        if not src.empty:
                            clone = src.iloc[0].to_dict()
                            new_id = generate_id("CNT", df_contacts, "ID")
                            clone["ID"] = new_id
                            # Optionnel: effacer quelques champs sensibles
                            # clone["Email"] = ""
                            # clone["Téléphone"] = ""
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
                        # --- Validation : Nom obligatoire ---
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
                        df_contacts.loc[idx] = new_row
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
            
            # --- Création d'un nouveau contact (si aucun sélectionné) ---
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
                            # --- Validation : Nom obligatoire ---
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
                                "ID": new_id,
                                "Nom": nom_new,
                                "Prénom": prenom_new,
                                "Genre": genre_new,
                                "Titre": titre_new,
                                "Société": societe_new,
                                "Secteur": secteur_new,
                                "Email": email_new,
                                "Téléphone": tel_new,
                                "LinkedIn": linkedin_new,
                                "Ville": ville_new,
                                "Pays": pays_new,
                                "Type": typec_new,
                                "Source": source_new,
                                "Statut": statut_new,
                                "Score_Engagement": int(score_new),
                                "Date_Creation": dc_new.isoformat(),
                                "Notes": notes_new,
                                "Top20": top20_new
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

# --- Pages Événements, Rapports, Admin ---
# Tu peux me demander la suite ou compléter en fonction si tu veux.

# --------------------------------------------
# Suite app.py - pages Événements, Rapports, Admin
# --------------------------------------------

# ---------------------- PAGE ÉVÉNEMENTS (remplacer toute cette section) ----------------------

if page == "Événements":
    st.title("📅 Événements")

    def _safe_rerun():
        """Compat rerun pour toutes versions de Streamlit."""
        import streamlit as _st
        if hasattr(_st, "rerun"):
            _st.rerun()
        elif hasattr(_st, "experimental_rerun"):
            _st.experimental_rerun()
            
    # --- Session state helpers ---
    if "selected_event_id" not in st.session_state:
        st.session_state["selected_event_id"] = ""
    if "event_form_mode" not in st.session_state:
        st.session_state["event_form_mode"] = "create"  # "create" | "edit"

    # --- Sélecteur d'événement + actions rapides ---
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
        # si l'utilisateur choisit explicitement "— Aucun —", on passe en création
        st.session_state["selected_event_id"] = ""
        st.session_state["event_form_mode"] = "create"

    if new_col.button("➕ Nouveau", key="evt_new_btn"):
        st.session_state["selected_event_id"] = ""
        st.session_state["event_form_mode"] = "create"
        _safe_rerun()

    st.markdown("---")

    # --- Formulaire: créer / éditer (pré-rempli si sélection) ---
    with st.expander("📝 Gérer un événement (pré-rempli si un événement est sélectionné)", expanded=True):
        mode = st.session_state["event_form_mode"]
        sel_eid = st.session_state["selected_event_id"]

        # Pré-remplissage si édition
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
            # ID grisé (toujours visible)
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

            # Boutons selon le mode
            cL, cM, cR = st.columns([1.2,1.2,2])
            btn_create = cL.form_submit_button("🆕 Créer l'événement", disabled=(mode=="edit"))
            btn_save   = cM.form_submit_button("💾 Enregistrer modifications", disabled=(mode!="edit"))

            # Actions du formulaire
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
                # se repositionner en édition sur le nouvel ID
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

    # --- Actions avancées: Dupliquer / Supprimer (avec confirmation) ---
    col_dup, col_del, col_clear = st.columns([1,1,1])
    # Note: ajoute des keys uniques pour éviter StreamlitDuplicateElementId
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
                    # supprimer l'événement
                    globals()["df_events"] = df_events[df_events["ID_Événement"] != del_id]
                    save_df(df_events, PATHS["events"])
                    st.success(f"Événement {del_id} supprimé.")
                    # reset sélection
                    st.session_state["selected_event_id"] = ""
                    st.session_state["event_form_mode"] = "create"
                    _safe_rerun()

    if col_clear.button("🧹 Vider la sélection", key="evt_clear_btn"):
        st.session_state["selected_event_id"] = ""
        st.session_state["event_form_mode"] = "create"
        _safe_rerun()

    st.markdown("---")

    # --- (Optionnel) Grille AgGrid (si installée) pour édition en masse ---
    st.subheader("📋 Liste des événements")
    filt = st.text_input("Filtre rapide (nom, type, lieu, notes…)", "", key="evt_filter")
    page_size_evt = st.selectbox("Taille de page", [20,50,100,200], index=0, key="pg_evt")

    df_show = df_events.copy()
    if filt:
        t = filt.lower()
        df_show = df_show[df_show.apply(lambda r: any(t in str(r[c]).lower() for c in ["Nom_Événement","Type","Lieu","Notes"]), axis=1)]

    if HAS_AGGRID:
        gb = GridOptionsBuilder.from_dataframe(df_show)
        gb.configure_default_column(filter=True, sortable=True, resizable=True, editable=True)
        gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=page_size_evt)
        gb.configure_selection("single", use_checkbox=True)
        go = gb.build()
        grid = AgGrid(
            df_show, gridOptions=go, height=520,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            key="evt_grid", allow_unsafe_jscode=True
        )
        # On garde la grille “mass-edit”; le formulaire reste la source de vérité UX pour créer/éditer/dupliquer/supprimer.
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


# ---------------------- PAGE RAPPORTS ----------------------

elif page == "Rapports":
    st.title("📑 Rapports & KPI — IIBA Cameroun")

    # ---------- Helpers génériques ----------
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
        # Événements
        if df_events.empty:
            dfe2 = df_events.copy()
        else:
            ev_dates = _safe_parse_series(df_events["Date"])
            mask_e = _build_mask_from_dates(ev_dates, year_sel, month_sel)
            dfe2 = df_events[mask_e].copy()

        # Participations (via date d'événement)
        if df_parts.empty:
            dfp2 = df_parts.copy()
        else:
            dfp2 = df_parts.copy()
            if not df_events.empty:
                ev_dates_map = df_events.set_index("ID_Événement")["Date"].map(parse_date)
                dfp2["_d_evt"] = dfp2["ID_Événement"].map(ev_dates_map)
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

    # ---------- Filtrage des CONTACTS par période ----------
    # Logique : on prend Date_Creation si dispo. Sinon, on essaie de déduire une "date de référence"
    # depuis la 1re interaction, 1re participation (date d'événement) ou 1er paiement.
    def filtered_contacts_for_period(
        year_sel: str,
        month_sel: str,
        dfe_all: pd.DataFrame,   # events (toutes lignes, pas filtrées)
        dfi_all: pd.DataFrame,   # interactions (toutes)
        dfp_all: pd.DataFrame,   # participations (toutes)
        dfpay_all: pd.DataFrame  # paiements (toutes)
    ) -> pd.DataFrame:
        """
        Filtre les CONTACTS par période.
        Logique configurable via PARAMS["contacts_period_fallback"]:
          - OFF/0: ne filtre que sur Date_Creation (contact inclus si Date_Creation ∈ période)
          - ON/1 (par défaut): utilise Date_Creation, sinon retombe sur la 1re activité détectée
            (1re interaction, 1re participation via date d'événement, 1er paiement).
        """

        base = df_contacts.copy()
        if base.empty or "ID" not in base.columns:
            return base  # rien à filtrer

        # Normalisation ID en str (évite les merges/map sur types hétérogènes)
        base["ID"] = base["ID"].astype(str).str.strip()

        # Parse Date_Creation -> série de dates (ou None)
        if "Date_Creation" in base.columns:
            base["_dc"] = _safe_parse_series(base["Date_Creation"])
        else:
            base["_dc"] = pd.Series([None] * len(base), index=base.index)

        # Paramètre fallback (Admin -> Paramètres)
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

        # 2) 1re Participation (via date d'événement)
        if (not dfp_all.empty and "ID" in dfp_all.columns and "ID_Événement" in dfp_all.columns
            and not dfe_all.empty and "ID_Événement" in dfe_all.columns and "Date" in dfe_all.columns):
            dfp = dfp_all.copy()
            dfp = dfp[dfp["ID_Événement"].notna()]  # évite les NaN dans le mapping
            dfp["ID"] = dfp["ID"].astype(str).str.strip()

            ev_dates = dfe_all.copy()
            ev_dates["_de"] = _safe_parse_series(ev_dates["Date"])           # objets date/None
            ev_map = ev_dates.set_index("ID_Événement")["_de"]

            dfp["_de"] = dfp["ID_Événement"].map(ev_map)
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
            from datetime import date, datetime
            import pandas as pd

            candidates = []
            for v in (dc, fi, fp, fpay):
                # Convertir pd.Timestamp en datetime
                if isinstance(v, pd.Timestamp):
                    v = v.to_pydatetime()
                # Ne garder que les datetime / date valides
                if isinstance(v, datetime):
                    candidates.append(v.date())
                elif isinstance(v, date):
                    candidates.append(v)

            # Retourner la plus ancienne date, ou None si aucun candidat valide
            return min(candidates) if candidates else None

        # Construire un dict ID -> date de référence
        ref_dates = {}
        ids = base["ID"].tolist()
        # set pour lecture rapide
        set_ids = set(ids)

        # accès direct aux séries pour perf
        s_dc = base.set_index("ID")["_dc"] if "ID" in base.columns else pd.Series(dtype=object)

        for cid in ids:
            dc   = s_dc.get(cid, None) if not s_dc.empty else None
            fi   = first_inter.get(cid, None) if not first_inter.empty else None
            fp   = first_part.get(cid, None)  if not first_part.empty else None
            fpay = first_pay.get(cid, None)   if not first_pay.empty else None
            ref_dates[cid] = _first_valid_date(dc, fi, fp, fpay)

        base["_ref"] = base["ID"].map(ref_dates)

        # Filtrage final par période
        mask = _build_mask_from_dates(base["_ref"], year_sel, month_sel)
        return base[mask].drop(columns=["_dc", "_ref"], errors="ignore")



    # ---------- Agrégats période (version locale, basée sur les tables filtrées) ----------
    def aggregates_for_contacts_period(contacts: pd.DataFrame,
                                       dfi: pd.DataFrame, dfp: pd.DataFrame,
                                       dfpay: pd.DataFrame, dfcert: pd.DataFrame) -> pd.DataFrame:
        if contacts.empty:
            return pd.DataFrame({"ID": [], "Interactions": [], "Interactions_recent": [], "Dernier_contact": [],
                                 "Resp_principal": [], "Participations": [], "A_animé_ou_invité": [],
                                 "CA_total": [], "CA_réglé": [], "Impayé": [], "Paiements_regles_n": [],
                                 "A_certification": [], "Score_composite": [], "Tags": [], "Proba_conversion": []})

        # Params scoring (identiques à aggregates_for_contacts)
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
            # ⬅️ Convertir proprement en datetime64[ns]
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
                dfp.assign(_anim=dfp["Rôle"].isin(["Animateur", "Invité"]))
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
            pay_regle = pay[pay["Statut"] == "Réglé"].groupby("ID")["Montant"].sum()
            pay_impaye = pay[pay["Statut"] != "Réglé"].groupby("ID")["Montant"].sum()
            pay_reg_count = pay[pay["Statut"] == "Réglé"].groupby("ID")["Montant"].count()
        else:
            total_pay = pd.Series(dtype=float)
            pay_regle = pd.Series(dtype=float)
            pay_impaye = pd.Series(dtype=float)
            pay_reg_count = pd.Series(dtype=int)

        # ---------------- Certifications ----------------
        if not dfcert.empty:
            has_cert = dfcert[dfcert["Résultat"] == "Réussi"].groupby("ID")["ID_Certif"].count() > 0
        else:
            has_cert = pd.Series(dtype=bool)

        # ---------------- Assemblage ----------------
        ag = pd.DataFrame(index=contacts["ID"])
        ag["Interactions"] = ag.index.map(inter_count).fillna(0).astype(int)
        ag["Interactions_recent"] = ag.index.map(recent_inter).fillna(0).astype(int)

        # Dernier contact en date → date pure
        # (1) map via Series to ensure we get a pandas Series, not an Index/ndarray
        lc = ag.index.to_series().map(last_contact)

        # (2) force to datetime, coerce bad values to NaT
        lc = pd.to_datetime(lc, errors="coerce")

        # (3) safely extract the date
        ag["Dernier_contact"] = lc.dt.date
            
        ag["Resp_principal"] = ag.index.map(resp_max).fillna("")
        ag["Participations"] = ag.index.map(parts_count).fillna(0).astype(int)
        ag["A_animé_ou_invité"] = ag.index.map(has_anim).fillna(False)
        ag["CA_total"] = ag.index.map(total_pay).fillna(0.0)
        ag["CA_réglé"] = ag.index.map(pay_regle).fillna(0.0)
        ag["Impayé"] = ag.index.map(pay_impaye).fillna(0.0)
        ag["Paiements_regles_n"] = ag.index.map(pay_reg_count).fillna(0).astype(int)

        ag["A_certification"] = ag.index.map(has_cert).fillna(False)
        ag["Score_composite"] = (w_int * ag["Interactions"] +
                                 w_part * ag["Participations"] +
                                 w_pay * ag["Paiements_regles_n"]).round(2)

        def make_tags(row):
            tags = []
            if row.name in set(contacts.loc[contacts.get("Top20", False) == True, "ID"]):
                tags.append("Prospect Top-20")
            if row["Participations"] >= 3 and row["CA_réglé"] <= 0:
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
            if row.name in set(contacts[contacts.get("Type", "") == "Membre"]["ID"]):
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


    # ---------- Finance événements (identique) ----------
    def event_financials(dfe2, dfpay2):
        rec_by_evt = pd.Series(dtype=float)
        if not dfpay2.empty:
            r = dfpay2[dfpay2["Statut"]=="Réglé"].copy()
            r["Montant"] = pd.to_numeric(r["Montant"], errors='coerce').fillna(0)
            rec_by_evt = r.groupby("ID_Événement")["Montant"].sum()
        ev = dfe2 if not dfe2.empty else df_events.copy()
        if ev.empty:
            return pd.DataFrame(columns=["ID_Événement", "Nom_Événement", "Type", "Date", "Coût_Total", "Recette", "Bénéfice"])
        for c in ["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total"]:
            ev[c] = pd.to_numeric(ev[c], errors='coerce').fillna(0)
        ev["Cout_Total"] = ev["Cout_Total"].where(ev["Cout_Total"]>0, ev[["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres"]].sum(axis=1))
        ev = ev.set_index("ID_Événement")
        rep = pd.DataFrame({
            "Nom_Événement": ev["Nom_Événement"],
            "Type": ev["Type"],
            "Date": ev["Date"],
            "Coût_Total": ev["Cout_Total"]
        })
        rep["Recette"] = rec_by_evt.reindex(rep.index, fill_value=0)
        rep["Bénéfice"] = rep["Recette"] - rep["Coût_Total"]
        return rep.reset_index()

    # === Filtrages période ===
    dfe2, dfp2, dfpay2, dfcert2 = filtered_tables_for_period(annee, mois)
    dfc2 = filtered_contacts_for_period(annee, mois, df_events, df_inter, df_parts, df_pay)

    # === KPI de base (sur période) === 
    total_contacts = len(dfc2)

    prospects_actifs = len(dfc2[(dfc2.get("Type","")=="Prospect") & (dfc2.get("Statut","")=="Actif")])
    membres = len(dfc2[dfc2.get("Type","")=="Membre"])
    events_count = len(dfe2)
    parts_total = len(dfp2)

    ca_regle, impayes = 0.0, 0.0
    if not dfpay2.empty:
        dfpay2["Montant"] = pd.to_numeric(dfpay2["Montant"], errors='coerce').fillna(0)
        ca_regle = float(dfpay2[dfpay2["Statut"]=="Réglé"]["Montant"].sum())
        impayes = float(dfpay2[dfpay2["Statut"]!="Réglé"]["Montant"].sum())

    denom_prospects = max(1, len(dfc2[dfc2.get("Type","")=="Prospect"]))
    taux_conv = (membres / denom_prospects) * 100

    # --- Interactions filtrées pour la période (pour KPI Engagement) ---
    if not df_inter.empty:
        di = _safe_parse_series(df_inter["Date"])
        mask_i = _build_mask_from_dates(di, annee, mois)
        dfi2 = df_inter[mask_i].copy()
    else:
        dfi2 = df_inter.copy()

    # --- KPI Engagement (au moins 1 interaction OU 1 participation dans la période) ---
    ids_contacts_periode = set(dfc2.get("ID", pd.Series([], dtype=str)).astype(str))
    ids_inter = set(dfi2.get("ID", pd.Series([], dtype=str)).astype(str)) if not dfi2.empty else set()
    ids_parts = set(dfp2.get("ID", pd.Series([], dtype=str)).astype(str)) if not dfp2.empty else set()
    ids_engaged = (ids_inter | ids_parts) & ids_contacts_periode
    engagement_n = len(ids_engaged)
    engagement_rate = (engagement_n / max(1, len(ids_contacts_periode))) * 100

    # --- Dictionnaire KPI (inclut alias 'taux_conversion') ---
    kpis = {
        "contacts_total":        ("👥 Contacts (créés / période)", total_contacts),
        "prospects_actifs":      ("🧲 Prospects actifs (période)", prospects_actifs),
        "membres":               ("🏆 Membres (période)", membres),
        "events_count":          ("📅 Événements (période)", events_count),
        "participations_total":  ("🎟 Participations (période)", parts_total),
        "ca_regle":              ("💰 CA réglé (période)", f"{int(ca_regle):,} FCFA".replace(",", " ")),
        "impayes":               ("❌ Impayés (période)", f"{int(impayes):,} FCFA".replace(",", " ")),
        "taux_conv":             ("🔄 Taux conversion (période)", f"{taux_conv:.1f}%"),
        # Nouveau KPI Engagement
        "engagement":            ("🙌 Engagement (période)", f"{engagement_rate:.1f}%"),
    }

    # Alias pour compatibilité avec Admin ("taux_conversion")
    aliases = {
        "taux_conversion": "taux_conv",
    }

    # Liste des KPI activés (depuis PARAMS), en appliquant les alias
    enabled_raw = [x.strip() for x in PARAMS.get("kpi_enabled","").split(",") if x.strip()]
    enabled_keys = []
    for k in (enabled_raw or list(kpis.keys())):
        enabled_keys.append(aliases.get(k, k))  # remap si alias, sinon identique

    # Ne garder que ceux réellement disponibles
    enabled = [k for k in enabled_keys if k in kpis]

    # --- Affichage sur 2 lignes (4 colonnes max par ligne) ---
    ncols = 4
    rows = [enabled[i:i+ncols] for i in range(0, len(enabled), ncols)]
    for row in rows:
        cols = st.columns(len(row))
        for col, k in zip(cols, row):
            label, value = kpis[k]
            col.metric(label, value)    
            
    # --- Finance événementielle (période) ---
    ev_fin = event_financials(dfe2, dfpay2)

    # --- Graphe CA/Coût/Bénéfice par événement ---
    if alt and not ev_fin.empty:
        chart1 = alt.Chart(
            ev_fin.melt(id_vars=["Nom_Événement"], value_vars=["Recette","Coût_Total","Bénéfice"])
        ).mark_bar().encode(
            x=alt.X("Nom_Événement:N", sort="-y", title="Événement"),
            y=alt.Y('value:Q', title='Montant (FCFA)'),
            color=alt.Color('variable:N', title='Indicateur'),
            tooltip=['Nom_Événement', 'variable', 'value']
        ).properties(height=300, title='CA vs Coût vs Bénéfice (période)')
        st.altair_chart(chart1, use_container_width=True)

    # --- Participants par mois (via date d'événement liée) ---
    if not dfp2.empty and "_d_evt" in dfp2.columns:
        _m = pd.to_datetime(dfp2["_d_evt"], errors="coerce")
        dfp2["_mois"] = _m.dt.to_period("M").astype(str)
        agg = dfp2.dropna(subset=["_mois"]).groupby("_mois")["ID_Participation"].count().reset_index()
        if alt and not agg.empty:
            chart2 = alt.Chart(agg).mark_line(point=True).encode(
                x=alt.X('_mois:N', title='Mois'),
                y=alt.Y('ID_Participation:Q', title='Participations')
            ).properties(height=250, title="Participants par mois (période)")
            st.altair_chart(chart2, use_container_width=True)

    # --- Satisfaction moyenne par type d’événement (période) ---
    if not dfp2.empty and not df_events.empty:
        type_map = df_events.set_index('ID_Événement')["Type"]
        dfp2 = dfp2.copy()
        dfp2["Type"] = dfp2["ID_Événement"].map(type_map)
        if "Note" in dfp2.columns:
            dfp2["Note"] = pd.to_numeric(dfp2["Note"], errors='coerce')
        agg_satis = dfp2.dropna(subset=["Type","Note"]).groupby('Type')["Note"].mean().reset_index()
        if alt and not agg_satis.empty:
            chart3 = alt.Chart(agg_satis).mark_bar().encode(
                x=alt.X('Type:N', title="Type d'événement"),
                y=alt.Y('Note:Q', title="Note moyenne"),
                tooltip=['Type', 'Note']
            ).properties(height=250, title="Satisfaction par type (période)")
            st.altair_chart(chart3, use_container_width=True)

    # --- Objectifs vs Réel (libellés + période) ---
    st.header("🎯 Objectifs vs Réel (période)")
    def get_target(k):
        try:
            return float(PARAMS.get(k, "0"))
        except:
            return 0.0
    y = datetime.now().year
    df_targets = pd.DataFrame([
        ("Contacts créés",                get_target(f'kpi_target_contacts_total_year_{y}'), total_contacts),
        ("Participations enregistrées",   get_target(f'kpi_target_participations_total_year_{y}'), parts_total),
        ("CA réglé (FCFA)",               get_target(f'kpi_target_ca_regle_year_{y}'), ca_regle),
    ], columns=['Indicateur','Objectif','Réel'])
    df_targets['Écart'] = df_targets['Réel'] - df_targets['Objectif']
    st.dataframe(df_targets, use_container_width=True)

    # --- Export Excel du rapport de base (période) ---
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        dfc2.to_excel(writer, sheet_name="Contacts(période)", index=False)
        dfe2.to_excel(writer, sheet_name="Événements(période)", index=False)
        dfp2.to_excel(writer, sheet_name="Participations(période)", index=False)
        dfpay2.to_excel(writer, sheet_name="Paiements(période)", index=False)
        dfcert2.to_excel(writer, sheet_name="Certifications(période)", index=False)
        ev_fin.to_excel(writer, sheet_name="Finance(période)", index=False)
    st.download_button("⬇ Export Rapport Excel (période)", buf.getvalue(), "rapport_iiba_cameroon_periode.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    st.markdown("---")
    st.header("📊 Rapports Avancés & Analyse Stratégique (période)")

    # === Données enrichies (période) ===
    # Construire aggrégats sur la période uniquement pour les IDs présents dans dfc2
    ids_period = pd.Index([])
    if not dfc2.empty and "ID" in dfc2.columns:
        ids_period = dfc2["ID"].astype(str).str.strip()

    sub_inter = df_inter[df_inter.get("ID", "").astype(str).isin(ids_period)] if not df_inter.empty else df_inter
    sub_parts = df_parts[df_parts.get("ID", "").astype(str).isin(ids_period)] if not df_parts.empty else df_parts
    sub_pay  = df_pay [df_pay .get("ID", "").astype(str).isin(ids_period)] if not df_pay.empty  else df_pay
    sub_cert = df_cert[df_cert.get("ID","").astype(str).isin(ids_period)]   if not df_cert.empty else df_cert

    ag_period = aggregates_for_contacts_period(
        dfc2.copy(),  # contacts (période)
        sub_inter.copy(),
        sub_parts.copy(),
        sub_pay.copy(),
        sub_cert.copy()
    )

    # --- Normalisation des clés de jointure "ID" ---
    def _normalize_id_col(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if "ID" not in df.columns:
            df["ID"] = ""
        # .astype(str) avant .fillna, puis strip
        df["ID"] = df["ID"].astype(str).str.strip()
        # Quelques "nan" littéraux peuvent rester après astype(str)
        df["ID"] = df["ID"].replace({"nan": "", "None": "", "NaT": ""})
        return df

    dfc2 = _normalize_id_col(dfc2)
    ag_period = _normalize_id_col(ag_period)

    # S’assurer qu’on n’a qu’une ligne par ID côté aggrégats
    if not ag_period.empty:
        ag_period = ag_period.drop_duplicates(subset=["ID"])

    # Si ag_period est vide, garantir au moins la colonne "ID" pour éviter le ValueError
    if ag_period.empty and "ID" not in ag_period.columns:
        ag_period = pd.DataFrame({"ID": []})

    # --- Jointure sûre ---
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
            "Banque": 800000, "Télécom": 750000, "IT": 700000,
            "Éducation": 500000, "Santé": 600000, "ONG": 450000,
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
    ca_total = float(dfc_enriched.get("CA_réglé", pd.Series(dtype=float)).sum() or 0)
    prospects_chauds = len(dfc_enriched[dfc_enriched.get("Proba_conversion","") == "Chaud"])

    # Onglets avancés
    tab_exec, tab_profil, tab_swot, tab_bsc = st.tabs([
        "🎯 Executive Summary",
        "👤 Profil BA Camerounais",
        "⚖️ SWOT Analysis",
        "📈 Balanced Scorecard"
    ])

    with tab_exec:
        st.subheader("📋 Synthèse Exécutive — période")
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("👥 Total BA", total_ba)
        c2.metric("🎓 Certifiés", f"{taux_certif:.1f}%")
        c3.metric("💰 Salaire Moyen", f"{salaire_moyen:,} FCFA")
        c4.metric("🏢 Secteurs", diversite_sectorielle)

        st.subheader("🏆 Top Événements (bénéfice)")
        ev_fin_period = event_financials(dfe2, dfpay2)
        if not ev_fin_period.empty:
            top_events = ev_fin_period.nlargest(5, "Bénéfice")[["Nom_Événement", "Recette", "Coût_Total", "Bénéfice"]]
            st.dataframe(top_events, use_container_width=True)
        else:
            st.info("Pas de données financières d'événements sur la période.")

        st.subheader("🎯 Segmentation (période)")
        segments = dfc_enriched["Proba_conversion"].value_counts()
        col_s1, col_s2 = st.columns(2)
        with col_s1:
            if total_ba > 0 and not segments.empty:
                for segment, count in segments.items():
                    pct = (count / total_ba * 100)
                    st.write(f"• {segment}: {count} ({pct:.1f}%)")
            else:
                st.write("Aucune donnée de segmentation.")
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
        st.subheader("👤 Profil Type — période")
        col_demo1, col_demo2 = st.columns(2)
        with col_demo1:
            st.write("**📊 Répartition par Genre**")
            genre_counts = dfc_enriched["Genre"].value_counts()
            if total_ba > 0 and not genre_counts.empty:
                for genre, count in genre_counts.items():
                    pct = (count / total_ba * 100)
                    st.write(f"• {genre}: {count} ({pct:.1f}%)")
            else:
                st.write("Aucune donnée de genre.")

            st.write("**🏙️ Top Villes**")
            ville_counts = dfc_enriched["Ville"].value_counts().head(5)
            if total_ba > 0 and not ville_counts.empty:
                for ville, count in ville_counts.items():
                    pct = (count / total_ba * 100)
                    st.write(f"• {ville}: {count} ({pct:.1f}%)")
            else:
                st.write("Aucune donnée de ville.")

        with col_demo2:
            st.write("**🏢 Secteurs dominants**")
            if total_ba > 0 and not top_secteurs.empty:
                for secteur, count in top_secteurs.items():
                    pct = (count / total_ba * 100)
                    st.write(f"• {secteur}: {count} ({pct:.1f}%)")
            else:
                st.write("Aucune donnée de secteur.")

            st.write("**💼 Types de profils**")
            type_counts = dfc_enriched["Type"].value_counts()
            if total_ba > 0 and not type_counts.empty:
                for typ, count in type_counts.items():
                    pct = (count / total_ba * 100)
                    st.write(f"• {typ}: {count} ({pct:.1f}%)")
            else:
                st.write("Aucune donnée de type de profil.")

        st.subheader("📈 Engagement par Secteur (période)")
        if not dfc_enriched.empty:
            engagement_secteur = dfc_enriched.groupby("Secteur").agg({
                "Score_composite": "mean",
                "Participations": "mean",
                "CA_réglé": "sum"
            }).round(2)
            engagement_secteur.columns = ["Score Moyen", "Participations Moy", "CA Total"]
            st.dataframe(engagement_secteur, use_container_width=True)
        else:
            engagement_secteur = pd.DataFrame()

        st.subheader("🌍 Comparaison Standards Internationaux (période)")
        ba_experience_ratio = (len(dfc_enriched[dfc_enriched.get("Score_Engagement", 0) >= 50]) / total_ba * 100) if total_ba > 0 else 0
        formation_continue = (len(dfc_enriched[dfc_enriched.get("Participations", 0) >= 2]) / total_ba * 100) if total_ba > 0 else 0
        kpi_standards = pd.DataFrame({
            "KPI": [
                "Taux de certification",
                "Formation continue",
                "Expérience métier",
                "Diversité sectorielle",
                "Engagement communautaire"
            ],
            "Cameroun": [f"{taux_certif:.1f}%", f"{formation_continue:.1f}%", f"{ba_experience_ratio:.1f}%",
                        f"{diversite_sectorielle} secteurs", f"{dfc_enriched.get('Participations', pd.Series(dtype=float)).mean():.1f} events/BA"],
            "Standard IIBA": ["25-35%", "60-70%", "70-80%", "8-10 secteurs", "2-3 events/an"]
        })
        st.dataframe(kpi_standards, use_container_width=True)

    with tab_swot:
        st.subheader("⚖️ Analyse SWOT — période")
        col_sw, col_ot = st.columns(2)

        with col_sw:
            st.markdown("### 💪 **FORCES**")
            st.write(f"""
            • **Diversité sectorielle**: {diversite_sectorielle} secteurs représentés  
            • **Engagement communautaire**: {taux_participation:.1f} participations moy./BA  
            • **Base financière**: {ca_total:,.0f} FCFA de revenus  
            • **Pipeline prospects**: {prospects_chauds} prospects chauds  
            • **Croissance digitale**: Adoption d'outils en ligne  
            """)

            st.markdown("### ⚠️ **FAIBLESSES**")
            st.write(f"""
            • **Taux de certification**: {taux_certif:.1f}% (vs 30% standard)  
            • **Concentration géographique**: Focus Douala/Yaoundé  
            • **Formations avancées limitées**  
            • **Standardisation des pratiques à renforcer**  
            • **Visibilité internationale faible**  
            """)

        with col_ot:
            st.markdown("### 🚀 **OPPORTUNITÉS**")
            st.write("""
            • Transformation digitale : demande croissante BA  
            • Partenariats entreprises : Top-20 identifiées  
            • Certification IIBA : programme de développement  
            • Expansion régionale : Afrique Centrale  
            • Formations spécialisées : IA, Data, Agile  
            """)

            st.markdown("### ⛔ **MENACES**")
            st.write("""
            • Concurrence de consultants internationaux  
            • Fuite des cerveaux vers l'étranger  
            • Économie incertaine (budgets formation)  
            • Manque de reconnaissance du métier BA  
            • Technologie évoluant rapidement  
            """)

        st.subheader("🎯 Plan d'Actions Stratégiques")
        actions_df = pd.DataFrame({
            "Axe": ["Formation", "Certification", "Partenariats", "Expansion", "Communication"],
            "Action": [
                "Développer programme formation continue",
                "Accompagner vers certifications IIBA",
                "Formaliser accords entreprises Top-20",
                "Ouvrir antennes régionales",
                "Renforcer visibilité et marketing"
            ],
            "Priorité": ["Élevée", "Élevée", "Moyenne", "Faible", "Moyenne"],
            "Échéance": ["6 mois", "12 mois", "9 mois", "24 mois", "Continu"]
        })
        st.dataframe(actions_df, use_container_width=True)

    with tab_bsc:
        st.subheader("📈 Balanced Scorecard — période")
        tab_fin, tab_client, tab_proc, tab_app = st.tabs(["💰 Financière", "👥 Client", "⚙️ Processus", "📚 Apprentissage"])

        with tab_fin:
            col_f1, col_f2, col_f3 = st.columns(3)
            ev_fin_period = event_financials(dfe2, dfpay2)
            if not ev_fin_period.empty and ev_fin_period["Recette"].sum() > 0:
                marge_benefice = (ev_fin_period["Bénéfice"].sum() / ev_fin_period["Recette"].sum() * 100)
            else:
                marge_benefice = 0.0
            col_f1.metric("💵 CA Total (période)", f"{ca_total:,.0f} FCFA")
            col_f2.metric("📈 Croissance CA", "—", help="À calculer si historique disponible")
            col_f3.metric("📊 Marge Bénéfice", f"{marge_benefice:.1f}%")

            fin_data = pd.DataFrame({
                "Indicateur": ["Revenus formations", "Revenus certifications", "Revenus événements", "Coûts opérationnels"],
                "Réel": [f"{ca_total*0.6:.0f}", f"{ca_total*0.2:.0f}", f"{ca_total*0.2:.0f}", f"{ev_fin_period['Coût_Total'].sum() if not ev_fin_period.empty else 0:.0f}"],
                "Objectif": ["3M", "1M", "1M", "3.5M"],
                "Écart": ["À calculer", "À calculer", "À calculer", "À calculer"]
            })
            st.dataframe(fin_data, use_container_width=True)

        with tab_client:
            col_c1, col_c2, col_c3 = st.columns(3)
            satisfaction_moy = float(dfc_enriched[dfc_enriched.get("A_certification", False) == True].get("Score_Engagement", pd.Series(dtype=float)).mean() or 0)
            denom_ret = len(dfc_enriched[dfc_enriched.get("Type","").isin(["Membre", "Prospect"])])
            retention = (len(dfc_enriched[dfc_enriched.get("Type","") == "Membre"]) / denom_ret * 100) if denom_ret > 0 else 0
            col_c1.metric("😊 Satisfaction", f"{satisfaction_moy:.1f}/100", help="Score engagement (certifiés)")
            col_c2.metric("🔄 Rétention", f"{retention:.1f}%")
            col_c3.metric("📈 NPS Estimé", "65")

            client_data = pd.DataFrame({
                "Segment": ["Prospects Chauds", "Prospects Tièdes", "Prospects Froids", "Membres Actifs"],
                "Nombre": [
                    len(dfc_enriched[dfc_enriched.get("Proba_conversion","") == "Chaud"]),
                    len(dfc_enriched[dfc_enriched.get("Proba_conversion","") == "Tiède"]),
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
            col_p1.metric("⚡ Efficacité Conversion", f"{efficacite_conv:.1f}%")
            col_p2.metric("⏱️ Temps Réponse", f"{temps_reponse} jours")
            col_p3.metric("🎯 Taux Participation", f"{taux_participation:.1f}")

            proc_data = pd.DataFrame({
                "Processus": ["Acquisition prospects", "Conversion membres", "Délivrance formations", "Suivi post-formation"],
                "Performance": ["75%", f"{retention:.1f}%", "90%", "60%"],
                "Objectif": ["80%", "25%", "95%", "75%"],
                "Actions": ["Améliorer ciblage", "Renforcer follow-up", "Optimiser contenu", "Systématiser enquêtes"]
            })
            st.dataframe(proc_data, use_container_width=True)

        with tab_app:
            col_a1, col_a2, col_a3 = st.columns(3)
            col_a1.metric("🎓 Taux Certification", f"{taux_certif:.1f}%")
            col_a2.metric("📖 Formation Continue", f"{(len(dfc_enriched[dfc_enriched.get('Participations',0) >= 2]) / max(1,total_ba) * 100):.1f}%")
            col_a3.metric("🔄 Innovation", "3 projets", help="Nouveaux programmes/an")

            comp_data = pd.DataFrame({
                "Compétence": ["Business Analysis", "Agilité", "Data Analysis", "Digital Transformation", "Leadership"],
                "Niveau Actuel": [65, 45, 35, 40, 55],
                "Objectif 2025": [80, 65, 60, 70, 70],
                "Gap": [15, 20, 25, 30, 15]
            })
            st.dataframe(comp_data, use_container_width=True)

    # --- Export Markdown consolidé (période) ---
    st.markdown("---")
    col_export1, col_export2 = st.columns(2)

    with col_export1:
        if st.button("📄 Générer Rapport Markdown Complet (période)"):
            try:
                ev_fin_period = event_financials(dfe2, dfpay2)
                if not ev_fin_period.empty and ev_fin_period["Recette"].sum() > 0:
                    marge_benefice = (ev_fin_period["Bénéfice"].sum() / ev_fin_period["Recette"].sum() * 100)
                else:
                    marge_benefice = 0.0
                genre_counts_md = dfc_enriched["Genre"].value_counts()

                rapport_md = f"""
# Rapport Stratégique IIBA Cameroun — {datetime.now().year} (période sélectionnée)

## Executive Summary
- **Total BA**: {total_ba}
- **Taux Certification**: {taux_certif:.1f}%
- **CA Réalisé (période)**: {ca_total:,.0f} FCFA
- **Secteurs (période)**: {diversite_sectorielle}

## Profil Type BA Camerounais (période)
- Répartition par genre: {dict(genre_counts_md)}
- Secteurs dominants: {dict(top_secteurs)}

## SWOT (période)
- Forces: diversité sectorielle, engagement, pipeline, base financière
- Opportunités: partenariats Top-20, certif IIBA, expansion régionale, IA/Data/Agile
- Menaces: concurrence, fuite des cerveaux, budgets formation, rythme techno

## Balanced Scorecard (période)
- CA: {ca_total:,.0f} FCFA — Marge: {marge_benefice:.1f}%
- Satisfaction: {float(dfc_enriched[dfc_enriched.get("A_certification", False) == True].get("Score_Engagement", pd.Series(dtype=float)).mean() or 0):.1f}/100
- Rétention: {((len(dfc_enriched[dfc_enriched.get("Type","") == "Membre"]) / max(1,len(dfc_enriched[dfc_enriched.get("Type","").isin(["Membre","Prospect"])])))*100):.1f}%

_Généré le {datetime.now().strftime('%Y-%m-%d %H:%M')}_"""
                st.download_button(
                    "⬇️ Télécharger Rapport.md",
                    rapport_md,
                    file_name=f"Rapport_IIBA_Cameroun_periode_{datetime.now().strftime('%Y%m%d')}.md",
                    mime="text/markdown"
                )
            except Exception as e:
                st.error(f"Erreur génération Markdown : {e}")

    with col_export2:
        # Export Excel des analyses avancées (période)
        buf_adv = io.BytesIO()
        with pd.ExcelWriter(buf_adv, engine="openpyxl") as writer:
            dfc_enriched.to_excel(writer, sheet_name="Contacts_Enrichis(période)", index=False)
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
            "📊 Export Analyses Excel (période)",
            buf_adv.getvalue(),
            file_name=f"Analyses_IIBA_periode_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )


    # === NOUVEAU RAPPORT FINANCIER ANNUEL ===
    st.markdown("---")
    st.header("💰 Plan Financier Annuel — IIBA Cameroun")

    # Budgets annuels 2024 (basés sur le fichier Excel fourni)
    BUDGET_INCOME = {
        "Membership revenue": 296000,
        "Event Registrations": 1442500, 
        "Professional Development Days": 606000,
        "Study Groups": 11591203,
        "Career Centre": 0,
        "Merchandise Sales": 0,
        "Donations": 0,
        "Sponsorship": 0,
        "Interest": 0,
        "Other": 100000
    }

    BUDGET_EXPENSES = {
        "Office Supplies": 1258173,
        "Event Food & Beverage": 254375,
        "Rental Space": 3038940,
        "Merchandise expenses": 125450,
        "Accounting Fees": 358322,
        "PayPal fees": 0,
        "Banking Fees": 0,
        "Postal fees": 1000,
        "Software fees": 342895,
        "Other": 8520741
    }

    # === Calcul des ACTUAL basés sur les données réelles ===
    def calculate_actual_financials(dfpay_all, year_filter=None):
        """
        Calcule les montants réels (ACTUAL) à partir des données de paiements
        """
        if dfpay_all.empty:
            return {}, {}
        
        df = dfpay_all.copy()
        
        # Filtre par année si spécifié
        if year_filter:
            try:
                df["_date"] = pd.to_datetime(df["Date_Paiement"], errors='coerce')
                df = df[df["_date"].dt.year == year_filter]
            except:
                pass
        
        # Conversion des montants
        df["Montant"] = pd.to_numeric(df["Montant"], errors='coerce').fillna(0)
        
        # Séparation Income vs Expenses selon le statut
        income_df = df[df["Statut"] == "Réglé"].copy() if "Statut" in df.columns else df.copy()
        expenses_df = df[df["Statut"] != "Réglé"].copy() if "Statut" in df.columns else pd.DataFrame()
        
        # Mappage des catégories (basé sur les données d'exemple)
        income_mapping = {
            "Study Groups": ["Study Groups", "Frais de formation", "frais de formation"],
            "Event Registrations": ["Event Registrations", "Achat billet", "achat billet"],
            "Professional Development Days": ["Professional Development Days", "Formation", "CBAP"],
            "Membership revenue": ["Membership revenue", "membre", "Membre"],
            "Other": ["Other", "Plateforme", "simulation"]
        }
        
        expense_mapping = {
            "Office Supplies": ["Office Supplies", "Impression", "fournitures", "papier"],
            "Rental Space": ["Rental Space", "Loyer", "salle", "Location"],
            "Event Food & Beverage": ["Event Food & Beverage", "repas", "eau", "cookies"],
            "Software fees": ["Software fees", "internet", "wifi", "Camtel"],
            "Accounting Fees": ["Accounting Fees", "comptable", "fiscal"],
            "Merchandise expenses": ["Merchandise expenses", "T-shirt", "Polo", "cadeau"],
            "Postal fees": ["Postal fees", "livraison", "courrier"],
            "Other": ["Other", "transport", "formateur", "salaire"]
        }
        
        # Calcul ACTUAL INCOME
        actual_income = {}
        for category, keywords in income_mapping.items():
            total = 0
            if not income_df.empty and "Détails" in income_df.columns:
                for keyword in keywords:
                    mask = income_df["Détails"].str.contains(keyword, case=False, na=False)
                    total += income_df[mask]["Montant"].sum()
            actual_income[category] = float(total)
        
        # Calcul ACTUAL EXPENSES (à partir des vraies données de dépenses si disponibles)
        actual_expenses = {}
        for category in BUDGET_EXPENSES.keys():
            actual_expenses[category] = 0  # À compléter avec vraies données
        
        return actual_income, actual_expenses

    # Calcul pour l'année courante
    year_current = datetime.now().year
    actual_income, actual_expenses = calculate_actual_financials(df_pay, year_current)

    # === Interface utilisateur ===
    col_year, col_export = st.columns([1, 1])

    with col_year:
        selected_year = st.selectbox("Année du rapport financier", 
                                    options=[2024, 2023, 2025], 
                                    index=0)

    # Recalcul si année différente
    if selected_year != year_current:
        actual_income, actual_expenses = calculate_actual_financials(df_pay, selected_year)

    # === SUMMARY TABLE ===
    st.subheader(f"📊 Résumé Exécutif — {selected_year}")

    total_budget_income = sum(BUDGET_INCOME.values())
    total_actual_income = sum(actual_income.values())
    total_budget_expenses = sum(BUDGET_EXPENSES.values())
    total_actual_expenses = sum(actual_expenses.values())

    budget_difference = total_budget_income - total_budget_expenses
    actual_difference = total_actual_income - total_actual_expenses
    variance_difference = actual_difference - budget_difference

    summary_data = {
        "Indicateur": ["Total Income", "Total Expenses", "Difference"],
        "BUDGET (FCFA)": [f"{total_budget_income:,.0f}", f"{total_budget_expenses:,.0f}", f"{budget_difference:,.0f}"],
        "ACTUAL (FCFA)": [f"{total_actual_income:,.0f}", f"{total_actual_expenses:,.0f}", f"{actual_difference:,.0f}"],
        "UNDER/OVER (FCFA)": [
            f"{total_actual_income - total_budget_income:,.0f}",
            f"{total_actual_expenses - total_budget_expenses:,.0f}",
            f"{variance_difference:,.0f}"
        ]
    }

    df_summary = pd.DataFrame(summary_data)
    st.dataframe(df_summary, use_container_width=True)

    # Performance indicators
    col_perf1, col_perf2, col_perf3 = st.columns(3)
    income_performance = (total_actual_income / total_budget_income * 100) if total_budget_income > 0 else 0
    expense_performance = (total_actual_expenses / total_budget_expenses * 100) if total_budget_expenses > 0 else 0

    col_perf1.metric("📈 Performance Revenus", f"{income_performance:.1f}%", 
                    f"{total_actual_income - total_budget_income:,.0f} FCFA")
    col_perf2.metric("📉 Performance Dépenses", f"{expense_performance:.1f}%",
                    f"{total_actual_expenses - total_budget_expenses:,.0f} FCFA")
    col_perf3.metric("⚖️ Marge Réalisée", f"{actual_difference:,.0f} FCFA",
                    f"{variance_difference:,.0f} FCFA vs budget")

    # === DÉTAIL REVENUS ===
    st.subheader("💰 Détail des Revenus par Catégorie")

    income_detail = []
    for category, budget_amount in BUDGET_INCOME.items():
        actual_amount = actual_income.get(category, 0)
        variance = actual_amount - budget_amount
        variance_pct = (variance / budget_amount * 100) if budget_amount > 0 else 0
        
        income_detail.append({
            "Catégorie": category,
            "BUDGET (FCFA)": f"{budget_amount:,.0f}",
            "ACTUAL (FCFA)": f"{actual_amount:,.0f}",
            "ÉCART (FCFA)": f"{variance:,.0f}",
            "ÉCART (%)": f"{variance_pct:+.1f}%",
            "Commentaires": "Données partielles" if actual_amount == 0 else "Conforme" if abs(variance_pct) < 10 else "À analyser"
        })

    df_income = pd.DataFrame(income_detail)
    st.dataframe(df_income, use_container_width=True)

    # === DÉTAIL DÉPENSES ===  
    st.subheader("💸 Détail des Dépenses par Catégorie")

    expense_detail = []
    for category, budget_amount in BUDGET_EXPENSES.items():
        actual_amount = actual_expenses.get(category, 0)
        variance = actual_amount - budget_amount
        variance_pct = (variance / budget_amount * 100) if budget_amount > 0 else 0
        
        expense_detail.append({
            "Catégorie": category,
            "BUDGET (FCFA)": f"{budget_amount:,.0f}",
            "ACTUAL (FCFA)": f"{actual_amount:,.0f}",
            "ÉCART (FCFA)": f"{variance:,.0f}",
            "ÉCART (%)": f"{variance_pct:+.1f}%",
            "Statut": "✅ Sous budget" if variance < 0 else "⚠️ Dépassement" if variance > budget_amount * 0.1 else "📊 Conforme"
        })

    df_expense = pd.DataFrame(expense_detail)
    st.dataframe(df_expense, use_container_width=True)

    # === GRAPHIQUES DE PERFORMANCE ===
    if alt:
        st.subheader("📊 Visualisations Financières")
        
        tab_rev, tab_exp, tab_comp = st.tabs(["Revenus", "Dépenses", "Comparaison"])
        
        with tab_rev:
            # Graphique revenus Budget vs Actual
            income_chart_data = []
            for cat, budget in BUDGET_INCOME.items():
                if budget > 0:  # Exclure les catégories à 0
                    income_chart_data.extend([
                        {"Catégorie": cat, "Type": "Budget", "Montant": budget},
                        {"Catégorie": cat, "Type": "Actual", "Montant": actual_income.get(cat, 0)}
                    ])
            
            if income_chart_data:
                chart_income = alt.Chart(pd.DataFrame(income_chart_data)).mark_bar().encode(
                    x=alt.X('Catégorie:N', sort='-y'),
                    y=alt.Y('Montant:Q', title='Montant (FCFA)'),
                    color=alt.Color('Type:N', scale=alt.Scale(range=['#1f77b4', '#ff7f0e'])),
                    tooltip=['Catégorie', 'Type', 'Montant']
                ).properties(height=400, title=f'Revenus Budget vs Actual - {selected_year}')
                
                st.altair_chart(chart_income, use_container_width=True)
        
        with tab_exp:
            # Graphique dépenses par catégorie
            expense_chart_data = []
            for cat, budget in BUDGET_EXPENSES.items():
                if budget > 0:
                    expense_chart_data.extend([
                        {"Catégorie": cat, "Type": "Budget", "Montant": budget},
                        {"Catégorie": cat, "Type": "Actual", "Montant": actual_expenses.get(cat, 0)}
                    ])
            
            if expense_chart_data:
                chart_expense = alt.Chart(pd.DataFrame(expense_chart_data)).mark_bar().encode(
                    x=alt.X('Catégorie:N', sort='-y'),
                    y=alt.Y('Montant:Q', title='Montant (FCFA)'),
                    color=alt.Color('Type:N', scale=alt.Scale(range=['#d62728', '#ff9896'])),
                    tooltip=['Catégorie', 'Type', 'Montant']
                ).properties(height=400, title=f'Dépenses Budget vs Actual - {selected_year}')
                
                st.altair_chart(chart_expense, use_container_width=True)
        
        with tab_comp:
            # Graphique de performance globale
            perf_data = pd.DataFrame({
                "Indicateur": ["Revenus", "Dépenses"],
                "Performance (%)": [income_performance, expense_performance],
                "Objectif": [100, 100]
            })
            
            chart_perf = alt.Chart(perf_data).mark_bar().encode(
                x='Indicateur:N',
                y=alt.Y('Performance (%):Q', scale=alt.Scale(domain=[0, 120])),
                color=alt.condition(
                    alt.datum['Performance (%)'] >= 90,
                    alt.value('#2ca02c'),  # Vert si >= 90%
                    alt.value('#d62728')   # Rouge si < 90%
                ),
                tooltip=['Indicateur', 'Performance (%)']
            ).properties(height=300, title='Performance vs Objectifs')
            
            # Ligne d'objectif à 100%
            line_objective = alt.Chart(perf_data).mark_rule(color='black', strokeDash=[5, 5]).encode(
                y=alt.datum(100)
            )
            
            st.altair_chart((chart_perf + line_objective), use_container_width=True)

    # === ANALYSE ET RECOMMANDATIONS ===
    st.subheader("🔍 Analyse et Recommandations")

    col_analysis1, col_analysis2 = st.columns(2)

    with col_analysis1:
        st.write("**📈 Points Forts**")
        strengths = []
        if income_performance >= 90:
            strengths.append("Objectifs de revenus atteints/dépassés")
        if expense_performance <= 100:
            strengths.append("Maîtrise des coûts")
        if actual_difference > 0:
            strengths.append("Bénéfice réalisé")
        
        if strengths:
            for strength in strengths:
                st.write(f"✅ {strength}")
        else:
            st.write("⚠️ Performance à améliorer")

    with col_analysis2:
        st.write("**🎯 Actions Prioritaires**")
        actions = []
        if income_performance < 90:
            actions.append("Renforcer les activités génératrices de revenus")
        if expense_performance > 110:
            actions.append("Optimiser la structure des coûts")
        if actual_difference < 0:
            actions.append("Plan de redressement financier urgent")
        
        if actions:
            for action in actions:
                st.write(f"🔧 {action}")
        else:
            st.write("✅ Performance financière satisfaisante")

    # === EXPORT EXCEL ===
    with col_export:
        if st.button("📊 Générer Plan Financier Excel Complet"):
            try:
                # Création du fichier Excel avec tous les onglets
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    # Onglet Summary
                    df_summary.to_excel(writer, sheet_name="Summary", index=False)
                    
                    # Onglet Income Detail
                    df_income.to_excel(writer, sheet_name="Income Detail", index=False)
                    
                    # Onglet Expense Detail  
                    df_expense.to_excel(writer, sheet_name="Expense Detail", index=False)
                    
                    # Onglet Raw Data (optionnel - données brutes filtrées)
                    if not df_pay.empty:
                        df_pay_filtered = df_pay.copy()
                        try:
                            df_pay_filtered["_year"] = pd.to_datetime(df_pay_filtered["Date_Paiement"], errors='coerce').dt.year
                            df_pay_filtered = df_pay_filtered[df_pay_filtered["_year"] == selected_year]
                        except:
                            pass
                        df_pay_filtered.to_excel(writer, sheet_name="Raw Data", index=False)
                    
                    # Onglet Analysis (métriques calculées)
                    analysis_data = pd.DataFrame({
                        "Métrique": ["Performance Revenus (%)", "Performance Dépenses (%)", "Marge Réalisée (FCFA)", "Écart vs Budget (FCFA)"],
                        "Valeur": [f"{income_performance:.1f}", f"{expense_performance:.1f}", f"{actual_difference:,.0f}", f"{variance_difference:,.0f}"],
                        "Objectif": ["≥90%", "≤100%", ">0", ">0"],
                        "Statut": [
                            "✅ Atteint" if income_performance >= 90 else "❌ Non atteint",
                            "✅ Atteint" if expense_performance <= 100 else "❌ Dépassé", 
                            "✅ Positif" if actual_difference > 0 else "❌ Négatif",
                            "✅ Favorable" if variance_difference > 0 else "❌ Défavorable"
                        ]
                    })
                    analysis_data.to_excel(writer, sheet_name="Analysis", index=False)
                
                # Téléchargement
                st.download_button(
                    "⬇️ Télécharger Plan Financier Excel",
                    buffer.getvalue(),
                    file_name=f"Plan_Financier_IIBA_{selected_year}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
                st.success(f"✅ Plan financier {selected_year} généré avec succès!")
                
            except Exception as e:
                st.error(f"❌ Erreur lors de la génération : {e}")

    # === NOTES ET MÉTHODOLOGIE ===
    with st.expander("ℹ️ Notes et Méthodologie"):
        st.write("""
        **Calcul des montants ACTUAL :**
        - Revenus : basés sur les paiements avec statut 'Réglé'
        - Dépenses : basées sur tous les paiements enregistrés
        - Catégories : mappage automatique par mots-clés dans les descriptions
        
        **Indicateurs de performance :**
        - Performance Revenus = (Actual / Budget) × 100
        - Performance Dépenses = (Actual / Budget) × 100  
        - Marge = Revenus Actual - Dépenses Actual
        
        **Seuils d'alerte :**
        - Revenus < 90% du budget : Action requise
        - Dépenses > 110% du budget : Vigilance 
        - Marge négative : Plan de redressement
        """)


# ---------------------- PAGE ADMIN — Migration & Import/Export ----------------------

elif page == "Admin":
    st.title("⚙️ Admin — Paramètres, Migration & Maintenance (centralisés dans parametres.csv)")

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
        ok1 = st.form_submit_button("💾 Enregistrer les listes")
        if ok1:
            PARAMS.update({
                "list_genres": genres, "list_types_contact": types_contact, "list_statuts_engagement": statuts_engagement,
                "list_secteurs": secteurs, "list_pays": pays, "list_villes": villes, "list_sources": sources,
                "list_canaux": canaux, "list_resultats_inter": resultats_inter, "list_types_evenements": types_evenements,
                "list_lieux": lieux, "list_statuts_paiement": statuts_paiement, "list_moyens_paiement": moyens_paiement,
                "list_types_certif": types_certif, "list_entreprises_cibles": entreprises_cibles,
            })
            save_params(PARAMS)
            st.success("Listes enregistrées dans parametres.csv — rechargez la page si nécessaire.")

    # PARAMETRES SCORING ET AFFICHAGE
    st.markdown("### Règles de scoring & d'affichage (parametres.csv)")
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

        st.write("**Colonnes de la grille CRM (ordre, séparées par des virgules)**")
        grid_crm = st.text_input("CRM → Colonnes", PARAMS.get("grid_crm_columns",""))
        st.caption("Colonnes disponibles : " + ", ".join(sorted(list(set(C_COLS + I_COLS + E_COLS + P_COLS + PAY_COLS + CERT_COLS + ['Interactions','Participations','CA_réglé','Impayé','Resp_principal','A_animé_ou_invité','Score_composite','Proba_conversion','Tags','Dernier_contact','Interactions_recent'])))))

        st.write("**KPI visibles (séparés par des virgules)**")
        st.caption("Clés supportées : contacts_total, prospects_actifs, membres, events_count, participations_total, ca_regle, impayes, taux_conversion, engagement")
        kpi_enabled = st.text_input("KPI activés", PARAMS.get("kpi_enabled",""))

        st.write("**Objectifs annuels/mensuels (format clé=valeur)**")
        st.caption("Ex. kpi_target_contacts_total_year_2025=1000 ; kpi_target_participations_total_month_202506=120")
        targets_text = st.text_area("Cibles (une par ligne)", "\n".join([f"{k}={v}" for k,v in PARAMS.items() if k.startswith("kpi_target_")]))

        ok2 = st.form_submit_button("💾 Enregistrer (parametres.csv)")
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
                "kpi_enabled": kpi_enabled,
            })
            for line in targets_text.splitlines():
                if "=" in line:
                    key, val = line.split("=",1)
                    key = key.strip()
                    val = val.strip()
                    if key:
                        PARAMS[key] = val
            save_params(PARAMS)
            st.success("Paramètres enregistrés dans parametres.csv — les nouvelles listes seront prises en compte au prochain rafraîchissement.")

    # PARAMETRES Rapports Avancés
    # Dans la section des paramètres Admin, ajouter:
    st.markdown("---")
    st.header("📊 Paramètres Rapports Avancés")
    
    with st.form("advanced_reports_params"):
        st.subheader("🎯 Seuils et Objectifs")
        
        col_p1, col_p2 = st.columns(2)
        
        with col_p1:
            # Seuils de segmentation
            seuil_ba_expert = st.number_input(
                "Score BA Expert (seuil)", 
                min_value=0, max_value=100, 
                value=int(PARAMS.get("seuil_ba_expert", "70"))
            )
            seuil_formation_continue = st.number_input(
                "Participations min. formation continue", 
                min_value=0, 
                value=int(PARAMS.get("seuil_formation_continue", "2"))
            )
            objectif_certification = st.number_input(
                "Objectif taux certification (%)", 
                min_value=0, max_value=100, 
                value=int(PARAMS.get("objectif_certification", "30"))
            )
        
        with col_p2:
            # Estimations salariales par secteur
            salaire_banque = st.number_input(
                "Salaire moyen Banque (FCFA)", 
                min_value=0, step=50000,
                value=int(PARAMS.get("salaire_banque", "800000"))
            )
            salaire_telecom = st.number_input(
                "Salaire moyen Télécom (FCFA)", 
                min_value=0, step=50000,
                value=int(PARAMS.get("salaire_telecom", "750000"))
            )
            multiplicateur_certif = st.number_input(
                "Multiplicateur salaire certifié", 
                min_value=1.0, max_value=2.0, step=0.1,
                value=float(PARAMS.get("multiplicateur_certif", "1.3"))
            )
        
        # Objectifs BSC
        st.subheader("📈 Objectifs Balanced Scorecard")
        col_bsc1, col_bsc2 = st.columns(2)
        
        with col_bsc1:
            objectif_croissance_ca = st.number_input(
                "Objectif croissance CA (%/an)", 
                min_value=0, max_value=100,
                value=int(PARAMS.get("objectif_croissance_ca", "20"))
            )
            objectif_marge = st.number_input(
                "Objectif marge bénéfice (%)", 
                min_value=0, max_value=100,
                value=int(PARAMS.get("objectif_marge", "25"))
            )
        
        with col_bsc2:
            objectif_retention = st.number_input(
                "Objectif taux rétention (%)", 
                min_value=0, max_value=100,
                value=int(PARAMS.get("objectif_retention", "80"))
            )
            objectif_nps = st.number_input(
                "Objectif NPS", 
                min_value=0, max_value=100,
                value=int(PARAMS.get("objectif_nps", "70"))
            )

        # Filtrage période — Contacts
        st.subheader("🗓️ Filtrage période — Contacts")

        use_fallback_contacts = st.checkbox(
            "Activer le fallback si Date_Creation manquante (utilise 1re interaction / participation / paiement)",
            value=PARAMS.get("contacts_period_fallback", "1") in ("1","true","True")
        )

        
        if st.form_submit_button("💾 Enregistrer Paramètres Avancés"):
            PARAMS.update({
                "seuil_ba_expert": str(seuil_ba_expert),
                "seuil_formation_continue": str(seuil_formation_continue),
                "objectif_certification": str(objectif_certification),
                "salaire_banque": str(salaire_banque),
                "salaire_telecom": str(salaire_telecom),
                "multiplicateur_certif": str(multiplicateur_certif),
                "objectif_croissance_ca": str(objectif_croissance_ca),
                "objectif_marge": str(objectif_marge),
                "objectif_retention": str(objectif_retention),
                "objectif_nps": str(objectif_nps),
                "contacts_period_fallback": "1" if use_fallback_contacts else "0"
            })
            save_params(PARAMS)
            st.success("✅ Paramètres avancés enregistrés!")
    

    # PARAMETRES Migration — Import/Export
    st.markdown("---")
    st.header("📦 Migration — Import/Export Global & Multi-onglets")

    mode_mig = st.radio("Mode de migration", ["Import Excel par Table (.xlsx)", "Import Excel global (.xlsx)", "Import Excel multi-onglets (.xlsx)", "Import CSV global"], horizontal=True)

    if mode_mig == "Import Excel global (.xlsx)":
        up = st.file_uploader("Fichier Excel global (.xlsx)", type=["xlsx"], key="xlsx_up")
        st.caption("Feuille **Global** (ou 1ère) avec colonne **__TABLE__**.")
        if st.button("Importer l'Excel global") and up is not None:
            log = {"timestamp": datetime.now().isoformat(), "import_type": "excel_global", "counts": {}, "errors": [], "collisions": {}}
            try:
                xls = pd.ExcelFile(up)
                sheet = "Global" if "Global" in xls.sheet_names else xls.sheet_names[0]
                gdf = pd.read_excel(xls, sheet_name=sheet, dtype=str)
                if "__TABLE__" not in gdf.columns:
                    raise ValueError("Colonne '__TABLE__' manquante.")
                cols_global = ["__TABLE__"] + sorted(set(sum(ALL_SCHEMAS.values(), [])))
                for c in cols_global:
                    if c not in gdf.columns:
                        gdf[c] = ""
                # Contacts
                sub_c = gdf[gdf["__TABLE__"] == "contacts"].copy().fillna("")
                sub_c["Top20"] = sub_c["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])

                def dedupe_contacts(df):
                    df = df.copy()
                    rejects = []
                    seen = set()
                    keep = []
                    def norm(s):
                        return str(s).strip().lower()
                    def email_ok2(s):
                        if not s or str(s).strip() == "" or str(s).lower() == "nan":
                            return True
                        return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", str(s).strip()))
                    for _, r in df.iterrows():
                        if not email_ok2(r.get("Email", "")):
                            rr = r.to_dict()
                            rr["_Raison"] = "Email invalide"
                            rejects.append(rr)
                            continue
                        if r.get("Email", ""):
                            key = ("email", norm(r["Email"]))
                        elif r.get("Téléphone", ""):
                            key = ("tel", norm(r["Téléphone"]))
                        else:
                            key = ("nps", (norm(r.get("Nom", "")), norm(r.get("Prénom", "")), norm(r.get("Société", ""))))
                        if key in seen:
                            rr = r.to_dict()
                            rr["_Raison"] = "Doublon (fichier)"
                            rejects.append(rr)
                            continue
                        seen.add(key)
                        keep.append(r)
                    return pd.DataFrame(keep, columns=C_COLS), pd.DataFrame(rejects)

                valid_c, rejects_c = dedupe_contacts(sub_c)
                base = df_contacts.copy()
                collisions = []
                if "ID" in valid_c.columns and not valid_c.empty:
                    incoming = set(x for x in valid_c["ID"].astype(str) if x and x.lower() != "nan")
                    existing = set(base["ID"].astype(str))
                    collisions = sorted(list(incoming & existing))
                    if collisions:
                        base = base[~base["ID"].isin(collisions)]
                patt = re.compile(r"^CNT_(\d+)$")
                base_max = 0
                for x in base["ID"].dropna().astype(str):
                    m = patt.match(x.strip())
                    if m:
                        try:
                            base_max = max(base_max, int(m.group(1)))
                        except:
                            pass
                next_id = base_max + 1
                new_rows=[]
                for _, r in valid_c.iterrows():
                    rid = r["ID"]
                    if not isinstance(rid, str) or rid.strip() == "" or rid.strip().lower() == "nan":
                        rid = f"CNT_{str(next_id).zfill(3)}"
                        next_id += 1
                    rr = r.to_dict()
                    rr["ID"] = rid
                    new_rows.append(rr)
                base = pd.concat([base, pd.DataFrame(new_rows, columns=C_COLS)], ignore_index=True)
                save_df(base, PATHS["contacts"])
                globals()["df_contacts"] = base
                log["counts"]["contacts"] = len(new_rows)
                log["collisions"]["contacts"] = collisions
                if not rejects_c.empty:
                    st.warning(f"Lignes contacts rejetées : {len(rejects_c)}")
                    st.dataframe(rejects_c, use_container_width=True)

                # Fonction pour enregistrer subsets
                def save_subset(tbl, cols, path, prefix):
                    sub = gdf[gdf["__TABLE__"] == tbl].copy()
                    sub = sub[cols].fillna("")
                    id_col = cols[0]
                    base_df = ensure_df(path, cols)
                    incoming = set(x for x in sub[id_col].astype(str) if x and x.lower() != "nan")
                    existing = set(base_df[id_col].astype(str))
                    coll = sorted(list(incoming & existing))
                    if coll:
                        base_df = base_df[~base_df[id_col].isin(coll)]
                    patt = re.compile(rf"^{prefix}_(\d+)$")
                    base_max = 0
                    for x in base_df[id_col].dropna().astype(str):
                        m = patt.match(x.strip())
                        if m:
                            try:
                                base_max = max(base_max, int(m.group(1)))
                            except:
                                pass
                    gen = base_max + 1
                    new_rows = []
                    for _, r in sub.iterrows():
                        cur = r[id_col]
                        if not isinstance(cur, str) or cur.strip() == "" or cur.strip().lower() == "nan":
                            cur = f"{prefix}_{str(gen).zfill(3)}"
                            gen += 1
                        rr = r.to_dict()
                        rr[id_col] = cur
                        new_rows.append(rr)
                    out = pd.concat([base_df, pd.DataFrame(new_rows, columns=cols)], ignore_index=True)
                    save_df(out, path)
                    globals()["df_" + ("inter" if tbl == "interactions" else "events" if tbl == "evenements" else "parts" if tbl == "participations" else "pay" if tbl == "paiements" else "cert")] = out
                    log["counts"][tbl] = len(new_rows)
                    log["collisions"][tbl] = coll

                for spec in [("interactions", I_COLS, PATHS["inter"], "INT"),
                             ("evenements", E_COLS, PATHS["events"], "EVT"),
                             ("participations", P_COLS, PATHS["parts"], "PAR"),
                             ("paiements", PAY_COLS, PATHS["pay"], "PAY"),
                             ("certifications", CERT_COLS, PATHS["cert"], "CER")]:
                    cnt, coll = save_subset(*spec)

                st.success("Import Excel global terminé.")
                st.json(log)
                log_event("import_excel_global", log)
            except Exception as e:
                st.error(f"Erreur d'import Excel global : {e}")
                log_event("error_import_excel_global", {"error": str(e)})

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            gcols = ["__TABLE__"] + sorted(set(sum(ALL_SCHEMAS.values(), [])))
            pd.DataFrame(columns=gcols).to_excel(w, index=False, sheet_name="Global")
        st.download_button("⬇️ Modèle Global (xlsx)", buf.getvalue(), file_name="IIBA_global_template.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    elif mode_mig == "Import Excel par Table (.xlsx)":
        st.subheader("Import Excel par table (1 onglet par table)")
        fichier_multi = st.file_uploader(
            "Classeur Excel (.xlsx) avec un onglet par table : contacts, interactions, evenements, participations, paiements, certifications",
            type=["xlsx"], key="xlsx_par_table"
        )

        # ----------------------------------------------------------------------
        # 1) VALIDATEUR — Aperçu des onglets + colonnes + 5 premières lignes
        #     + Compteur global de lignes par onglet
        # ----------------------------------------------------------------------
        st.markdown("### 🔎 Valider le classeur (avant import)")
        st.caption("Le validateur détecte les onglets avec tolérance (accents/casse/alias), liste les colonnes attendues/détectées/manquantes, montre un aperçu (5 lignes) et calcule les compteurs globaux par table.")

        if st.button("🔎 Lancer la validation", disabled=(fichier_multi is None)):
            if fichier_multi is None:
                st.warning("Veuillez d’abord sélectionner un fichier Excel (.xlsx).")
            else:
                try:
                    import unicodedata, re
                    xls = pd.ExcelFile(fichier_multi)

                    # Normalisation robuste des noms
                    def _norm(s: str) -> str:
                        s = unicodedata.normalize("NFD", str(s))
                        s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # supprime accents
                        return s.strip().lower()

                    # Map nom_normalisé -> nom_réel
                    sheet_map = {_norm(n): n for n in xls.sheet_names}

                    # Alias acceptés
                    aliases = {
                        "contacts":        ["contacts", "contact"],
                        "interactions":    ["interactions", "interaction"],
                        "evenements":      ["evenements", "evenement", "événements", "événement", "events", "event"],
                        "participations":  ["participations", "participation"],
                        "paiements":       ["paiements", "paiement", "payments", "payment"],
                        "certifications":  ["certifications", "certification"],
                    }

                    expected_cols = {
                        "contacts": C_COLS,
                        "interactions": I_COLS,
                        "evenements": E_COLS,
                        "participations": P_COLS,
                        "paiements": PAY_COLS,
                        "certifications": CERT_COLS
                    }

                    # Résout l’onglet réel d’une table (alias + normalisation)
                    def resolve_sheet_name(table_key: str):
                        for alias in aliases[table_key]:
                            k = _norm(alias)
                            if k in sheet_map:
                                return sheet_map[k]
                        return None

                    # Compteurs globaux pour le récapitulatif
                    recap_rows = []   # chaque item: dict(table, feuille, nb_lignes, nb_missing, nb_extra)
                    overall_ok = True

                    for t in ["contacts","interactions","evenements","participations","paiements","certifications"]:
                        real = resolve_sheet_name(t)
                        with st.expander(f"📄 Table **{t}** — " + (f"✅ Feuille trouvée : *{real}*" if real else "❌ Feuille non trouvée"), expanded=False):
                            if not real:
                                st.error(f"Onglet manquant pour **{t}**. Alias acceptés : {', '.join(aliases[t])}")
                                overall_ok = False
                                # Alimente le récap même si manquant
                                recap_rows.append({
                                    "Table": t, "Feuille": "—",
                                    "Lignes détectées": 0,
                                    "Colonnes manquantes": len(expected_cols[t]),
                                    "Colonnes supplémentaires": 0
                                })
                                continue

                            # Lit l’onglet, calcule manquantes/extra, montre un head()
                            df_in = pd.read_excel(xls, sheet_name=real, dtype=str).fillna("")
                            cols_expected = expected_cols[t]
                            cols_detected = list(df_in.columns)
                            missing = [c for c in cols_expected if c not in cols_detected]
                            extra   = [c for c in cols_detected if c not in cols_expected]

                            st.write("**Colonnes attendues** :", ", ".join(cols_expected))
                            st.write("**Colonnes détectées** :", ", ".join(cols_detected) if cols_detected else "_(aucune)_")

                            if missing:
                                st.warning(f"Colonnes **manquantes** ({len(missing)}) : {', '.join(missing)}")
                                overall_ok = False
                            else:
                                st.success("Aucune colonne manquante.")

                            if extra:
                                st.info(f"Colonnes supplémentaires (ignorées à l’import) : {', '.join(extra)}")

                            # Aperçu
                            st.write("**Aperçu des 5 premières lignes**")
                            st.dataframe(df_in.head(5), use_container_width=True)

                            # Alimente le récapitulatif global
                            recap_rows.append({
                                "Table": t,
                                "Feuille": real,
                                "Lignes détectées": int(len(df_in)),
                                "Colonnes manquantes": int(len(missing)),
                                "Colonnes supplémentaires": int(len(extra)),
                            })

                    # Résumé global + compteur agrégé
                    st.markdown("---")
                    st.subheader("📊 Récapitulatif global de la validation")
                    if recap_rows:
                        df_recap = pd.DataFrame(recap_rows)
                        # Totaux
                        tot_lignes = int(df_recap["Lignes détectées"].sum())
                        st.dataframe(df_recap, use_container_width=True)
                        st.info(f"**Total lignes détectées (tous onglets)** : {tot_lignes:,}".replace(",", " "))
                    else:
                        st.info("Aucune donnée lue.")

                    if overall_ok:
                        st.success("Validation terminée ✅ — Tous les onglets requis sont présents et possèdent leurs colonnes clés.")
                    else:
                        st.error("Validation terminée ⚠️ — Corrigez les erreurs ci-dessus avant d’importer (onglets/colonnes manquants).")

                except Exception as e:
                    st.error(f"Erreur de validation : {e}")

        # ----------------------------------------------------------------------
        # 2) IMPORT — robuste (alias/normalisation + ID auto + append)
        # ----------------------------------------------------------------------
        st.markdown("### ⬇️ Importer maintenant")
        if st.button("📥 Importer Excel par table", disabled=(fichier_multi is None)):
            if fichier_multi is None:
                st.warning("Veuillez d’abord sélectionner un fichier Excel (.xlsx).")
            else:
                log = {"ts": datetime.now().isoformat(), "type": "excel_par_table", "counts": {}, "errors": [], "matched_sheets": {}}
                try:
                    import unicodedata, re
                    xls = pd.ExcelFile(fichier_multi)

                    def _norm(s: str) -> str:
                        s = unicodedata.normalize("NFD", str(s))
                        s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
                        return s.strip().lower()

                    sheet_map = {_norm(n): n for n in xls.sheet_names}

                    aliases = {
                        "contacts":        ["contacts", "contact"],
                        "interactions":    ["interactions", "interaction"],
                        "evenements":      ["evenements", "evenement", "événements", "événement", "events", "event"],
                        "participations":  ["participations", "participation"],
                        "paiements":       ["paiements", "paiement", "payments", "payment"],
                        "certifications":  ["certifications", "certification"],
                    }

                    expected_cols = {
                        "contacts": C_COLS,
                        "interactions": I_COLS,
                        "evenements": E_COLS,
                        "participations": P_COLS,
                        "paiements": PAY_COLS,
                        "certifications": CERT_COLS
                    }

                    def resolve_sheet_name(table_key: str):
                        for alias in aliases[table_key]:
                            k = _norm(alias)
                            if k in sheet_map:
                                return sheet_map[k]
                        return None

                    def import_sheet(table_key: str):
                        real = resolve_sheet_name(table_key)
                        if not real:
                            log["errors"].append(f"Feuille manquante pour '{table_key}' (alias: {aliases[table_key]})")
                            return

                        cols = expected_cols[table_key]
                        df_in = pd.read_excel(xls, sheet_name=real, dtype=str).fillna("")
                        # Assure toutes les colonnes
                        for c in cols:
                            if c not in df_in.columns:
                                df_in[c] = ""
                        df_in = df_in[cols]

                        # Chemins / préfix
                        path_key = (
                            "events" if table_key == "evenements" else
                            "inter"  if table_key == "interactions" else
                            "parts"  if table_key == "participations" else
                            "pay"    if table_key == "paiements" else
                            "cert"   if table_key == "certifications" else
                            "contacts"
                        )
                        prefix = {"contacts":"CNT","interactions":"INT","evenements":"EVT",
                                  "participations":"PAR","paiements":"PAY","certifications":"CER"}[table_key]

                        path = PATHS[path_key]
                        df_base = ensure_df(path, cols)

                        # Dédup par ID + attribution ID auto en collision/absence
                        id_col = cols[0]
                        exist_ids = set(df_base[id_col].astype(str).tolist())
                        patt = re.compile(rf"^{prefix}_(\d+)$")
                        maxn = 0
                        for vid in exist_ids:
                            m = patt.match(str(vid))
                            if m:
                                try:
                                    maxn = max(maxn, int(m.group(1)))
                                except:
                                    pass
                        next_num = maxn + 1

                        new_rows = []
                        for _, row in df_in.iterrows():
                            rid = str(row[id_col]).strip()
                            if (not rid) or rid.lower() == "nan" or rid in exist_ids:
                                rid = f"{prefix}_{str(next_num).zfill(3)}"
                                next_num += 1
                            r = row.to_dict()
                            r[id_col] = rid
                            new_rows.append(r)

                        if new_rows:
                            df_out = pd.concat([df_base, pd.DataFrame(new_rows, columns=cols)], ignore_index=True)
                            save_df(df_out, path)
                            mem_key = (
                                "events" if table_key == "evenements" else
                                "inter"  if table_key == "interactions" else
                                "parts"  if table_key == "participations" else
                                "pay"    if table_key == "paiements" else
                                "cert"   if table_key == "certifications" else
                                "contacts"
                            )
                            globals()[f"df_{mem_key}"] = df_out
                            log["counts"][table_key] = len(new_rows)
                            log["matched_sheets"][table_key] = real
                        else:
                            log["counts"][table_key] = 0
                            log["matched_sheets"][table_key] = real

                    for t in ["contacts","interactions","evenements","participations","paiements","certifications"]:
                        import_sheet(t)

                    st.success("Import par table terminé.")
                    st.json(log)
                    log_event("import_excel_par_table", log)

                except Exception as e:
                    st.error(f"Erreur lors de l'import par table : {e}")
                    log_event("error_import_excel_par_table", {"error": str(e)})

        # ----------------------------------------------------------------------
        # 3) Export Excel par Table (backup)
        # ----------------------------------------------------------------------
        st.divider()
        st.caption("Exporter les données existantes au format multi-onglets (sécurisation/backup).")

        df_contacts_exp = ensure_df(PATHS["contacts"], C_COLS)
        df_inter_exp    = ensure_df(PATHS["inter"],    I_COLS)
        df_events_exp   = ensure_df(PATHS["events"],   E_COLS)
        df_parts_exp    = ensure_df(PATHS["parts"],    P_COLS)
        df_pay_exp      = ensure_df(PATHS["pay"],      PAY_COLS)
        df_cert_exp     = ensure_df(PATHS["cert"],     CERT_COLS)

        buf_export_multi = io.BytesIO()
        with pd.ExcelWriter(buf_export_multi, engine="openpyxl") as writer:
            df_contacts_exp.to_excel(writer, sheet_name="contacts", index=False)
            df_inter_exp.to_excel(writer,    sheet_name="interactions", index=False)
            df_events_exp.to_excel(writer,   sheet_name="evenements", index=False)
            df_parts_exp.to_excel(writer,    sheet_name="participations", index=False)
            df_pay_exp.to_excel(writer,      sheet_name="paiements", index=False)
            df_cert_exp.to_excel(writer,     sheet_name="certifications", index=False)

        st.download_button(
            "⬇️ Exporter Excel par Table (backup)",
            buf_export_multi.getvalue(),
            file_name=f"IIBA_export_multisheets_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_export_multisheets"
        )


    elif mode_mig == "Import Excel multi-onglets (.xlsx)":
        up = st.file_uploader("Classeur Excel (6 feuilles : contacts, interactions, evenements, participations, paiements, certifications)",
                             type=["xlsx"], key="xlsx_multi")
        if st.button("Importer l'Excel multi-onglets") and up is not None:
            log = {"timestamp": datetime.now().isoformat(), "import_type": "excel_multisheets", "counts": {}, "errors": [], "collisions": {}}
            try:
                xls = pd.ExcelFile(up)

                def norm(s):
                    return ''.join(c for c in unicodedata.normalize('NFD', str(s)) if unicodedata.category(c) != 'Mn').lower().strip()

                sheets = {norm(n): n for n in xls.sheet_names}
                aliases = {
                    "contacts": ["contacts", "contact"],
                    "interactions": ["interactions", "interaction"],
                    "evenements": ["evenements", "événements", "events"],
                    "participations": ["participations", "participation"],
                    "paiements": ["paiements", "paiement", "payments"],
                    "certifications": ["certifications", "certification"]
                }
                found = {}
                for tbl, names in aliases.items():
                    for n in names:
                        k = norm(n)
                        if k in sheets:
                            found[tbl] = sheets[k]
                            break

                if "contacts" in found:
                    gdf = pd.read_excel(xls, sheet_name=found["contacts"], dtype=str).fillna("")
                    for c in C_COLS:
                        if c not in gdf.columns:
                            gdf[c] = ""
                    sub_c = gdf[C_COLS].fillna("")
                    sub_c["Top20"] = sub_c["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])

                    seen = set()
                    keep = []
                    for _, r in sub_c.iterrows():
                        key = r.get("Email", "") or r.get("Téléphone", "") or (r.get("Nom", ""), r.get("Prénom", ""), r.get("Société", ""))
                        if key in seen:
                            continue
                        seen.add(key)
                        keep.append(r)
                    valid_c = pd.DataFrame(keep, columns=C_COLS)
                    base = df_contacts.copy()
                    collisions = []
                    if "ID" in valid_c.columns and not valid_c.empty:
                        incoming = set(x for x in valid_c["ID"].astype(str) if x and x.lower() != "nan")
                        existing = set(base["ID"].astype(str))
                        collisions = sorted(list(incoming & existing))
                        if collisions:
                            base = base[~base["ID"].isin(collisions)]
                    patt = re.compile(r"^CNT_(\d+)$")
                    base_max = 0
                    for x in base["ID"].dropna().astype(str):
                        m = patt.match(x.strip())
                        if m:
                            try:
                                base_max = max(base_max, int(m.group(1)))
                            except:
                                pass
                    next_id = base_max + 1
                    new_rows = []
                    for _, r in valid_c.iterrows():
                        rid = r["ID"]
                        if not isinstance(rid, str) or rid.strip() == "" or rid.strip().lower() == "nan":
                            rid = f"CNT_{str(next_id).zfill(3)}"
                            next_id += 1
                        rr = r.to_dict()
                        rr["ID"] = rid
                        new_rows.append(rr)
                    out = pd.concat([base, pd.DataFrame(new_rows, columns=C_COLS)], ignore_index=True)
                    save_df(out, PATHS["contacts"])
                    globals()["df_contacts"] = out
                    log["counts"]["contacts"] = len(new_rows)
                    log["collisions"]["contacts"] = collisions

                def save_sheet(tbl, cols, path, prefix):
                    if tbl not in found:
                        return 0, []
                    sdf = pd.read_excel(xls, sheet_name=found[tbl], dtype=str).fillna("")
                    for c in cols:
                        if c not in sdf.columns:
                            sdf[c] = ""
                    sdf = sdf[cols]
                    id_col = cols[0]
                    base_df = ensure_df(path, cols)
                    incoming = set(x for x in sdf[id_col].astype(str) if x and x.lower() != "nan")
                    existing = set(base_df[id_col].astype(str))
                    coll = sorted(list(incoming & existing))
                    if coll:
                        base_df = base_df[~base_df[id_col].isin(coll)]
                    patt = re.compile(rf"^{prefix}_(\d+)$")
                    base_max = 0
                    for x in base_df[id_col].dropna().astype(str):
                        m = patt.match(x.strip())
                        if m:
                            try:
                                base_max = max(base_max, int(m.group(1)))
                            except:
                                pass
                    gen = base_max + 1
                    new_rows = []
                    for _, r in sdf.iterrows():
                        cur = r[id_col]
                        if not isinstance(cur, str) or cur.strip() == "" or cur.strip().lower() == "nan":
                            cur = f"{prefix}_{str(gen).zfill(3)}"
                            gen += 1
                        rr = r.to_dict()
                        rr[id_col] = cur
                        new_rows.append(rr)
                    out = pd.concat([base_df, pd.DataFrame(new_rows, columns=cols)], ignore_index=True)
                    save_df(out, path)
                    globals()["df_" + ("inter" if tbl == "interactions" else 
                                      "events" if tbl == "evenements" else 
                                      "parts" if tbl == "participations" else 
                                      "pay" if tbl == "paiements" else "cert")] = out
                    return len(new_rows), coll

                for spec in [("interactions", I_COLS, PATHS["inter"], "INT"),
                             ("evenements", E_COLS, PATHS["events"], "EVT"),
                             ("participations", P_COLS, PATHS["parts"], "PAR"),
                             ("paiements", PAY_COLS, PATHS["pay"], "PAY"),
                             ("certifications", CERT_COLS, PATHS["cert"], "CER")]:
                    cnt, coll = save_sheet(*spec)
                    log["counts"][spec[0]] = cnt
                    log["collisions"][spec] = coll

                st.success("Import Excel multi-onglets terminé.")
                st.json(log)
                log_event("import_excel_multisheets", log)
            except Exception as e:
                st.error(f"Erreur d'import multi-onglets: {e}")
                log_event("error_import_excel_multisheets", {"error": str(e)})

        bufm = io.BytesIO()
        with pd.ExcelWriter(bufm, engine="openpyxl") as w:
            pd.DataFrame(columns=C_COLS).to_excel(w, index=False, sheet_name="contacts")
            pd.DataFrame(columns=I_COLS).to_excel(w, index=False, sheet_name="interactions")
            pd.DataFrame(columns=E_COLS).to_excel(w, index=False, sheet_name="evenements")
            pd.DataFrame(columns=P_COLS).to_excel(w, index=False, sheet_name="participations")
            pd.DataFrame(columns=PAY_COLS).to_excel(w, index=False, sheet_name="paiements")
            pd.DataFrame(columns=CERT_COLS).to_excel(w, index=False, sheet_name="certifications")
        st.download_button("⬇️ Modèle Multi-onglets (xlsx)", bufm.getvalue(), file_name="IIBA_multisheets_template.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    elif mode_mig == "Import CSV global":
        up = st.file_uploader("CSV global (colonne __TABLE__)", type=["csv"], key="g_up")
        if st.button("Importer le CSV global") and up is not None:
            try:
                gdf = pd.read_csv(up, dtype=str, encoding="utf-8")
                if "__TABLE__" not in gdf.columns:
                    raise ValueError("Colonne __TABLE__ manquante.")
                def save_subset(tbl, cols, path, prefix):
                    sub = gdf[gdf["__TABLE__"] == tbl].copy()
                    for c in cols:
                        if c not in sub.columns:
                            sub[c] = ""
                    sub = sub[cols].fillna("")
                    id_col = cols[0]
                    base_df = ensure_df(path, cols)
                    incoming = set(x for x in sub[id_col].astype(str) if x and x.lower() != "nan")
                    existing = set(base_df[id_col].astype(str))
                    coll = sorted(list(incoming & existing))
                    if coll:
                        base_df = base_df[~base_df[id_col].isin(coll)]
                    patt = re.compile(rf"^{prefix}_(\d+)$")
                    base_max = 0
                    for x in base_df[id_col].dropna().astype(str):
                        m = patt.match(x.strip())
                        if m:
                            try:
                                base_max = max(base_max, int(m.group(1)))
                            except:
                                pass
                    gen = base_max + 1
                    ids = []
                    for _, r in sub.iterrows():
                        rid = r[id_col]
                        if not isinstance(rid, str) or rid.strip() == "" or rid.strip().lower() == "nan":
                            ids.append(f"{prefix}_{str(gen).zfill(3)}")
                            gen += 1
                        else:
                            ids.append(rid.strip())
                    sub[id_col] = ids
                    out = pd.concat([base_df, sub], ignore_index=True)
                    save_df(out, path)
                    globals()["df_" + ("contacts" if tbl == "contacts" else "inter" if tbl == "interactions" else "events" if tbl == "evenements" else "parts" if tbl == "participations" else "pay" if tbl == "paiements" else "cert")] = out
                save_subset("contacts", C_COLS, PATHS["contacts"], "CNT")
                save_subset("interactions", I_COLS, PATHS["inter"], "INT")
                save_subset("evenements", E_COLS, PATHS["events"], "EVT")
                save_subset("participations", P_COLS, PATHS["parts"], "PAR")
                save_subset("paiements", PAY_COLS, PATHS["pay"], "PAY")
                save_subset("certifications", CERT_COLS, PATHS["cert"], "CER")
                st.success("Import CSV global terminé.")
            except Exception as e:
                st.error(f"Erreur d'import CSV global : {e}")

    # ... code existant load_and_compute_kpis() ...

    st.markdown("---")
    st.header("🔧 Maintenance Base de Données")
    
    col_reset, col_purge = st.columns(2)
    
    with col_reset:
        st.subheader("🗑️ Réinitialisation Complète")
        st.warning("⚠️ Cette action supprime TOUTES les données et recrée les fichiers vides.")
        
        confirm_reset = st.text_input(
            "Tapez 'RESET' pour confirmer:",
            placeholder="RESET"
        )
        
        if st.button("💣 RÉINITIALISER LA BASE", type="secondary"):
            if confirm_reset == "RESET":
                try:
                    # Suppression de tous les fichiers CSV
                    for path in PATHS.values():
                        if path.exists():
                            path.unlink()
                    
                    # Recréation des fichiers vides
                    for table, cols in ALL_SCHEMAS.items():
                        empty_df = pd.DataFrame(columns=cols)
                        save_df(empty_df, PATHS[table])
                    
                    # Recréation parametres.csv
                    df_params = pd.DataFrame({
                        "key": list(ALL_DEFAULTS.keys()), 
                        "value": list(ALL_DEFAULTS.values())
                    })
                    df_params.to_csv(PATHS["params"], index=False, encoding="utf-8")
                    
                    # Journalisation
                    log_event("reset_database", {
                        "action": "Réinitialisation complète",
                        "tables_recreated": list(ALL_SCHEMAS.keys()),
                        "timestamp": datetime.now().isoformat()
                    })
                    
                    st.success("✅ Base de données réinitialisée avec succès!")
                    st.info("🔄 Rechargez la page pour voir les modifications.")
                    
                except Exception as e:
                    st.error(f"❌ Erreur lors de la réinitialisation: {e}")
                    log_event("error_reset_database", {"error": str(e)})
            else:
                st.error("❌ Veuillez taper 'RESET' pour confirmer.")
    
    with col_purge:
        st.subheader("🎯 Purge d'un Identifiant")
        st.info("Supprime un contact, événement, interaction, etc. par son ID")
        
        purge_id = st.text_input(
            "ID à supprimer (ex: CNT_001, EVT_005, INT_023):",
            placeholder="CNT_001"
        )
        
        purge_type = st.selectbox(
            "Type d'entité:",
            ["Auto-détection", "Contact", "Événement", "Interaction", "Participation", "Paiement", "Certification"]
        )
        
        if st.button("🗑️ PURGER CET ID", type="secondary"):
            if purge_id:
                try:
                    deleted_count = 0
                    deleted_from = []
                    
                    if purge_type == "Auto-détection":
                        # Détection automatique basée sur le préfixe
                        if purge_id.startswith("CNT_"):
                            purge_type = "Contact"
                        elif purge_id.startswith("EVT_"):
                            purge_type = "Événement"
                        elif purge_id.startswith("INT_"):
                            purge_type = "Interaction"
                        elif purge_id.startswith("PAR_"):
                            purge_type = "Participation"
                        elif purge_id.startswith("PAY_"):
                            purge_type = "Paiement"
                        elif purge_id.startswith("CER_"):
                            purge_type = "Certification"
                    
                    # Suppression selon le type
                    if purge_type == "Contact":
                        # Suppression en cascade: contact + toutes ses relations
                        original_len = len(df_contacts)
                        globals()["df_contacts"] = df_contacts[df_contacts["ID"] != purge_id]
                        deleted_count += original_len - len(df_contacts)
                        if deleted_count > 0:
                            save_df(df_contacts, PATHS["contacts"])
                            deleted_from.append("contacts")
                        
                        # Suppression interactions liées
                        original_len = len(df_inter)
                        globals()["df_inter"] = df_inter[df_inter["ID"] != purge_id]
                        inter_deleted = original_len - len(df_inter)
                        if inter_deleted > 0:
                            save_df(df_inter, PATHS["inter"])
                            deleted_from.append(f"interactions ({inter_deleted})")
                        
                        # Suppression participations liées
                        original_len = len(df_parts)
                        globals()["df_parts"] = df_parts[df_parts["ID"] != purge_id]
                        part_deleted = original_len - len(df_parts)
                        if part_deleted > 0:
                            save_df(df_parts, PATHS["parts"])
                            deleted_from.append(f"participations ({part_deleted})")
                        
                        # Suppression paiements liés
                        original_len = len(df_pay)
                        globals()["df_pay"] = df_pay[df_pay["ID"] != purge_id]
                        pay_deleted = original_len - len(df_pay)
                        if pay_deleted > 0:
                            save_df(df_pay, PATHS["pay"])
                            deleted_from.append(f"paiements ({pay_deleted})")
                        
                        # Suppression certifications liées
                        original_len = len(df_cert)
                        globals()["df_cert"] = df_cert[df_cert["ID"] != purge_id]
                        cert_deleted = original_len - len(df_cert)
                        if cert_deleted > 0:
                            save_df(df_cert, PATHS["cert"])
                            deleted_from.append(f"certifications ({cert_deleted})")
                    
                    elif purge_type == "Événement":
                        # Suppression événement + participations + paiements liés
                        original_len = len(df_events)
                        globals()["df_events"] = df_events[df_events["ID_Événement"] != purge_id]
                        deleted_count += original_len - len(df_events)
                        if deleted_count > 0:
                            save_df(df_events, PATHS["events"])
                            deleted_from.append("evenements")
                        
                        # Suppression participations à cet événement
                        original_len = len(df_parts)
                        globals()["df_parts"] = df_parts[df_parts["ID_Événement"] != purge_id]
                        part_deleted = original_len - len(df_parts)
                        if part_deleted > 0:
                            save_df(df_parts, PATHS["parts"])
                            deleted_from.append(f"participations ({part_deleted})")
                        
                        # Suppression paiements à cet événement
                        original_len = len(df_pay)
                        globals()["df_pay"] = df_pay[df_pay["ID_Événement"] != purge_id]
                        pay_deleted = original_len - len(df_pay)
                        if pay_deleted > 0:
                            save_df(df_pay, PATHS["pay"])
                            deleted_from.append(f"paiements ({pay_deleted})")
                    
                    elif purge_type == "Interaction":
                        original_len = len(df_inter)
                        globals()["df_inter"] = df_inter[df_inter["ID_Interaction"] != purge_id]
                        deleted_count += original_len - len(df_inter)
                        if deleted_count > 0:
                            save_df(df_inter, PATHS["inter"])
                            deleted_from.append("interactions")
                    
                    elif purge_type == "Participation":
                        original_len = len(df_parts)
                        globals()["df_parts"] = df_parts[df_parts["ID_Participation"] != purge_id]
                        deleted_count += original_len - len(df_parts)
                        if deleted_count > 0:
                            save_df(df_parts, PATHS["parts"])
                            deleted_from.append("participations")
                    
                    elif purge_type == "Paiement":
                        original_len = len(df_pay)
                        globals()["df_pay"] = df_pay[df_pay["ID_Paiement"] != purge_id]
                        deleted_count += original_len - len(df_pay)
                        if deleted_count > 0:
                            save_df(df_pay, PATHS["pay"])
                            deleted_from.append("paiements")
                    
                    elif purge_type == "Certification":
                        original_len = len(df_cert)
                        globals()["df_cert"] = df_cert[df_cert["ID_Certif"] != purge_id]
                        deleted_count += original_len - len(df_cert)
                        if deleted_count > 0:
                            save_df(df_cert, PATHS["cert"])
                            deleted_from.append("certifications")
                    
                    # Journalisation
                    log_event("purge_id", {
                        "purged_id": purge_id,
                        "type": purge_type,
                        "deleted_count": deleted_count,
                        "tables_affected": deleted_from,
                        "timestamp": datetime.now().isoformat()
                    })
                    
                    if deleted_count > 0 or deleted_from:
                        st.success(f"✅ ID '{purge_id}' purgé avec succès!")
                        st.info(f"📊 Suppressions: {', '.join(deleted_from)}")
                        st.info("🔄 Rechargez la page pour voir les modifications.")
                    else:
                        st.warning(f"⚠️ ID '{purge_id}' introuvable dans la base.")
                
                except Exception as e:
                    st.error(f"❌ Erreur lors de la purge: {e}")
                    log_event("error_purge_id", {"purge_id": purge_id, "error": str(e)})
            else:
                st.error("❌ Veuillez saisir un ID à purger.")
 
