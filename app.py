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

# ---------------------- PAGE ÉVÉNEMENTS ----------------------

if page == "Événements":
    st.title("📅 Événements")
    
    with st.expander("➕ Créer un nouvel événement", expanded=False):
        with st.form("new_event"):
            c1, c2, c3 = st.columns(3)
            nom = c1.text_input("Nom de l'événement")
            typ = c2.selectbox("Type", SET["types_evenements"])
            dat = c3.date_input("Date", value=date.today())

            c4, c5, c6 = st.columns(3)
            lieu = c4.selectbox("Lieu", SET["lieux"])
            duree = c5.number_input("Durée (h)", min_value=0.0, step=0.5, value=2.0)
            formateur = c6.text_input("Formateur(s)")

            obj = st.text_area("Objectif")

            couts = st.columns(5)
            c_salle = couts[0].number_input("Coût salle", min_value=0.0, step=1000.0)
            c_form = couts[1].number_input("Coût formateur", min_value=0.0, step=1000.0)
            c_log = couts[2].number_input("Coût logistique", min_value=0.0, step=1000.0)
            c_pub = couts[3].number_input("Coût pub", min_value=0.0, step=1000.0)
            c_aut = couts[4].number_input("Autres coûts", min_value=0.0, step=1000.0)

            notes = st.text_area("Notes")
            ok = st.form_submit_button("💾 Créer l'événement")

            if ok:
                new_id = generate_id("EVT", df_events, "ID_Événement")
                row = {
                    "ID_Événement": new_id, "Nom_Événement": nom, "Type": typ, "Date": dat.isoformat(),
                    "Durée_h": str(duree), "Lieu": lieu, "Formateur": formateur, "Objectif": obj, "Periode": "",
                    "Cout_Salle": c_salle, "Cout_Formateur": c_form, "Cout_Logistique": c_log, "Cout_Pub": c_pub,
                    "Cout_Autres": c_aut, "Cout_Total": 0, "Notes": notes
                }
                globals()["df_events"] = pd.concat([df_events, pd.DataFrame([row])], ignore_index=True)
                save_df(df_events, PATHS["events"])
                st.success(f"Événement créé ({new_id}).")

    # Édition, Duplication, Suppression avec filtre
    filt = st.text_input("Filtre rapide (nom, type, lieu, notes…)", "")
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
        grid = AgGrid(df_show, gridOptions=go, height=520, update_mode=GridUpdateMode.MODEL_CHANGED,
                      data_return_mode=DataReturnMode.FILTERED_AND_SORTED, key="evt_grid", allow_unsafe_jscode=True)

        col1, col2, col3 = st.columns(3)
        if col1.button("💾 Appliquer les modifications"):
            new_df = pd.DataFrame(grid["data"])
            for c in E_COLS:
                if c not in new_df.columns:
                    new_df[c] = ""
            globals()["df_events"] = new_df[E_COLS].copy()
            save_df(df_events, PATHS["events"])
            st.success("Modifications enregistrées.")
        if col2.button("🗑️ Supprimer l'événement sélectionné"):
            if grid.get("selected_rows"):
                del_id = grid["selected_rows"][0].get("ID_Événement")
                globals()["df_events"] = df_events[df_events["ID_Événement"] != del_id]
                save_df(df_events, PATHS["events"])
                st.success(f"Événement supprimé ({del_id}).")
            else:
                st.warning("Sélectionnez une ligne dans la grille pour supprimer.")
        if col3.button("🧬 Dupliquer l'événement sélectionné"):
            if grid.get("selected_rows"):
                src = grid["selected_rows"][0]
                new_id = generate_id("EVT", df_events, "ID_Événement")
                clone = {k: src.get(k, "") for k in E_COLS}
                clone["ID_Événement"] = new_id
                globals()["df_events"] = pd.concat([df_events, pd.DataFrame([clone])], ignore_index=True)
                save_df(df_events, PATHS["events"])
                st.success(f"Événement dupliqué sous l'ID {new_id}.")
            else:
                st.warning("Sélectionnez une ligne dans la grille pour dupliquer.")
    else:
        st.dataframe(df_show, use_container_width=True)
        st.info("Installez `streamlit-aggrid` pour éditer/dupliquer directement dans la grille.")


# ---------------------- PAGE RAPPORTS ----------------------

elif page == "Rapports":
    st.title("📑 Rapports & KPI — IIBA Cameroun")

    def filtered_tables_for_period(year_sel:str, month_sel:str):
        def in_period(d:pd.Series) -> pd.Series:
            p = d.map(lambda x: parse_date(x) if pd.notna(x) else None)
            m = p.notna()
            if year_sel != "Toutes":
                y = int(year_sel)
                m = m & p.map(lambda x: x and x.year == y)
            if month_sel != "Tous":
                mm = int(month_sel)
                m = m & p.map(lambda x: x and x.month == mm)
            return m.fillna(False)
        dfe2 = df_events[in_period(df_events["Date"])].copy() if not df_events.empty else df_events.copy()
        dfp2 = df_parts.copy()
        if not df_events.empty and not df_parts.empty:
            evd = df_events.set_index("ID_Événement")["Date"].map(parse_date)
            dfp2["_d"] = dfp2["ID_Événement"].map(evd)
            if year_sel != "Toutes":
                dfp2 = dfp2[dfp2["_d"].map(lambda x: x and x.year == int(year_sel))]
            if month_sel != "Tous":
                dfp2 = dfp2[dfp2["_d"].map(lambda x: x and x.month == int(month_sel))]
        dfpay2 = df_pay[in_period(df_pay["Date_Paiement"])].copy() if not df_pay.empty else df_pay.copy()
        dfcert2 = df_cert[in_period(df_cert["Date_Obtention"]) | in_period(df_cert["Date_Examen"])].copy() if not df_cert.empty else df_cert.copy()
        return dfe2, dfp2, dfpay2, dfcert2

    def event_financials(dfe2:pd.DataFrame, dfpay2:pd.DataFrame) -> pd.DataFrame:
        rec_by_evt = pd.Series(dtype=float)
        if not dfpay2.empty:
            r = dfpay2[dfpay2["Statut"]=="Réglé"].copy()
            r["Montant"] = pd.to_numeric(r["Montant"], errors="coerce").fillna(0.0)
            rec_by_evt = r.groupby("ID_Événement")["Montant"].sum()
        ev = df_events.copy() if dfe2.empty else dfe2.copy()
        for c in ["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total"]:
            ev[c] = pd.to_numeric(ev[c], errors="coerce").fillna(0.0)
        ev["Cout_Total"] = np.where(ev["Cout_Total"]>0, ev["Cout_Total"], ev[["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres"]].sum(axis=1))
        ev = ev.set_index("ID_Événement")
        rep = pd.DataFrame({"Nom_Événement": ev["Nom_Événement"], "Type": ev["Type"], "Date": ev["Date"], "Coût_Total": ev["Cout_Total"]})
        rep["Recette"] = rec_by_evt
        rep["Recette"] = rep["Recette"].fillna(0.0)
        rep["Bénéfice"] = rep["Recette"] - rep["Coût_Total"]
        return rep.reset_index()

    dfe2, dfp2, dfpay2, dfcert2 = filtered_tables_for_period(annee, mois)
    dfc2 = df_contacts.copy()

    total_contacts = len(dfc2)
    prospects_actifs = len(dfc2[(dfc2["Type"] == "Prospect") & (dfc2["Statut"] == "Actif")])
    membres = len(dfc2[dfc2["Type"] == "Membre"])
    events_count = len(dfe2) if not dfe2.empty else 0
    parts_total = len(dfp2) if not dfp2.empty else 0
    ca_regle = impayes = 0.0
    if not dfpay2.empty:
        dfpay2["Montant"] = pd.to_numeric(dfpay2["Montant"], errors="coerce").fillna(0.0)
        ca_regle = float(dfpay2[dfpay2["Statut"] == "Réglé"]["Montant"].sum())
        impayes = float(dfpay2[dfpay2["Statut"] != "Réglé"]["Montant"].sum())
    prospects_total = len(dfc2[dfc2["Type"] == "Prospect"])
    prospects_convertis = len(dfc2[dfc2["Type"] == "Membre"])
    taux_conv = (prospects_convertis / prospects_total * 100) if prospects_total else 0.0

    kpis = {
        "contacts_total": ("👥 Contacts", total_contacts),
        "prospects_actifs": ("🧲 Prospects actifs", prospects_actifs),
        "membres": ("🏆 Membres", membres),
        "events_count": ("📅 Événements", events_count),
        "participations_total": ("🎟️ Participations", parts_total),
        "ca_regle": ("💰 CA réglé", f"{int(ca_regle):,} FCFA".replace(","," ")),
        "impayes": ("⛔ Impayés", f"{int(impayes):,} FCFA".replace(","," ")),
        "taux_conversion": ("🔁 Taux de conversion", f"{taux_conv:.1f}%"),
    }
    enabled = [x.strip() for x in str(PARAMS.get("kpi_enabled", "")).split(",") if x.strip() and x.strip() in kpis]
    cols = st.columns(max(1, len(enabled)))
    for i, k in enumerate(enabled):
        cols[i].metric(kpis[k][0], kpis[k][1])

    st.markdown("---")
    ev_fin = event_financials(dfe2, dfpay2)
    if alt and not ev_fin.empty:
        st.subheader("💹 CA vs Coût par événement (et Bénéfice)")
        ev_fin_melt = ev_fin.melt(id_vars=["ID_Événement", "Nom_Événement"], value_vars=["Recette", "Coût_Total", "Bénéfice"], var_name="Metric", value_name="Montant")
        chart = alt.Chart(ev_fin_melt).mark_bar().encode(
            x=alt.X("Nom_Événement:N", sort='-y'),
            y=alt.Y("Montant:Q"),
            color="Metric:N",
            tooltip=["Nom_Événement", "Metric", "Montant"]
        ).properties(height=320).interactive()
        st.altair_chart(chart, use_container_width=True)
    if not dfp2.empty:
        st.subheader("👥 Participants par mois")
        dfp2["_d"] = dfp2.get("_d")
        if "_d" not in dfp2 or dfp2["_d"].isna().all():
            evd = df_events.set_index("ID_Événement")["Date"].map(parse_date)
            dfp2["_d"] = dfp2["ID_Événement"].map(evd)
        dfp2["_mois"] = pd.to_datetime(dfp2["_d"]).dt.to_period("M").astype(str)
        agg = dfp2.groupby("_mois")["ID_Participation"].count().reset_index(name="Participants")
        if alt:
            line = alt.Chart(agg).mark_line(point=True).encode(x="__mois:N", y="Participants:Q").transform_calculate(__mois="datum._mois")
            st.altair_chart(line.properties(height=280), use_container_width=True)
        else:
            st.dataframe(agg)
    if not df_parts.empty and not df_events.empty:
        st.subheader("😊 Satisfaction moyenne par type d'événement")
        dfp = df_parts.copy()
        dfp["Note"] = pd.to_numeric(dfp["Note"], errors="coerce")
        types = df_events.set_index("ID_Événement")["Type"]
        dfp["Type"] = dfp["ID_Événement"].map(types)
        ag = dfp.groupby("Type")["Note"].mean().reset_index()
        if alt:
            bar = alt.Chart(ag).mark_bar().encode(x="Type:N", y="Note:Q", tooltip=["Type", "Note"])
            st.altair_chart(bar.properties(height=280), use_container_width=True)
        else:
            st.dataframe(ag)

    st.markdown("---")
    st.subheader("🎯 Objectifs vs Réel")

    def get_target(key):
        try:
            return float(PARAMS.get(key, "0") or 0)
        except:
            return 0

    y = datetime.now().year
    goals = [
        ("contacts_total", total_contacts),
        ("ca_regle", ca_regle),
        ("participations_total", parts_total),
    ]
    rows = []
    for k, val in goals:
        tgt = get_target(f"kpi_target_{k}_year_{y}")
        delta = val - tgt
        rows.append({"KPI": k, "Objectif": tgt, "Réel": val, "Écart": delta})
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
        
    # Affichage des KPI calculés
    prospects = load_and_compute_kpis()
    st.markdown("---")
    st.subheader("🎯 Table KPI détaillés")
    st.dataframe(prospects, use_container_width=True)

    # Export rapport Excel complet
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_contacts.to_excel(writer, index=False, sheet_name="contacts")
        df_inter.to_excel(writer, index=False, sheet_name="interactions")
        df_events.to_excel(writer, index=False, sheet_name="evenements")
        df_parts.to_excel(writer, index=False, sheet_name="participations")
        df_pay.to_excel(writer, index=False, sheet_name="paiements")
        df_cert.to_excel(writer, index=False, sheet_name="certifications")
        ev_fin.to_excel(writer, index=False, sheet_name="finance_events")
    st.download_button("⬇️ Exporter le rapport (Excel)", buf.getvalue(), file_name="IIBA_rapport.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ---------------------- PAGE ADMIN ----------------------

# ---------------------- PAGE ADMIN — Migration & Import/Export ----------------------
elif page == "Admin":
    st.title("⚙️ Admin — Import & Calcul Automatique des KPI")

    # Upload du fichier Excel
    excel_file = st.file_uploader(
        label="📂 Sélectionnez le fichier Excel (Stagiaire-Maeva.xlsx)",
        type=["xlsx"]
    )
    if excel_file is None:
        st.info("Veuillez uploader votre fichier Excel pour démarrer le calcul des KPI.")
        st.stop()
        
    @st.cache_data
    def load_and_compute_kpis(excel_buffer) -> pd.DataFrame:
        # Chargement de tous les onglets
        all_sheets = pd.read_excel(path, sheet_name=None)

        # Préparation de la feuille RelanceProspect avec slice de colonnes corrigé
        df_rel = all_sheets.get("RelanceProspect", pd.DataFrame())
        df_rel_sel = pd.concat([
            df_rel[["ID", "STATUT"]],
            df_rel.loc[:, "DATE RELANCE 1":"DATE RELANCE 5"]
        ], axis=1)

        # Concaténation de tous les onglets prospects
        prospects = pd.concat([
            all_sheets.get("ListeProspects", pd.DataFrame()).dropna(subset=["Id"]),
            all_sheets.get("New", pd.DataFrame()).loc[:, [
                "Id", "Nom", "Prénom", "Email", "Phone",
                "Approval Status", "Paiements effectués"
            ]],
            df_rel_sel,
            all_sheets.get("Afterwork Online", pd.DataFrame()).loc[:, [
                "First Name", "Last Name", "Email", "Approval Status",
                "Heure d’inscription", "Heure de participation",
                "Temps de présence en séance (minutes)"
            ]],
            all_sheets.get("Webinaires Gratuits", pd.DataFrame()).loc[:, [
                "Nom d’utilisateur (nom original)", "Approval Status",
                "Temps de présence en séance (minutes)"
            ]],
            all_sheets.get("Groupe d’étude", pd.DataFrame()).loc[:, [
                "First Name", "Approval Status",
                "Heure d’inscription", "Heure de participation",
                "Temps de présence en séance (minutes)"
            ]],
            all_sheets.get("Cartographie", pd.DataFrame()).loc[:, [
                "First Name", "Approval Status",
                "Heure d’inscription", "Heure de participation",
                "Temps de présence en séance (minutes)"
            ]]
        ], ignore_index=True, sort=False)

        # Standardisation des colonnes
        prospects.rename(columns={
            "ID": "Id", "STATUT": "StatutMessages", "Approval Status": "EtatApprobation",
            "Paiements effectués": "MontantsPayes",
            "Temps de présence en séance (minutes)": "DureePresence"
        }, inplace=True)

        # Calculs temporels
        today = datetime.now()
        prospects["DateDerniereInteraction"] = prospects[[
            "DATE RELANCE 5", "DATE RELANCE 4", "DATE RELANCE 3",
            "DATE RELANCE2", "DATE RELANCE 1", "Heure d’inscription"
        ]].bfill(axis=1).iloc[:, 0]
        prospects["JoursDepuisInteraction"] = (
            today - pd.to_datetime(prospects["DateDerniereInteraction"], errors="coerce")
        ).dt.days

        # KPI 1. Statuts et segmentation
        prospects["Actif30j"] = prospects["JoursDepuisInteraction"] <= 30
        prospects["Membre"] = prospects["EtatApprobation"].str.lower() == "approved"
        prospects["Injoignable"] = prospects["StatutMessages"].str.contains(
            "indisponible|ne répond jamais", case=False, na=False
        )
        prospects["A_Ne_Pas_Contacter"] = prospects["StatutMessages"].str.contains(
            "rouge|ne pas contacter", case=False, na=False
        )

        # KPI 2. Événements et participations
        events = ["Afterwork Online", "Webinaires Gratuits", "Groupe d’étude", "Cartographie"]
        for ev in events:
            col_part = f"{ev}_Participations"
            mask = prospects["Heure d’inscription"].notna() & prospects["Heure de participation"].notna()
            prospects[col_part] = prospects[mask].groupby("Id")["Id"].transform("count")
        prospects["TauxParticipation"] = (
            prospects[[f"{ev}_Participations" for ev in events]].sum(axis=1) /
            prospects["Actif30j"].astype(int).replace(0, pd.NA)
        )

        # KPI 3. Chiffre d’affaires et impayés
        prospects["MontantsPayes"] = pd.to_numeric(
            prospects["MontantsPayes"].str.replace(r"[^\d\.]", "", regex=True),
            errors="coerce"
        ).fillna(0)
        prospects["MontantPrevu"] = 200000
        prospects["MontantImpayé"] = prospects["MontantPrevu"] - prospects["MontantsPayes"]
        prospects["PaiementPartiel"] = prospects["MontantsPayes"].between(1, prospects["MontantPrevu"] - 1)

        # KPI 4. Taux de conversion
        total_contacts = len(prospects)
        total_inscrits = prospects["Membre"].sum()
        prospects["Conv_Prospect_Inscrit"] = total_inscrits / total_contacts
        prospects["Conv_Inscrit_Participant"] = (
            prospects["Membre"] &
            (prospects[[f"{ev}_Participations" for ev in events]].sum(axis=1) > 0)
        )

        # KPI 5. Engagement et score d’engagement
        prospects["NbRelances"] = prospects[[
            "DATE RELANCE 1", "DATE RELANCE2",
            "DATE RELANCE 3", "DATE RELANCE4", "DATE RELANCE 5"
        ]].notna().sum(axis=1)
        prospects["ScoreEngagement"] = (
            prospects["NbRelances"] +
            prospects["DureePresence"].gt(0).astype(int) * 3 +
            (prospects["MontantsPayes"] > 0).astype(int) * 5
        )

        # KPI 6. Score global et segmentation
        max_eng = prospects["ScoreEngagement"].max()
        max_part = prospects[[f"{ev}_Participations" for ev in events]].sum(axis=1).max()
        max_pay = prospects["MontantsPayes"].max()
        max_conv = prospects["Conv_Prospect_Inscrit"].max()
        prospects["ScoreGlobal"] = (
            0.30 * (prospects["ScoreEngagement"] / max_eng) +
            0.30 * (prospects[[f"{ev}_Participations" for ev in events]].sum(axis=1) / max_part) +
            0.20 * (prospects["MontantsPayes"] / max_pay) +
            0.20 * (prospects["Conv_Prospect_Inscrit"] / max_conv)
        ) * 100

        def classer(score):
            if score >= 70: return "Chaud"
            if score >= 40: return "Tiède"
            return "Froid"

        prospects["SegmentGlobal"] = prospects["ScoreGlobal"].apply(classer)

        return prospects

    # Exécution et affichage
    prospects = load_and_compute_kpis()

    st.subheader("📈 KPI Globaux et Segmentation")
    st.dataframe(
        prospects[[
            "Id", "Nom", "Prénom", "Actif30j", "Membre", "Injoignable",
            "TauxParticipation", "MontantImpayé", "Conv_Prospect_Inscrit",
            "Conv_Inscrit_Participant", "ScoreEngagement",
            "ScoreGlobal", "SegmentGlobal"
        ]],
        use_container_width=True
    )

    # Export du fichier Excel prêt à l’import
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        prospects.to_excel(writer, sheet_name="Prospects_KPI", index=False)
    st.download_button(
        "⬇️ Export prospects_KPI_prepared.xlsx",
        buf.getvalue(),
        file_name="prospects_KPI_prepared.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
