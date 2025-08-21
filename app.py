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

    st.write("HAS_AGGRID =", HAS_AGGRID)

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
        st.caption("Clés supportées : contacts_total, prospects_actifs, membres, events_count, participations_total, ca_regle, impayes, taux_conversion")
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

    st.markdown("---")
    st.header("📦 Migration — Import/Export Global & Multi-onglets")

    mode_mig = st.radio("Mode de migration", ["Import Excel global (.xlsx)", "Import Excel multi-onglets (.xlsx)", "Import CSV global", "Par table (CSV)"], horizontal=True)

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


