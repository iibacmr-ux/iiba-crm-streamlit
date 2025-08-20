# -*- coding: utf-8 -*-
"""
IIBA Cameroun — CRM (monofichier)
Version : Import Excel global + validations + rapports (corrigée)
Dépendances : streamlit, pandas, numpy, altair, openpyxl, streamlit-aggrid
"""
from datetime import datetime, date
from pathlib import Path
import io, json, re

import numpy as np
import pandas as pd
import streamlit as st

# AgGrid (facultatif)
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
    HAS_AGGRID = True
except Exception:
    HAS_AGGRID = False

try:
    import altair as alt
except Exception:
    alt = None

st.set_page_config(page_title="IIBA Cameroun — CRM", page_icon="📊", layout="wide")

# ------------------------------------------------------------------
# Fichiers de données
# ------------------------------------------------------------------
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "inter": DATA_DIR / "interactions.csv",
    "events": DATA_DIR / "evenements.csv",
    "parts": DATA_DIR / "participations.csv",
    "pay": DATA_DIR / "paiements.csv",
    "cert": DATA_DIR / "certifications.csv",
    "settings": DATA_DIR / "settings.json",
}

# ------------------------------------------------------------------
# Référentiels par défaut (Paramètres)
# ------------------------------------------------------------------
DEFAULT = {
    "genres": ["Homme", "Femme", "Autre"],
    "secteurs": ["Banque", "Télécom", "IT", "Éducation", "Santé", "ONG", "Industrie", "Public", "Autre"],
    "types_contact": ["Membre", "Prospect", "Formateur", "Partenaire"],
    "sources": ["Afterwork", "Formation", "LinkedIn", "Recommandation", "Site Web", "Salon", "Autre"],
    "statuts_engagement": ["Actif", "Inactif", "À relancer"],
    "canaux": ["Appel", "Email", "WhatsApp", "Zoom", "Présentiel", "Autre"],
    "villes": ["Douala", "Yaoundé", "Limbe", "Bafoussam", "Garoua", "Autres"],
    "pays": ["Cameroun", "Côte d'Ivoire", "Sénégal", "France", "Canada", "Autres"],
    "types_evenements": ["Formation", "Groupe d'étude", "BA MEET UP", "Webinaire", "Conférence", "Certification"],
    "lieux": ["Présentiel", "Zoom", "Hybride"],
    "resultats_inter": ["Positif", "Négatif", "À suivre", "Sans suite"],
    "statuts_paiement": ["Réglé", "Partiel", "Non payé"],
    "moyens_paiement": ["Mobile Money", "Virement", "CB", "Cash"],
    "types_certif": ["ECBA", "CCBA", "CBAP", "PBA"],
    "entreprises_cibles": ["Dangote", "MUPECI", "SALAM", "SUNU IARD", "ENEO", "PAD", "PAK"],
}

def load_settings():
    if PATHS["settings"].exists():
        try:
            d = json.loads(PATHS["settings"].read_text(encoding="utf-8"))
        except Exception:
            d = DEFAULT.copy()
    else:
        d = DEFAULT.copy()
    for k, v in DEFAULT.items():
        if k not in d or not isinstance(d[k], list): d[k] = v
    return d

def save_settings(d: dict):
    PATHS["settings"].write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

SET = load_settings()

# ------------------------------------------------------------------
# Schémas des tables
# ------------------------------------------------------------------
C_COLS = ["ID","Nom","Prénom","Genre","Titre","Société","Secteur","Email","Téléphone","LinkedIn",
          "Ville","Pays","Type","Source","Statut","Score_Engagement","Date_Creation","Notes","Top20"]
I_COLS = ["ID_Interaction","ID","Date","Canal","Objet","Résumé","Résultat","Prochaine_Action","Relance","Responsable"]
E_COLS = ["ID_Événement","Nom_Événement","Type","Date","Durée_h","Lieu","Formateur","Objectif","Periode",
          "Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total","Notes"]
P_COLS = ["ID_Participation","ID","ID_Événement","Rôle","Inscription","Arrivée","Temps_Present","Feedback","Note","Commentaire"]
PAY_COLS = ["ID_Paiement","ID","ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence","Notes","Relance"]
CERT_COLS = ["ID_Certif","ID","Type_Certif","Date_Examen","Résultat","Score","Date_Obtention","Validité","Renouvellement","Notes"]

ALL_SCHEMAS = {
    "contacts": C_COLS,
    "interactions": I_COLS,
    "evenements": E_COLS,
    "participations": P_COLS,
    "paiements": PAY_COLS,
    "certifications": CERT_COLS,
}

# ------------------------------------------------------------------
# Utilitaires
# ------------------------------------------------------------------
def ensure_df(path: Path, columns: list) -> pd.DataFrame:
    if path.exists():
        try: df = pd.read_csv(path, dtype=str, encoding="utf-8")
        except Exception: df = pd.DataFrame(columns=columns)
    else:
        df = pd.DataFrame(columns=columns)
    for c in columns:
        if c not in df.columns: df[c] = ""
    return df[columns]

def save_df(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False, encoding="utf-8")

def parse_date(s: str):
    if not s or pd.isna(s): return None
    for fmt in ("%Y-%m-%d","%d/%m/%Y","%Y/%m/%d"):
        try: return datetime.strptime(str(s), fmt).date()
        except Exception: pass
    try: return pd.to_datetime(s).date()
    except Exception: return None

def email_ok(s: str) -> bool:
    if not s or str(s).strip()=="" or str(s).lower()=="nan": return True
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", str(s).strip()))

def phone_ok(s: str) -> bool:
    if not s or str(s).strip()=="" or str(s).lower()=="nan": return True
    s2 = re.sub(r"[ \.\-]", "", str(s)).replace("+","")
    return s2.isdigit() and len(s2)>=8

def generate_id(prefix: str, df: pd.DataFrame, id_col: str, width: int=3) -> str:
    if df.empty or id_col not in df.columns:
        return f"{prefix}_{str(1).zfill(width)}"
    patt = re.compile(rf"^{re.escape(prefix)}_(\d+)$")
    mx = 0
    for x in df[id_col].dropna().astype(str):
        m = patt.match(x.strip())
        if m:
            try: mx = max(mx, int(m.group(1)))
            except Exception: pass
    return f"{prefix}_{str(mx+1).zfill(width)}"

def dedupe_contacts(df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    """Retourne (df_dedup, df_rejects) avec raisons des rejets."""
    df = df.copy()
    rejects = []
    seen = set()
    keep = []
    def norm(s): return str(s).strip().lower()
    for _, r in df.iterrows():
        # validation email/tel
        if not email_ok(r.get("Email","")):
            rr = r.to_dict(); rr["_Raison"]="Email invalide"; rejects.append(rr); continue
        if not phone_ok(r.get("Téléphone","")):
            rr = r.to_dict(); rr["_Raison"]="Téléphone invalide"; rejects.append(rr); continue
        # clé de doublon
        if r.get("Email",""):
            key = ("email", norm(r["Email"]))
        elif r.get("Téléphone",""):
            key = ("tel", norm(r["Téléphone"]))
        else:
            key = ("nps", (norm(r.get("Nom","")), norm(r.get("Prénom","")), norm(r.get("Société",""))))
        if key in seen:
            rr = r.to_dict(); rr["_Raison"]="Doublon détecté"; rejects.append(rr); continue
        seen.add(key); keep.append(r)
    return pd.DataFrame(keep, columns=C_COLS), pd.DataFrame(rejects)

# ------------------------------------------------------------------
# Charger les données existantes
# ------------------------------------------------------------------
df_contacts = ensure_df(PATHS["contacts"], C_COLS)
df_inter    = ensure_df(PATHS["inter"], I_COLS)
df_events   = ensure_df(PATHS["events"], E_COLS)
df_parts    = ensure_df(PATHS["parts"], P_COLS)
df_pay      = ensure_df(PATHS["pay"], PAY_COLS)
df_cert     = ensure_df(PATHS["cert"], CERT_COLS)

# Flag Top20 auto
if not df_contacts.empty:
    df_contacts["Top20"] = df_contacts["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])

# ------------------------------------------------------------------
# Navigation
# ------------------------------------------------------------------
st.sidebar.title("Navigation")
page = st.sidebar.radio("Aller à",
    ["CRM (Grille centrale)", "Événements", "Rapports", "Dashboard", "Admin"], index=0
)
this_year = datetime.now().year
annee = st.sidebar.selectbox("Année", ["Toutes"]+[str(this_year-1), str(this_year), str(this_year+1)], index=1)
mois   = st.sidebar.selectbox("Mois", ["Tous"]+[f"{m:02d}" for m in range(1,13)], index=0)

# ------------------------------------------------------------------
# Fonctions analytiques (abrégées)
# ------------------------------------------------------------------
def filtered_tables_for_period(year_sel: str, month_sel: str):
    def in_period(d: pd.Series) -> pd.Series:
        p = d.map(parse_date)
        m = p.notna()
        if year_sel!="Toutes":
            y = int(year_sel); m = m & p.map(lambda x: x.year==y if x else False)
        if month_sel!="Tous":
            mm = int(month_sel); m = m & p.map(lambda x: x.month==mm if x else False)
        return m.fillna(False)
    dfe2 = df_events[in_period(df_events["Date"])].copy() if not df_events.empty else df_events.copy()
    dfp2 = df_parts.copy()
    if not df_events.empty and not df_parts.empty:
        evd = df_events.set_index("ID_Événement")["Date"].map(parse_date)
        dfp2["_d"] = dfp2["ID_Événement"].map(evd)
        if year_sel!="Toutes": dfp2 = dfp2[dfp2["_d"].map(lambda x: x and x.year==int(year_sel))]
        if month_sel!="Tous": dfp2 = dfp2[dfp2["_d"].map(lambda x: x and x.month==int(month_sel))]
    dfpay2 = df_pay[in_period(df_pay["Date_Paiement"])].copy() if not df_pay.empty else df_pay.copy()
    dfcert2 = df_cert[in_period(df_cert["Date_Obtention"]) | in_period(df_cert["Date_Examen"])].copy() if not df_cert.empty else df_cert.copy()
    return dfe2, dfp2, dfpay2, dfcert2

def df_event_financials(dfe2: pd.DataFrame, dfpay2: pd.DataFrame) -> pd.DataFrame:
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
    rep = pd.DataFrame({"Nom_Événement":ev["Nom_Événement"],"Type":ev["Type"],"Date":ev["Date"],"Coût_Total":ev["Cout_Total"]})
    rep["Recette"] = rec_by_evt; rep["Recette"] = rep["Recette"].fillna(0.0)
    rep["Bénéfice"] = rep["Recette"] - rep["Coût_Total"]
    return rep.reset_index()

# ------------------------------------------------------------------
# Page CRM — Grille centrale (contacts)
# ------------------------------------------------------------------
if page == "CRM (Grille centrale)":
    st.title("👥 CRM — Grille centrale (Contacts)")
    st.caption("Sélectionnez un contact dans la grille. Utilisez les panneaux pour créer interactions, participations, paiements, certifications sans changer de page.")

    with st.expander("➕ Nouveau contact", expanded=False):
        with st.form("quick_new_contact"):
            cc1, cc2, cc3, cc4 = st.columns(4)
            q_nom = cc1.text_input("Nom*")
            q_prenom = cc2.text_input("Prénom*")
            q_genre = cc3.selectbox("Genre", SET["genres"])
            q_titre = cc4.text_input("Titre/Fonction")
            cc5, cc6, cc7 = st.columns(3)
            q_soc = cc5.text_input("Société")
            q_sect = cc6.selectbox("Secteur", SET["secteurs"])
            q_email = cc7.text_input("Email")
            cc8, cc9, cc10 = st.columns(3)
            q_tel = cc8.text_input("Téléphone")
            q_ville = cc9.selectbox("Ville", SET["villes"])
            q_pays = cc10.selectbox("Pays", SET["pays"])
            cc11, cc12, cc13 = st.columns(3)
            q_type = cc11.selectbox("Type", SET["types_contact"])
            q_src = cc12.selectbox("Source", SET["sources"])
            q_stat = cc13.selectbox("Statut d'engagement", SET["statuts_engagement"])
            q_notes = st.text_area("Notes")
            submitted_quick = st.form_submit_button("💾 Créer le contact")
            if submitted_quick:
                if not q_nom or not q_prenom: st.error("Nom et Prénom sont obligatoires.")
                elif not email_ok(q_email):   st.error("Email invalide.")
                elif not phone_ok(q_tel):     st.error("Téléphone invalide (8 chiffres min.).")
                else:
                    new_id = generate_id("CNT", df_contacts, "ID")
                    top20 = (q_soc or "").strip() in SET["entreprises_cibles"]
                    new_row = {"ID": new_id, "Nom": q_nom, "Prénom": q_prenom, "Genre": q_genre, "Titre": q_titre,
                               "Société": q_soc, "Secteur": q_sect, "Email": q_email, "Téléphone": q_tel, "LinkedIn":"",
                               "Ville": q_ville, "Pays": q_pays, "Type": q_type, "Source": q_src, "Statut": q_stat,
                               "Score_Engagement": 0, "Date_Creation": date.today().isoformat(), "Notes": q_notes, "Top20": top20}
                    df_contacts = pd.concat([df_contacts, pd.DataFrame([new_row])], ignore_index=True)
                    save_df(df_contacts, PATHS["contacts"])
                    st.success(f"Contact créé (ID {new_id}).")

    colf1, colf2, colf3, colf4 = st.columns([2,2,2,2])
    q = colf1.text_input("Recherche (nom, société, email)…", "")
    type_filtre = colf2.selectbox("Type", ["Tous"]+SET["types_contact"])
    statut_eng = colf3.selectbox("Statut d'engagement", ["Tous"]+SET["statuts_engagement"])
    top20_only = colf4.checkbox("Top-20 seulement", value=False)

    dfc = df_contacts.copy()
    if q:
        qs = q.lower()
        dfc = dfc[dfc.apply(lambda r: qs in str(r["Nom"]).lower() or qs in str(r["Prénom"]).lower() or qs in str(r["Société"]).lower() or qs in str(r["Email"]).lower(), axis=1)]
    if type_filtre != "Tous": dfc = dfc[dfc["Type"]==type_filtre]
    if statut_eng != "Tous": dfc = dfc[dfc["Statut"]==statut_eng]
    if top20_only: dfc = dfc[dfc["Top20"]==True]

    sel_id = None
    if HAS_AGGRID and not dfc.empty:
        gob = GridOptionsBuilder.from_dataframe(dfc[["ID","Nom","Prénom","Société","Type","Statut","Email","Téléphone","Ville","Pays","Top20"]])
        gob.configure_selection("single", use_checkbox=True)
        gob.configure_grid_options(domLayout='autoHeight')
        grid = AgGrid(dfc[["ID","Nom","Prénom","Société","Type","Statut","Email","Téléphone","Ville","Pays","Top20"]],
                      gridOptions=gob.build(), height=350, update_mode=GridUpdateMode.SELECTION_CHANGED)
        if grid and grid.get("selected_rows"):
            sel_id = grid["selected_rows"][0]["ID"]
    else:
        st.dataframe(dfc[["ID","Nom","Prénom","Société","Type","Statut","Email","Téléphone","Ville","Pays","Top20"]], use_container_width=True)
        sel_id = st.selectbox("Sélectionner un contact", [""]+dfc["ID"].tolist()) or None

    st.markdown("---")
    cL, cR = st.columns([1,2])
    with cL:
        st.subheader("Fiche Contact")
        if sel_id:
            c = df_contacts[df_contacts["ID"]==sel_id].iloc[0].to_dict()
            st.markdown(f"**{c.get('Prénom','')} {c.get('Nom','')}** — {c.get('Titre','')} chez **{c.get('Société','')}**")
            st.write(f"{c.get('Email','')} • {c.get('Téléphone','')} • {c.get('LinkedIn','')}")
            st.write(f"{c.get('Ville','')}, {c.get('Pays','')} — {c.get('Secteur','')}")
            st.write(f"Type: **{c.get('Type','')}** | Statut: **{c.get('Statut','')}** | Score: **{c.get('Score_Engagement','')}** | Top20: **{c.get('Top20','')}**")
        else:
            st.info("Sélectionnez un contact pour afficher sa fiche et actions.")

    with cR:
        st.subheader("Actions liées au contact sélectionné")
        if not sel_id:
            st.info("Sélectionnez un contact pour créer une interaction, participation, paiement ou certification.")
        else:
            tabs = st.tabs(["➕ Interaction", "➕ Participation", "➕ Paiement", "➕ Certification", "📑 Vue 360°"])
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
                        new_id = generate_id("INT", df_inter, "ID_Interaction")
                        row = {"ID_Interaction":new_id,"ID":sel_id,"Date":dti.isoformat(),"Canal":canal,"Objet":obj,"Résumé":resume,"Résultat":resu,"Prochaine_Action":"","Relance":(rel.isoformat() if rel else ""),"Responsable":resp}
                        df_inter = pd.concat([df_inter, pd.DataFrame([row])], ignore_index=True)
                        save_df(df_inter, PATHS["inter"])
                        st.success(f"Interaction enregistrée ({new_id}).")
            with tabs[1]:
                with st.form("add_part"):
                    if df_events.empty: st.warning("Créez d'abord un événement.")
                    else:
                        ide = st.selectbox("Événement", df_events["ID_Événement"].tolist())
                        role = st.selectbox("Rôle", ["Participant","Animateur","Invité"])
                        fb = st.selectbox("Feedback", ["Très satisfait","Satisfait","Moyen","Insatisfait"])
                        note = st.number_input("Note (1-5)", min_value=1, max_value=5, value=5)
                        ok = st.form_submit_button("💾 Enregistrer la participation")
                        if ok:
                            new_id = generate_id("PAR", df_parts, "ID_Participation")
                            row = {"ID_Participation":new_id,"ID":sel_id,"ID_Événement":ide,"Rôle":role,"Inscription":"","Arrivée":"","Temps_Present":"","Feedback":fb,"Note":str(note),"Commentaire":""}
                            df_parts = pd.concat([df_parts, pd.DataFrame([row])], ignore_index=True)
                            save_df(df_parts, PATHS["parts"])
                            st.success(f"Participation ajoutée ({new_id}).")
            with tabs[2]:
                with st.form("add_pay"):
                    if df_events.empty: st.warning("Créez d'abord un événement.")
                    else:
                        ide = st.selectbox("Événement", df_events["ID_Événement"].tolist())
                        dtp = st.date_input("Date paiement", value=date.today())
                        montant = st.number_input("Montant (FCFA)", min_value=0, step=1000)
                        moyen = st.selectbox("Moyen", SET["moyens_paiement"])
                        statut = st.selectbox("Statut", SET["statuts_paiement"])
                        ref = st.text_input("Référence")
                        ok = st.form_submit_button("💾 Enregistrer le paiement")
                        if ok:
                            new_id = generate_id("PAY", df_pay, "ID_Paiement")
                            row = {"ID_Paiement":new_id,"ID":sel_id,"ID_Événement":ide,"Date_Paiement":dtp.isoformat(),"Montant":str(montant),"Moyen":moyen,"Statut":statut,"Référence":ref,"Notes":"","Relance":""}
                            df_pay = pd.concat([df_pay, pd.DataFrame([row])], ignore_index=True)
                            save_df(df_pay, PATHS["pay"])
                            st.success(f"Paiement enregistré ({new_id}).")
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
                        new_id = generate_id("CER", df_cert, "ID_Certif")
                        row = {"ID_Certif":new_id,"ID":sel_id,"Type_Certif":tc,"Date_Examen":dte.isoformat(),"Résultat":res,"Score":str(sc),"Date_Obtention":(dto.isoformat() if dto else ""), "Validité":"","Renouvellement":"","Notes":""}
                        df_cert = pd.concat([df_cert, pd.DataFrame([row])], ignore_index=True)
                        save_df(df_cert, PATHS["cert"])
                        st.success(f"Certification ajoutée ({new_id}).")
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

# ------------------------------------------------------------------
# Événements (CRUD simplifié avec coûts)
# ------------------------------------------------------------------
elif page == "Événements":
    st.title("📅 Événements")
    with st.expander("➕ Créer / modifier un événement", expanded=True):
        mode = st.radio("Mode", ["Créer","Modifier"], horizontal=True)
        selected_evt = None
        if mode=="Modifier" and not df_events.empty:
            selected_evt = st.selectbox("Sélectionner", [""]+df_events["ID_Événement"].tolist())
        data = {k:"" for k in E_COLS}
        if mode=="Modifier" and selected_evt:
            data = df_events[df_events["ID_Événement"]==selected_evt].iloc[0].to_dict()
        with st.form("evt_form"):
            a,b,c = st.columns(3)
            nom = a.text_input("Nom Événement*", value=data.get("Nom_Événement",""))
            typ = b.selectbox("Type", SET["types_evenements"], index=SET["types_evenements"].index(data.get("Type", SET["types_evenements"][0])) if data.get("Type") in SET["types_evenements"] else 0)
            lieu = c.selectbox("Lieu", SET["lieux"], index=SET["lieux"].index(data.get("Lieu", SET["lieux"][0])) if data.get("Lieu") in SET["lieux"] else 0)
            d,e,f = st.columns(3)
            from datetime import timedelta
            default_date = parse_date(data.get("Date")) or date.today()
            dte = d.date_input("Date", value=default_date)
            try:
                duree_init = float(data.get("Durée_h") or 0.0)
            except Exception:
                duree_init = 0.0
            duree = e.number_input("Durée (h)", min_value=0.0, max_value=999.0, value=duree_init)
            formateur = f.text_input("Formateur(s)", value=data.get("Formateur",""))
            obj = st.text_area("Objectif", value=data.get("Objectif",""))
            st.markdown("##### Coûts (FCFA)")
            c1,c2,c3,c4,c5 = st.columns(5)
            def _num(x): 
                try: return float(x)
                except Exception: return 0.0
            cout_salle = c1.number_input("Salle", min_value=0.0, value=_num(data.get("Cout_Salle")))
            cout_form = c2.number_input("Formateur", min_value=0.0, value=_num(data.get("Cout_Formateur")))
            cout_log  = c3.number_input("Logistique", min_value=0.0, value=_num(data.get("Cout_Logistique")))
            cout_pub  = c4.number_input("Publicité", min_value=0.0, value=_num(data.get("Cout_Pub")))
            cout_aut  = c5.number_input("Autres", min_value=0.0, value=_num(data.get("Cout_Autres")))
            cout_total = cout_salle + cout_form + cout_log + cout_pub + cout_aut
            notes = st.text_area("Notes", value=data.get("Notes",""))
            ok = st.form_submit_button("💾 Enregistrer")
            if ok:
                if not nom: st.error("Nom obligatoire."); st.stop()
                if mode=="Créer":
                    new_id = generate_id("EVT", df_events, "ID_Événement")
                    row = {"ID_Événement":new_id,"Nom_Événement":nom,"Type":typ,"Date":dte.isoformat(),"Durée_h":duree,"Lieu":lieu,
                           "Formateur":formateur,"Objectif":obj,"Periode":dte.strftime("%B %Y"),
                           "Cout_Salle":cout_salle,"Cout_Formateur":cout_form,"Cout_Logistique":cout_log,"Cout_Pub":cout_pub,"Cout_Autres":cout_aut,"Cout_Total":cout_total,"Notes":notes}
                    df_events = pd.concat([df_events, pd.DataFrame([row])], ignore_index=True)
                    save_df(df_events, PATHS["events"])
                    st.success(f"Événement créé ({new_id}).")
                else:
                    idx = df_events.index[df_events["ID_Événement"]==selected_evt][0]
                    df_events.loc[idx, ["Nom_Événement","Type","Date","Durée_h","Lieu","Formateur","Objectif","Periode","Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total","Notes"]] = \
                        [nom,typ,dte.isoformat(),duree,lieu,formateur,obj,dte.strftime("%B %Y"),cout_salle,cout_form,cout_log,cout_pub,cout_aut,cout_total,notes]
                    save_df(df_events, PATHS["events"])
                    st.success(f"Événement modifié ({selected_evt}).")

    st.markdown("### Liste des événements")
    st.dataframe(df_events, use_container_width=True)

# ------------------------------------------------------------------
# Rapports (KPI + graphiques)
# ------------------------------------------------------------------
elif page == "Rapports":
    st.title("📑 Rapports & Graphiques")
    dfe2, dfp2, dfpay2, dfcert2 = filtered_tables_for_period(annee, mois)
    dfc2 = df_contacts.copy()

    total_contacts = len(dfc2)
    prospects_actifs = len(dfc2[(dfc2["Type"]=="Prospect") & (dfc2["Statut"]=="Actif")])
    membres = len(dfc2[dfc2["Type"]=="Membre"])
    events_count = len(dfe2) if not dfe2.empty else 0
    parts_total = len(dfp2) if not dfp2.empty else 0
    ca_regle = impayes = 0.0
    if not dfpay2.empty:
        dfpay2["Montant"] = pd.to_numeric(dfpay2["Montant"], errors="coerce").fillna(0.0)
        ca_regle = float(dfpay2[dfpay2["Statut"]=="Réglé"]["Montant"].sum())
        impayes = float(dfpay2[dfpay2["Statut"]!="Réglé"]["Montant"].sum())
    cert_ok = len(dfcert2[dfcert2["Résultat"]=="Réussi"]) if not dfcert2.empty else 0
    if annee != "Toutes":
        an_mask = dfc2["Date_Creation"].map(lambda x: parse_date(x).year == int(annee) if parse_date(x) else False)
        prospects_convertis = len(dfc2[an_mask & (dfc2["Type"]=="Membre")])
        prospects_total = len(dfc2[dfc2["Type"]=="Prospect"])
        taux_conv = (prospects_convertis / prospects_total * 100) if prospects_total else 0.0
    else:
        prospects_convertis = len(dfc2[dfc2["Type"]=="Membre"])
        prospects_total = len(dfc2[dfc2["Type"]=="Prospect"])
        taux_conv = (prospects_convertis / prospects_total * 100) if prospects_total else 0.0

    kpi = pd.DataFrame({
        "KPI": ["Total contacts","Prospects actifs","Membres","Événements","Participations","CA réglé (FCFA)","Impayés (FCFA)","Certifs obtenues","Prospects convertis","Taux de conversion (%)"],
        "Valeur": [total_contacts,prospects_actifs,membres,events_count,parts_total,int(ca_regle),int(impayes),cert_ok,prospects_convertis,round(taux_conv,1)]
    })
    st.markdown("### KPI principaux (période sélectionnée)")
    st.dataframe(kpi, use_container_width=True)

    st.markdown("### Événements : Recettes / Coûts / Bénéfices")
    rep_ev = df_event_financials(dfe2, dfpay2)
    st.dataframe(rep_ev.sort_values("Bénéfice", ascending=False), use_container_width=True)

    if alt is not None:
        st.markdown("#### Graphiques")
        if not rep_ev.empty:
            ch1 = alt.Chart(rep_ev.sort_values("Recette", ascending=False).head(20)).mark_bar().encode(
                x=alt.X("Nom_Événement:N", sort='-y', title="Événement"),
                y=alt.Y("Recette:Q", title="CA (FCFA)"),
                tooltip=["Nom_Événement","Date","Recette","Coût_Total","Bénéfice"]
            ).properties(height=350)
            st.altair_chart(ch1, use_container_width=True)

        rep_type = rep_ev.groupby("Type")["Recette"].sum().reset_index().sort_values("Recette", ascending=False)
        if not rep_type.empty:
            ch2 = alt.Chart(rep_type).mark_bar().encode(
                x=alt.X("Type:N", sort='-y', title="Type d'événement"),
                y=alt.Y("Recette:Q", title="CA (FCFA)"),
                tooltip=["Type","Recette"]
            ).properties(height=300)
            st.altair_chart(ch2, use_container_width=True)

        mca = dfpay2.copy()
        if not mca.empty:
            mca["Date_Paiement"] = mca["Date_Paiement"].map(parse_date)
            mca = mca[(~mca["Date_Paiement"].isna()) & (mca["Statut"]=="Réglé")]
            mca["Mois"] = mca["Date_Paiement"].map(lambda x: x.strftime("%Y-%m"))
            mca["Montant"] = pd.to_numeric(mca["Montant"], errors="coerce").fillna(0.0)
            mca = mca.groupby("Mois")["Montant"].sum().reset_index().rename(columns={"Montant":"CA"}).sort_values("Mois")
            ch3 = alt.Chart(mca).mark_line(point=True).encode(
                x=alt.X("Mois:T", title="Mois"),
                y=alt.Y("CA:Q", title="CA (FCFA)"),
                tooltip=["Mois","CA"]
            ).properties(height=300)
            st.altair_chart(ch3, use_container_width=True)
    else:
        st.info("Altair n'est pas installé. `pip install altair`.")

    # Export Excel multi-feuilles (base actuelle)
    try:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df_contacts.to_excel(writer, index=False, sheet_name="contacts")
            df_inter.to_excel(writer, index=False, sheet_name="interactions")
            df_events.to_excel(writer, index=False, sheet_name="evenements")
            df_parts.to_excel(writer, index=False, sheet_name="participations")
            df_pay.to_excel(writer, index=False, sheet_name="paiements")
            df_cert.to_excel(writer, index=False, sheet_name="certifications")
        st.download_button("⬇️ Télécharger la base (Excel, 6 feuilles)", buffer.getvalue(),
                           file_name="IIBA_base.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        st.warning(f"Export Excel indisponible : {e}")

# ------------------------------------------------------------------
# Dashboard
# ------------------------------------------------------------------
elif page == "Dashboard":
    st.title("📊 Dashboard 360")
    dfe2, dfp2, dfpay2, dfcert2 = filtered_tables_for_period(annee, mois)
    dfc2 = df_contacts.copy()
    total_contacts = len(dfc2)
    prospects_actifs = len(dfc2[(dfc2["Type"]=="Prospect") & (dfc2["Statut"]=="Actif")])
    membres = len(dfc2[dfc2["Type"]=="Membre"])
    events_count = len(dfe2) if not dfe2.empty else 0
    parts_total = len(dfp2) if not dfp2.empty else 0
    ca_regle = impayes = 0.0
    if not dfpay2.empty:
        dfpay2["Montant"] = pd.to_numeric(dfpay2["Montant"], errors="coerce").fillna(0.0)
        ca_regle = float(dfpay2[dfpay2["Statut"]=="Réglé"]["Montant"].sum())
        impayes = float(dfpay2[dfpay2["Statut"]!="Réglé"]["Montant"].sum())
    cert_ok = len(dfcert2[dfcert2["Résultat"]=="Réussi"]) if not dfcert2.empty else 0
    if annee != "Toutes":
        an_mask = dfc2["Date_Creation"].map(lambda x: parse_date(x).year == int(annee) if parse_date(x) else False)
        prospects_convertis = len(dfc2[an_mask & (dfc2["Type"]=="Membre")])
        prospects_total = len(dfc2[dfc2["Type"]=="Prospect"])
        taux_conv = (prospects_convertis / prospects_total * 100) if prospects_total else 0.0
    else:
        prospects_convertis = len(dfc2[dfc2["Type"]=="Membre"])
        prospects_total = len(dfc2[dfc2["Type"]=="Prospect"])
        taux_conv = (prospects_convertis / prospects_total * 100) if prospects_total else 0.0

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("👥 Contacts", total_contacts)
    c2.metric("🧲 Prospects actifs", prospects_actifs)
    c3.metric("🏆 Membres", membres)
    c4.metric("💰 CA réglé", f"{int(ca_regle):,} FCFA".replace(",", " "))

# ------------------------------------------------------------------
# Admin — Paramètres & Migration (incl. import Excel global)
# ------------------------------------------------------------------
elif page == "Admin":
    st.title("⚙️ Admin — Paramètres & Migration")

    st.markdown("#### Paramètres (listes déroulantes)")
    with st.form("set_form"):
        c1,c2,c3 = st.columns(3)
        genres = c1.text_area("Genres", "\n".join(SET["genres"]))
        types_contact = c2.text_area("Types de contact", "\n".join(SET["types_contact"]))
        statuts_eng = c3.text_area("Statuts d'engagement", "\n".join(SET["statuts_engagement"]))
        s1,s2,s3 = st.columns(3)
        secteurs = s1.text_area("Secteurs", "\n".join(SET["secteurs"]))
        pays = s2.text_area("Pays", "\n".join(SET["pays"]))
        villes = s3.text_area("Villes", "\n".join(SET["villes"]))
        s4,s5,s6 = st.columns(3)
        sources = s4.text_area("Sources", "\n".join(SET["sources"]))
        canaux = s5.text_area("Canaux", "\n".join(SET["canaux"]))
        resint = s6.text_area("Résultats interaction", "\n".join(SET["resultats_inter"]))
        e1,e2,e3 = st.columns(3)
        types_evt = e1.text_area("Types événements", "\n".join(SET["types_evenements"]))
        lieux = e2.text_area("Lieux", "\n".join(SET["lieux"]))
        moyens = e3.text_area("Moyens paiement", "\n".join(SET["moyens_paiement"]))
        e4,e5 = st.columns(2)
        statpay = e4.text_area("Statuts paiement", "\n".join(SET["statuts_paiement"]))
        tcert = e5.text_area("Types certification", "\n".join(SET["types_certif"]))
        top20 = st.text_area("Entreprises cibles (Top-20 / GECAM)", "\n".join(SET["entreprises_cibles"]))
        ok = st.form_submit_button("💾 Enregistrer")
        if ok:
            try:
                SET.update({
                    "genres": [x.strip() for x in genres.splitlines() if x.strip()],
                    "types_contact": [x.strip() for x in types_contact.splitlines() if x.strip()],
                    "statuts_engagement": [x.strip() for x in statuts_eng.splitlines() if x.strip()],
                    "secteurs": [x.strip() for x in secteurs.splitlines() if x.strip()],
                    "pays": [x.strip() for x in pays.splitlines() if x.strip()],
                    "villes": [x.strip() for x in villes.splitlines() if x.strip()],
                    "sources": [x.strip() for x in sources.splitlines() if x.strip()],
                    "canaux": [x.strip() for x in canaux.splitlines() if x.strip()],
                    "resultats_inter": [x.strip() for x in resint.splitlines() if x.strip()],
                    "types_evenements": [x.strip() for x in types_evt.splitlines() if x.strip()],
                    "lieux": [x.strip() for x in lieux.splitlines() if x.strip()],
                    "moyens_paiement": [x.strip() for x in moyens.splitlines() if x.strip()],
                    "statuts_paiement": [x.strip() for x in statpay.splitlines() if x.strip()],
                    "types_certif": [x.strip() for x in tcert.splitlines() if x.strip()],
                    "entreprises_cibles": [x.strip() for x in top20.splitlines() if x.strip()],
                })
                save_settings(SET)
                st.success("Paramètres enregistrés.")
            except Exception as e:
                st.error(f"Erreur : {e}")

    st.markdown("---")
    st.header("📦 Migration — Global Excel / CSV + rapport")

    mode_mig = st.radio("Mode de migration", ["Import Excel global (.xlsx)", "Import CSV global", "Par table (CSV)"], horizontal=True)

    # -----------------------------
    # Import EXCEL GLOBAL
    # -----------------------------
    if mode_mig == "Import Excel global (.xlsx)":
        up = st.file_uploader("Fichier Excel global (.xlsx)", type=["xlsx"], key="xlsx_up")
        st.caption("Le classeur doit contenir une feuille **Global** (ou la première feuille) avec la colonne **__TABLE__** pour indiquer la table cible.")
        if st.button("Importer l'Excel global"):
            log = {"timestamp": datetime.now().isoformat(), "import_type":"excel_global", "counts": {}, "errors": []}
            try:
                if up is None:
                    raise ValueError("Aucun fichier fourni.")
                xls = pd.ExcelFile(up)
                sheet = "Global" if "Global" in xls.sheet_names else xls.sheet_names[0]
                gdf = pd.read_excel(xls, sheet_name=sheet, dtype=str)
                if "__TABLE__" not in gdf.columns:
                    raise ValueError("La colonne '__TABLE__' est manquante.")
                # Normalisation : colonnes manquantes → vides
                gcols = ["__TABLE__"] + sorted(set(sum(ALL_SCHEMAS.values(), [])))
                for c in gcols:
                    if c not in gdf.columns:
                        gdf[c] = ""
                # CONTACTS : validation + dédup (incluant base existante)
                sub_c = gdf[gdf["__TABLE__"]=="contacts"].copy()
                sub_c = sub_c[C_COLS].fillna("")
                sub_c["Top20"] = sub_c["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])
                valid_c, rejects_c = dedupe_contacts(sub_c)
                # Filtrage des doublons par rapport à la base existante
                def _key_contact(r):
                    def norm(s): return str(s).strip().lower()
                    if r.get("Email",""):
                        return ("email", norm(r["Email"]))
                    elif r.get("Téléphone",""):
                        return ("tel", norm(r["Téléphone"]))
                    else:
                        return ("nps", (norm(r.get("Nom","")), norm(r.get("Prénom","")), norm(r.get("Société",""))))
                existing_keys = set()
                for rec in df_contacts.fillna("").to_dict(orient="records"):
                    existing_keys.add(_key_contact(rec))
                keep_rows = []
                for _, r in valid_c.iterrows():
                    k = _key_contact(r)
                    if k in existing_keys:
                        rr = r.to_dict(); rr["_Raison"] = "Doublon avec base existante"
                        rejects_c = pd.concat([rejects_c, pd.DataFrame([rr])], ignore_index=True) if isinstance(rejects_c, pd.DataFrame) else pd.DataFrame([rr])
                    else:
                        keep_rows.append(r)
                        existing_keys.add(k)
                valid_c = pd.DataFrame(keep_rows, columns=C_COLS) if keep_rows else pd.DataFrame(columns=C_COLS)
                # IDs manquants : séquence continue à partir de la base existante
                patt = re.compile(r"^CNT_(\d+)$")
                base_max = 0
                for x in df_contacts["ID"].dropna().astype(str):
                    m = patt.match(x.strip())
                    if m:
                        try: base_max = max(base_max, int(m.group(1)))
                        except Exception: pass
                next_id = base_max + 1
                ids = []
                for _, r in valid_c.iterrows():
                    rid = r["ID"]
                    if not isinstance(rid, str) or rid.strip()=="" or rid.strip().lower()=="nan":
                        rid = f"CNT_{str(next_id).zfill(3)}"
                        next_id += 1
                    ids.append(rid)
                if not valid_c.empty:
                    valid_c["ID"] = ids
                    # Append à la base existante
                    df_contacts = pd.concat([df_contacts, valid_c[C_COLS]], ignore_index=True)
                    save_df(df_contacts, PATHS["contacts"])
                log["counts"]["contacts"] = int(len(valid_c))

                # AUTRES TABLES (IDs auto si vides + append)
                def save_subset(tbl, cols, path, prefix):
                    sub = gdf[gdf["__TABLE__"]==tbl].copy()
                    sub = sub[cols].fillna("")
                    id_col = cols[0]
                    if id_col in sub.columns:
                        # Continuité d'IDs : repartir du max existant de la table
                        patt = re.compile(rf"^{prefix}_(\d+)$")
                        base_df = ensure_df(path, cols)
                        base_max = 0
                        for x in base_df[id_col].dropna().astype(str):
                            m = patt.match(x.strip())
                            if m:
                                try: base_max = max(base_max, int(m.group(1)))
                                except Exception: pass
                        gen = base_max + 1
                        new_ids = []
                        for _, r in sub.iterrows():
                            cur = r[id_col]
                            if not isinstance(cur, str) or cur.strip()=="" or cur.strip().lower()=="nan":
                                new_ids.append(f"{prefix}_{str(gen).zfill(3)}"); gen += 1
                            else:
                                new_ids.append(cur.strip())
                        sub[id_col] = new_ids
                    # Append à la base existante
                    base_df = ensure_df(path, cols)
                    sub = pd.concat([base_df, sub], ignore_index=True)
                    save_df(sub, path)
                    log["counts"][tbl] = int(len(sub))

                save_subset("interactions", I_COLS, PATHS["inter"], "INT")
                save_subset("evenements", E_COLS, PATHS["events"], "EVT")
                save_subset("participations", P_COLS, PATHS["parts"], "PAR")
                save_subset("paiements", PAY_COLS, PATHS["pay"], "PAY")
                save_subset("certifications", CERT_COLS, PATHS["cert"], "CER")

                st.success("Import Excel global terminé.")
                # Rapport
                st.markdown("#### Rapport d'import")
                st.json(log)
                if isinstance(rejects_c, pd.DataFrame) and not rejects_c.empty:
                    st.warning(f"Lignes **contacts** rejetées : {len(rejects_c)}")
                    st.dataframe(rejects_c, use_container_width=True)
                else:
                    st.info("Aucune ligne contact rejetée.")
            except Exception as e:
                st.error(f"Erreur d'import Excel global : {e}")

        st.markdown("##### Modèle Excel à télécharger")
        try:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                gcols = ["__TABLE__"] + sorted(set(sum(ALL_SCHEMAS.values(), [])))
                pd.DataFrame(columns=gcols).to_excel(w, index=False, sheet_name="Global")
            st.download_button("⬇️ Modèle Global (xlsx)", buf.getvalue(),
                               file_name="IIBA_global_template.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as e:
            st.warning(f"Impossible de générer le modèle Excel : {e}")

    # -----------------------------
    # Import CSV GLOBAL
    # -----------------------------
    elif mode_mig == "Import CSV global":
        up = st.file_uploader("CSV global (colonne __TABLE__)", type=["csv"], key="g_up")
        if st.button("Importer le CSV global") and up is not None:
            try:
                gdf = pd.read_csv(up, dtype=str, encoding="utf-8")
                if "__TABLE__" not in gdf.columns:
                    raise ValueError("La colonne __TABLE__ est manquante.")
                def save_subset(tbl, cols, path, prefix):
                    sub = gdf[gdf["__TABLE__"]==tbl].copy()
                    for c in cols:
                        if c not in sub.columns: sub[c] = ""
                    sub = sub[cols].fillna("")
                    id_col = cols[0]
                    # dédup/validation pour contacts
                    if tbl=="contacts":
                        sub["Top20"] = sub["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])
                        valid, rejects = dedupe_contacts(sub)
                        sub = valid
                        if not rejects.empty:
                            st.warning(f"Lignes contacts rejetées : {len(rejects)}")
                            st.dataframe(rejects, use_container_width=True)
                    if id_col in sub.columns:
                        # Continuité des IDs
                        patt = re.compile(rf"^{prefix}_(\d+)$")
                        base_df = ensure_df(path, cols)
                        base_max = 0
                        for x in base_df[id_col].dropna().astype(str):
                            m = patt.match(x.strip())
                            if m:
                                try: base_max = max(base_max, int(m.group(1)))
                                except Exception: pass
                        gen = base_max + 1
                        ids=[]
                        for _, r in sub.iterrows():
                            rid = r[id_col]
                            if not isinstance(rid,str) or rid.strip()=="" or rid.strip().lower()=="nan":
                                ids.append(f"{prefix}_{str(gen).zfill(3)}"); gen+=1
                            else:
                                ids.append(rid.strip())
                        sub[id_col] = ids
                    # append
                    base_df = ensure_df(path, cols)
                    out = pd.concat([base_df, sub], ignore_index=True)
                    save_df(out, path)
                save_subset("contacts", C_COLS, PATHS["contacts"], "CNT")
                save_subset("interactions", I_COLS, PATHS["inter"], "INT")
                save_subset("evenements", E_COLS, PATHS["events"], "EVT")
                save_subset("participations", P_COLS, PATHS["parts"], "PAR")
                save_subset("paiements", PAY_COLS, PATHS["pay"], "PAY")
                save_subset("certifications", CERT_COLS, PATHS["cert"], "CER")
                st.success("Import CSV global terminé.")
            except Exception as e:
                st.error(f"Erreur d'import CSV global : {e}")

        # Export global CSV
        gcols = ["__TABLE__"] + sorted(set(sum(ALL_SCHEMAS.values(), [])))
        rows = []
        for tbl, df in [("contacts", df_contacts), ("interactions", df_inter), ("evenements", df_events), ("participations", df_parts), ("paiements", df_pay), ("certifications", df_cert)]:
            d = df.copy().fillna("")
            d["__TABLE__"] = tbl
            for c in gcols:
                if c not in d.columns: d[c] = ""
            rows.append(d[gcols])
        gexport = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=gcols)
        st.download_button("⬇️ Export global (CSV)", gexport.to_csv(index=False).encode("utf-8"),
                           file_name="IIBA_global_export.csv", mime="text/csv")

    # -----------------------------
    # Mode PAR TABLE (CSV)
    # -----------------------------
    else:
        cimp, cexp = st.columns(2)
        with cimp:
            st.subheader("Importer (par table)")
            up_kind = st.selectbox("Table", list(ALL_SCHEMAS.keys()))
            up = st.file_uploader("CSV", type=["csv"])
            if st.button("Importer", key="imp_table") and up is not None:
                df_new = pd.read_csv(up, dtype=str, encoding="utf-8")
                cols = ALL_SCHEMAS[up_kind]
                for c in cols:
                    if c not in df_new.columns: df_new[c] = ""
                df_new = df_new[cols]
                save_df(df_new, PATHS["contacts" if up_kind=="contacts" else
                                      "inter" if up_kind=="interactions" else
                                      "events" if up_kind=="evenements" else
                                      "parts" if up_kind=="participations" else
                                      "pay" if up_kind=="paiements" else
                                      "cert"])
                st.success("Import terminé.")
        with cexp:
            st.subheader("Exporter (par table)")
            kind = st.selectbox("Table à exporter", list(ALL_SCHEMAS.keys()))
            if st.button("Exporter", key="exp_table"):
                dfx = (df_contacts if kind=="contacts" else
                       df_inter    if kind=="interactions" else
                       df_events   if kind=="evenements" else
                       df_parts    if kind=="participations" else
                       df_pay      if kind=="paiements" else
                       df_cert)
                st.download_button("⬇️ Télécharger CSV", dfx.to_csv(index=False).encode("utf-8"),
                                   file_name=f"{kind}.csv", mime="text/csv")

st.sidebar.markdown("---")
st.sidebar.caption("© IIBA Cameroun — CRM monofichier")
