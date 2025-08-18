# -*- coding: utf-8 -*-
"""
IIBA Cameroun — CRM (monofichier)
---------------------------------
- ✅ Vue CRM centrale (AgGrid) : gestion Contacts + panneaux latéraux (Interactions, Participations, Paiements, Certifications)
- ✅ Rapports avancés : CA mensuel, bénéfice par événement, CA par type, prospects réguliers non convertis, Top‑20 GECAM
- ✅ Dashboard (KPI clés + relances)
- ✅ Admin : Paramètres & Migration (import/export CSV)
Dépendances : streamlit, pandas, numpy, altair, openpyxl, streamlit-aggrid
"""
import io
import json
import re
from datetime import datetime, date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

# AgGrid (facultatif : fallback auto vers st.dataframe si non installé)
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
    HAS_AGGRID = True
except Exception:
    HAS_AGGRID = False

# Graphiques
try:
    import altair as alt
except Exception:
    alt = None

st.set_page_config(page_title="IIBA Cameroun — CRM", page_icon="📊", layout="wide")

# -----------------------------
# Chemins & fichiers de données
# -----------------------------
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "inter": DATA_DIR / "interactions.csv",
    "events": DATA_DIR / "evenements.csv",
    "parts": DATA_DIR / "participations.csv",
    "pay": DATA_DIR / "paiements.csv",
    "cert": DATA_DIR / "certifications.csv",
    "settings": DATA_DIR / "settings.json",
}

# ------------------------------------
# Référentiels par défaut (Paramètres)
# ------------------------------------
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
    # complétion des clés manquantes
    for k, v in DEFAULT.items():
        if k not in d or not isinstance(d[k], list):
            d[k] = v
    return d

def save_settings(d: dict):
    PATHS["settings"].write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

SET = load_settings()

# ----------------
# Schémas CSV
# ----------------
C_COLS = ["ID","Nom","Prénom","Genre","Titre","Société","Secteur","Email","Téléphone","LinkedIn",
          "Ville","Pays","Type","Source","Statut","Score_Engagement","Date_Creation","Notes","Top20"]
I_COLS = ["ID_Interaction","ID","Date","Canal","Objet","Résumé","Résultat","Prochaine_Action","Relance","Responsable"]
E_COLS = ["ID_Événement","Nom_Événement","Type","Date","Durée_h","Lieu","Formateur","Objectif","Periode",
          "Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total","Notes"]
P_COLS = ["ID_Participation","ID","ID_Événement","Rôle","Inscription","Arrivée","Temps_Present","Feedback","Note","Commentaire"]
PAY_COLS = ["ID_Paiement","ID","ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence","Notes","Relance"]
CERT_COLS = ["ID_Certif","ID","Type_Certif","Date_Examen","Résultat","Score","Date_Obtention","Validité","Renouvellement","Notes"]

def ensure_df(path: Path, columns: list) -> pd.DataFrame:
    if path.exists():
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8")
        except Exception:
            df = pd.DataFrame(columns=columns)
    else:
        df = pd.DataFrame(columns=columns)
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    return df[columns]

def save_df(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False, encoding="utf-8")

def generate_id(prefix: str, df: pd.DataFrame, id_col: str, width: int=3) -> str:
    if df.empty or id_col not in df.columns:
        return f"{prefix}_{str(1).zfill(width)}"
    patt = re.compile(rf"^{re.escape(prefix)}_(\d+)$")
    mx = 0
    for x in df[id_col].dropna().astype(str):
        m = patt.match(x.strip())
        if m:
            try:
                n = int(m.group(1)); mx = max(mx, n)
            except Exception:
                pass
    return f"{prefix}_{str(mx+1).zfill(width)}"

def parse_date(s: str):
    if not s or pd.isna(s): return None
    for fmt in ("%Y-%m-%d","%d/%m/%Y","%Y/%m/%d"):
        try:
            return datetime.strptime(str(s), fmt).date()
        except Exception:
            continue
    try:
        return pd.to_datetime(s).date()
    except Exception:
        return None

def email_ok(s: str) -> bool:
    if not s: return True
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s))

def phone_ok(s: str) -> bool:
    if not s: return True
    s2 = re.sub(r"[ \.\-]", "", s).replace("+","")
    return s2.isdigit() and len(s2)>=8

# ----------------
# Charger données
# ----------------
df_contacts = ensure_df(PATHS["contacts"], C_COLS)
df_inter = ensure_df(PATHS["inter"], I_COLS)
df_events = ensure_df(PATHS["events"], E_COLS)
df_parts = ensure_df(PATHS["parts"], P_COLS)
df_pay = ensure_df(PATHS["pay"], PAY_COLS)
df_cert = ensure_df(PATHS["cert"], CERT_COLS)

# Top20 flag auto
if not df_contacts.empty:
    df_contacts["Top20"] = df_contacts["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])

# ----------------
# Filtres globaux
# ----------------
st.sidebar.title("Navigation")
page = st.sidebar.radio("Aller à",
    ["CRM (Grille centrale)", "Événements", "Rapports", "Dashboard", "Admin"],
    index=0
)

this_year = datetime.now().year
years = [str(this_year-1), str(this_year), str(this_year+1)]
annee = st.sidebar.selectbox("Année", ["Toutes"]+years, index=1)
mois = st.sidebar.selectbox("Mois", ["Tous"]+[f"{m:02d}" for m in range(1,13)], index=0)

# ----------------
# Fonctions analytiques
# ----------------
def filtered_tables_for_period(year_sel: str, month_sel: str):
    def in_period_series(d: pd.Series, year_sel: str, month_sel: str) -> pd.Series:
        parsed = d.map(parse_date)
        if year_sel != "Toutes":
            yy = int(year_sel)
            mask = parsed.map(lambda x: (x is not None) and (x.year == yy))
        else:
            mask = parsed.map(lambda x: x is not None)
        if month_sel != "Tous":
            mm = int(month_sel)
            mask = mask & parsed.map(lambda x: (x is not None) and (x.month == mm))
        return mask.fillna(False)

    dfe2 = df_events[in_period_series(df_events["Date"], year_sel, month_sel)].copy() if not df_events.empty else df_events.copy()
    dfp2 = df_parts.copy()
    if not df_events.empty and not df_parts.empty:
        ev_dates = df_events.set_index("ID_Événement")["Date"].map(parse_date)
        dfp2["_dtevt"] = dfp2["ID_Événement"].map(ev_dates)
        if year_sel != "Toutes":
            dfp2 = dfp2[dfp2["_dtevt"].map(lambda x: (x is not None) and (x.year == int(year_sel)))]
        if month_sel != "Tous":
            dfp2 = dfp2[dfp2["_dtevt"].map(lambda x: (x is not None) and (x.month == int(month_sel)))]
    dfpay2 = df_pay[in_period_series(df_pay["Date_Paiement"], year_sel, month_sel)].copy() if not df_pay.empty else df_pay.copy()
    dfcert2 = df_cert[in_period_series(df_cert["Date_Obtention"], year_sel, month_sel) | in_period_series(df_cert["Date_Examen"], year_sel, month_sel)].copy() if not df_cert.empty else df_cert.copy()
    return dfe2, dfp2, dfpay2, dfcert2

def df_event_financials(dfe2: pd.DataFrame, dfpay2: pd.DataFrame) -> pd.DataFrame:
    rec_by_evt = pd.Series(dtype=float)
    if not dfpay2.empty:
        rec = dfpay2[dfpay2["Statut"]=="Réglé"].copy()
        rec["Montant"] = pd.to_numeric(rec["Montant"], errors="coerce").fillna(0.0)
        rec_by_evt = rec.groupby("ID_Événement")["Montant"].sum()
    ev = df_events.copy() if dfe2.empty else dfe2.copy()
    for c in ["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total"]:
        ev[c] = pd.to_numeric(ev[c], errors="coerce").fillna(0.0)
    ev["Cout_Total"] = np.where(ev["Cout_Total"]>0, ev["Cout_Total"], ev[["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres"]].sum(axis=1))
    ev = ev.set_index("ID_Événement")
    rep = pd.DataFrame({
        "Nom_Événement": ev["Nom_Événement"],
        "Type": ev["Type"],
        "Date": ev["Date"],
        "Coût_Total": ev["Cout_Total"],
    })
    rep["Recette"] = rec_by_evt
    rep["Recette"] = rep["Recette"].fillna(0.0)
    rep["Bénéfice"] = rep["Recette"] - rep["Coût_Total"]
    rep = rep.reset_index()
    return rep

def monthly_ca(dfpay: pd.DataFrame, year_sel: str) -> pd.DataFrame:
    if dfpay.empty: return pd.DataFrame(columns=["Mois","CA"])
    d = dfpay.copy()
    d["Date_Paiement"] = d["Date_Paiement"].map(parse_date)
    d = d[(~d["Date_Paiement"].isna()) & (d["Statut"]=="Réglé")]
    if year_sel != "Toutes":
        yy = int(year_sel)
        d = d[d["Date_Paiement"].map(lambda x: x.year == yy)]
    d["Mois"] = d["Date_Paiement"].map(lambda x: x.strftime("%Y-%m"))
    d["Montant"] = pd.to_numeric(d["Montant"], errors="coerce").fillna(0.0)
    m = d.groupby("Mois")["Montant"].sum().reset_index().rename(columns={"Montant":"CA"})
    return m.sort_values("Mois")

def contact_type_distribution(dfc: pd.DataFrame) -> pd.DataFrame:
    if dfc.empty: return pd.DataFrame(columns=["Type","Count"])
    x = dfc["Type"].value_counts().reset_index()
    x.columns = ["Type","Count"]
    return x

def ca_by_event_type(rep_events: pd.DataFrame) -> pd.DataFrame:
    if rep_events.empty: return pd.DataFrame(columns=["Type","Recette"])
    x = rep_events.groupby("Type")["Recette"].sum().reset_index()
    return x.sort_values("Recette", ascending=False)

def avg_satisfaction_by_event_type(dfp2: pd.DataFrame, dfe2: pd.DataFrame) -> pd.DataFrame:
    if dfp2.empty or dfe2.empty: return pd.DataFrame(columns=["Type","SatisfactionMoy"])
    tmp = dfp2.copy()
    tmp["Note"] = pd.to_numeric(tmp["Note"], errors="coerce")
    ev_type = dfe2.set_index("ID_Événement")["Type"]
    tmp["Type"] = tmp["ID_Événement"].map(ev_type)
    res = tmp.dropna(subset=["Note","Type"]).groupby("Type")["Note"].mean().reset_index()
    res = res.rename(columns={"Note":"SatisfactionMoy"})
    return res.sort_values("SatisfactionMoy", ascending=False)

def prospects_reguliers_non_convertis(dfc: pd.DataFrame, dfp: pd.DataFrame, dfpay: pd.DataFrame, seuil: int=3) -> pd.DataFrame:
    if dfc.empty: return pd.DataFrame(columns=["ID","Nom","Prénom","Société","Type","Statut","Participations","A_Paye"])
    part_counts = dfp.groupby("ID")["ID_Participation"].count() if not dfp.empty else pd.Series(dtype=int)
    has_payment = set(dfpay[dfpay["Statut"]=="Réglé"]["ID"].tolist()) if not dfpay.empty else set()
    mask_prospects = dfc["Type"].eq("Prospect")
    df_pros = dfc[mask_prospects].copy()
    df_pros["Participations"] = df_pros["ID"].map(part_counts).fillna(0).astype(int)
    df_pros["A_Paye"] = df_pros["ID"].apply(lambda x: x in has_payment)
    res = df_pros[(df_pros["Participations"] >= seuil) & (~df_pros["A_Paye"])]
    return res.sort_values("Participations", ascending=False)

def top20_metrics(dfc: pd.DataFrame, dfpay: pd.DataFrame) -> pd.DataFrame:
    if dfc.empty: return pd.DataFrame(columns=["Société","Contacts","Membres","CA"])
    dfc2 = dfc.copy()
    dfc2["Top20"] = dfc2["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])
    top = dfc2[dfc2["Top20"]].copy()
    if not dfpay.empty:
        dfpay2 = dfpay.copy()
        dfpay2["Montant"] = pd.to_numeric(dfpay2["Montant"], errors="coerce").fillna(0.0)
        dfpay2 = dfpay2[dfpay2["Statut"]=="Réglé"]
    rows = []
    for soc, grp in top.groupby("Société"):
        ids = set(grp["ID"].tolist())
        ca = float(dfpay2[dfpay2["ID"].isin(ids)]["Montant"].sum()) if not dfpay.empty and not dfpay2.empty else 0.0
        rows.append({"Société": soc, "Contacts": int(grp.shape[0]), "Membres": int((grp['Type']=="Membre").sum()), "CA": ca})
    return pd.DataFrame(rows).sort_values("CA", ascending=False)

# ----------------
# CRM — Grille centrale (Contacts)
# ----------------
if page == "CRM (Grille centrale)":
    st.title("👥 CRM — Grille centrale (Contacts)")
    st.caption("Sélectionnez un contact dans la grille. Utilisez les panneaux pour créer interactions, participations, paiements, certifications sans changer de page.")
    # Création rapide d'un nouveau contact
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
                if not q_nom or not q_prenom:
                    st.error("Nom et Prénom sont obligatoires.")
                elif not email_ok(q_email):
                    st.error("Email invalide.")
                elif not phone_ok(q_tel):
                    st.error("Téléphone invalide (8 chiffres min.).")
                else:
                    new_id = generate_id("CNT", df_contacts, "ID")
                    top20 = (q_soc or "").strip() in SET["entreprises_cibles"]
                    new_row = {
                        "ID": new_id, "Nom": q_nom, "Prénom": q_prenom, "Genre": q_genre, "Titre": q_titre,
                        "Société": q_soc, "Secteur": q_sect, "Email": q_email, "Téléphone": q_tel, "LinkedIn": "",
                        "Ville": q_ville, "Pays": q_pays, "Type": q_type, "Source": q_src, "Statut": q_stat,
                        "Score_Engagement": 0, "Date_Creation": date.today().isoformat(), "Notes": q_notes, "Top20": top20
                    }
                    import pandas as pd
                    df_contacts = pd.concat([df_contacts, pd.DataFrame([new_row])], ignore_index=True)
                    df_contacts.to_csv(PATHS["contacts"], index=False, encoding="utf-8")
                    st.success(f"Contact créé (ID {new_id}). Actualisez la grille si nécessaire.")


    # Recherche/filtre simple
    colf1, colf2, colf3, colf4 = st.columns([2,2,2,2])
    q = colf1.text_input("Recherche (nom, société, email)…", "")
    type_filtre = colf2.selectbox("Type", ["Tous"]+SET["types_contact"])
    statut_eng = colf3.selectbox("Statut d'engagement", ["Tous"]+SET["statuts_engagement"])
    top20_only = colf4.checkbox("Top‑20 seulement", value=False)

    dfc = df_contacts.copy()
    if q:
        qs = q.lower()
        dfc = dfc[dfc.apply(lambda r: qs in str(r["Nom"]).lower() or qs in str(r["Prénom"]).lower() or qs in str(r["Société"]).lower() or qs in str(r["Email"]).lower(), axis=1)]
    if type_filtre != "Tous":
        dfc = dfc[dfc["Type"]==type_filtre]
    if statut_eng != "Tous":
        dfc = dfc[dfc["Statut"]==statut_eng]
    if top20_only:
        dfc = dfc[dfc["Top20"]==True]

    # Grille
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

    # Panneau GAUCHE : Fiche & Edition Contact
    with cL:
        st.subheader("Fiche Contact")
        if sel_id:
            c = df_contacts[df_contacts["ID"]==sel_id].iloc[0].to_dict()
            st.markdown(f"**{c.get('Prénom','')} {c.get('Nom','')}** — {c.get('Titre','')} chez **{c.get('Société','')}**")
            st.write(f"{c.get('Email','')} • {c.get('Téléphone','')} • {c.get('LinkedIn','')}")
            st.write(f"{c.get('Ville','')}, {c.get('Pays','')} — {c.get('Secteur','')}")
            st.write(f"Type: **{c.get('Type','')}** | Statut: **{c.get('Statut','')}** | Score: **{c.get('Score_Engagement','')}** | Top20: **{c.get('Top20','')}**")
            with st.expander("✏️ Modifier ce contact"):
                with st.form("edit_contact"):
                    c1,c2 = st.columns(2)
                    nom = c1.text_input("Nom*", value=c.get("Nom",""))
                    prenom = c2.text_input("Prénom*", value=c.get("Prénom",""))
                    c3,c4 = st.columns(2)
                    genre = c3.selectbox("Genre", SET["genres"], index=SET["genres"].index(c.get("Genre", SET["genres"][0])) if c.get("Genre") in SET["genres"] else 0)
                    titre = c4.text_input("Titre", value=c.get("Titre",""))
                    c5,c6,c7 = st.columns(3)
                    soc = c5.text_input("Société", value=c.get("Société",""))
                    secteur = c6.selectbox("Secteur", SET["secteurs"], index=SET["secteurs"].index(c.get("Secteur", SET["secteurs"][0])) if c.get("Secteur") in SET["secteurs"] else 0)
                    email = c7.text_input("Email", value=c.get("Email",""))
                    c8,c9,c10 = st.columns(3)
                    tel = c8.text_input("Téléphone", value=c.get("Téléphone",""))
                    ville = c9.selectbox("Ville", SET["villes"], index=SET["villes"].index(c.get("Ville", SET["villes"][0])) if c.get("Ville") in SET["villes"] else 0)
                    pays = c10.selectbox("Pays", SET["pays"], index=SET["pays"].index(c.get("Pays", SET["pays"][0])) if c.get("Pays") in SET["pays"] else 0)
                    c11,c12,c13 = st.columns(3)
                    typec = c11.selectbox("Type", SET["types_contact"], index=SET["types_contact"].index(c.get("Type", SET["types_contact"][0])) if c.get("Type") in SET["types_contact"] else 0)
                    src = c12.selectbox("Source", SET["sources"], index=SET["sources"].index(c.get("Source", SET["sources"][0])) if c.get("Source") in SET["sources"] else 0)
                    statut = c13.selectbox("Statut d'engagement", SET["statuts_engagement"], index=SET["statuts_engagement"].index(c.get("Statut", SET["statuts_engagement"][0])) if c.get("Statut") in SET["statuts_engagement"] else 0)
                    score = st.number_input("Score engagement", min_value=0, max_value=9999, value=int(pd.to_numeric(str(c.get("Score_Engagement") or 0), errors="coerce") or 0))
                    notes = st.text_area("Notes", value=c.get("Notes",""))
                    ok = st.form_submit_button("💾 Enregistrer")
                    if ok:
                        if not nom or not prenom: st.error("Nom/Prénom obligatoires."); st.stop()
                        if not email_ok(email): st.error("Email invalide."); st.stop()
                        if not phone_ok(tel): st.error("Téléphone invalide."); st.stop()
                        idx = df_contacts.index[df_contacts["ID"]==sel_id][0]
                        top20 = soc.strip() in SET["entreprises_cibles"]
                        df_contacts.loc[idx, ["Nom","Prénom","Genre","Titre","Société","Secteur","Email","Téléphone","Ville","Pays","Type","Source","Statut","Score_Engagement","Notes","Top20"]] = \
                            [nom,prenom,genre,titre,soc,secteur,email,tel,ville,pays,typec,src,statut,score,notes,top20]
                        save_df(df_contacts, PATHS["contacts"])
                        st.success("Contact mis à jour.")
        else:
            st.info("Sélectionnez un contact pour afficher sa fiche et actions.")

    # Panneau DROIT : Actions rapides & 360
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
                    rel = st.date_input("Relance", value=None)
                    ok = st.form_submit_button("💾 Enregistrer l'interaction")
                    if ok:
                        new_id = generate_id("INT", df_inter, "ID_Interaction")
                        row = {"ID_Interaction":new_id,"ID":sel_id,"Date":dti.isoformat(),"Canal":canal,"Objet":obj,"Résumé":resume,"Résultat":resu,"Prochaine_Action":"","Relance":(rel.isoformat() if rel else ""),"Responsable":resp}
                        df_inter = pd.concat([df_inter, pd.DataFrame([row])], ignore_index=True)
                        save_df(df_inter, PATHS["inter"])
                        st.success(f"Interaction enregistrée ({new_id}).")

            with tabs[1]:
                with st.form("add_part"):
                    if df_events.empty:
                        st.warning("Créez d'abord un événement."); 
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
                    dto = st.date_input("Date Obtention", value=None)
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

# ----------------
# Événements (CRUD simple avec coûts)
# ----------------
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
            dte = d.date_input("Date", value=parse_date(data.get("Date")) or date.today())
            duree = e.number_input("Durée (h)", min_value=0.0, max_value=999.0, value=float(data.get("Durée_h") or 0.0))
            formateur = f.text_input("Formateur(s)", value=data.get("Formateur",""))
            obj = st.text_area("Objectif", value=data.get("Objectif",""))
            st.markdown("##### Coûts (FCFA)")
            c1,c2,c3,c4,c5 = st.columns(5)
            cout_salle = c1.number_input("Salle", min_value=0.0, value=float(data.get("Cout_Salle") or 0.0))
            cout_form = c2.number_input("Formateur", min_value=0.0, value=float(data.get("Cout_Formateur") or 0.0))
            cout_log = c3.number_input("Logistique", min_value=0.0, value=float(data.get("Cout_Logistique") or 0.0))
            cout_pub = c4.number_input("Publicité", min_value=0.0, value=float(data.get("Cout_Pub") or 0.0))
            cout_aut = c5.number_input("Autres", min_value=0.0, value=float(data.get("Cout_Autres") or 0.0))
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

# ----------------
# Rapports avancés
# ----------------
elif page == "Rapports":
    st.title("📑 Rapports & Graphiques")
    dfe2, dfp2, dfpay2, dfcert2 = filtered_tables_for_period(annee, mois)
    dfc2 = df_contacts.copy()

    total_contacts = len(dfc2)
    prospects_actifs = len(dfc2[(dfc2["Type"]=="Prospect") & (dfc2["Statut"]=="Actif")])
    membres = len(dfc2[dfc2["Type"]=="Membre"])
    events_count = len(dfe2) if not dfe2.empty else 0
    parts_total = len(dfp2) if not dfp2.empty else 0
    ca_regle = 0.0; impayes = 0.0
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

        rep_type = ca_by_event_type(rep_ev)
        if not rep_type.empty:
            ch2 = alt.Chart(rep_type).mark_bar().encode(
                x=alt.X("Type:N", sort='-y', title="Type d'événement"),
                y=alt.Y("Recette:Q", title="CA (FCFA)"),
                tooltip=["Type","Recette"]
            ).properties(height=300)
            st.altair_chart(ch2, use_container_width=True)

        mca = monthly_ca(dfpay2, annee)
        if not mca.empty:
            ch3 = alt.Chart(mca).mark_line(point=True).encode(
                x=alt.X("Mois:T", title="Mois"),
                y=alt.Y("CA:Q", title="CA (FCFA)"),
                tooltip=["Mois","CA"]
            ).properties(height=300)
            st.altair_chart(ch3, use_container_width=True)

        dist = contact_type_distribution(dfc2)
        if not dist.empty:
            ch4 = alt.Chart(dist).mark_arc().encode(
                theta="Count:Q",
                color="Type:N",
                tooltip=["Type","Count"]
            ).properties(height=300)
            st.altair_chart(ch4, use_container_width=True)

        sat = avg_satisfaction_by_event_type(dfp2, dfe2)
        if not sat.empty:
            ch5 = alt.Chart(sat).mark_bar().encode(
                x=alt.X("Type:N", sort='-y', title="Type d'événement"),
                y=alt.Y("SatisfactionMoy:Q", title="Note moyenne /5"),
                tooltip=["Type","SatisfactionMoy"]
            ).properties(height=300)
            st.altair_chart(ch5, use_container_width=True)
    else:
        st.info("Altair n'est pas installé. Exécutez : `pip install altair`.")

    st.markdown("### Prospects réguliers non convertis")
    seuil = st.slider("Seuil de participations minimales", 1, 10, 3)
    res_pros = prospects_reguliers_non_convertis(dfc2, dfp2, dfpay2, seuil=seuil)
    st.dataframe(res_pros[["ID","Nom","Prénom","Société","Type","Statut","Participations","A_Paye"]], use_container_width=True)

    st.markdown("### Entreprises Top‑20 (GECAM) — Synthèse")
    top20_tbl = top20_metrics(dfc2, dfpay2)
    st.dataframe(top20_tbl, use_container_width=True)

    st.markdown("### Export CSV / Excel")
    kpi_csv = kpi.to_csv(index=False).encode("utf-8")
    rep_ev_csv = rep_ev.to_csv(index=False).encode("utf-8")
    res_pros_csv = res_pros.to_csv(index=False).encode("utf-8")
    top20_csv = top20_tbl.to_csv(index=False).encode("utf-8")
    c1,c2,c3,c4 = st.columns(4)
    c1.download_button("⬇️ KPI.csv", kpi_csv, file_name="kpi_periode.csv", mime="text/csv")
    c2.download_button("⬇️ Evenements.csv", rep_ev_csv, file_name="evenements_finance.csv", mime="text/csv")
    c3.download_button("⬇️ Prospects_non_convertis.csv", res_pros_csv, file_name="prospects_non_convertis.csv", mime="text/csv")
    c4.download_button("⬇️ Top20.csv", top20_csv, file_name="top20_entreprises.csv", mime="text/csv")

    try:
        import openpyxl  # ensure engine
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            kpi.to_excel(writer, index=False, sheet_name="KPI")
            rep_ev.sort_values("Bénéfice", ascending=False).to_excel(writer, index=False, sheet_name="Evenements")
            res_pros.to_excel(writer, index=False, sheet_name="Prospects")
            top20_tbl.to_excel(writer, index=False, sheet_name="Top20")
            mca = monthly_ca(dfpay2, annee)
            if not mca.empty:
                mca.to_excel(writer, index=False, sheet_name="CA_Mensuel")
            dist = contact_type_distribution(dfc2)
            if not dist.empty:
                dist.to_excel(writer, index=False, sheet_name="Types_Contacts")
        st.download_button("⬇️ Rapport_IIBA_Cameroun.xlsx", buffer.getvalue(),
                           file_name="Rapport_IIBA_Cameroun.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        st.warning(f"Export Excel indisponible : {e}")

# ----------------
# Dashboard (KPI + relances)
# ----------------
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
    c4.metric("🎓 Certifs obtenues", cert_ok)
    c5,c6,c7,c8 = st.columns(4)
    c5.metric("📅 Événements", events_count)
    c6.metric("🧾 Participations", parts_total)
    c7.metric("💰 CA réglé", f"{int(ca_regle):,} FCFA".replace(",", " "))
    c8.metric("⏳ Impayés", f"{int(impayes):,} FCFA".replace(",", " "))
    c9,c10 = st.columns(2)
    c9.metric("🔄 Prospects convertis", prospects_convertis)
    c10.metric("📈 Taux de conversion", f"{taux_conv:.1f}%")

    st.subheader("🔔 Relances à traiter")
    if df_inter.empty:
        st.info("Aucune interaction.")
    else:
        df_rel = df_inter.copy()
        df_rel["_relance"] = df_rel["Relance"].map(parse_date)
        today = date.today()
        overdue = df_rel[df_rel["_relance"].map(lambda x: x is not None and x < today)]
        soon = df_rel[df_rel["_relance"].map(lambda x: x is not None and today <= x <= today + timedelta(days=7))]
        cA, cB = st.columns(2)
        cA.write(f"**En retard** : {len(overdue)}")
        cA.dataframe(overdue[["ID_Interaction","ID","Objet","Relance","Responsable"]], use_container_width=True)
        cB.write(f"**Dans 7 jours** : {len(soon)}")
        cB.dataframe(soon[["ID_Interaction","ID","Objet","Relance","Responsable"]], use_container_width=True)

# ----------------
# Admin (Paramètres & Migration)
# ----------------
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
        top20 = st.text_area("Entreprises cibles (Top‑20 / GECAM)", "\n".join(SET["entreprises_cibles"]))
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

    
st.markdown("#### Migration (import/export CSV)")
st.write("Deux modes : **Global** (un seul CSV consolidé) ou **Par table**.")

mode_mig = st.radio("Mode de migration", ["Global (consolidé)", "Par table"], horizontal=True)

if mode_mig == "Global (consolidé)":
    st.subheader("Import global")
    up = st.file_uploader("CSV global (colonne __TABLE__)", type=["csv"], key="g_up")
    st.caption("Le fichier global doit contenir une colonne **__TABLE__** avec les valeurs : contacts, interactions, evenements, participations, paiements, certifications. Utilisez le modèle exporté ci-dessous.")
    if st.button("Importer le CSV global"):
        import pandas as pd
        try:
            gdf = pd.read_csv(up, dtype=str, encoding="utf-8")
            if "__TABLE__" not in gdf.columns:
                st.error("Le CSV doit contenir la colonne __TABLE__.")
            else:
                # Normalise colonnes
                def save_subset(tbl, cols, path):
                    sub = gdf[gdf["__TABLE__"]==tbl].copy()
                    for c in cols:
                        if c not in sub.columns: sub[c] = ""
                    sub = sub[cols].fillna("")
                    # Assign IDs if empty
                    id_col = cols[0]
                    prefix = {"contacts":"CNT","interactions":"INT","evenements":"EVT","participations":"PAR","paiements":"PAY","certifications":"CER"}[tbl]
                    # dedup only for contacts
                    if tbl=="contacts":
                        # dédoublonnage par email/tel ou (Nom,Prénom,Société)
                        def norm(x): return str(x).strip().lower()
                        seen=set(); keep=[]
                        for _, r in sub.iterrows():
                            if r.get("Email"): key=("email",norm(r["Email"]))
                            elif r.get("Téléphone"): key=("tel",norm(r["Téléphone"]))
                            else: key=("nps",(norm(r.get("Nom","")),norm(r.get("Prénom","")),norm(r.get("Société",""))))
                            if key in seen: continue
                            seen.add(key); keep.append(r)
                        sub = pd.DataFrame(keep, columns=cols)
                    # IDs
                    if id_col in sub.columns:
                        ids = []
                        i = 1
                        for _, r in sub.iterrows():
                            rid = r[id_col]
                            if not isinstance(rid, str) or rid.strip()=="" or rid.strip().lower()=="nan":
                                ids.append(f"{prefix}_{str(i).zfill(3)}"); i+=1
                            else:
                                ids.append(rid.strip())
                        sub[id_col] = ids
                    sub.to_csv(path, index=False, encoding="utf-8")
                save_subset("contacts", C_COLS, PATHS["contacts"])
                save_subset("interactions", I_COLS, PATHS["inter"])
                save_subset("evenements", E_COLS, PATHS["events"])
                save_subset("participations", P_COLS, PATHS["parts"])
                save_subset("paiements", PAY_COLS, PATHS["pay"])
                save_subset("certifications", CERT_COLS, PATHS["cert"])
                st.success("Import global terminé.")
        except Exception as e:
            st.error(f"Erreur d'import global : {e}")

    st.subheader("Export global")
    import pandas as pd
    gcols = ["__TABLE__"] + sorted(set(C_COLS + I_COLS + E_COLS + P_COLS + PAY_COLS + CERT_COLS))
    rows = []
    for tbl, df in [("contacts", df_contacts), ("interactions", df_inter), ("evenements", df_events), ("participations", df_parts), ("paiements", df_pay), ("certifications", df_cert)]:
        if df is None: continue
        d = df.copy().fillna("")
        d["__TABLE__"] = tbl
        # inject missing columns
        for c in gcols:
            if c not in d.columns: d[c] = ""
        d = d[gcols]
        rows.append(d)
    gexport = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=gcols)
    st.download_button("⬇️ Export global (CSV)", gexport.to_csv(index=False).encode("utf-8"), file_name="IIBA_global_export.csv", mime="text/csv")

else:
    cimp, cexp = st.columns(2)
    with cimp:
        st.write("**Importer (par table)**")
        up_kind = st.selectbox("Table à importer", ["contacts","interactions","evenements","participations","paiements","certifications"])
        up = st.file_uploader("CSV", type=["csv"])
        if st.button("Importer", key="imp_table") and up is not None:
            df_new = pd.read_csv(up, dtype=str, encoding="utf-8")
            if up_kind=="contacts":
                df_new = df_new[C_COLS]
                save_df(df_new, PATHS["contacts"])
            elif up_kind=="interactions":
                df_new = df_new[I_COLS]
                save_df(df_new, PATHS["inter"])
            elif up_kind=="evenements":
                df_new = df_new[E_COLS]
                save_df(df_new, PATHS["events"])
            elif up_kind=="participations":
                df_new = df_new[P_COLS]
                save_df(df_new, PATHS["parts"])
            elif up_kind=="paiements":
                df_new = df_new[PAY_COLS]
                save_df(df_new, PATHS["pay"])
            else:
                df_new = df_new[CERT_COLS]
                save_df(df_new, PATHS["cert"])
            st.success("Import terminé.")
    with cexp:
        st.write("**Exporter (par table)**")
        kind = st.selectbox("Table à exporter", ["contacts","interactions","evenements","participations","paiements","certifications"])
        if st.button("Exporter", key="exp_table"):
            if kind=="contacts": dfx = df_contacts
            elif kind=="interactions": dfx = df_inter
            elif kind=="evenements": dfx = df_events
            elif kind=="participations": dfx = df_parts
            elif kind=="paiements": dfx = df_pay
            else: dfx = df_cert
            st.download_button("⬇️ Télécharger CSV", dfx.to_csv(index=False).encode("utf-8"), file_name=f"{kind}.csv", mime="text/csv")


st.sidebar.markdown("---")
st.sidebar.caption("© IIBA Cameroun — CRM monofichier")
