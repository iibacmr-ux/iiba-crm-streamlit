# -*- coding: utf-8 -*-
"""
IIBA Cameroun — CRM Streamlit (version COMPLÈTE + Rapports & Graphiques)
------------------------------------------------------------------------
- Pages : Dashboard 360, Fiche 360 Contact, Contacts, Interactions, Événements, Participations, Paiements,
          Certifications, Rapports, Paramètres
- Référentiels cohérents (statut d'engagement ≠ statuts de paiement, types_certif, etc.)
- Intégrité : IDs via selectbox (pas de saisie libre)
- Événements : coûts (Salle/Formateur/Logistique/Pub/Autres/Total)
- KPI : Prospects actifs, convertis, taux de conversion, CA/Impayés, etc.
- Analyses : Bénéfice par événement, Prospects réguliers non convertis, Top‑20 (GECAM)
- Rapports : Graphiques Altair + Exports CSV/Excel multi-feuilles
"""
import os
import io
import json
import re
from pathlib import Path
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd
import streamlit as st

# Graphiques
try:
    import altair as alt
except Exception:
    alt = None

APP_NAME = "IIBA Cameroun — CRM"
st.set_page_config(page_title=APP_NAME, page_icon="📊", layout="wide")

# --------------------------
# Chemins & persistance
# --------------------------
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "interactions": DATA_DIR / "interactions.csv",
    "evenements": DATA_DIR / "evenements.csv",
    "participations": DATA_DIR / "participations.csv",
    "paiements": DATA_DIR / "paiements.csv",
    "certifications": DATA_DIR / "certifications.csv",
    "settings": DATA_DIR / "settings.json",
}

# --------------------------
# Référentiels (DEFAULT)
# --------------------------
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
    "entreprises_cibles": ["Dangote", "MUPECI", "SALAM", "SUNU IARD", "ENEO", "PAD", "PAK"]
}

def load_settings() -> dict:
    if PATHS["settings"].exists():
        try:
            with open(PATHS["settings"], "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = DEFAULT.copy()
    else:
        data = DEFAULT.copy()
    for k, v in DEFAULT.items():
        if k not in data or not isinstance(data[k], list):
            data[k] = v
    return data

def save_settings(d: dict):
    with open(PATHS["settings"], "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

SET = load_settings()

# --------------------------
# Schémas de colonnes
# --------------------------
C_COLS = [
    "ID","Nom","Prénom","Genre","Titre","Société","Secteur","Email","Téléphone","LinkedIn",
    "Ville","Pays","Type","Source","Statut","Score_Engagement","Date_Creation","Notes","Top20"
]
I_COLS = [
    "ID_Interaction","ID","Date","Canal","Objet","Résumé","Résultat","Prochaine_Action","Relance","Responsable"
]
E_COLS = [
    "ID_Événement","Nom_Événement","Type","Date","Durée_h","Lieu","Formateur","Objectif","Periode",
    "Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total","Notes"
]
P_COLS = [
    "ID_Participation","ID","ID_Événement","Rôle","Inscription","Arrivée","Temps_Present","Feedback","Note","Commentaire"
]
PAY_COLS = [
    "ID_Paiement","ID","ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence","Notes","Relance"
]
CERT_COLS = [
    "ID_Certif","ID","Type_Certif","Date_Examen","Résultat","Score","Date_Obtention","Validité","Renouvellement","Notes"
]

# --------------------------
# Helpers
# --------------------------
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
    df = df[columns]
    return df

def save_df(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False, encoding="utf-8")

def generate_id(prefix: str, df: pd.DataFrame, id_col: str, width: int = 3) -> str:
    if id_col not in df.columns or df.empty:
        return f"{prefix}_{str(1).zfill(width)}"
    patt = re.compile(rf"^{re.escape(prefix)}_(\d+)$")
    max_n = 0
    for x in df[id_col].dropna().astype(str):
        m = patt.match(x.strip())
        if m:
            try:
                n = int(m.group(1))
                if n > max_n:
                    max_n = n
            except ValueError:
                pass
    return f"{prefix}_{str(max_n+1).zfill(width)}"

def parse_date(s: str):
    if not s or pd.isna(s):
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(s), fmt).date()
        except Exception:
            continue
    try:
        return pd.to_datetime(s).date()
    except Exception:
        return None

def email_ok(s: str) -> bool:
    if not s:
        return True
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s))

def phone_ok(s: str) -> bool:
    if not s:
        return True
    s2 = re.sub(r"[ \.\-]", "", s)
    s2 = s2.replace("+", "")
    return s2.isdigit() and len(s2) >= 8

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

# --------------------------
# Charger les données
# --------------------------
df_contacts = ensure_df(PATHS["contacts"], C_COLS)
df_inter = ensure_df(PATHS["interactions"], I_COLS)
df_events = ensure_df(PATHS["evenements"], E_COLS)
df_parts = ensure_df(PATHS["participations"], P_COLS)
df_pay = ensure_df(PATHS["paiements"], PAY_COLS)
df_cert = ensure_df(PATHS["certifications"], CERT_COLS)

if not df_contacts.empty:
    df_contacts["Top20"] = df_contacts["Société"].fillna("").apply(lambda x: str(x).strip() in SET["entreprises_cibles"])

# --------------------------
# Barre latérale & période
# --------------------------
st.sidebar.title("Navigation IIBA CRM")
page = st.sidebar.radio(
    "Aller à :",
    ["Dashboard 360", "Fiche 360 Contact", "Contacts", "Interactions",
     "Événements", "Participations", "Paiements", "Certifications", "Rapports", "Paramètres"]
)

this_year = datetime.now().year
years = sorted({this_year-1, this_year, this_year+1})
annee = st.sidebar.selectbox("Année", ["Toutes"] + [str(y) for y in years], index=1)
mois = st.sidebar.selectbox("Mois", ["Tous"] + [f"{m:02d}" for m in range(1,13)], index=0)

# --------------------------
# Fonctions analytiques
# --------------------------
def filtered_tables_for_period(year_sel: str, month_sel: str):
    dfe2 = df_events[in_period_series(df_events["Date"], year_sel, month_sel)].copy() if not df_events.empty else df_events.copy()
    dfp2 = df_parts.copy()
    if not df_events.empty and not df_parts.empty:
        ev_dates = df_events.set_index("ID_Événement")["Date"].map(parse_date)
        dfp2 = df_parts.copy()
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
    ev["Cout_Total"] = np.where(ev["Cout_Total"]>0, ev["Cout_Total"],
                                ev[["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres"]].sum(axis=1))
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
    if dfpay.empty:
        return pd.DataFrame(columns=["Mois","CA"])
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
    if dfc.empty:
        return pd.DataFrame(columns=["Type","Count"])
    x = dfc["Type"].value_counts().reset_index()
    x.columns = ["Type","Count"]
    return x

def ca_by_event_type(rep_events: pd.DataFrame) -> pd.DataFrame:
    if rep_events.empty:
        return pd.DataFrame(columns=["Type","Recette"])
    x = rep_events.groupby("Type")["Recette"].sum().reset_index()
    return x.sort_values("Recette", ascending=False)

def avg_satisfaction_by_event_type(dfp2: pd.DataFrame, dfe2: pd.DataFrame) -> pd.DataFrame:
    if dfp2.empty or dfe2.empty:
        return pd.DataFrame(columns=["Type","SatisfactionMoy"])
    tmp = dfp2.copy()
    tmp["Note"] = pd.to_numeric(tmp["Note"], errors="coerce")
    ev_type = dfe2.set_index("ID_Événement")["Type"]
    tmp["Type"] = tmp["ID_Événement"].map(ev_type)
    res = tmp.dropna(subset=["Note","Type"]).groupby("Type")["Note"].mean().reset_index()
    res = res.rename(columns={"Note":"SatisfactionMoy"})
    return res.sort_values("SatisfactionMoy", ascending=False)

def prospects_reguliers_non_convertis(dfc: pd.DataFrame, dfp: pd.DataFrame, dfpay: pd.DataFrame, seuil: int = 3) -> pd.DataFrame:
    if dfc.empty:
        return pd.DataFrame(columns=["ID","Nom","Prénom","Société","Type","Statut","Participations","A_Paye"])
    part_counts = dfp.groupby("ID")["ID_Participation"].count() if not dfp.empty else pd.Series(dtype=int)
    has_payment = set(dfpay[dfpay["Statut"]=="Réglé"]["ID"].tolist()) if not dfpay.empty else set()
    mask_prospects = dfc["Type"].eq("Prospect")
    df_pros = dfc[mask_prospects].copy()
    df_pros["Participations"] = df_pros["ID"].map(part_counts).fillna(0).astype(int)
    df_pros["A_Paye"] = df_pros["ID"].apply(lambda x: x in has_payment)
    res = df_pros[(df_pros["Participations"] >= seuil) & (~df_pros["A_Paye"])]
    return res.sort_values("Participations", ascending=False)

def top20_metrics(dfc: pd.DataFrame, dfpay: pd.DataFrame) -> pd.DataFrame:
    if dfc.empty:
        return pd.DataFrame(columns=["Société","Contacts","Membres","CA"])
    dfc2 = dfc.copy()
    dfc2["Top20"] = dfc2["Société"].fillna("").apply(lambda x: x in SET["entreprises_cibles"])
    top = dfc2[dfc2["Top20"]].copy()
    # CA par société (somme des paiements réglés des IDs de cette société)
    if not dfpay.empty:
        dfpay2 = dfpay.copy()
        dfpay2["Montant"] = pd.to_numeric(dfpay2["Montant"], errors="coerce").fillna(0.0)
        dfpay2 = dfpay2[dfpay2["Statut"]=="Réglé"]
    agg_rows = []
    for soc, grp in top.groupby("Société"):
        contacts = int(grp.shape[0])
        membres = int((grp["Type"]=="Membre").sum())
        if not dfpay.empty:
            ids = set(grp["ID"].tolist())
            ca = float(dfpay2[dfpay2["ID"].isin(ids)]["Montant"].sum()) if not dfpay2.empty else 0.0
        else:
            ca = 0.0
        agg_rows.append({"Société": soc, "Contacts": contacts, "Membres": membres, "CA": ca})
    agg = pd.DataFrame(agg_rows)
    return agg.sort_values("CA", ascending=False)

# --------------------------
# Dashboard 360
# --------------------------
if page == "Dashboard 360":
    st.title("📊 Dashboard 360 — IIBA Cameroun")
    st.caption("Vue synthèse des activités, finances et engagement — filtrez par période via le menu latéral.")

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
    try:
        score_moy = pd.to_numeric(dfc2["Score_Engagement"], errors="coerce").dropna().mean()
    except Exception:
        score_moy = np.nan

    if annee != "Toutes":
        an_mask = dfc2["Date_Creation"].map(lambda x: parse_date(x).year == int(annee) if parse_date(x) else False)
        prospects_convertis = len(dfc2[an_mask & (dfc2["Type"]=="Membre")])
        prospects_total = len(dfc2[dfc2["Type"]=="Prospect"])
        taux_conv = (prospects_convertis / prospects_total * 100) if prospects_total else 0.0
    else:
        prospects_convertis = len(dfc2[dfc2["Type"]=="Membre"])
        prospects_total = len(dfc2[dfc2["Type"]=="Prospect"])
        taux_conv = (prospects_convertis / prospects_total * 100) if prospects_total else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("👥 Total contacts", total_contacts)
    c2.metric("🧲 Prospects actifs", prospects_actifs)
    c3.metric("🏆 Membres", membres)
    c4.metric("🎓 Certifications obtenues", cert_ok)

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("📅 Événements (période)", events_count)
    c6.metric("🧾 Participations (période)", parts_total)
    c7.metric("💰 CA réglé (période)", f"{int(ca_regle):,} FCFA".replace(",", " "))
    c8.metric("⏳ Impayés (période)", f"{int(impayes):,} FCFA".replace(",", " "))

    c9, c10 = st.columns(2)
    c9.metric("🔄 Prospects convertis", prospects_convertis)
    c10.metric("📈 Taux de conversion (%)", f"{taux_conv:.1f}%")

    st.subheader("🔔 Relances à traiter")
    if df_inter.empty:
        st.info("Aucune interaction enregistrée.")
    else:
        df_rel = df_inter.copy()
        df_rel["_relance"] = df_rel["Relance"].map(parse_date)
        today = date.today()
        overdue = df_rel[df_rel["_relance"].map(lambda x: x is not None and x < today)]
        soon = df_rel[df_rel["_relance"].map(lambda x: x is not None and today <= x <= today + timedelta(days=7))]
        cA, cB = st.columns(2)
        cA.write(f"**En retard** : {len(overdue)}")
        cA.dataframe(overdue[["ID_Interaction","ID","Objet","Relance","Responsable"]], use_container_width=True)
        cB.write(f"**Dans les 7 jours** : {len(soon)}")
        cB.dataframe(soon[["ID_Interaction","ID","Objet","Relance","Responsable"]], use_container_width=True)

    # Analyse rapide événements (Top 5 bénéfices/recettes/coûts)
    st.subheader("🏅 Analyse événements (période)")
    rep_ev = df_event_financials(dfe2, dfpay2)
    if rep_ev.empty:
        st.info("Aucune donnée événement disponible pour la période.")
    else:
        cTop1, cTop2, cTop3 = st.columns(3)
        cTop1.write("**Top 5 bénéfices**")
        cTop1.dataframe(rep_ev.sort_values("Bénéfice", ascending=False).head(5)[["Nom_Événement","Date","Recette","Coût_Total","Bénéfice"]], use_container_width=True)
        cTop2.write("**Top 5 recettes**")
        cTop2.dataframe(rep_ev.sort_values("Recette", ascending=False).head(5)[["Nom_Événement","Date","Recette","Coût_Total","Bénéfice"]], use_container_width=True)
        cTop3.write("**Top 5 coûts**")
        cTop3.dataframe(rep_ev.sort_values("Coût_Total", ascending=False).head(5)[["Nom_Événement","Date","Recette","Coût_Total","Bénéfice"]], use_container_width=True)

# --------------------------
# Fiche 360° Contact
# --------------------------
elif page == "Fiche 360 Contact":
    st.title("👤 Fiche 360° — Contact")
    if df_contacts.empty:
        st.info("Aucun contact.")
    else:
        sel = st.selectbox("Sélectionnez un contact", df_contacts["ID"].tolist())
        c = df_contacts[df_contacts["ID"]==sel].iloc[0].to_dict()
        st.markdown(f"### {c['Prénom']} {c['Nom']}  —  {c.get('Titre','')} chez {c.get('Société','')}")
        st.write(f"**Email** : {c.get('Email','')}  |  **Téléphone** : {c.get('Téléphone','')}  |  **LinkedIn** : {c.get('LinkedIn','')}")
        st.write(f"**Type** : {c.get('Type','')}  |  **Statut** : {c.get('Statut','')}  |  **Score** : {c.get('Score_Engagement','')}  |  **Top20** : {c.get('Top20','')}")
        st.write(f"**Ville/Pays** : {c.get('Ville','')} / {c.get('Pays','')}  |  **Secteur** : {c.get('Secteur','')}")
        st.write(f"**Notes** : {c.get('Notes','')}")

        st.subheader("📞 Interactions")
        dfi = df_inter[df_inter["ID"]==sel].copy()
        st.dataframe(dfi[["ID_Interaction","Date","Canal","Objet","Résultat","Relance","Responsable"]], use_container_width=True)

        st.subheader("🎫 Participations")
        dfp = df_parts[df_parts["ID"]==sel].copy()
        if not dfp.empty and not df_events.empty:
            ev_names = df_events.set_index("ID_Événement")["Nom_Événement"]
            dfp["Événement"] = dfp["ID_Événement"].map(ev_names)
        st.dataframe(dfp[["ID_Participation","Événement","Rôle","Inscription","Arrivée","Temps_Present"]], use_container_width=True)

        st.subheader("💳 Paiements")
        dfpa = df_pay[df_pay["ID"]==sel].copy()
        st.dataframe(dfpa[["ID_Paiement","ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence"]], use_container_width=True)

        st.subheader("🎓 Certifications")
        dfce = df_cert[df_cert["ID"]==sel].copy()
        st.dataframe(dfce[["ID_Certif","Type_Certif","Date_Examen","Résultat","Score","Date_Obtention","Validité"]], use_container_width=True)

# --------------------------
# Contacts
# --------------------------
elif page == "Contacts":
    st.title("📇 Contacts")
    with st.expander("➕ Ajouter / Modifier un contact", expanded=True):
        colA, colB, colC = st.columns(3)
        mode = colA.selectbox("Mode", ["Créer","Modifier"])
        selected_id = None
        if mode == "Modifier" and not df_contacts.empty:
            selected_id = colB.selectbox("Sélectionner", [""]+df_contacts["ID"].tolist())
        else:
            colB.write("")
        data = {k:"" for k in C_COLS}
        if mode=="Modifier" and selected_id:
            data = df_contacts[df_contacts["ID"]==selected_id].iloc[0].to_dict()

        with st.form("form_contact"):
            st.markdown("#### Identification")
            c1, c2, c3, c4 = st.columns(4)
            nom = c1.text_input("Nom*", value=data.get("Nom",""))
            prenom = c2.text_input("Prénom*", value=data.get("Prénom",""))
            genre = c3.selectbox("Genre", SET["genres"], index=SET["genres"].index(data.get("Genre", SET["genres"][0])) if data.get("Genre") in SET["genres"] else 0)
            titre = c4.text_input("Titre/Fonction", value=data.get("Titre",""))

            st.markdown("#### Coordonnées")
            c5, c6, c7 = st.columns(3)
            email = c5.text_input("Email", value=data.get("Email",""), help="Format : nom@domaine.tld")
            tel = c6.text_input("Téléphone", value=data.get("Téléphone",""), help="Ex. +237 6XXXXXXXX")
            linkedin = c7.text_input("LinkedIn", value=data.get("LinkedIn",""))

            st.markdown("#### Professionnel & localisation")
            c8, c9, c10, c11 = st.columns(4)
            societe = c8.text_input("Société", value=data.get("Société",""))
            secteur = c9.selectbox("Secteur", SET["secteurs"], index=SET["secteurs"].index(data.get("Secteur", SET["secteurs"][0])) if data.get("Secteur") in SET["secteurs"] else 0)
            ville = c10.selectbox("Ville", SET["villes"], index=SET["villes"].index(data.get("Ville", SET["villes"][0])) if data.get("Ville") in SET["villes"] else 0)
            pays = c11.selectbox("Pays", SET["pays"], index=SET["pays"].index(data.get("Pays", SET["pays"][0])) if data.get("Pays") in SET["pays"] else 0)

            st.markdown("#### Suivi")
            c12, c13, c14, c15 = st.columns(4)
            typec = c12.selectbox("Type", SET["types_contact"], index=SET["types_contact"].index(data.get("Type", SET["types_contact"][0])) if data.get("Type") in SET["types_contact"] else 0)
            src = c13.selectbox("Source", SET["sources"], index=SET["sources"].index(data.get("Source", SET["sources"][0])) if data.get("Source") in SET["sources"] else 0)
            statut = c14.selectbox("Statut", SET["statuts_engagement"], index=SET["statuts_engagement"].index(data.get("Statut", SET["statuts_engagement"][0])) if data.get("Statut") in SET["statuts_engagement"] else 0)
            score = c15.number_input("Score engagement", min_value=0, max_value=9999, value=int(pd.to_numeric(str(data.get("Score_Engagement","0")), errors="coerce") or 0))
            notes = st.text_area("Notes", value=data.get("Notes",""))

            submitted = st.form_submit_button("💾 Enregistrer")
            if submitted:
                if not nom or not prenom:
                    st.error("Nom et Prénom sont obligatoires.")
                elif not email_ok(email):
                    st.error("Email invalide.")
                elif not phone_ok(tel):
                    st.error("Téléphone invalide (8 chiffres min.).")
                else:
                    top20 = societe.strip() in SET["entreprises_cibles"]
                    if mode=="Créer":
                        new_id = generate_id("CNT", df_contacts, "ID")
                        new_row = {
                            "ID": new_id, "Nom": nom, "Prénom": prenom, "Genre": genre, "Titre": titre,
                            "Société": societe, "Secteur": secteur, "Email": email, "Téléphone": tel, "LinkedIn": linkedin,
                            "Ville": ville, "Pays": pays, "Type": typec, "Source": src, "Statut": statut,
                            "Score_Engagement": score, "Date_Creation": date.today().isoformat(), "Notes": notes, "Top20": top20
                        }
                        df_contacts = pd.concat([df_contacts, pd.DataFrame([new_row])], ignore_index=True)
                        save_df(df_contacts, PATHS["contacts"])
                        st.success(f"Contact créé (ID {new_id}).")
                    else:
                        idx = df_contacts.index[df_contacts["ID"]==selected_id][0]
                        df_contacts.loc[idx, ["Nom","Prénom","Genre","Titre","Société","Secteur","Email","Téléphone","LinkedIn",
                                              "Ville","Pays","Type","Source","Statut","Score_Engagement","Notes","Top20"]] = \
                                              [nom,prenom,genre,titre,societe,secteur,email,tel,linkedin,ville,pays,typec,src,statut,score,notes,top20]
                        save_df(df_contacts, PATHS["contacts"])
                        st.success(f"Contact modifié (ID {selected_id}).")

    st.markdown("### Liste des contacts")
    st.dataframe(df_contacts, use_container_width=True)

# --------------------------
# Interactions
# --------------------------
elif page == "Interactions":
    st.title("📞 Interactions")
    with st.expander("➕ Ajouter une interaction", expanded=True):
        with st.form("form_inter"):
            idc = st.selectbox("Contact (ID - Nom Prénom)", [""] + (df_contacts["ID"] + " — " + df_contacts["Nom"] + " " + df_contacts["Prénom"]).tolist())
            canal = st.selectbox("Canal", SET["canaux"])
            datei = st.date_input("Date", value=date.today())
            obj = st.text_input("Objet")
            resu = st.selectbox("Résultat", SET["resultats_inter"])
            resume = st.text_area("Résumé")
            prochaine = st.text_input("Prochaine action")
            rel = st.date_input("Relance", value=None)
            resp = st.selectbox("Responsable", ["Aymard","Alix","Autre"])

            ok = st.form_submit_button("💾 Enregistrer")
            if ok:
                if not idc or idc == "":
                    st.error("Veuillez choisir un contact.")
                else:
                    the_id = idc.split("—")[0].strip()
                    new_id = generate_id("INT", df_inter, "ID_Interaction")
                    new_row = {
                        "ID_Interaction": new_id, "ID": the_id, "Date": datei.isoformat(), "Canal": canal,
                        "Objet": obj, "Résumé": resume, "Résultat": resu, "Prochaine_Action": prochaine,
                        "Relance": rel.isoformat() if rel else "", "Responsable": resp
                    }
                    df_inter = pd.concat([df_inter, pd.DataFrame([new_row])], ignore_index=True)
                    save_df(df_inter, PATHS["interactions"])
                    st.success(f"Interaction créée (ID {new_id}).")

    st.markdown("### Historique des interactions")
    st.dataframe(df_inter, use_container_width=True)

# --------------------------
# Événements
# --------------------------
elif page == "Événements":
    st.title("📅 Événements")
    with st.expander("➕ Ajouter / Modifier un événement", expanded=True):
        mode = st.radio("Mode", ["Créer","Modifier"], horizontal=True)
        selected_evt = None
        if mode=="Modifier" and not df_events.empty:
            selected_evt = st.selectbox("Sélectionner", [""]+df_events["ID_Événement"].tolist())

        data = {k:"" for k in E_COLS}
        if mode=="Modifier" and selected_evt:
            data = df_events[df_events["ID_Événement"]==selected_evt].iloc[0].to_dict()

        with st.form("form_evt"):
            a,b,c = st.columns(3)
            nom = a.text_input("Nom Événement*", value=data.get("Nom_Événement",""))
            typ = b.selectbox("Type", SET["types_evenements"], index=SET["types_evenements"].index(data.get("Type", SET["types_evenements"][0])) if data.get("Type") in SET["types_evenements"] else 0)
            lieu = c.selectbox("Lieu", SET["lieux"], index=SET["lieux"].index(data.get("Lieu", SET["lieux"][0])) if data.get("Lieu") in SET["lieux"] else 0)

            d,e,f = st.columns(3)
            dte = d.date_input("Date", value=parse_date(data.get("Date")) or date.today())
            duree = e.number_input("Durée (h)", min_value=0.0, max_value=999.0, value=float(data.get("Durée_h") or 0.0))
            formateur = f.text_input("Formateur(s)", value=data.get("Formateur",""))

            obj = st.text_area("Objectif", value=data.get("Objectif",""))
            notes = st.text_area("Notes", value=data.get("Notes",""))

            st.markdown("#### Coûts (FCFA)")
            c1,c2,c3,c4,c5 = st.columns(5)
            cout_salle = c1.number_input("Salle", min_value=0.0, value=float(data.get("Cout_Salle") or 0.0))
            cout_form = c2.number_input("Formateur", min_value=0.0, value=float(data.get("Cout_Formateur") or 0.0))
            cout_log = c3.number_input("Logistique", min_value=0.0, value=float(data.get("Cout_Logistique") or 0.0))
            cout_pub = c4.number_input("Publicité", min_value=0.0, value=float(data.get("Cout_Pub") or 0.0))
            cout_aut = c5.number_input("Autres", min_value=0.0, value=float(data.get("Cout_Autres") or 0.0))
            cout_total = cout_salle + cout_form + cout_log + cout_pub + cout_aut

            sub = st.form_submit_button("💾 Enregistrer")
            if sub:
                if not nom:
                    st.error("Nom obligatoire.")
                else:
                    if mode=="Créer":
                        new_id = generate_id("EVT", df_events, "ID_Événement")
                        new_row = {
                            "ID_Événement": new_id, "Nom_Événement": nom, "Type": typ,
                            "Date": dte.isoformat(), "Durée_h": duree, "Lieu": lieu,
                            "Formateur": formateur, "Objectif": obj, "Periode": dte.strftime("%B %Y"),
                            "Cout_Salle": cout_salle, "Cout_Formateur": cout_form, "Cout_Logistique": cout_log,
                            "Cout_Pub": cout_pub, "Cout_Autres": cout_aut, "Cout_Total": cout_total, "Notes": notes
                        }
                        df_events = pd.concat([df_events, pd.DataFrame([new_row])], ignore_index=True)
                        save_df(df_events, PATHS["evenements"])
                        st.success(f"Événement créé (ID {new_id}).")
                    else:
                        idx = df_events.index[df_events["ID_Événement"]==selected_evt][0]
                        df_events.loc[idx, ["Nom_Événement","Type","Date","Durée_h","Lieu","Formateur","Objectif","Periode",
                                            "Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total","Notes"]] = \
                                            [nom,typ,dte.isoformat(),duree,lieu,formateur,obj,dte.strftime("%B %Y"),
                                             cout_salle,cout_form,cout_log,cout_pub,cout_aut,cout_total,notes]
                        save_df(df_events, PATHS["evenements"])
                        st.success(f"Événement modifié (ID {selected_evt}).")

    st.markdown("### Liste des événements")
    st.dataframe(df_events, use_container_width=True)

# --------------------------
# Participations
# --------------------------
elif page == "Participations":
    st.title("🎫 Participations")
    with st.expander("➕ Ajouter une participation", expanded=True):
        with st.form("form_part"):
            if df_contacts.empty or df_events.empty:
                st.warning("Veuillez d'abord créer des Contacts et des Événements.")
            else:
                idc = st.selectbox("Contact", df_contacts["ID"].tolist())
                ide = st.selectbox("Événement", df_events["ID_Événement"].tolist())
                role = st.selectbox("Rôle", ["Participant","Animateur","Invité"])
                inscr = st.text_input("Inscription (heure/pointage)", "")
                arr = st.text_input("Arrivée (heure/pointage)", "")
                tps = st.text_input("Temps Présent", "")
                fb = st.selectbox("Feedback", ["Très satisfait","Satisfait","Moyen","Insatisfait"])
                note = st.number_input("Note (1-5)", min_value=1, max_value=5, value=5)
                com = st.text_area("Commentaire", "")
                ok = st.form_submit_button("💾 Enregistrer")
                if ok:
                    new_id = generate_id("PAR", df_parts, "ID_Participation")
                    new_row = {"ID_Participation":new_id,"ID":idc,"ID_Événement":ide,"Rôle":role,"Inscription":inscr,
                               "Arrivée":arr,"Temps_Present":tps,"Feedback":fb,"Note":str(note),"Commentaire":com}
                    df_parts = pd.concat([df_parts, pd.DataFrame([new_row])], ignore_index=True)
                    save_df(df_parts, PATHS["participations"])
                    st.success(f"Participation ajoutée (ID {new_id}).")

    st.markdown("### Liste des participations")
    st.dataframe(df_parts, use_container_width=True)

# --------------------------
# Paiements
# --------------------------
elif page == "Paiements":
    st.title("💳 Paiements")
    with st.expander("➕ Enregistrer un paiement", expanded=True):
        with st.form("form_pay"):
            if df_contacts.empty or df_events.empty:
                st.warning("Veuillez d'abord enregistrer des Contacts et des Événements.")
            else:
                idc = st.selectbox("Contact", df_contacts["ID"].tolist())
                ide = st.selectbox("Événement", df_events["ID_Événement"].tolist())
                dtp = st.date_input("Date Paiement", value=date.today())
                montant = st.number_input("Montant (FCFA)", min_value=0, step=1000)
                moyen = st.selectbox("Moyen", SET["moyens_paiement"])
                statut = st.selectbox("Statut", SET["statuts_paiement"])
                ref = st.text_input("Référence", "")
                note = st.text_input("Notes", "")
                rel = st.date_input("Relance", value=None)

                ok = st.form_submit_button("💾 Enregistrer")
                if ok:
                    new_id = generate_id("PAY", df_pay, "ID_Paiement")
                    new_row = {"ID_Paiement":new_id,"ID":idc,"ID_Événement":ide,"Date_Paiement":dtp.isoformat(),
                               "Montant":str(montant),"Moyen":moyen,"Statut":statut,"Référence":ref,"Notes":note,
                               "Relance": (rel.isoformat() if rel else "")}
                    df_pay = pd.concat([df_pay, pd.DataFrame([new_row])], ignore_index=True)
                    save_df(df_pay, PATHS["paiements"])
                    st.success(f"Paiement enregistré (ID {new_id}).")

    st.markdown("### Liste des paiements")
    st.dataframe(df_pay, use_container_width=True)

# --------------------------
# Certifications
# --------------------------
elif page == "Certifications":
    st.title("🎓 Certifications IIBA")
    with st.expander("➕ Ajouter une certification", expanded=True):
        with st.form("form_cert"):
            if df_contacts.empty:
                st.warning("Veuillez d'abord créer des Contacts.")
            else:
                idc = st.selectbox("Contact", df_contacts["ID"].tolist())
                tc = st.selectbox("Type Certif", SET["types_certif"])
                dte = st.date_input("Date Examen", value=date.today())
                res = st.selectbox("Résultat", ["Réussi","Échoué","En cours","Reporté"])
                sc = st.number_input("Score", min_value=0, max_value=100, value=0)
                dto = st.date_input("Date Obtention", value=None)
                val = st.text_input("Validité", "")
                ren = st.text_input("Renouvellement", "")
                note = st.text_area("Notes", "")

                ok = st.form_submit_button("💾 Enregistrer")
                if ok:
                    new_id = generate_id("CER", df_cert, "ID_Certif")
                    new_row = {"ID_Certif":new_id,"ID":idc,"Type_Certif":tc,"Date_Examen":dte.isoformat(),
                               "Résultat":res,"Score":str(sc),
                               "Date_Obtention": (dto.isoformat() if dto else ""),
                               "Validité":val,"Renouvellement":ren,"Notes":note}
                    df_cert = pd.concat([df_cert, pd.DataFrame([new_row])], ignore_index=True)
                    save_df(df_cert, PATHS["certifications"])
                    st.success(f"Certification ajoutée (ID {new_id}).")

    st.markdown("### Liste des certifications")
    st.dataframe(df_cert, use_container_width=True)

# --------------------------
# RAPPORTS (nouvelle page)
# --------------------------
elif page == "Rapports":
    st.title("📑 Rapports & Graphiques")
    st.caption("Analyses consolidées, graphiques interactifs et exports (CSV/Excel).")

    dfe2, dfp2, dfpay2, dfcert2 = filtered_tables_for_period(annee, mois)
    dfc2 = df_contacts.copy()

    # KPI Résumé
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

    # Événements : Recette/Coût/Bénéfice
    st.markdown("### Événements : Recettes / Coûts / Bénéfices")
    rep_ev = df_event_financials(dfe2, dfpay2)
    st.dataframe(rep_ev.sort_values("Bénéfice", ascending=False), use_container_width=True)

    # Graphiques
    if alt is not None:
        st.markdown("#### Graphiques")
        # CA par événement (barres)
        if not rep_ev.empty:
            ch1 = alt.Chart(rep_ev.sort_values("Recette", ascending=False).head(20)).mark_bar().encode(
                x=alt.X("Nom_Événement:N", sort='-y', title="Événement"),
                y=alt.Y("Recette:Q", title="CA (FCFA)"),
                tooltip=["Nom_Événement","Date","Recette","Coût_Total","Bénéfice"]
            ).properties(height=350)
            st.altair_chart(ch1, use_container_width=True)

        # CA par type d'événement (barres)
        rep_type = ca_by_event_type(rep_ev)
        if not rep_type.empty:
            ch2 = alt.Chart(rep_type).mark_bar().encode(
                x=alt.X("Type:N", sort='-y', title="Type d'événement"),
                y=alt.Y("Recette:Q", title="CA (FCFA)"),
                tooltip=["Type","Recette"]
            ).properties(height=300)
            st.altair_chart(ch2, use_container_width=True)

        # CA mensuel (ligne)
        mca = monthly_ca(dfpay2, annee)
        if not mca.empty:
            ch3 = alt.Chart(mca).mark_line(point=True).encode(
                x=alt.X("Mois:T", title="Mois"),
                y=alt.Y("CA:Q", title="CA (FCFA)"),
                tooltip=["Mois","CA"]
            ).properties(height=300)
            st.altair_chart(ch3, use_container_width=True)

        # Répartition types de contacts (camembert)
        dist = contact_type_distribution(dfc2)
        if not dist.empty:
            ch4 = alt.Chart(dist).mark_arc().encode(
                theta="Count:Q",
                color="Type:N",
                tooltip=["Type","Count"]
            ).properties(height=300)
            st.altair_chart(ch4, use_container_width=True)

        # Satisfaction moyenne par type d'événement (barres)
        sat = avg_satisfaction_by_event_type(dfp2, dfe2)
        if not sat.empty:
            ch5 = alt.Chart(sat).mark_bar().encode(
                x=alt.X("Type:N", sort='-y', title="Type d'événement"),
                y=alt.Y("SatisfactionMoy:Q", title="Note moyenne /5"),
                tooltip=["Type","SatisfactionMoy"]
            ).properties(height=300)
            st.altair_chart(ch5, use_container_width=True)
    else:
        st.info("Altair n'est pas installé. Exécutez : `pip install altair` pour voir les graphiques.")

    # Prospects réguliers non convertis
    st.markdown("### Prospects réguliers non convertis")
    seuil = st.slider("Seuil de participations minimales", 1, 10, 3)
    res_pros = prospects_reguliers_non_convertis(dfc2, dfp2, dfpay2, seuil=seuil)
    st.dataframe(res_pros[["ID","Nom","Prénom","Société","Type","Statut","Participations","A_Paye"]], use_container_width=True)

    # Entreprises Top‑20
    st.markdown("### Entreprises Top‑20 (GECAM) — Synthèse")
    top20_tbl = top20_metrics(dfc2, dfpay2)
    st.dataframe(top20_tbl, use_container_width=True)

    # Exports CSV/Excel
    st.markdown("### Export des rapports")
    cexp1, cexp2, cexp3 = st.columns(3)
    kpi_csv = kpi.to_csv(index=False).encode("utf-8")
    rep_ev_csv = rep_ev.to_csv(index=False).encode("utf-8")
    res_pros_csv = res_pros.to_csv(index=False).encode("utf-8")
    top20_csv = top20_tbl.to_csv(index=False).encode("utf-8")

    cexp1.download_button("⬇️ KPI.csv", kpi_csv, file_name="kpi_periode.csv", mime="text/csv")
    cexp2.download_button("⬇️ Evenements.csv", rep_ev_csv, file_name="evenements_finance.csv", mime="text/csv")
    cexp3.download_button("⬇️ Prospects_non_convertis.csv", res_pros_csv, file_name="prospects_non_convertis.csv", mime="text/csv")
    st.download_button("⬇️ Top20_entreprises.csv", top20_csv, file_name="top20_entreprises.csv", mime="text/csv")

    try:
        import openpyxl  # ensure engine available
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
        st.download_button(
            "⬇️ Rapport_complet.xlsx",
            buffer.getvalue(),
            file_name="Rapport_IIBA_Cameroun.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        st.warning(f"Export Excel indisponible : {e}")

# --------------------------
# Paramètres
# --------------------------
elif page == "Paramètres":
    st.title("⚙️ Paramètres & Référentiels")
    st.caption("Adapter les listes déroulantes et référentiels du CRM.")
    with st.form("form_set"):
        st.markdown("#### Contacts & Engagement")
        c1, c2, c3 = st.columns(3)
        genres = c1.text_area("Genres (ligne par valeur)", "\n".join(SET["genres"]))
        types_contact = c2.text_area("Types de contact", "\n".join(SET["types_contact"]))
        statuts_eng = c3.text_area("Statuts d'engagement", "\n".join(SET["statuts_engagement"]))

        st.markdown("#### Secteurs, Pays, Villes")
        s1, s2, s3 = st.columns(3)
        secteurs = s1.text_area("Secteurs", "\n".join(SET["secteurs"]))
        pays = s2.text_area("Pays", "\n".join(SET["pays"]))
        villes = s3.text_area("Villes", "\n".join(SET["villes"]))

        st.markdown("#### Sources, Canaux & Résultats d'interaction")
        s4, s5, s6 = st.columns(3)
        sources = s4.text_area("Sources", "\n".join(SET["sources"]))
        canaux = s5.text_area("Canaux", "\n".join(SET["canaux"]))
        resint = s6.text_area("Résultats d'interaction", "\n".join(SET["resultats_inter"]))

        st.markdown("#### Événements & Paiements")
        e1, e2, e3 = st.columns(3)
        types_evt = e1.text_area("Types d'événements", "\n".join(SET["types_evenements"]))
        lieux = e2.text_area("Lieux", "\n".join(SET["lieux"]))
        moyens = e3.text_area("Moyens de paiement", "\n".join(SET["moyens_paiement"]))

        st.markdown("#### Statuts de paiement & Certifications")
        e4, e5 = st.columns(2)
        statpay = e4.text_area("Statuts de paiement", "\n".join(SET["statuts_paiement"]))
        tcert = e5.text_area("Types de certification", "\n".join(SET["types_certif"]))

        st.markdown("#### Entreprises cibles (Top‑20 / GECAM)")
        top20 = st.text_area("Entreprises cibles", "\n".join(SET["entreprises_cibles"]))        

        sub = st.form_submit_button("💾 Enregistrer les paramètres")
        if sub:
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
                st.success("Paramètres enregistrés. Rechargez la page (Ctrl+R) pour appliquer partout.")
            except Exception as e:
                st.error(f"Erreur d'enregistrement : {e}")

st.sidebar.markdown("---")
st.sidebar.caption("© IIBA Cameroun — CRM Streamlit")
