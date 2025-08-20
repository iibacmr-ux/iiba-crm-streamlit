# -*- coding: utf-8 -*-
"""
IIBA Cameroun — CRM (monofichier Streamlit)
Features clés : CRM Grille centrale, Rapports, Import/Export Global & Multi-onglets, Reset DB, Purge ID
Mises à jour : Pagination AgGrid, sélection fiable, filtres sur Événements
"""

from datetime import datetime, date
from pathlib import Path
import io, json, re, unicodedata, os

import numpy as np
import pandas as pd
import streamlit as st

# AgGrid
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
    HAS_AGGRID = True
except Exception:
    HAS_AGGRID = False

# Altair (optionnel)
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

TABLE_ID_COL = {
    "contacts": "ID",
    "interactions": "ID_Interaction",
    "evenements": "ID_Événement",
    "participations": "ID_Participation",
    "paiements": "ID_Paiement",
    "certifications": "ID_Certif",
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
    s2 = re.sub(r"[ \.\-\(\)]", "", str(s)).replace("+","")
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

def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', str(s)) if unicodedata.category(c) != 'Mn')

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
    ["CRM (Grille centrale)", "Événements", "Rapports", "Admin"], index=0
)
this_year = datetime.now().year
annee = st.sidebar.selectbox("Année", ["Toutes"]+[str(this_year-1), str(this_year), str(this_year+1)], index=1)
mois   = st.sidebar.selectbox("Mois", ["Tous"]+[f"{m:02d}" for m in range(1,13)], index=0)

# ------------------------------------------------------------------
# Fonctions analytiques
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

# ------------------------------------------------------------------
# Page CRM — Grille centrale (contacts) avec pagination & sélection fiable
# ------------------------------------------------------------------
if page == "CRM (Grille centrale)":
    st.title("👥 CRM — Grille centrale (Contacts)")

    # Barre de contrôles
    colf1, colf2, colf3, colf4 = st.columns([2,1,1,1])
    q = colf1.text_input("Recherche (nom, société, email)…", "")
    page_size = colf2.selectbox("Taille de page", [20, 50, 100, 200], index=0)
    type_filtre = colf3.selectbox("Type", ["Tous"]+SET["types_contact"])
    top20_only = colf4.checkbox("Top-20 uniquement", value=False)

    # Filtrage
    dfc = df_contacts.copy()
    if q:
        qs = q.lower()
        dfc = dfc[dfc.apply(lambda r: qs in str(r["Nom"]).lower() or qs in str(r["Prénom"]).lower() or qs in str(r["Société"]).lower() or qs in str(r["Email"]).lower(), axis=1)]
    if type_filtre != "Tous": dfc = dfc[dfc["Type"]==type_filtre]
    if top20_only: dfc = dfc[dfc["Top20"]==True]

    # Grille paginée (hauteur fixe => scroll page rétabli)
    sel_id = None
    table_cols = ["ID","Nom","Prénom","Société","Type","Statut","Email","Téléphone","Ville","Pays","Top20"]
    if HAS_AGGRID and not dfc.empty:
        gob = GridOptionsBuilder.from_dataframe(dfc[table_cols])
        gob.configure_selection("single", use_checkbox=True)
        gob.configure_pagination(paginationAutoPageSize=False, paginationPageSize=page_size)
        gob.configure_side_bar()  # colonne filtre/colonnes
        grid = AgGrid(
            dfc[table_cols],
            gridOptions=gob.build(),
            height=520,                      # hauteur fixe => barre de scroll de la page préservée
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            allow_unsafe_jscode=True
        )
        if grid and grid.get("selected_rows"):
            sel_id = grid["selected_rows"][0]["ID"]
            st.session_state["selected_contact_id"] = sel_id
    else:
        st.info("Pour la pagination et la sélection avancée, installez `streamlit-aggrid`. Affichage fallback sans pagination.")
        st.dataframe(dfc[table_cols], use_container_width=True)
        sel_id = st.selectbox("Sélectionner un contact", [""]+dfc["ID"].tolist()) or None
        if sel_id: st.session_state["selected_contact_id"] = sel_id

    # Fallback si déjà sélectionné avant
    if not sel_id and "selected_contact_id" in st.session_state:
        sel_id = st.session_state["selected_contact_id"]

    st.markdown("---")
    cL, cR = st.columns([1,2])
    with cL:
        st.subheader("Fiche Contact")
        if sel_id:
            c = df_contacts[df_contacts["ID"]==sel_id]
            if not c.empty:
                c = c.iloc[0].to_dict()
                st.markdown(f"**{c.get('Prénom','')} {c.get('Nom','')}** — {c.get('Titre','')} chez **{c.get('Société','')}**")
                st.write(f"{c.get('Email','')} • {c.get('Téléphone','')} • {c.get('LinkedIn','')}")
                st.write(f"{c.get('Ville','')}, {c.get('Pays','')} — {c.get('Secteur','')}")
                st.write(f"Type: **{c.get('Type','')}** | Statut: **{c.get('Statut','')}** | Score: **{c.get('Score_Engagement','')}** | Top20: **{c.get('Top20','')}**")
            else:
                st.warning("L'ID sélectionné n'a pas été trouvé (rafraîchir la page si besoin).")
        else:
            st.info("Sélectionnez un contact dans la grille.")

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
# Événements — Grille filtrable (AgGrid)
# ------------------------------------------------------------------
elif page == "Événements":
    st.title("📅 Événements")

    filt_text = st.text_input("Filtre rapide (nom, type, lieu, notes…)", "")
    page_size_evt = st.selectbox("Taille de page", [20, 50, 100, 200], index=0, key="pg_evt")

    df_show = df_events.copy()
    if filt_text:
        t = filt_text.lower()
        df_show = df_show[df_show.apply(lambda r: any(t in str(r[c]).lower() for c in ["Nom_Événement","Type","Lieu","Notes"]), axis=1)]

    if HAS_AGGRID:
        gob = GridOptionsBuilder.from_dataframe(df_show)
        gob.configure_pagination(paginationAutoPageSize=False, paginationPageSize=page_size_evt)
        gob.configure_default_column(filter=True, sortable=True, resizable=True)
        gob.configure_side_bar()  # panneau filtres
        AgGrid(df_show, gridOptions=gob.build(), height=520, update_mode=GridUpdateMode.NO_UPDATE, allow_unsafe_jscode=True)
    else:
        st.dataframe(df_show, use_container_width=True)

# ------------------------------------------------------------------
# Rapports (KPI + exports)
# ------------------------------------------------------------------
elif page == "Rapports":
    st.title("📑 Rapports & Export")
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

    # Export Excel multi-onglets
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
# Admin — déjà fourni dans vos versions précédentes (Reset DB, Purge ID, Migration)
# ------------------------------------------------------------------
elif page == "Admin":
    st.title("⚙️ Admin — Maintenance minimale")
    st.caption("Cette version de démonstration se concentre sur la pagination/filtrage. Je peux réintégrer ici l'ensemble de vos blocs Migration (Global/Multi-onglets), Reset DB et Purge ID identiques à la version précédente si vous le souhaitez.")

    # Réinitialiser la base (simple)
    if st.button("🗑️ Réinitialiser la base (toutes les tables)"):
        try:
            # Supprimer CSV existants
            for key, p in PATHS.items():
                if key == "settings": 
                    continue
                if p.exists(): p.unlink(missing_ok=True)
            # Recréer vides
            global df_contacts, df_inter, df_events, df_parts, df_pay, df_cert
            df_contacts = ensure_df(PATHS["contacts"], C_COLS); save_df(df_contacts, PATHS["contacts"])
            df_inter    = ensure_df(PATHS["inter"], I_COLS);     save_df(df_inter, PATHS["inter"])
            df_events   = ensure_df(PATHS["events"], E_COLS);    save_df(df_events, PATHS["events"])
            df_parts    = ensure_df(PATHS["parts"], P_COLS);     save_df(df_parts, PATHS["parts"])
            df_pay      = ensure_df(PATHS["pay"], PAY_COLS);     save_df(df_pay, PATHS["pay"])
            df_cert     = ensure_df(PATHS["cert"], CERT_COLS);   save_df(df_cert, PATHS["cert"])
            st.success("Base réinitialisée (toutes les tables vides).")
        except Exception as e:
            st.error(f"Échec de la réinitialisation : {e}")

    st.markdown("---")
    # Purger un ID
    tbl = st.selectbox("Table à purger", list(ALL_SCHEMAS.keys()))
    id_col = tbl and TABLE_ID_COL[tbl]
    target_id = st.text_input(f"ID à supprimer ({id_col})")
    if st.button("🧽 Purger l'ID"):
        if not target_id.strip():
            st.error("Veuillez saisir un ID.")
        else:
            try:
                path = PATHS["contacts" if tbl=="contacts" else
                             "inter" if tbl=="interactions" else
                             "events" if tbl=="evenements" else
                             "parts" if tbl=="participations" else
                             "pay" if tbl=="paiements" else
                             "cert"]
                df = ensure_df(path, ALL_SCHEMAS[tbl])
                before = len(df)
                df = df[df[id_col] != target_id.strip()]
                save_df(df, path)
                st.success(f"Supprimé {before-len(df)} ligne(s) de {tbl}.")
            except Exception as e:
                st.error(f"Échec de la purge : {e}")
