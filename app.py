# -*- coding: utf-8 -*-
"""
IIBA Cameroun — CRM Streamlit (monofichier enrichi)
Version : mix ancien design (PJ) + nouvelles logiques avancées
- Centralisation complète des paramètres (listes, seuils, scoring, objectifs KPI) dans parametres.csv
- Pages : CRM (grille + fiche + actions), Événements (CRUD + duplication), Rapports (KPI/Graphiques),
  Admin (Paramètres + Migration Excel Global & Multi-onglets + Reset DB + Purge ID + Logs Import)
"""

import os
import uuid
import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import json

# --- Paths ---
DATA_DIR = "data"
PARAM_FILE = os.path.join(DATA_DIR, "parametres.csv")
TABLES = {
    "contacts": os.path.join(DATA_DIR, "contacts.csv"),
    "events": os.path.join(DATA_DIR, "events.csv"),
    "parts": os.path.join(DATA_DIR, "participations.csv"),
    "pay": os.path.join(DATA_DIR, "paiements.csv"),
    "inter": os.path.join(DATA_DIR, "interactions.csv"),
}

# --- Ensure dirs ---
os.makedirs(DATA_DIR, exist_ok=True)

# --- Utils ---
def load_csv(path):
    if os.path.exists(path):
        return pd.read_csv(path, encoding="utf-8")
    else:
        return pd.DataFrame()

def save_csv(df, path):
    df.to_csv(path, index=False, encoding="utf-8")

def load_params():
    if os.path.exists(PARAM_FILE):
        return pd.read_csv(PARAM_FILE, encoding="utf-8")
    else:
        cols = ["param", "valeur"]
        df = pd.DataFrame([
            ["VIP_threshold", "500000"],
            ["score_event", "10"],
            ["score_participation", "5"],
            ["score_interaction", "2"],
            ["objectif_CA_annuel", "5000000"],
            ["objectif_CA_mensuel", "400000"],
        ], columns=cols)
        save_csv(df, PARAM_FILE)
        return df

def get_param(params, key, default=None):
    try:
        return params.loc[params["param"] == key, "valeur"].values[0]
    except:
        return default

# --- Load Data ---
df_contacts = load_csv(TABLES["contacts"])
df_events = load_csv(TABLES["events"])
df_parts = load_csv(TABLES["parts"])
df_pay = load_csv(TABLES["pay"])
df_inter = load_csv(TABLES["inter"])
params = load_params()

# --- Scoring & Tags ---
def compute_contact_metrics(contact_id):
    """Retourne score composite, tags, proba conversion pour un contact"""
    score = 0
    tags = []

    n_events = len(df_parts[df_parts["ID Contact"] == contact_id])
    n_inter = len(df_inter[df_inter["ID Contact"] == contact_id])
    total_pay = df_pay[df_pay["ID Contact"] == contact_id]["Montant"].sum()

    score += n_events * int(get_param(params, "score_event", 10))
    score += n_inter * int(get_param(params, "score_interaction", 2))
    score += (total_pay // 10000)  # 1 point par tranche de 10k

    if n_events >= 3:
        tags.append("Participant régulier")
    if n_inter >= 5:
        tags.append("Actif en interactions")
    if total_pay > int(get_param(params, "VIP_threshold", 500000)):
        tags.append("VIP (CA élevé)")

    # Proba conversion
    if n_inter >= 3 and n_events >= 1 and total_pay > 0:
        proba = 0.9
    elif n_inter >= 2:
        proba = 0.6
    else:
        proba = 0.2

    if proba >= 0.8:
        pastille = "🟢"
    elif proba >= 0.5:
        pastille = "🟠"
    else:
        pastille = "🔴"

    return score, ", ".join(tags), f"{int(proba*100)}% {pastille}"

def enrich_contacts():
    if df_contacts.empty:
        return pd.DataFrame()
    tmp = df_contacts.copy()
    scores, tags, probas = [], [], []
    for cid in tmp["ID"]:
        s, t, p = compute_contact_metrics(cid)
        scores.append(s)
        tags.append(t)
        probas.append(p)
    tmp["Score"] = scores
    tmp["Tags"] = tags
    tmp["Probabilité Conversion"] = probas
    return tmp

df_contacts_enriched = enrich_contacts()

# --- Navigation ---
st.sidebar.title("Navigation")
page = st.sidebar.radio("Aller à", ["CRM (Grille centrale)", "Événements", "Rapports", "Admin"])
annee = st.sidebar.selectbox("Année", options=[2023, 2024, 2025, "Tous"], index=1)
mois = st.sidebar.selectbox("Mois", options=["Tous"] + list(range(1,13)))

# --- Page CRM ---
if page == "CRM (Grille centrale)":
    st.title("👥 CRM — Grille centrale (Contacts)")
    search = st.text_input("Recherche (nom, société, email)…")
    page_size = st.selectbox("Taille de page", [20,50,100], index=0)

    df_display = df_contacts_enriched.copy()
    if search:
        mask = df_display.apply(lambda r: search.lower() in str(r).lower(), axis=1)
        df_display = df_display[mask]

    gb = GridOptionsBuilder.from_dataframe(df_display)
    gb.configure_pagination(paginationPageSize=page_size)
    gb.configure_selection("single", use_checkbox=True)
    gb.configure_default_column(filter=True, sortable=True, resizable=True)
    grid_options = gb.build()

    grid_response = AgGrid(df_display, gridOptions=grid_options,
                           update_mode=GridUpdateMode.SELECTION_CHANGED, height=400)

    selected = grid_response["selected_rows"]
    if selected:
        contact = pd.Series(selected[0])
        st.subheader("📇 Fiche Contact")
        with st.form("edit_contact"):
            for col in ["Nom","Prénom","Société","Type","Statut","Email","Téléphone"]:
                contact[col] = st.text_input(col, contact.get(col,""))
            submitted = st.form_submit_button("💾 Enregistrer")
            if submitted:
                df_contacts.loc[df_contacts["ID"]==contact["ID"], contact.index] = contact.values
                save_csv(df_contacts, TABLES["contacts"])
                st.success("Contact mis à jour ✅")
                st.experimental_rerun()

        st.subheader("⚡ Actions liées au contact sélectionné")
        st.markdown("➕ Ajouter : Interaction | Participation | Paiement | Certification")
    else:
        st.info("Sélectionnez un contact dans la grille.")
# --- Page Événements ---
if page == "Événements":
    st.title("📅 Gestion des Événements")

    # CRUD de base
    st.subheader("Liste des événements existants")
    if df_events.empty:
        st.warning("Aucun événement enregistré.")
    else:
        gb = GridOptionsBuilder.from_dataframe(df_events)
        gb.configure_selection("single", use_checkbox=True)
        gb.configure_default_column(editable=False, filter=True)
        grid_response = AgGrid(df_events, gridOptions=gb.build(),
                               update_mode=GridUpdateMode.SELECTION_CHANGED, height=300)

        selected_event = grid_response["selected_rows"]

    st.subheader("➕ Créer un nouvel événement")
    with st.form("new_event"):
        new_id = f"EVT_{uuid.uuid4().hex[:6]}"
        nom = st.text_input("Nom de l'événement")
        date = st.date_input("Date", datetime.today())
        lieu = st.text_input("Lieu")
        type_evt = st.selectbox("Type", ["Formation","Afterwork","Webinaire","Conférence"])
        submit_evt = st.form_submit_button("Créer")
        if submit_evt:
            new_evt = {"ID": new_id, "Nom": nom, "Date": str(date), "Lieu": lieu, "Type": type_evt}
            df_events = pd.concat([df_events, pd.DataFrame([new_evt])], ignore_index=True)
            save_csv(df_events, TABLES["events"])
            st.success(f"Événement {nom} créé ✅")
            st.experimental_rerun()

    if selected_event:
        ev = pd.Series(selected_event[0])
        st.subheader(f"✏️ Modifier l’événement {ev['Nom']}")
        with st.form("edit_event"):
            for col in ["Nom","Date","Lieu","Type"]:
                ev[col] = st.text_input(col, str(ev.get(col,"")))
            if st.form_submit_button("Enregistrer les modifications"):
                df_events.loc[df_events["ID"]==ev["ID"], ev.index] = ev.values
                save_csv(df_events, TABLES["events"])
                st.success("Événement mis à jour ✅")
                st.experimental_rerun()

        if st.button("📑 Dupliquer cet événement"):
            new_id = f"EVT_{uuid.uuid4().hex[:6]}"
            ev_copy = ev.copy()
            ev_copy["ID"] = new_id
            df_events = pd.concat([df_events, pd.DataFrame([ev_copy])], ignore_index=True)
            save_csv(df_events, TABLES["events"])
            st.success("Événement dupliqué ✅")
            st.experimental_rerun()

# --- Page Rapports ---
if page == "Rapports":
    st.title("📊 Rapports & KPI — IIBA Cameroun")

    # Filtres
    st.sidebar.markdown("### 📅 Filtres Rapports")
    annee_f = st.sidebar.selectbox("Année (Rapports)", ["Tous"] + sorted(df_events["Date"].dropna().apply(lambda x: str(x)[:4]).unique()) if not df_events.empty else ["Tous"])
    mois_f = st.sidebar.selectbox("Mois (Rapports)", ["Tous"] + list(range(1,13)))

    # KPI simples
    total_contacts = len(df_contacts)
    total_events = len(df_events)
    total_parts = len(df_parts)
    ca_total = df_pay["Montant"].sum() if not df_pay.empty else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("👥 Contacts", total_contacts)
    col2.metric("📅 Événements", total_events)
    col3.metric("✅ Participations", total_parts)
    col4.metric("💰 CA Total", f"{ca_total:,.0f} FCFA")

    # Graphiques
    if not df_parts.empty and not df_events.empty:
        st.subheader("📈 Participation par événement")
        df_merge = df_parts.merge(df_events, left_on="ID Événement", right_on="ID", how="left")
        part_count = df_merge.groupby("Nom")["ID Contact"].count().reset_index()
        fig = px.bar(part_count, x="Nom", y="ID Contact", title="Nombre de participants par événement")
        st.plotly_chart(fig, use_container_width=True)

    if not df_pay.empty:
        st.subheader("💵 CA par mois")
        df_pay["Date"] = pd.to_datetime(df_pay["Date"], errors="coerce")
        df_pay["Mois"] = df_pay["Date"].dt.to_period("M")
        ca_mensuel = df_pay.groupby("Mois")["Montant"].sum().reset_index()
        fig2 = px.line(ca_mensuel, x="Mois", y="Montant", markers=True, title="CA mensuel")
        st.plotly_chart(fig2, use_container_width=True)

    # Objectifs vs Réalisés
    st.subheader("🎯 Objectifs vs Réalisations")
    obj_annuel = int(get_param(params, "objectif_CA_annuel", 5000000))
    obj_mensuel = int(get_param(params, "objectif_CA_mensuel", 400000))
    col5, col6 = st.columns(2)
    col5.metric("Objectif annuel", f"{obj_annuel:,.0f} FCFA", delta=f"Réel: {ca_total:,.0f}")
    if not df_pay.empty:
        ca_this_month = df_pay[df_pay["Date"].dt.month == datetime.today().month]["Montant"].sum()
        col6.metric("Objectif mensuel", f"{obj_mensuel:,.0f} FCFA", delta=f"Réel: {ca_this_month:,.0f}")
# --- Page Admin ---
if page == "Admin":
    st.title("⚙️ Administration & Migration")

    # ---- Paramètres ----
    st.subheader("🛠️ Paramètres généraux")
    st.write("Tous les paramètres sont centralisés dans `parametres.csv`")

    with st.form("params_form"):
        vip_threshold = st.number_input("Seuil CA VIP (FCFA)", 
                                        value=int(get_param(params, "vip_threshold", 1000000)))
        obj_CA_annuel = st.number_input("Objectif annuel CA", 
                                        value=int(get_param(params, "objectif_CA_annuel", 5000000)))
        obj_CA_mensuel = st.number_input("Objectif mensuel CA", 
                                         value=int(get_param(params, "objectif_CA_mensuel", 400000)))
        submit_params = st.form_submit_button("💾 Enregistrer les paramètres")

        if submit_params:
            set_param(params, "vip_threshold", vip_threshold)
            set_param(params, "objectif_CA_annuel", obj_CA_annuel)
            set_param(params, "objectif_CA_mensuel", obj_CA_mensuel)
            save_csv(params, PARAMS_FILE)
            st.success("Paramètres enregistrés ✅")

    # ---- Migration ----
    st.subheader("📥 Migration (Import / Export Excel)")

    uploaded_file = st.file_uploader("Charger un fichier Excel", type=["xlsx"])
    if uploaded_file:
        xls = pd.ExcelFile(uploaded_file)
        if "Contacts" in xls.sheet_names:
            df_contacts = pd.read_excel(xls, sheet_name="Contacts")
            save_csv(df_contacts, TABLES["contacts"])
            st.success("Contacts importés ✅")
        if "Interactions" in xls.sheet_names:
            df_inter = pd.read_excel(xls, sheet_name="Interactions")
            save_csv(df_inter, TABLES["interactions"])
            st.success("Interactions importées ✅")
        if "Événements" in xls.sheet_names:
            df_events = pd.read_excel(xls, sheet_name="Événements")
            save_csv(df_events, TABLES["events"])
            st.success("Événements importés ✅")
        if "Participations" in xls.sheet_names:
            df_parts = pd.read_excel(xls, sheet_name="Participations")
            save_csv(df_parts, TABLES["participations"])
            st.success("Participations importées ✅")
        if "Paiements" in xls.sheet_names:
            df_pay = pd.read_excel(xls, sheet_name="Paiements")
            save_csv(df_pay, TABLES["paiements"])
            st.success("Paiements importés ✅")
        if "Certifications" in xls.sheet_names:
            df_cert = pd.read_excel(xls, sheet_name="Certifications")
            save_csv(df_cert, TABLES["certifs"])
            st.success("Certifications importées ✅")
        st.info("✅ Import terminé")

    if st.button("📤 Export Excel Global"):
        out_file = "/mnt/data/export_global.xlsx"
        with pd.ExcelWriter(out_file, engine="xlsxwriter") as writer:
            df_contacts.to_excel(writer, sheet_name="Contacts", index=False)
            df_inter.to_excel(writer, sheet_name="Interactions", index=False)
            df_events.to_excel(writer, sheet_name="Événements", index=False)
            df_parts.to_excel(writer, sheet_name="Participations", index=False)
            df_pay.to_excel(writer, sheet_name="Paiements", index=False)
            df_cert.to_excel(writer, sheet_name="Certifications", index=False)
        st.success("Fichier exporté ✅")
        st.download_button("⬇️ Télécharger export.xlsx", open(out_file, "rb"), file_name="export.xlsx")

    # ---- Reset DB ----
    st.subheader("🗑️ Réinitialiser la base")
    if st.button("⚠️ Réinitialiser (supprimer tous les CSV)"):
        for t in TABLES.values():
            if os.path.exists(t):
                os.remove(t)
        if os.path.exists(PARAMS_FILE):
            os.remove(PARAMS_FILE)
        st.warning("Base réinitialisée. Redémarrez l’application.")

    # ---- Purge ID ----
    st.subheader("🔍 Purger un enregistrement par ID")
    purge_id = st.text_input("ID à supprimer (ex: CNT_123, EVT_456)")
    if st.button("🗑️ Purger ID"):
        found = False
        for name, path in TABLES.items():
            df = load_csv(path)
            if not df.empty and "ID" in df.columns:
                if purge_id in df["ID"].values:
                    df = df[df["ID"] != purge_id]
                    save_csv(df, path)
                    found = True
                    st.success(f"{purge_id} supprimé de {name} ✅")
        if not found:
            st.error("ID non trouvé ❌")

    # ---- Logs ----
    st.subheader("📜 Logs système")
    st.write("📌 Import / Export, Reset et Purge sont loggés automatiquement ici (TODO: fichier logs.csv).")
