# -*- coding: utf-8 -*-
"""
IIBA Cameroun — CRM Streamlit (monofichier)
Version : Centralisation complète des paramètres (listes, scoring, affichage, KPI/targets) dans parametres.csv
Pages : CRM (Vue 360°), Événements (CRUD + duplication), Rapports (KPI/Graphiques), Admin (Paramètres + Migration + Reset + Purge)
"""

import os
import uuid
import pandas as pd
import numpy as np
from datetime import datetime
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
import plotly.express as px

# -----------------------
# CONFIGURATION STREAMLIT
# -----------------------
st.set_page_config(
    page_title="IIBA Cameroun CRM",
    layout="wide",
    initial_sidebar_state="expanded"
)

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Fichiers internes
FILES = {
    "contacts": os.path.join(DATA_DIR, "contacts.csv"),
    "events": os.path.join(DATA_DIR, "events.csv"),
    "participations": os.path.join(DATA_DIR, "participations.csv"),
    "interactions": os.path.join(DATA_DIR, "interactions.csv"),
    "paiements": os.path.join(DATA_DIR, "paiements.csv"),
    "certifications": os.path.join(DATA_DIR, "certifications.csv"),
    "parametres": os.path.join(DATA_DIR, "parametres.csv")
}

# -----------------------
# UTILITAIRES
# -----------------------
def load_csv(path, default_cols):
    """Charger un CSV en s’assurant des colonnes minimales"""
    if os.path.exists(path):
        df = pd.read_csv(path, encoding="utf-8")
        for c in default_cols:
            if c not in df.columns:
                df[c] = None
        return df
    else:
        return pd.DataFrame(columns=default_cols)

def save_csv(df, path):
    df.to_csv(path, index=False, encoding="utf-8")

def generate_id(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"

# -----------------------
# CHARGEMENT DES PARAMÈTRES
# -----------------------
DEFAULT_PARAMS = {
    "VIP_CA": 200000,  # seuil CA pour VIP
    "KPI_LIST": "Prospects_Transformes,Taux_Participation,CA_par_Event",
    "VISIBLE_COLS_CONTACTS": "Nom,Prénom,Email,Entreprise,Score_composite,Proba_conversion,Tags",
    "OBJ_CA_ANNUEL": 1000000,
    "OBJ_CONVERSION": 0.25
}

if not os.path.exists(FILES["parametres"]):
    pd.DataFrame([DEFAULT_PARAMS]).to_csv(FILES["parametres"], index=False, encoding="utf-8")

PARAMS = pd.read_csv(FILES["parametres"]).iloc[0].to_dict()

# -----------------------
# CHARGEMENT DES DONNÉES
# -----------------------
df_contacts = load_csv(FILES["contacts"], ["ID","Nom","Prénom","Email","Téléphone","Entreprise","Source","Statut","Score_composite","Proba_conversion","Tags"])
df_events = load_csv(FILES["events"], ["ID","Nom","Type","Date","Lieu","Coût","Recette"])
df_parts = load_csv(FILES["participations"], ["ID","ID_Contact","ID_Event","Statut"])
df_inter = load_csv(FILES["interactions"], ["ID","ID_Contact","Date","Canal","Objet","Résumé","Résultat"])
df_pay = load_csv(FILES["paiements"], ["ID","ID_Contact","ID_Event","Montant","Date","Moyen"])
df_cert = load_csv(FILES["certifications"], ["ID","ID_Contact","Type","Date","Statut"])

# -----------------------
# SCORE & TAGGING AUTOMATIQUE
# -----------------------
def compute_score(contact_id):
    parts = df_parts[df_parts["ID_Contact"] == contact_id].shape[0]
    inters = df_inter[df_inter["ID_Contact"] == contact_id].shape[0]
    pay = df_pay[df_pay["ID_Contact"] == contact_id]["Montant"].sum()
    return parts*2 + inters + (pay/10000)

def compute_tags(contact_id):
    tags = []
    parts = df_parts[df_parts["ID_Contact"] == contact_id].shape[0]
    inters = df_inter[df_inter["ID_Contact"] == contact_id].shape[0]
    pay = df_pay[df_pay["ID_Contact"] == contact_id]["Montant"].sum()

    if pay >= PARAMS["VIP_CA"]:
        tags.append("VIP")
    if parts >= 3 and inters >= 3:
        tags.append("Régulier-non-converti")
    if "Formateur" in (df_cert[df_cert["ID_Contact"] == contact_id]["Type"].tolist()):
        tags.append("Futur formateur")

    return ",".join(tags)

def compute_proba(contact_id):
    parts = df_parts[df_parts["ID_Contact"] == contact_id].shape[0]
    inters = df_inter[df_inter["ID_Contact"] == contact_id].shape[0]
    pay = df_pay[df_pay["ID_Contact"] == contact_id]["Montant"].sum()

    if parts >= 1 and inters >= 3 and pay > 0:
        return 0.9
    elif parts >= 1 and inters >= 2:
        return 0.6
    elif inters >= 1:
        return 0.3
    return 0.1

# Mise à jour automatique des scores
for idx, row in df_contacts.iterrows():
    cid = row["ID"]
    df_contacts.at[idx, "Score_composite"] = compute_score(cid)
    df_contacts.at[idx, "Tags"] = compute_tags(cid)
    df_contacts.at[idx, "Proba_conversion"] = compute_proba(cid)

save_csv(df_contacts, FILES["contacts"])
# -----------------------
# PAGE CRM
# -----------------------
def page_crm():
    st.title("📌 CRM - Vue 360° Contacts")

    gb = GridOptionsBuilder.from_dataframe(df_contacts)
    gb.configure_selection("single", use_checkbox=True)
    gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
    gb.configure_default_column(filter=True, sortable=True, resizable=True)
    grid_options = gb.build()

    grid_response = AgGrid(
        df_contacts,
        gridOptions=grid_options,
        data_return_mode=DataReturnMode.FILTERED,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        fit_columns_on_grid_load=True,
        height=500,
    )

    selected = grid_response["selected_rows"]
    if selected:
        contact = selected[0]
        st.subheader("👤 Fiche Contact")
        with st.form("edit_contact"):
            nom = st.text_input("Nom", contact["Nom"])
            prenom = st.text_input("Prénom", contact["Prénom"])
            email = st.text_input("Email", contact["Email"])
            tel = st.text_input("Téléphone", contact["Téléphone"])
            entreprise = st.text_input("Entreprise", contact["Entreprise"])
            statut = st.selectbox("Statut", ["Prospect", "Client", "Inactif"], index=0 if pd.isna(contact["Statut"]) else ["Prospect","Client","Inactif"].index(contact["Statut"]))
            submit = st.form_submit_button("💾 Enregistrer")

            if submit:
                df_contacts.loc[df_contacts["ID"] == contact["ID"], ["Nom","Prénom","Email","Téléphone","Entreprise","Statut"]] = [nom, prenom, email, tel, entreprise, statut]
                df_contacts.loc[df_contacts["ID"] == contact["ID"], "Score_composite"] = compute_score(contact["ID"])
                df_contacts.loc[df_contacts["ID"] == contact["ID"], "Tags"] = compute_tags(contact["ID"])
                df_contacts.loc[df_contacts["ID"] == contact["ID"], "Proba_conversion"] = compute_proba(contact["ID"])
                save_csv(df_contacts, FILES["contacts"])
                st.success("✅ Contact mis à jour avec succès !")
                st.rerun()

        st.subheader("⚡ Actions liées")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("➕ Ajouter une interaction"):
                new_id = generate_id("INT")
                df_inter.loc[len(df_inter)] = [new_id, contact["ID"], datetime.today().strftime("%Y-%m-%d"), "Email", "Prise de contact", "", ""]
                save_csv(df_inter, FILES["interactions"])
                st.success("✅ Interaction ajoutée !")
                st.rerun()
        with col2:
            if st.button("➕ Ajouter à un événement"):
                if not df_events.empty:
                    ev_choice = st.selectbox("Événement", df_events["Nom"].tolist())
                    if st.button("Confirmer ajout"):
                        ev_id = df_events[df_events["Nom"] == ev_choice]["ID"].iloc[0]
                        new_id = generate_id("PART")
                        df_parts.loc[len(df_parts)] = [new_id, contact["ID"], ev_id, "Inscrit"]
                        save_csv(df_parts, FILES["participations"])
                        st.success(f"✅ {contact['Nom']} ajouté à {ev_choice}")
                        st.rerun()
                else:
                    st.warning("⚠ Aucun événement disponible.")


# -----------------------
# PAGE ÉVÉNEMENTS
# -----------------------
def page_evenements():
    st.title("🎉 Gestion des Événements")

    gb = GridOptionsBuilder.from_dataframe(df_events)
    gb.configure_selection("single", use_checkbox=True)
    gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
    grid_options = gb.build()

    grid_response = AgGrid(
        df_events,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        fit_columns_on_grid_load=True,
        height=400,
    )

    st.subheader("➕ Créer un événement")
    with st.form("add_event"):
        nom = st.text_input("Nom de l’événement")
        type_ev = st.selectbox("Type", ["Webinaire","Afterwork","Formation","Conférence"])
        date_ev = st.date_input("Date")
        lieu = st.text_input("Lieu")
        cout = st.number_input("Coût (FCFA)", 0)
        recette = st.number_input("Recette (FCFA)", 0)
        submit = st.form_submit_button("✅ Ajouter")

        if submit:
            new_id = generate_id("EVT")
            df_events.loc[len(df_events)] = [new_id, nom, type_ev, date_ev, lieu, cout, recette]
            save_csv(df_events, FILES["events"])
            st.success("Événement ajouté !")
            st.rerun()

    if grid_response["selected_rows"]:
        ev = grid_response["selected_rows"][0]
        st.subheader(f"⚙️ Actions pour {ev['Nom']}")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📋 Dupliquer cet événement"):
                new_id = generate_id("EVT")
                ev_copy = ev.copy()
                ev_copy["ID"] = new_id
                df_events.loc[len(df_events)] = ev_copy
                save_csv(df_events, FILES["events"])
                st.success("✅ Événement dupliqué")
                st.rerun()
        with col2:
            if st.button("🗑 Supprimer cet événement"):
                df_events.drop(df_events[df_events["ID"] == ev["ID"]].index, inplace=True)
                save_csv(df_events, FILES["events"])
                st.success("✅ Événement supprimé")
                st.rerun()
# -----------------------
# PAGE RAPPORTS
# -----------------------
def page_rapports():
    st.title("📊 Rapports & KPI")

    st.subheader("🎯 Indicateurs clés")
    total_contacts = len(df_contacts)
    total_events = len(df_events)
    total_interactions = len(df_inter)
    total_paiements = df_pay["Montant"].sum() if not df_pay.empty else 0

    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Contacts", total_contacts)
    kpi2.metric("Événements", total_events)
    kpi3.metric("Interactions", total_interactions)
    kpi4.metric("Recettes (FCFA)", f"{total_paiements:,.0f}")

    st.subheader("📈 Graphiques")
    filtre_annee = st.selectbox("Filtrer par année", ["Tous"] + sorted(df_events["Date"].astype(str).str[:4].unique().tolist()))

    df_ev = df_events.copy()
    if filtre_annee != "Tous":
        df_ev = df_ev[df_ev["Date"].astype(str).str.startswith(filtre_annee)]

    if not df_ev.empty:
        fig = px.bar(df_ev, x="Type", title="Répartition des événements par type")
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.pie(df_ev, names="Lieu", title="Événements par lieu")
        st.plotly_chart(fig2, use_container_width=True)

        if not df_pay.empty:
            fig3 = px.histogram(df_pay, x="Date", y="Montant", title="Recettes par mois", histfunc="sum")
            st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("⚠ Aucun événement trouvé pour ce filtre.")


# -----------------------
# PAGE ADMIN
# -----------------------
def page_admin():
    st.title("⚙️ Administration & Migration")

    st.subheader("📂 Migration Excel")
    uploaded = st.file_uploader("Importer un fichier Excel global ou multi-onglets", type=["xlsx"])
    if uploaded:
        try:
            excel = pd.ExcelFile(uploaded)
            log = []
            if "Contacts" in excel.sheet_names:
                df_new = pd.read_excel(uploaded, sheet_name="Contacts")
                before = len(df_contacts)
                df_contacts_updated = pd.concat([df_contacts, df_new]).drop_duplicates("ID", keep="last")
                save_csv(df_contacts_updated, FILES["contacts"])
                log.append(f"Contacts importés : {len(df_contacts_updated) - before}")
            if "Événements" in excel.sheet_names:
                df_new = pd.read_excel(uploaded, sheet_name="Événements")
                before = len(df_events)
                df_events_updated = pd.concat([df_events, df_new]).drop_duplicates("ID", keep="last")
                save_csv(df_events_updated, FILES["events"])
                log.append(f"Événements importés : {len(df_events_updated) - before}")
            if "Participations" in excel.sheet_names:
                df_new = pd.read_excel(uploaded, sheet_name="Participations")
                before = len(df_parts)
                df_parts_updated = pd.concat([df_parts, df_new]).drop_duplicates("ID", keep="last")
                save_csv(df_parts_updated, FILES["participations"])
                log.append(f"Participations importées : {len(df_parts_updated) - before}")
            if "Paiements" in excel.sheet_names:
                df_new = pd.read_excel(uploaded, sheet_name="Paiements")
                before = len(df_pay)
                df_pay_updated = pd.concat([df_pay, df_new]).drop_duplicates("ID", keep="last")
                save_csv(df_pay_updated, FILES["paiements"])
                log.append(f"Paiements importés : {len(df_pay_updated) - before}")
            if "Interactions" in excel.sheet_names:
                df_new = pd.read_excel(uploaded, sheet_name="Interactions")
                before = len(df_inter)
                df_inter_updated = pd.concat([df_inter, df_new]).drop_duplicates("ID", keep="last")
                save_csv(df_inter_updated, FILES["interactions"])
                log.append(f"Interactions importées : {len(df_inter_updated) - before}")

            st.success("✅ Migration terminée")
            st.write("### Rapport de migration")
            for l in log:
                st.write("-", l)

        except Exception as e:
            st.error(f"Erreur lors de l’import : {e}")

    st.subheader("💾 Export Excel complet")
    if st.button("📤 Exporter toutes les données"):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df_contacts.to_excel(writer, sheet_name="Contacts", index=False)
            df_events.to_excel(writer, sheet_name="Événements", index=False)
            df_parts.to_excel(writer, sheet_name="Participations", index=False)
            df_pay.to_excel(writer, sheet_name="Paiements", index=False)
            df_inter.to_excel(writer, sheet_name="Interactions", index=False)
        st.download_button("⬇ Télécharger l’export Excel", data=output.getvalue(), file_name="CRM_export.xlsx")

    st.subheader("🧹 Réinitialiser la base")
    if st.button("⚠ Supprimer tous les CSV"):
        for f in FILES.values():
            if os.path.exists(f): os.remove(f)
        st.warning("✅ Base réinitialisée (tous les CSV supprimés).")

    st.subheader("🗑 Purger un ID spécifique")
    purge_id = st.text_input("ID à purger (Contact, Événement, etc.)")
    if st.button("Purger l’ID"):
        purged = False
        for name, df, file in [
            ("Contacts", df_contacts, FILES["contacts"]),
            ("Événements", df_events, FILES["events"]),
            ("Participations", df_parts, FILES["participations"]),
            ("Paiements", df_pay, FILES["paiements"]),
            ("Interactions", df_inter, FILES["interactions"]),
        ]:
            if purge_id in df["ID"].values:
                df.drop(df[df["ID"] == purge_id].index, inplace=True)
                save_csv(df, file)
                st.success(f"✅ {name} : ID {purge_id} supprimé")
                purged = True
        if not purged:
            st.info("⚠ ID introuvable.")


# -----------------------
# ROUTAGE PAGES
# -----------------------
PAGES = {
    "CRM": page_crm,
    "Événements": page_evenements,
    "Rapports": page_rapports,
    "Admin": page_admin,
}

def main():
    st.sidebar.title("📌 Navigation")
    choice = st.sidebar.radio("Aller vers", list(PAGES.keys()))
    PAGES[choice]()

if __name__ == "__main__":
    main()
