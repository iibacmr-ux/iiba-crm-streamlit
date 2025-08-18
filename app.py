import streamlit as st
import pandas as pd
import os, json
from datetime import datetime, date
from st_aggrid import AgGrid, GridOptionsBuilder
import io
import openpyxl
import traceback

# --- CONFIGURATION ---
st.set_page_config(page_title="IIBA Cameroun CRM", page_icon="📊", layout="wide")

DATA = {
    "contacts": "contacts.csv",
    "interactions": "interactions.csv",
    "evenements": "evenements.csv",
    "participations": "participations.csv",
    "paiements": "paiements.csv",
    "certifications": "certifications.csv",
    "settings": "settings.json"
}

DEFAULT = {
    "statuts_paiement": ["Réglé", "Partiel", "Non payé"],
    "resultats_inter": ["Positif", "Négatif", "Neutre", "À relancer", "À suivre", "Sans suite"],
    "types_contact": ["Membre", "Prospect", "Formateur", "Partenaire"],
    "sources": ["Afterwork", "Formation", "LinkedIn", "Recommandation", "Site Web", "Salon", "Autre"],
    "statuts_engagement": ["Actif", "Inactif", "À relancer"],
    "secteurs": ["IT", "Finance", "Éducation", "Santé", "Consulting", "Autre", "Côte d’Ivoire", "Sénégal"],
    "pays": ["Cameroun", "France", "Canada", "Belgique", "Autre"],
    "canaux": ["Email", "Téléphone", "WhatsApp", "LinkedIn", "Réunion", "Autre"],
    "types_evenements": ["Atelier", "Conférence", "Formation", "Webinaire", "Afterwork", "BA MEET UP", "Groupe d’étude"],
    "moyens_paiement": ["Chèque", "Espèces", "Virement", "CB", "Mobile Money", "Autre"]
}

@st.cache_data
def load_settings():
    if os.path.exists(DATA["settings"]):
        return json.load(open(DATA["settings"], encoding="utf-8"))
    json.dump(DEFAULT, open(DATA["settings"], "w", encoding="utf-8"), indent=2)
    return DEFAULT

def save_settings(s):
    json.dump(s, open(DATA["settings"], "w", encoding="utf-8"), indent=2)
    st.cache_data.clear()

SET = load_settings()

def load_df(file, cols):
    if os.path.exists(file):
        df = pd.read_csv(file, encoding="utf-8")
    else:
        df = pd.DataFrame(columns=cols)
    for c, v in cols.items():
        if c not in df.columns:
            df[c] = v() if callable(v) else v
    return df[list(cols.keys())]

def save_df(df, file):
    df.to_csv(file, index=False, encoding="utf-8")

def generate_id(prefix, df, col):
    nums = [int(str(x).split("_")[1]) for x in df[col] if isinstance(x, str) and "_" in str(x)]
    n = max(nums) if nums else 0
    return f"{prefix}_{n+1:03d}"

C_COLS = {
    "ID": lambda: None, "Nom": "", "Prénom": "", "Genre": "", "Titre": "",
    "Société": "", "Secteur": SET['secteurs'][0], "Email": "", "Téléphone": "",
    "Ville": "", "Pays": SET['pays'][0], "Type": SET['types_contact'][0], "Source": SET['sources'][0],
    "Statut": SET['statuts_paiement'][0], "LinkedIn": "", "Notes": "", "Date_Creation": lambda: date.today().isoformat()
}

I_COLS = {
    "ID_Interaction": lambda: None, "ID": "", "Date": date.today().isoformat(), "Canal": SET['canaux'][0],
    "Objet": "", "Résumé": "", "Résultat": SET['resultats_inter'][0], "Responsable": "",
    "Prochaine_Action": "", "Relance": ""
}

E_COLS = {
    "ID_Événement": lambda: None, "Nom_Événement": "", "Type": SET['types_evenements'][0], "Date": date.today().isoformat(),
    "Durée_h": 0.0, "Lieu": "", "Formateur(s)": "", "Invité(s)": "", "Objectif": "", "Période": "Matinée",
    "Notes": "", "Coût_Total": 0.0, "Recettes": 0.0, "Bénéfice": 0.0
}

P_COLS = {
    "ID_Participation": lambda: None, "ID": "", "ID_Événement": "", "Rôle": "Participant", 
    "Inscription": date.today().isoformat(), "Arrivée": "", "Temps_Present": "AUTO", "Feedback": 3, 
    "Note": 0, "Commentaire": "", "Nom Participant": "", "Nom Événement": ""
}

PAY_COLS = {
    "ID_Paiement": lambda: None, "ID": "", "ID_Événement": "", "Date_Paiement": date.today().isoformat(),
    "Montant": 0.0, "Moyen": SET['moyens_paiement'][0], "Statut": SET['statuts_paiement'][0],
    "Référence": "", "Notes": "", "Relance": "", "Nom Contact": "", "Nom Événement": ""
}

CERT_COLS = {
    "ID_Certif": lambda: None, "ID": "", "Type_Certif": SET['types_contact'][0], "Date_Examen": date.today().isoformat(),
    "Résultat": "Réussi", "Score": 0, "Date_Obtention": date.today().isoformat(),
    "Validité": "", "Renouvellement": "", "Notes": "", "Nom Contact": ""
}

# --- Handle navigation redirection ---

if "redirect_page" in st.session_state:
    page = st.session_state.pop("redirect_page")
else:
    page = st.sidebar.selectbox("Menu", ["Dashboard", "Vue 360°", "Contacts", "Interactions", "Evenements", "Participations", "Paiements", "Certifications", "Rapports", "Migration", "Paramètres"])

# --------- Pages ---------

# --- PAGE Dashboard
if page == "Dashboard":
    st.title("📈 Tableau de Bord Stratégique")

    dfc = load_df(DATA["contacts"], C_COLS)
    dfi = load_df(DATA["interactions"], I_COLS)
    dfe = load_df(DATA["evenements"], E_COLS)
    dfp = load_df(DATA["participations"], P_COLS)
    dfpay = load_df(DATA["paiements"], PAY_COLS)
    dfcert = load_df(DATA["certifications"], CERT_COLS)

    years = sorted({d[:4] for d in dfc["Date_Creation"]}) or [str(date.today().year)]
    months = ["Tous"] + [f"{i:02d}" for i in range(1, 13)]
    col1, col2 = st.columns(2)
    yr = col1.selectbox("Année", years)
    mn = col2.selectbox("Mois", months, index=0)

    def fil(df, col):
        return df[(df[col].str[:4] == yr) & ((mn == "Tous") | (df[col].str[5:7] == mn))]

    dfc_filtered = fil(dfc, "Date_Creation")
    dfe_filtered = fil(dfe, "Date")
    dfp_filtered = fil(dfp, "Inscription")
    dfpay_filtered = fil(dfpay, "Date_Paiement")
    dfcert_filtered = fil(dfcert, "Date_Obtention")

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Prospects Actifs", len(dfc_filtered[dfc_filtered["Type"] == "Prospect"]))
    c1.metric("Membres IIBA", len(dfc_filtered[dfc_filtered["Type"] == "Membre"]))

    c2.metric("Événements", len(dfe_filtered))
    c2.metric("Participations", len(dfp_filtered))

    ca_total = dfpay_filtered[dfpay_filtered["Statut"] == "Réglé"]["Montant"].sum()
    impayes_count = len(dfpay_filtered[dfpay_filtered["Statut"] != "Réglé"])
    c3.metric("CA réglé (FCFA)", f"{ca_total:,.0f}")
    c3.metric("Paiements en attente", impayes_count)

    certifs_obtenues = len(dfcert_filtered[dfcert_filtered["Résultat"] == "Réussi"])
    avg_engagement_score = dfp_filtered["Feedback"].mean() if not dfp_filtered.empty else 0
    c4.metric("Certifications Obtenues", certifs_obtenues)
    c4.metric("Score Engagement moyen", f"{avg_engagement_score:.1f}")

    # Bouton export unifié CSV
    if st.button("⬇️ Export unifié CSV"):
        merged_df = dfc.merge(dfi, on="ID", how="left").merge(dfp, on="ID", how="left")
        csv_data = merged_df.to_csv(index=False)
        st.download_button("Télécharger CSV combiné", csv_data, file_name="crm_union.csv")

# --- PAGE Vue 360
elif page == "Vue 360°":
    st.title("👁 Vue 360° des Contacts")
    df = load_df(DATA["contacts"], C_COLS)

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    gb.configure_selection('single', use_checkbox=True)
    grid_response = AgGrid(df, gb.build(), height=350, fit_columns_on_grid_load=True, key='vue360')
    selected = grid_response['selected_rows']

    col_add, col_edit, col_inter, col_part, col_pay = st.columns(5)

    if col_add.button("➕ Nouveau contact"):
        st.session_state["redirect_page"] = "Contacts"
        st.session_state["contact_action"] = "new"
        st.session_state["contact_id"] = None
        st.experimental_rerun()

    if selected:
        sel_id = selected[0]['ID']
        st.write(f"Selected contact: **{sel_id}** {selected[0].get('Nom','')} {selected[0].get('Prénom','')}")

        if col_edit.button("✏️ Editer contact"):
            st.session_state["redirect_page"] = "Contacts"
            st.session_state["contact_action"] = "edit"
            st.session_state["contact_id"] = sel_id
            st.experimental_rerun()
        if col_inter.button("💬 Interactions"):
            st.session_state["redirect_page"] = "Interactions"
            st.session_state["focus_contact"] = sel_id
            st.experimental_rerun()
        if col_part.button("🙋 Participations"):
            st.session_state["redirect_page"] = "Participations"
            st.session_state["focus_contact"] = sel_id
            st.experimental_rerun()
        if col_pay.button("💳 Paiements"):
            st.session_state["redirect_page"] = "Paiements"
            st.session_state["focus_contact"] = sel_id
            st.experimental_rerun()
    else:
        st.info("Sélectionnez un contact dans le tableau ci-dessus pour activer les actions.")

# --- PAGE Migration (template, import, historique) ---
elif page == "Migration":
    st.title("📦 Migration et Import de données")

    migration_tabs = st.tabs(["Télécharger Template", "Importer Données", "Historique"])

    # Template Excel
    with migration_tabs[0]:
        st.header("Télécharger le template Excel")
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            pd.DataFrame(columns=C_COLS.keys()).to_excel(writer, sheet_name='Contacts', index=False)
            pd.DataFrame(columns=I_COLS.keys()).to_excel(writer, sheet_name='Interactions', index=False)
            pd.DataFrame(columns=E_COLS.keys()).to_excel(writer, sheet_name='Événements', index=False)
            pd.DataFrame(columns=P_COLS.keys()).to_excel(writer, sheet_name='Participations', index=False)
            pd.DataFrame(columns=PAY_COLS.keys()).to_excel(writer, sheet_name='Paiements', index=False)
            pd.DataFrame(columns=CERT_COLS.keys()).to_excel(writer, sheet_name='Certifications', index=False)
        output.seek(0)
        st.download_button(
            label="Télécharger template Excel",
            data=output,
            file_name="template_iiba_cameroun.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # Import données
    with migration_tabs[1]:
        st.header("Importer un fichier Excel complété")
        uploaded_file = st.file_uploader("Charger un fichier .xlsx", type=["xlsx"])
        if uploaded_file:
            try:
                wb = openpyxl.load_workbook(uploaded_file)
            except Exception as e:
                st.error(f"Erreur lecture fichier Excel : {e}")
                wb = None
            if wb:
                required_sheets = {
                    "Contacts": C_COLS,
                    "Interactions": I_COLS,
                    "Événements": E_COLS,
                    "Participations": P_COLS,
                    "Paiements": PAY_COLS,
                    "Certifications": CERT_COLS,
                }
                missing_sheets = [s for s in required_sheets if s not in wb.sheetnames]
                if missing_sheets:
                    st.error(f"Feuilles manquantes dans le fichier : {missing_sheets}")
                else:
                    data_to_import = {}
                    errors = []
                    for sheet, schema in required_sheets.items():
                        df = pd.read_excel(uploaded_file, sheet_name=sheet)
                        missing_cols = [c for c in schema if c not in df.columns]
                        if missing_cols:
                            errors.append(f"Feuille {sheet} : colonnes manquantes {missing_cols}")
                        else:
                            data_to_import[sheet] = df

                    if errors:
                        for err in errors:
                            st.error(err)
                    else:
                        st.success("Fichier valide, prêt à l'import.")
                        for sheet, df in data_to_import.items():
                            st.subheader(f"Aperçu - {sheet}")
                            st.dataframe(df.head(10))

                        if st.button("Confirmer import"):
                            log_lines = []
                            import_success = True
                            try:
                                for sheet, new_df in data_to_import.items():
                                    existing_df = load_df(DATA[sheet.lower()], required_sheets[sheet])
                                    id_col = list(required_sheets[sheet].keys())[0]
                                    # Remove existing rows with matching IDs, then append
                                    existing_ids = set(existing_df[id_col].dropna())
                                    new_ids = set(new_df[id_col].dropna())
                                    updated_ids = existing_ids & new_ids
                                    filtered_df = existing_df[~existing_df[id_col].isin(updated_ids)]
                                    combined = pd.concat([filtered_df, new_df], ignore_index=True)
                                    save_df(combined, DATA[sheet.lower()])
                                log_lines.append(f"{datetime.now()} - Import réussi\n")
                            except Exception as e:
                                import_success = False
                                log_lines.append(f"{datetime.now()} - Erreur import : {e}\n")
                                st.error(f"Erreur lors de l'import: {e}")
                            with open("migrations.log", "a", encoding="utf-8") as f_log:
                                f_log.writelines(log_lines)
                            if import_success:
                                st.success("Import exécuté avec succès.")

    # Historique des imports
    with migration_tabs[2]:
        st.header("Historique des migrations")
        try:
            with open("migrations.log", "r", encoding="utf-8") as f_log:
                log_content = f_log.read()
                st.text_area("Logs", log_content, height=400)
        except FileNotFoundError:
            st.info("Aucun historique de migration disponible.")

# --- PAGE RAPPORTS AVANCÉS ---
elif page == "Rapports":
    st.title("📊 Rapports avancés")

    dfc = load_df(DATA["contacts"], C_COLS)
    dfe = load_df(DATA["evenements"], E_COLS)
    dfp = load_df(DATA["participations"], P_COLS)
    dfpay = load_df(DATA["paiements"], PAY_COLS)
    dfcert = load_df(DATA["certifications"], CERT_COLS)

    years = sorted({d[:4] for d in dfc["Date_Creation"]}) or [str(date.today().year)]
    yr = st.selectbox("Année", years)
    months = ["Tous"] + [f"{i:02d}" for i in range(1, 13)]
    mn = st.selectbox("Mois", months, index=0)

    def fil(df, col):
        return df[(df[col].str[:4] == yr) & ((mn == "Tous") | (df[col].str[5:7] == mn))]

    dfc_f = fil(dfc, "Date_Creation")
    dfe_f = fil(dfe, "Date")
    dfp_f = fil(dfp, "Inscription")
    dfpay_f = fil(dfpay, "Date_Paiement")
    dfcert_f = fil(dfcert, "Date_Obtention")

    total_contacts = len(dfc_f)
    prospects = len(dfc_f[dfc_f["Type"] == "Prospect"])
    membres = len(dfc_f[dfc_f["Type"] == "Membre"])
    nb_events = len(dfe_f)
    nb_participations = len(dfp_f)
    ca = dfpay_f[dfpay_f["Statut"] == "Réglé"]["Montant"].sum()
    impayes = len(dfpay_f[dfpay_f["Statut"] != "Réglé"])
    nb_certifs = len(dfcert_f[dfcert_f["Résultat"] == "Réussi"])

    taux_conversion = (membres / max(prospects + membres, 1)) * 100
    taux_participation = (nb_participations / max(nb_events, 1))

    st.markdown("| KPI | Valeur |")
    st.markdown("| --- | -----: |")
    st.markdown(f"| Total contacts | {total_contacts} |")
    st.markdown(f"| Nouveaux prospects | {prospects} |")
    st.markdown(f"| Membres | {membres} |")
    st.markdown(f"| Nombre d'événements | {nb_events} |")
    st.markdown(f"| Nombre de participations | {nb_participations} |")
    st.markdown(f"| Taux de participation moyen par événement | {taux_participation:.2f} |")
    st.markdown(f"| Chiffre d'affaires encaissé (FCFA) | {ca:,.0f} |")
    st.markdown(f"| Paiements en attente | {impayes} |")
    st.markdown(f"| Certifications obtenues | {nb_certifs} |")
    st.markdown(f"| Taux de conversion prospects > membres (%) | {taux_conversion:.2f} |")

    if st.button("Exporter rapport Excel"):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            dfc_f.to_excel(writer, sheet_name='Contacts', index=False)
            dfe_f.to_excel(writer, sheet_name='Evenements', index=False)
            dfp_f.to_excel(writer, sheet_name='Participations', index=False)
            dfpay_f.to_excel(writer, sheet_name='Paiements', index=False)
            dfcert_f.to_excel(writer, sheet_name='Certifications', index=False)
        output.seek(0)
        st.download_button(
            label="Télécharger le rapport Excel",
            data=output,
            file_name=f"rapport_{yr}_{mn}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# --- PAGE Contacts détaillée avec UI moderne, formulaire complet, validation, export et grille interactive ---
elif page == "Contacts":
    st.title("👤 Gestion des Contacts")

    df = load_df(DATA["contacts"], C_COLS)
    contact_action = st.session_state.get('contact_action', 'view')
    contact_id = st.session_state.get('contact_id', None)

    if contact_action == 'edit' and contact_id:
        rec = df.loc[df['ID'] == contact_id].squeeze()
    else:
        rec = None

    with st.form("form_contact"):
        if rec is not None:
            st.text_input("ID", rec["ID"], disabled=True)
        nom = st.text_input("Nom", rec["Nom"] if rec is not None else "")
        prenom = st.text_input("Prénom", rec["Prénom"] if rec is not None else "")
        genre = st.selectbox("Genre", ["", "Homme", "Femme", "Autre"],
                             index=(["", "Homme", "Femme", "Autre"].index(rec["Genre"]) if rec is not None else 0))
        titre = st.text_input("Titre", rec["Titre"] if rec is not None else "")
        societe = st.text_input("Société", rec["Société"] if rec is not None else "")
        secteur = st.selectbox("Secteur", SET["secteurs"],
                               index=(SET["secteurs"].index(rec["Secteur"]) if rec is not None else 0))
        typec = st.selectbox("Type", SET["types_contact"],
                             index=(SET["types_contact"].index(rec["Type"]) if rec is not None else 0))
        source = st.selectbox("Source", SET["sources"],
                              index=(SET["sources"].index(rec["Source"]) if rec is not None else 0))
        statut = st.selectbox("Statut", SET.get("statuts_paiement", ["Réglé"]),
                              index=(SET["statuts_paiement"].index(rec["Statut"]) if rec is not None else 0))
        email = st.text_input("Email", rec["Email"] if rec is not None else "")
        tel = st.text_input("Téléphone", rec["Téléphone"] if rec is not None else "")
        ville = st.text_input("Ville", rec["Ville"] if rec is not None else "")
        pays = st.selectbox("Pays", SET["pays"],
                            index=(SET["pays"].index(rec["Pays"]) if rec is not None else 0))
        linkedin = st.text_input("LinkedIn", rec["LinkedIn"] if rec is not None else "")
        notes = st.text_area("Notes", rec["Notes"] if rec is not None else "")
        dc = st.text_input("Date Création", rec["Date_Creation"] if rec is not None else date.today().isoformat())

        submit = st.form_submit_button("Enregistrer")

    if submit:
        if rec is not None:
            idx = df[df["ID"] == rec["ID"]].index[0]
            df.loc[idx] = [rec["ID"], nom, prenom, genre, titre, societe, secteur,
                           email, tel, ville, pays, typec, source, statut,
                           linkedin, notes, dc]
        else:
            new_id = generate_id("CNT", df, "ID")
            new = {"ID": new_id, "Nom": nom, "Prénom": prenom, "Genre": genre, "Titre": titre,
                   "Société": societe, "Secteur": secteur, "Email": email, "Téléphone": tel,
                   "Ville": ville, "Pays": pays, "Type": typec, "Source": source,
                   "Statut": statut, "LinkedIn": linkedin, "Notes": notes, "Date_Creation": dc}
            df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
        save_df(df, DATA["contacts"])
        st.success("Contact enregistré")
        st.session_state.pop("contact_action", None)
        st.session_state.pop("contact_id", None)

    c1, c2 = st.columns([3,1])
    with c1:
        st.markdown("### Liste des contacts")
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(sortable=True, filterable=True, resizable=True)
        gb.configure_selection(selection_mode="single", use_checkbox=True)
        grid_response = AgGrid(df, gridOptions=gb.build(), height=400, fit_columns_on_grid_load=True)
        selected = grid_response["selected_rows"]
    with c2:
        if selected:
            sel_id = selected[0]["ID"]
            st.write(f"Contact sélectionné: **{selected[0]['Nom']} {selected[0]['Prénom']}** (ID: {sel_id})")
            if st.button("Modifier ce contact"):
                st.session_state["contact_action"] = "edit"
                st.session_state["contact_id"] = sel_id
                st.experimental_rerun()
            if st.button("Voir Interactions"):
                st.session_state["focus_contact"] = sel_id
                st.session_state["redirect_page"] = "Interactions"
                st.experimental_rerun()
            if st.button("Voir Participations"):
                st.session_state["focus_contact"] = sel_id
                st.session_state["redirect_page"] = "Participations"
                st.experimental_rerun()
            if st.button("Voir Paiements"):
                st.session_state["focus_contact"] = sel_id
                st.session_state["redirect_page"] = "Paiements"
                st.experimental_rerun()
        else:
            st.info("Sélectionnez un contact dans la liste pour voir les actions.")

    if st.button("⬇️ Exporter tous les contacts (CSV)"):
        csv_data = df.to_csv(index=False)
        st.download_button("Télécharger CSV", csv_data, file_name="contacts_export.csv")

# --- PAGE Interactions avec filtrage contact + UI avancée ---
elif page == "Interactions":
    st.title("💬 Interactions")
    dfc = load_df(DATA["contacts"], C_COLS)
    dfi = load_df(DATA["interactions"], I_COLS)

    focus_contact = st.session_state.get("focus_contact")
    if focus_contact:
        dfi = dfi[dfi["ID"] == focus_contact]
        st.markdown(f"Affichage des interactions pour contact **{focus_contact}**")

    with st.form("form_interaction"):
        idc = st.selectbox("ID Contact", [""] + dfc["ID"].tolist(), index=(dfc[dfc["ID"]==focus_contact].index[0]+1 if focus_contact else 0))
        date_i = st.date_input("Date interaction", date.today())
        canal = st.selectbox("Canal", SET["canaux"])
        objet = st.text_input("Objet", "")
        resume = st.text_area("Résumé", "")
        resultat = st.selectbox("Résultat", SET["resultats_inter"])
        responsable = st.text_input("Responsable", "")
        prochaine_action = st.text_area("Prochaine action", "")
        relance = st.date_input("Date relance (optionnel)", value=None)

        submit = st.form_submit_button("Enregistrer interaction")

    if submit and idc:
        new = {"ID_Interaction": generate_id("INT", dfi, "ID_Interaction"), "ID": idc,
               "Date": date_i.isoformat(), "Canal": canal, "Objet": objet,
               "Résumé": resume, "Résultat": resultat, "Responsable": responsable,
               "Prochaine_Action": prochaine_action, "Relance": relance.isoformat() if relance else ""}
        
        dfi = pd.concat([dfi, pd.DataFrame([new])], ignore_index=True)
        save_df(dfi, DATA["interactions"])
        st.success("Interaction enregistrée")
        st.experimental_rerun()

    st.markdown("### Liste des interactions")
    gb = GridOptionsBuilder.from_dataframe(dfi)
    gb.configure_default_column(sortable=True, filterable=True, resizable=True)
    AgGrid(dfi, gridOptions=gb.build(), height=400, fit_columns_on_grid_load=True)

# --- PAGE Événements avec vue détaillée et formulaire ---
elif page == "Evenements":
    st.title("📅 Événements")
    dfe = load_df(DATA["evenements"], E_COLS)

    with st.form("form_evenement"):
        nom = st.text_input("Nom de l'événement")
        typ = st.selectbox("Type", SET["types_evenements"])
        dt = st.date_input("Date", date.today())
        duree = st.number_input("Durée (heures)", min_value=0.0, step=0.5)
        lieu = st.text_input("Lieu")
        formateurs = st.text_area("Formateur(s)")
        invites = st.text_area("Invité(s)")
        objectif = st.text_area("Objectif")
        periode = st.selectbox("Période", ["Matinée", "Après-midi", "Journée"])
        notes = st.text_area("Notes")

        submit = st.form_submit_button("Enregistrer événement")

    if submit:
        new = {"ID_Événement": generate_id("EVT", dfe, "ID_Événement"), "Nom_Événement": nom,
               "Type": typ, "Date": dt.isoformat(), "Durée_h": duree, "Lieu": lieu,
               "Formateur(s)": formateurs, "Invité(s)": invites, "Objectif": objectif,
               "Période": periode, "Notes": notes, "Coût_Total": 0.0, "Recettes": 0.0, "Bénéfice": 0.0}
        dfe = pd.concat([dfe, pd.DataFrame([new])], ignore_index=True)
        save_df(dfe, DATA["evenements"])
        st.success("Événement enregistré")
        st.experimental_rerun()

    st.markdown("### Liste des événements")
    gb = GridOptionsBuilder.from_dataframe(dfe)
    gb.configure_default_column(sortable=True, filterable=True, resizable=True)
    AgGrid(dfe, gridOptions=gb.build(), height=400, fit_columns_on_grid_load=True)

# --- PAGE Participations avec filtre contact + grille ---
elif page == "Participations":
    st.title("🙋 Participations")
    dfp = load_df(DATA["participations"], P_COLS)
    dfc = load_df(DATA["contacts"], C_COLS)
    dfe = load_df(DATA["evenements"], E_COLS)

    focus_contact = st.session_state.get("focus_contact", None)
    if focus_contact:
        dfp = dfp[dfp["ID"] == focus_contact]
        st.markdown(f"Participations pour contact **{focus_contact}**")

    with st.form("form_participation"):
        idc = st.selectbox("ID Contact", [""] + dfc["ID"].tolist())
        ide = st.selectbox("ID Événement", [""] + dfe["ID_Événement"].tolist())
        role = st.selectbox("Rôle", ["Participant", "Organisateur", "Formateur", "Invité"])
        inscription = st.date_input("Date inscription", date.today())
        arrivee = st.text_input("Heure arrivée (HH:MM)")
        feedback = st.slider("Feedback", 1, 5, 3)
        note = st.number_input("Note (0-20)", min_value=0, max_value=20)
        commentaire = st.text_area("Commentaire")

        submit = st.form_submit_button("Enregistrer participation")

    if submit and idc and ide:
        new = {"ID_Participation": generate_id("PAR", dfp, "ID_Participation"), "ID": idc, "ID_Événement": ide,
               "Rôle": role, "Inscription": inscription.isoformat(), "Arrivée": arrivee,
               "Temps_Present": "AUTO", "Feedback": feedback, "Note": note,
               "Commentaire": commentaire, "Nom Participant": "", "Nom Événement": ""}
        dfp = pd.concat([dfp, pd.DataFrame([new])], ignore_index=True)
        save_df(dfp, DATA["participations"])
        st.success("Participation enregistrée")
        st.experimental_rerun()

    st.markdown("### Liste des participations")
    gb = GridOptionsBuilder.from_dataframe(dfp)
    gb.configure_default_column(sortable=True, filterable=True, resizable=True)
    AgGrid(dfp, gridOptions=gb.build(), height=400, fit_columns_on_grid_load=True)

# --- PAGE Paiements avec filtre contact + formulaire ---
elif page == "Paiements":
    st.title("💳 Paiements")
    dfpay = load_df(DATA["paiements"], PAY_COLS)
    dfc = load_df(DATA["contacts"], C_COLS)
    dfe = load_df(DATA["evenements"], E_COLS)

    focus_contact = st.session_state.get("focus_contact", None)
    if focus_contact:
        dfpay = dfpay[dfpay["ID"] == focus_contact]
        st.markdown(f"Paiements pour contact **{focus_contact}**")

    with st.form("form_paiement"):
        idc = st.selectbox("ID Contact", [""] + dfc["ID"].tolist())
        ide = st.selectbox("ID Événement", [""] + dfe["ID_Événement"].tolist())
        date_pay = st.date_input("Date paiement", date.today())
        montant = st.number_input("Montant FCFA", min_value=0.0, step=100.0)
        moyen = st.selectbox("Moyen de paiement", SET["moyens_paiement"])
        statut = st.selectbox("Statut paiement", SET["statuts_paiement"])
        reference = st.text_input("Référence paiement")
        notes = st.text_area("Notes")
        relance = st.date_input("Date relance (optionnelle)", value=None)

        submit = st.form_submit_button("Enregistrer paiement")

    if submit and idc and ide:
        new = {"ID_Paiement": generate_id("PAY", dfpay, "ID_Paiement"), "ID": idc, "ID_Événement": ide,
               "Date_Paiement": date_pay.isoformat(), "Montant": montant, "Moyen": moyen, "Statut": statut,
               "Référence": reference, "Notes": notes, "Relance": relance.isoformat() if relance else "", 
               "Nom Contact": "", "Nom Événement": ""}
        dfpay = pd.concat([dfpay, pd.DataFrame([new])], ignore_index=True)
        save_df(dfpay, DATA["paiements"])
        st.success("Paiement enregistré")
        st.experimental_rerun()

    st.markdown("### Liste des paiements")
    gb = GridOptionsBuilder.from_dataframe(dfpay)
    gb.configure_default_column(sortable=True, filterable=True, resizable=True)
    AgGrid(dfpay, gridOptions=gb.build(), height=400, fit_columns_on_grid_load=True)

# --- PAGE Certifications avec filtre contact + formulaire ---
elif page == "Certifications":
    st.title("📜 Certifications")
    dfcert = load_df(DATA["certifications"], CERT_COLS)
    dfc = load_df(DATA["contacts"], C_COLS)

    focus_contact = st.session_state.get("focus_contact", None)
    if focus_contact:
        dfcert = dfcert[dfcert["ID"] == focus_contact]
        st.markdown(f"Certifications pour contact **{focus_contact}**")

    with st.form("form_certification"):
        idc = st.selectbox("ID Contact", [""] + dfc["ID"].tolist())
        type_certif = st.selectbox("Type de certification", SET["types_contact"])
        date_exam = st.date_input("Date examen", date.today())
        resultat = st.selectbox("Résultat", ["Réussi", "Échoué", "En attente"])
        score = st.number_input("Score", min_value=0, step=1)
        date_obtention = st.date_input("Date obtention", date.today())
        notes = st.text_area("Notes")

        submit = st.form_submit_button("Enregistrer certification")

    if submit and idc:
        new = {"ID_Certif": generate_id("CER", dfcert, "ID_Certif"), "ID": idc, "Type_Certif": type_certif,
               "Date_Examen": date_exam.isoformat(), "Résultat": resultat, "Score": score,
               "Date_Obtention": date_obtention.isoformat(), "Validité": "AUTO", "Renouvellement": "AUTO",
               "Notes": notes, "Nom Contact": ""}
        dfcert = pd.concat([dfcert, pd.DataFrame([new])], ignore_index=True)
        save_df(dfcert, DATA["certifications"])
        st.success("Certification enregistrée")
        st.experimental_rerun()

    st.markdown("### Liste des certifications")
    gb = GridOptionsBuilder.from_dataframe(dfcert)
    gb.configure_default_column(sortable=True, filterable=True, resizable=True)
    AgGrid(dfcert, gridOptions=gb.build(), height=400, fit_columns_on_grid_load=True)

# --- PAGE Paramètres avec affichage et sauvegarde des référentiels ---
elif page == "Paramètres":
    st.title("⚙️ Paramètres")

    col1, col2 = st.columns(2)
    with col1:
        statuts_paiement = st.text_area("Statuts de paiement", "\n".join(SET["statuts_paiement"]))
        resultats_inter = st.text_area("Résultats d'interaction", "\n".join(SET["resultats_inter"]))
        types_contact = st.text_area("Types de contact", "\n".join(SET["types_contact"]))
        sources = st.text_area("Sources", "\n".join(SET["sources"]))
    with col2:
        statuts_engagement = st.text_area("Statuts d'engagement", "\n".join(SET["statuts_engagement"]))
        secteurs = st.text_area("Secteurs", "\n".join(SET["secteurs"]))
        pays = st.text_area("Pays", "\n".join(SET["pays"]))
        canaux = st.text_area("Canaux de communication", "\n".join(SET["canaux"]))
        types_evenements = st.text_area("Types d'événements", "\n".join(SET["types_evenements"]))
        moyens_paiement = st.text_area("Moyens de paiement", "\n".join(SET["moyens_paiement"]))

    if st.button("💾 Sauvegarder paramètres"):
        SET["statuts_paiement"] = statuts_paiement.split("\n")
        SET["resultats_inter"] = resultats_inter.split("\n")
        SET["types_contact"] = types_contact.split("\n")
        SET["sources"] = sources.split("\n")
        SET["statuts_engagement"] = statuts_engagement.split("\n")
        SET["secteurs"] = secteurs.split("\n")
        SET["pays"] = pays.split("\n")
        SET["canaux"] = canaux.split("\n")
        SET["types_evenements"] = types_evenements.split("\n")
        SET["moyens_paiement"] = moyens_paiement.split("\n")
        save_settings(SET)
        st.success("Paramètres sauvegardés avec succès !")
