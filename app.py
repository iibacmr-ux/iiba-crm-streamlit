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
    "contacts": "contacts.csv", "interactions": "interactions.csv",
    "evenements": "evenements.csv", "participations": "participations.csv",
    "paiements": "paiements.csv", "certifications": "certifications.csv",
    "settings": "settings.json"
}

DEFAULT = {
    "statuts_paiement":["Réglé","Partiel","Non payé"],
    "resultats_inter":["Positif","Négatif","Neutre","À relancer","À suivre","Sans suite"],
    "types_contact":["Membre","Prospect","Formateur","Partenaire"],
    "sources":["Afterwork","Formation","LinkedIn","Recommandation","Site Web","Salon","Autre"],
    "statuts_engagement":["Actif","Inactif","À relancer"],
    "secteurs":["IT","Finance","Éducation","Santé","Consulting","Autre","Côte d’Ivoire","Sénégal"],
    "pays":["Cameroun","France","Canada","Belgique","Autre"],
    "canaux":["Email","Téléphone","WhatsApp","LinkedIn","Réunion","Autre"],
    "types_evenements":["Atelier","Conférence","Formation","Webinaire","Afterwork","BA MEET UP","Groupe d’étude"],
    "moyens_paiement":["Chèque","Espèces","Virement","CB","Mobile Money","Autre"]
}

# --- PARAMÈTRES ---

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

# --- FONCTIONS DONNÉES ---

def generate_id(prefix, df, col):
    nums = [int(str(x).split("_")[1]) for x in df[col] if isinstance(x, str) and "_" in str(x)]
    n = max(nums) if nums else 0
    return f"{prefix}_{n+1:03d}"

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

# --- SCHÉMAS ---
C_COLS = {
    "ID": lambda: None, "Nom": "", "Prénom": "", "Genre": "", "Titre": "",
    "Société": "", "Secteur": SET["secteurs"][0], "Email": "", "Téléphone": "",
    "Ville": "", "Pays": SET["pays"][0], "Type": SET["types_contact"][0],
    "Source": SET["sources"][0], "Statut": SET.get("statuts_paiement", ["Réglé"])[0],
    "LinkedIn": "", "Notes": "", "Date_Creation": lambda: date.today().isoformat()
}

I_COLS = {
    "ID_Interaction": lambda: None, "ID": "", "Date": date.today().isoformat(),
    "Canal": SET["canaux"][0], "Objet": "", "Résumé": "",
    "Résultat": SET.get("resultats_inter", ["Positif"])[0], "Responsable": "",
    "Prochaine_Action": "", "Relance": ""
}

E_COLS = {
    "ID_Événement": lambda: None, "Nom_Événement": "", "Type": SET["types_evenements"][0],
    "Date": date.today().isoformat(), "Durée_h": 0, "Lieu": "",
    "Formateur(s)": "", "Invité(s)": "", "Objectif": "", "Période": "Matinée", "Notes": "",
    "Coût_Total": 0.0, "Recettes": 0.0, "Bénéfice": 0.0
}

P_COLS = {
    "ID_Participation": lambda: None, "ID": "", "ID_Événement": "",
    "Rôle": "Participant", "Inscription": date.today().isoformat(),
    "Arrivée": "", "Temps_Présent": "", "Feedback": 3, "Note": 0,
    "Commentaire": "", "Nom Participant": "", "Nom Événement": ""
}

PAY_COLS = {
    "ID_Paiement": lambda: None, "ID": "", "ID_Événement": "",
    "Date_Paiement": date.today().isoformat(), "Montant": 0.0,
    "Moyen": SET["moyens_paiement"][0], "Statut": SET["statuts_paiement"][0],
    "Référence": "", "Notes": "", "Relance": "", "Nom Contact": "", "Nom Événement": ""
}

CERT_COLS = {
    "ID_Certif": lambda: None, "ID": "", "Type_Certif": SET["types_contact"][0],
    "Date_Examen": date.today().isoformat(), "Résultat": "Réussi", "Score": 0,
    "Date_Obtention": date.today().isoformat(), "Validité": "", "Renouvellement": "",
    "Notes": "", "Nom Contact": ""
}

# --- PAGES ---
PAGES = ["Dashboard 360", "Vue 360","Contacts","Interactions","Événements",
         "Participations","Paiements","Certifications","Rapports","Paramètres","Migration"]

page = st.sidebar.selectbox("Menu", PAGES)

# --- PAGE MIGRATION ---
def write_empty_sheet(writer, sheet_name, schema):
    df_empty = pd.DataFrame(columns=schema.keys())
    df_empty.to_excel(writer, sheet_name=sheet_name, index=False)

# --- DASHBOARD 360 ---
if page == "Dashboard 360":
    st.title("📈 Tableau de Bord Stratégique")
    dfc = load_df(DATA["contacts"], C_COLS)
    dfi = load_df(DATA["interactions"], I_COLS)
    dfe = load_df(DATA["evenements"], E_COLS)
    dfp = load_df(DATA["participations"], P_COLS)
    dfpay = load_df(DATA["paiements"], PAY_COLS)
    dfcert = load_df(DATA["certifications"], CERT_COLS)

    yrs = sorted({d[:4] for d in dfc["Date_Creation"]}) or [str(date.today().year)]
    mths = ["Tous"] + [f"{i:02d}" for i in range(1, 13)]
    col1, col2 = st.columns(2)
    yr = col1.selectbox("Année", yrs)
    mn = col2.selectbox("Mois", mths, index=0)

    def fil(df, col):
        return df[(df[col].str[:4] == yr) & ((mn == "Tous") | (df[col].str[5:7] == mn))]

    dfc2 = fil(dfc, "Date_Creation")
    dfp2 = fil(dfp, "Inscription")
    dfpay2 = fil(dfpay, "Date_Paiement")
    dfcert2 = fil(dfcert, "Date_Obtention")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Prospects Actifs", len(dfc2[dfc2["Type"] == "Prospect"]))
    c1.metric("Membres IIBA", len(dfc2[dfc2["Type"] == "Membre"]))
    c2.metric("Événements", len(fil(dfe, "Date")))
    c2.metric("Participations", len(dfp2))
    benef = dfpay2[dfpay2["Statut"] == "Réglé"]["Montant"].sum()
    c3.metric("CA réglé", f"{benef:,.0f}")
    c3.metric("Impayés", len(dfpay2[dfpay2["Statut"] != "Réglé"]))
    c4.metric("Certifs Obtenues", len(dfcert2[dfcert2["Résultat"] == "Réussi"]))
    sc = dfp2["Feedback"].mean() if not dfp2.empty else 0
    c4.metric("Score engagement", f"{sc:.1f}")

    if st.button("⬇️ Export unifié CSV"):
        uni = dfc.merge(dfi, on="ID", how="left").merge(dfp, on="ID", how="left")
        st.download_button("Télécharger", uni.to_csv(index=False), file_name="crm_union.csv")

elif page == "Vue 360°":
    st.title("👁 Vue 360° des Contacts")
    df = load_df(DATA["contacts"], C_COLS)

    # Sélection d'un contact pour focus (tableau)
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    gb.configure_selection(selection_mode="single", use_checkbox=True)
    grid_response = AgGrid(df, gridOptions=gb.build(), height=350, fit_columns_on_grid_load=True, key="contact_grid")
    selected = grid_response['selected_rows']

    st.markdown("### Actions disponibles")

    col1, col2, col3, col4, col5 = st.columns(5)

    # Bouton Créer nouveau contact
    if col1.button("➕ Nouveau contact"):
        st.session_state["contact_action"] = "new"
        st.session_state["contact_id"] = None
        st.switch_page("Contacts")

    # Si un contact est sélectionné dans la grille
    if selected:
        contact_id = selected[0]['ID']
        st.write(f"Contact sélectionné : **{contact_id}** {selected['Nom']} {selected['Prénom']}")

        # Actions sur le contact
        if col2.button("✏️ Modifier ce contact"):
            st.session_state["contact_action"] = "edit"
            st.session_state["contact_id"] = contact_id
            st.switch_page("Contacts")
        if col3.button("💬 Interactions"):
            st.session_state["focus_contact"] = contact_id
            st.switch_page("Interactions")
        if col4.button("🙋 Participations"):
            st.session_state["focus_contact"] = contact_id
            st.switch_page("Participations")
        if col5.button("💳 Paiements"):
            st.session_state["focus_contact"] = contact_id
            st.switch_page("Paiements")
    else:
        st.info("Sélectionnez un contact dans la grille ci-dessus pour afficher les actions.")

    st.markdown("---")
    st.caption("La grille AG Grid permet de filtrer et sélectionner vos contacts. Les boutons d'action facilitent la navigation Salesforce-like au sein du CRM.")


elif page == "Contacts":
    st.title("👤 Contacts")

    df = load_df(DATA["contacts"], C_COLS)

    sel = st.selectbox("Sélectionner un contact par ID", [""] + df["ID"].tolist())
    rec = df[df["ID"] == sel].iloc[0] if sel else None

    # Formulaire pour affichage/modification/création contact
    with st.form("form_contacts", clear_on_submit=False):
        if sel:
            st.text_input("ID", rec["ID"], disabled=True)
        nom = st.text_input("Nom", rec["Nom"] if rec is not None else "")
        prenom = st.text_input("Prénom", rec["Prénom"] if rec is not None else "")
        genre = st.selectbox("Genre", ["", "Homme", "Femme", "Autre"],
                             index=(["", "Homme", "Femme", "Autre"].index(rec["Genre"]) if rec is not None else 0))
        titre = st.text_input("Titre", rec["Titre"] if rec is not None else "")
        societe = st.text_input("Société", rec["Société"] if rec is not None else "")
        secteur = st.selectbox("Secteur", SET["secteurs"], index=(SET["secteurs"].index(rec["Secteur"]) if rec is not None else 0))
        typec = st.selectbox("Type", SET["types_contact"], index=(SET["types_contact"].index(rec["Type"]) if rec is not None else 0))
        source = st.selectbox("Source", SET["sources"], index=(SET["sources"].index(rec["Source"]) if rec is not None else 0))
        statut = st.selectbox("Statut", SET.get("statuts_paiement", ["Réglé"]), index=(SET["statuts_paiement"].index(rec["Statut"]) if rec is not None else 0))
        email = st.text_input("Email", rec["Email"] if rec is not None else "")
        tel = st.text_input("Téléphone", rec["Téléphone"] if rec is not None else "")
        ville = st.text_input("Ville", rec["Ville"] if rec is not None else "")
        pays = st.selectbox("Pays", SET["pays"], index=(SET["pays"].index(rec["Pays"]) if rec is not None else 0))
        linkedin = st.text_input("LinkedIn", rec["LinkedIn"] if rec is not None else "")
        notes = st.text_area("Notes", rec["Notes"] if rec is not None else "")
        dc = st.text_input("Date Création", rec["Date_Creation"] if rec is not None else date.today().isoformat())

        submit = st.form_submit_button("Enregistrer")

        if submit:
            if rec is not None:
                idx = df[df["ID"] == sel].index[0]
                df.loc[idx] = [sel, nom, prenom, genre, titre, societe, secteur,
                               email, tel, ville, pays, typec, source, statut,
                               linkedin, notes, dc]
            else:
                new_id = generate_id("CNT", df, "ID")
                new = {"ID": new_id, "Nom": nom, "Prénom": prenom, "Genre": genre, "Titre": titre,
                       "Société": societe, "Secteur": secteur, "Email": email, "Téléphone": tel,
                       "Ville": ville, "Pays": pays, "Type": typec, "Source": source,
                       "Statut": statut, "Linkedin": linkedin, "Notes": notes, "Date_Creation": dc}
                df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)

            save_df(df, DATA["contacts"])
            st.success("Contact enregistré")

    # Export CSV complet
    if st.button("⬇️ Exporter la liste complète au format CSV"):
        st.download_button("Télécharger CSV", df.to_csv(index=False), file_name="contacts.csv")

    # Affichage tableau interactif avec AgGrid
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True, resizable=True)
    gb.configure_selection(selection_mode="single", use_checkbox=True)
    grid_response = AgGrid(df, gridOptions=gb.build(), height=400, fit_columns_on_grid_load=True, key="contacts_grid")

    # Récupérer ligne sélectionnée
    selected = grid_response['selected_rows']
    if selected:
        sel_id = selected[0]['ID']
        st.write(f"Contact sélectionné : {sel_id}")

# --- PAGE Interactions ---
elif page == "Interactions":
    st.title("💬 Interactions")
    df = load_df(DATA["interactions"], I_COLS)
    dfc = load_df(DATA["contacts"], C_COLS)
    opts = [""] + dfc["ID"].tolist()

    with st.form("f_inter"):
        idc = st.selectbox("ID Contact", opts)
        date_i = st.date_input("Date", date.today())
        canal = st.selectbox("Canal", SET["canaux"])
        objet = st.text_input("Objet", "")
        resume = st.text_area("Résumé", "")
        resultat = st.selectbox("Résultat", SET.get("resultats_inter", ["Positif"])[0])
        responsable = st.text_input("Responsable", "")
        pa = st.text_area("Prochaine_Action", "")
        rel = st.date_input("Relance (opt.)", value=None)
        sub = st.form_submit_button("Enregistrer")

        if sub and idc:
            new = {"ID_Interaction": generate_id("INT", df, "ID_Interaction"), "ID": idc,
                   "Date": date_i.isoformat(), "Canal": canal, "Objet": objet, "Résumé": resume,
                   "Résultat": resultat, "Responsable": responsable,
                   "Prochaine_Action": pa, "Relance": (rel.isoformat() if rel else "")}
            df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
            save_df(df, DATA["interactions"])
            st.success("Interaction enregistrée")

    if st.button("⬇️ Export Interactions CSV"):
        st.download_button("Télécharger CSV", df.to_csv(index=False), file_name="interactions.csv")

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build())

# --- PAGE Événements ---
elif page == "Événements":
    st.title("📅 Événements")
    df = load_df(DATA["evenements"], E_COLS)

    with st.form("f_event"):
        nom = st.text_input("Nom Événement", "")
        typ = st.selectbox("Type", SET["types_evenements"])
        dt = st.date_input("Date", date.today())
        duree = st.number_input("Durée (h)", min_value=0.0, step=0.5)
        lieu = st.text_input("Lieu", "")
        form = st.text_area("Formateur(s)", "")
        inv = st.text_area("Invité(s)", "")
        obj = st.text_area("Objectif", "")
        per = st.selectbox("Période", ["Matinée","Après-midi","Journée"])
        notes = st.text_area("Notes", "")
        sub = st.form_submit_button("Enregistrer")

        if sub:
            new = {"ID_Événement": generate_id("EVT", df, "ID_Événement"), "Nom_Événement": nom,
                   "Type": typ, "Date": dt.isoformat(), "Durée_h": duree, "Lieu": lieu,
                   "Formateur(s)": form, "Invité(s)": inv, "Objectif": obj, "Période": per, "Notes": notes,
                   "Coût_Total": 0.0, "Recettes": 0.0, "Bénéfice": 0.0}
            df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
            save_df(df, DATA["evenements"])
            st.success("Événement enregistré")

    if st.button("⬇️ Export Événements CSV"):
        st.download_button("Télécharger CSV", df.to_csv(index=False), file_name="evenements.csv")

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build())

# --- PAGE Participations ---
elif page == "Participations":
    st.title("🙋 Participations")
    df = load_df(DATA["participations"], P_COLS)
    dfc = load_df(DATA["contacts"], C_COLS)
    dfe = load_df(DATA["evenements"], E_COLS)
    opts_c = [""] + dfc["ID"].tolist()
    opts_e = [""] + dfe["ID_Événement"].tolist()

    with st.form("f_part"):
        idc = st.selectbox("ID Contact", opts_c)
        ide = st.selectbox("ID Événement", opts_e)
        role = st.selectbox("Rôle", ["Participant","Organisateur","Formateur","Invité"])
        ins = st.date_input("Inscription", date.today())
        arr = st.text_input("Arrivée (hh:mm)", "")
        feedback = st.slider("Feedback", 1, 5, 3)
        note = st.number_input("Note", min_value=0, max_value=20)
        comm = st.text_area("Commentaire", "")
        sub = st.form_submit_button("Enregistrer")

        if sub and idc and ide:
            new = {"ID_Participation": generate_id("PAR", df, "ID_Participation"),
                   "ID": idc, "ID_Événement": ide, "Rôle": role,
                   "Inscription": ins.isoformat(), "Arrivée": arr,
                   "Temps_Présent": "AUTO", "Feedback": feedback, "Note": note,
                   "Commentaire": comm, "Nom Participant": "", "Nom Événement": ""}
            df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
            save_df(df, DATA["participations"])
            st.success("Participation enregistrée")

    if st.button("⬇️ Export Participations CSV"):
        st.download_button("Télécharger CSV", df.to_csv(index=False), file_name="participations.csv")

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build())

# --- PAGE Paiements ---
elif page == "Paiements":
    st.title("💳 Paiements")
    df = load_df(DATA["paiements"], PAY_COLS)

    with st.form("f_pay"):
        idc = st.text_input("ID Contact", "")
        ide = st.text_input("ID Événement", "")
        dp = st.date_input("Date Paiement", date.today())
        mont = st.number_input("Montant", min_value=0.0, step=100.0)
        moy = st.selectbox("Moyen", SET["moyens_paiement"])
        stat = st.selectbox("Statut", SET.get("statuts_paiement", ["Réglé"]))
        ref = st.text_input("Référence", "")
        notes = st.text_area("Notes", "")
        rel = st.date_input("Relance (opt.)", value=None)
        sub = st.form_submit_button("Enregistrer")

        if sub and idc and ide:
            new = {"ID_Paiement": generate_id("PAY", df, "ID_Paiement"), "ID": idc,
                   "ID_Événement": ide, "Date_Paiement": dp.isoformat(), "Montant": mont,
                   "Moyen": moy, "Statut": stat, "Référence": ref, "Notes": notes,
                   "Relance": (rel.isoformat() if rel else ""), "Nom Contact": "", "Nom Événement": ""}
            df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
            save_df(df, DATA["paiements"])
            st.success("Paiement enregistré")

    if st.button("⬇️ Export Paiements CSV"):
        st.download_button("Télécharger CSV", df.to_csv(index=False), file_name="paiements.csv")

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build())

# --- PAGE Certifications ---
elif page == "Certifications":
    st.title("📜 Certifications")
    df = load_df(DATA["certifications"], CERT_COLS)

    with st.form("f_cert"):
        idc = st.text_input("ID Contact", "")
        tc = st.selectbox("Type Certif", SET["types_contact"])
        de = st.date_input("Date Examen", date.today())
        res = st.selectbox("Résultat", ["Réussi","Échoué","En attente"])
        score = st.number_input("Score", min_value=0, step=1)
        dob = st.date_input("Date Obtention", date.today())
        valid = "AUTO"
        ren = "AUTO"
        notes = st.text_area("Notes", "")
        sub = st.form_submit_button("Enregistrer")

        if sub and idc:
            new = {"ID_Certif": generate_id("CER", df, "ID_Certif"), "ID": idc,
                   "Type_Certif": tc, "Date_Examen": de.isoformat(), "Résultat": res,
                   "Score": score, "Date_Obtention": dob.isoformat(),
                   "Validité": valid, "Renouvellement": ren, "Notes": notes, "Nom Contact": ""}
            df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
            save_df(df, DATA["certifications"])
            st.success("Certification enregistrée")

    if st.button("⬇️ Export Certifications CSV"):
        st.download_button("Télécharger CSV", df.to_csv(index=False), file_name="certifications.csv")

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(sortable=True, filterable=True)
    AgGrid(df, gridOptions=gb.build())

# --- PAGE Paramètres ---
elif page == "Paramètres":
    st.title("⚙️ Paramètres")
    st.markdown("### Référentiels principaux")

    col1, col2 = st.columns(2)

    with col1:
        with st.expander("💰 Statuts de paiement"):
            statuts_paiement = st.text_area("Liste des statuts de paiement", "\n".join(SET["statuts_paiement"]))
        with st.expander("📨 Résultats d'interaction"):
            resultats_inter = st.text_area("Liste des résultats possibles d'une interaction", "\n".join(SET["resultats_inter"]))
        with st.expander("🧑💼 Types de contact"):
            types_contact = st.text_area("Types de contact", "\n".join(SET["types_contact"]))
        with st.expander("📋 Sources"):
            sources = st.text_area("Sources", "\n".join(SET["sources"]))

    with col2:
        with st.expander("🕹 Statuts d'engagement"):
            statuts_engagement = st.text_area("Statuts d'engagement", "\n".join(SET["statuts_engagement"]))
        with st.expander("🏢 Secteurs"):
            secteurs = st.text_area("Secteurs", "\n".join(SET["secteurs"]))
        with st.expander("🌍 Pays"):
            pays = st.text_area("Pays", "\n".join(SET["pays"]))
        with st.expander("🛠 Canaux"):
            canaux = st.text_area("Canaux de communication", "\n".join(SET["canaux"]))
        with st.expander("🎫 Types d'événements"):
            types_evenements = st.text_area("Types d'événements", "\n".join(SET["types_evenements"]))
        with st.expander("💵 Moyens de paiement"):
            moyens_paiement = st.text_area("Moyens de paiement", "\n".join(SET["moyens_paiement"]))

    if st.button("💾 Sauvegarder Paramètres"):
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
        st.success("Paramètres mis à jour ✅")


if page == "Migration":
    st.title("📦 Migration / Import & Export de données")
    migration_tabs = st.tabs(["Télécharger Template", "Importer données", "Historique"])

    # Onglet 1: Template
    with migration_tabs[0]:
        st.header("Télécharger le fichier template Excel")
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            write_empty_sheet(writer, 'Contacts', C_COLS)
            write_empty_sheet(writer, 'Interactions', I_COLS)
            write_empty_sheet(writer, 'Événements', E_COLS)
            write_empty_sheet(writer, 'Participations', P_COLS)
            write_empty_sheet(writer, 'Paiements', PAY_COLS)
            write_empty_sheet(writer, 'Certifications', CERT_COLS)
        output.seek(0)
        st.download_button(
            label="Télécharger le template Excel",
            data=output,
            file_name="template_IIBA_Cameroun.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # Onglet 2: Import
    with migration_tabs[1]:
        st.header("Importer un fichier Excel complété")
        uploaded_file = st.file_uploader("Choisissez un fichier Excel (.xlsx) à importer", type=["xlsx"])
        if uploaded_file:
            try:
                wb = openpyxl.load_workbook(uploaded_file)
            except Exception as e:
                st.error(f"Erreur à la lecture du fichier Excel : {e}")
                wb = None

            if wb:
                required_sheets = {
                    'Contacts': C_COLS,
                    'Interactions': I_COLS,
                    'Événements': E_COLS,
                    'Participations': P_COLS,
                    'Paiements': PAY_COLS,
                    'Certifications': CERT_COLS
                }
                missing_sheets = [s for s in required_sheets if s not in wb.sheetnames]
                if missing_sheets:
                    st.error(f"Feuilles manquantes: {missing_sheets}")
                else:
                    data_to_import = {}
                    errors = []
                    for sheet, schema in required_sheets.items():
                        df = pd.read_excel(uploaded_file, sheet_name=sheet)
                        missing_cols = [col for col in schema.keys() if col not in df.columns]
                        if missing_cols:
                            errors.append(f"Feuille '{sheet}': colonnes manquantes: {missing_cols}")
                        else:
                            data_to_import[sheet] = df

                    if errors:
                        st.error("Erreurs dans le fichier :")
                        for err in errors:
                            st.write(f"- {err}")
                    else:
                        st.success("Fichier validé. Colonnes conformes.")

                        for sheet, df in data_to_import.items():
                            st.subheader(f"Aperçu - {sheet}")
                            st.dataframe(df.head())

                        summary = {}
                        for sheet, new_df in data_to_import.items():
                            file_path = DATA[sheet.lower()]
                            existing_df = load_df(file_path, required_sheets[sheet])
                            id_col = list(required_sheets[sheet].keys())[0]
                            new_ids = set(new_df[id_col].dropna())
                            if existing_df.empty:
                                added = len(new_ids)
                                updated = 0
                            else:
                                existing_ids = set(existing_df[id_col].dropna())
                                added = len(new_ids - existing_ids)
                                updated = len(new_ids & existing_ids)
                            summary[sheet] = {"à ajouter": added, "à mettre à jour": updated}

                        st.markdown("### Opérations prévues")
                        for sheet, counts in summary.items():
                            st.write(f"- **{sheet}** : {counts['à ajouter']} ajouts, {counts['à mettre à jour']} MAJ")

                        if st.button("Confirmer et importer"):
                            user = "admin"
                            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            log_lines = [f"{timestamp} - Import lancé par {user}\n"]
                            rollback_data = {}
                            try:
                                existing_data = {s: load_df(DATA[s.lower()], required_sheets[s]) for s in required_sheets}
                                for sheet in data_to_import:
                                    id_col = list(required_sheets[sheet].keys())[0]
                                    new_df = data_to_import[sheet]
                                    orig_df = existing_data.get(sheet, pd.DataFrame())
                                    rollback_data[sheet] = orig_df.copy()
                                    filtered_orig = orig_df[~orig_df[id_col].isin(new_df[id_col].dropna())]
                                    merged_df = pd.concat([filtered_orig, new_df], ignore_index=True)
                                    save_df(merged_df, DATA[sheet.lower()])
                                log_lines.append("Migration OK\n")
                                st.success("Import réussi.")
                            except Exception as e:
                                log_lines.append(f"ERREUR migration : {e}\n")
                                log_lines.append(traceback.format_exc())
                                # rollback
                                for sheet, df in rollback_data.items():
                                    save_df(df, DATA[sheet.lower()])
                                st.error(f"Erreur: rollback effectué. Détail: {e}")
                            with open("migrations.log", "a", encoding="utf-8") as f_log:
                                f_log.writelines(log_lines)

    # Onglet 3: Historique
    with migration_tabs[2]:
        st.header("Historique des migrations")
        try:
            with open("migrations.log", "r", encoding="utf-8") as f_log:
                st.text_area("Logs de migration", value=f_log.read(), height=400)
        except FileNotFoundError:
            st.info("Pas encore de logs de migration.")

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
    months = ["Tous"] + [f"{i:02d}" for i in range(1,13)]
    mn = st.selectbox("Mois", months)

    def fil(df, col):
        return df[(df[col].str[:4]==yr) & ((mn=="Tous") | (df[col].str[5:7]==mn))]

    dfc2 = fil(dfc, "Date_Creation")
    dfe2 = fil(dfe, "Date")
    dfpay2 = fil(dfpay, "Date_Paiement")
    dfp2 = fil(dfp, "Inscription")
    dfcert2 = fil(dfcert, "Date_Obtention")

    total_contacts = len(dfc2)
    new_prospects = len(dfc2[dfc2["Type"]=="Prospect"])
    new_membres = len(dfc2[dfc2["Type"]=="Membre"])
    nb_events = len(dfe2)
    nb_participations = len(dfp2)
    ca_total = dfpay2[dfpay2["Statut"]=="Réglé"]["Montant"].sum()
    impayes = len(dfpay2[dfpay2["Statut"]!="Réglé"])
    nb_certifs = len(dfcert2[dfcert2["Résultat"]=="Réussi"])
    taux_conversion = (new_membres/(new_prospects+new_membres)*100) if (new_prospects+new_membres)>0 else 0
    taux_partic_event = (nb_participations/nb_events) if nb_events>0 else 0

    st.markdown("| Indicateur | Valeur |")
    st.markdown(f"|---|---|")
    st.markdown(f"| Nouveaux contacts | {total_contacts} |")
    st.markdown(f"| Nouveaux prospects | {new_prospects} |")
    st.markdown(f"| Membres (nouveaux ou actifs) | {new_membres} |")
    st.markdown(f"| Evénements | {nb_events} |")
    st.markdown(f"| Participations | {nb_participations} |")
    st.markdown(f"| Taux de participation par événement | {taux_partic_event:.1f} |")
    st.markdown(f"| Chiffre d'affaires encaissé | {ca_total:,.0f} FCFA |")
    st.markdown(f"| Impayés | {impayes} |")
    st.markdown(f"| Taux conversion prospects > membres | {taux_conversion:.1f}% |")
    st.markdown(f"| Certifications obtenues | {nb_certifs} |")

    # Export Excel Rapports
    if st.button("Exporter le rapport en Excel"):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            dfc2.to_excel(writer, sheet_name='Contacts', index=False)
            dfe2.to_excel(writer, sheet_name='Evenements', index=False)
            dfp2.to_excel(writer, sheet_name='Participations', index=False)
            dfpay2.to_excel(writer, sheet_name='Paiements', index=False)
            dfcert2.to_excel(writer, sheet_name='Certifications', index=False)
        output.seek(0)
        st.download_button(
            label="Télécharger rapport Excel",
            data=output,
            file_name="rapport_IIBA.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ... (les autres pages restent inchangées : Dashboard 360, Contacts, etc.)
