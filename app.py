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
PAGES = ["Dashboard 360","Contacts","Interactions","Événements",
         "Participations","Paiements","Certifications","Rapports","Paramètres","Migration"]

page = st.sidebar.selectbox("Menu", PAGES)

# --- PAGE MIGRATION ---
def write_empty_sheet(writer, sheet_name, schema):
    df_empty = pd.DataFrame(columns=schema.keys())
    df_empty.to_excel(writer, sheet_name=sheet_name, index=False)

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
