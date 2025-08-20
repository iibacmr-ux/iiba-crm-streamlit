
import streamlit as st
import pandas as pd
import io, os, shutil
from datetime import datetime

# -----------------------------
# CONFIGURATION & CHEMINS
# -----------------------------
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

PATHS = {
    "contacts": os.path.join(DATA_DIR, "contacts.csv"),
    "inter": os.path.join(DATA_DIR, "interactions.csv"),
    "events": os.path.join(DATA_DIR, "evenements.csv"),
    "parts": os.path.join(DATA_DIR, "participations.csv"),
    "pay": os.path.join(DATA_DIR, "paiements.csv"),
    "cert": os.path.join(DATA_DIR, "certifications.csv"),
}

ALL_SCHEMAS = {
    "contacts": ["ID", "Nom", "Prénom", "Email", "Téléphone", "Société", "Source", "Statut"],
    "interactions": ["ID", "ID Contact", "Date", "Canal", "Objet", "Résumé", "Résultat", "Prochaine Action", "Relance", "Responsable"],
    "evenements": ["ID", "Nom", "Date", "Lieu", "Type"],
    "participations": ["ID", "ID Contact", "ID Evenement", "Statut"],
    "paiements": ["ID", "ID Contact", "Montant", "Moyen", "Statut", "Date"],
    "certifications": ["ID", "ID Contact", "Type", "Date"],
}

# -----------------------------
# HELPERS
# -----------------------------
def ensure_df(path, cols):
    if os.path.exists(path):
        try:
            return pd.read_csv(path, dtype=str)
        except:
            return pd.DataFrame(columns=cols)
    return pd.DataFrame(columns=cols)

def save_df(df, path):
    df.to_csv(path, index=False, encoding="utf-8")

def reset_base():
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR)
    for tbl, cols in ALL_SCHEMAS.items():
        save_df(pd.DataFrame(columns=cols), PATHS[tbl])

def purge_id(tbl, id_value):
    df = ensure_df(PATHS[tbl], ALL_SCHEMAS[tbl])
    df = df[df[ALL_SCHEMAS[tbl][0]] != id_value]
    save_df(df, PATHS[tbl])

# -----------------------------
# INTERFACE STREAMLIT
# -----------------------------
st.title("CRM IIBA Cameroun")

mode = st.sidebar.radio("Section", ["Contacts", "Admin"])

# Charger les bases
df_contacts = ensure_df(PATHS["contacts"], ALL_SCHEMAS["contacts"])
df_inter = ensure_df(PATHS["inter"], ALL_SCHEMAS["interactions"])
df_events = ensure_df(PATHS["events"], ALL_SCHEMAS["evenements"])
df_parts = ensure_df(PATHS["parts"], ALL_SCHEMAS["participations"])
df_pay = ensure_df(PATHS["pay"], ALL_SCHEMAS["paiements"])
df_cert = ensure_df(PATHS["cert"], ALL_SCHEMAS["certifications"])

if mode == "Contacts":
    st.subheader("Liste des contacts")
    st.dataframe(df_contacts, use_container_width=True)

elif mode == "Admin":
    st.subheader("Administration")

    # Réinitialiser la base
    if st.button("⚠️ Réinitialiser la base"):
        reset_base()
        st.success("Base réinitialisée (tous les CSV supprimés et recréés).")

    # Purger un ID
    st.markdown("### Purger un ID")
    tbl = st.selectbox("Table", list(ALL_SCHEMAS.keys()))
    id_value = st.text_input("ID à supprimer")
    if st.button("Supprimer l'ID"):
        if id_value.strip():
            purge_id(tbl, id_value.strip())
            st.success(f"ID {id_value} supprimé de {tbl}.")
        else:
            st.warning("Veuillez saisir un ID.")

    # Import / Export Excel
    st.markdown("### Import / Export Excel")

    up_excel = st.file_uploader("Importer Excel (.xlsx)", type=["xlsx"])
    if st.button("Importer Excel") and up_excel is not None:
        try:
            xls = pd.ExcelFile(up_excel)
            # Mode multi-onglets
            for tbl, cols in ALL_SCHEMAS.items():
                if tbl in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=tbl, dtype=str)
                    for c in cols:
                        if c not in df.columns: df[c] = ""
                    save_df(df[cols], PATHS[tbl])
            st.success("Import Excel multi-onglets terminé.")
        except Exception as e:
            st.error(f"Erreur d'import Excel : {e}")

    # Export Excel
    if st.button("Exporter Excel multi-onglets"):
        try:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                for tbl, df in [
                    ("contacts", df_contacts),
                    ("interactions", df_inter),
                    ("evenements", df_events),
                    ("participations", df_parts),
                    ("paiements", df_pay),
                    ("certifications", df_cert),
                ]:
                    df.to_excel(writer, sheet_name=tbl, index=False)
            st.download_button("⬇️ Télécharger Excel", buf.getvalue(),
                               file_name="IIBA_export.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as e:
            st.error(f"Erreur d'export Excel : {e}")
