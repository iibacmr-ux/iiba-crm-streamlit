# -*- coding: utf-8 -*-
"""
IIBA Cameroun — CRM Streamlit (monofichier)
Version enrichie : paramétrage complet via Admin, rollback auto, scoring, KPI, rapports
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import os
from pathlib import Path
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# -----------------------
# FICHIERS & CONSTANTES
# -----------------------
DATA_DIR = Path(".")
PARAM_FILE = DATA_DIR / "parametres.csv"
BACKUP_DIR = DATA_DIR / "backups"
BACKUP_DIR.mkdir(exist_ok=True)

CRITICAL_KEYS = [
    "Genres", "Types_contact", "Statuts_engagement", "Secteurs", "Pays", "Villes",
    "Sources", "Canaux", "Resultats_interaction", "Types_evenements", "Lieux",
    "Statuts_paiement", "Moyens_paiement", "Types_certification", "Entreprises_cibles",
    "Seuil_VIP", "Poids_Interaction", "Poids_Participation", "Poids_Paiement_regle",
    "Fenetre_interactions_jours", "Interactions_min", "Participations_min",
    "Colonnes_CRM", "KPI_visibles", "KPI_activés", "Cibles"
]

# -----------------------
# HELPERS PARAMETRES
# -----------------------

def backup_param_file():
    """Crée une sauvegarde horodatée de parametres.csv"""
    if PARAM_FILE.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = BACKUP_DIR / f"parametres_backup_{ts}.csv"
        PARAM_FILE.rename(backup_path)
        backup_path.rename(PARAM_FILE)  # garder le fichier en place
        return backup_path
    return None

def load_parametres():
    """Charge le fichier de paramètres en DataFrame pivoté clé→valeur"""
    if not PARAM_FILE.exists():
        # init par défaut
        default_data = {k: "" for k in CRITICAL_KEYS}
        df = pd.DataFrame(list(default_data.items()), columns=["clé", "valeur"])
        df.to_csv(PARAM_FILE, index=False)
    df = pd.read_csv(PARAM_FILE)
    if "clé" not in df or "valeur" not in df:
        st.error("⚠️ Fichier parametres.csv corrompu. Restauration depuis backup…")
        restore_last_backup()
        df = pd.read_csv(PARAM_FILE)
    return df

def save_parametres(df_new):
    """Sauvegarde avec rollback si clé critique manquante"""
    try:
        # Vérif clés critiques
        missing = [k for k in CRITICAL_KEYS if k not in df_new["clé"].values]
        if missing:
            st.error(f"❌ Sauvegarde annulée. Clés manquantes : {missing}")
            return False

        # Backup
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = BACKUP_DIR / f"parametres_backup_{ts}.csv"
        df_current = pd.read_csv(PARAM_FILE)
        df_current.to_csv(backup_path, index=False)

        # Save
        df_new.to_csv(PARAM_FILE, index=False)
        st.success("✅ Paramètres sauvegardés avec succès")
        return True
    except Exception as e:
        st.error(f"⚠️ Erreur sauvegarde : {e}")
        return False

def restore_last_backup():
    """Restaure le dernier backup valide"""
    backups = sorted(BACKUP_DIR.glob("parametres_backup_*.csv"), reverse=True)
    if backups:
        last = backups[0]
        st.warning(f"⏪ Restauration depuis {last.name}")
        df = pd.read_csv(last)
        df.to_csv(PARAM_FILE, index=False)
        return df
    else:
        st.error("❌ Aucun backup disponible")
        return pd.DataFrame(columns=["clé", "valeur"])

def get_param(key, default=None):
    """Accès direct à une clé paramètre"""
    df = load_parametres()
    row = df.loc[df["clé"] == key, "valeur"]
    if not row.empty:
        return row.values[0]
    return default

# -----------------------
# CHARGEMENT PARAMETRES
# -----------------------
parametres = load_parametres()

# -----------------------
# CRM PAGE (fix grille vide)
# -----------------------
def page_crm():
    st.header("📇 CRM — Grille centrale")
    # Exemple dataframe vide ou chargé
    if os.path.exists("contacts.csv"):
        df = pd.read_csv("contacts.csv")
    else:
        df = pd.DataFrame(columns=["ID","Nom","Prénom","Email","Type","Statut"])

    if df.empty:
        st.info("ℹ️ Aucun contact enregistré pour le moment.")
        return

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination()
    gb.configure_default_column(editable=False, groupable=True)
    gb.configure_selection("single", use_checkbox=True)
    gridOptions = gb.build()

    grid_response = AgGrid(
        df,
        gridOptions=gridOptions,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        height=400,
        fit_columns_on_grid_load=True
    )

    selected = grid_response["selected_rows"]
    if selected:
        st.subheader("📌 Détail du contact sélectionné")
        st.json(selected[0])

# -----------------------
# EVENEMENTS PAGE (fix sélection)
# -----------------------
def page_evenements():
    st.header("📅 Gestion des Événements")
    if os.path.exists("events.csv"):
        df = pd.read_csv("events.csv")
    else:
        df = pd.DataFrame(columns=["ID","Nom_événement","Date","Lieu","Type"])

    if df.empty:
        st.info("ℹ️ Aucun événement enregistré pour le moment.")
        return

    selected_event = None
    selected_event_name = st.selectbox("Sélectionnez un événement :", [""] + df["Nom_événement"].tolist())
    if selected_event_name:
        selected_event = df[df["Nom_événement"] == selected_event_name].iloc[0].to_dict()
        st.write("📌 Événement sélectionné :", selected_event)
# ---------------------------------------------------------
# OUTILS DONNÉES (chargement commun) + SCORING/TAGS/PROBA
# ---------------------------------------------------------
DATA_FILES = {
    "contacts": "contacts.csv",
    "events": "events.csv",
    "participations": "participations.csv",
    "interactions": "interactions.csv",
    "paiements": "paiements.csv",
    "certifications": "certifications.csv",
}

def df_or_empty(name, cols=None):
    path = DATA_FILES[name]
    if os.path.exists(path):
        df = pd.read_csv(path, encoding="utf-8")
        if cols:
            for c in cols:
                if c not in df.columns:
                    df[c] = np.nan
        return df
    else:
        return pd.DataFrame(columns=cols or [])

def save_df(name, df):
    df.to_csv(DATA_FILES[name], index=False, encoding="utf-8")

def to_list_param(key, sep="|"):
    """Retourne une liste à partir d'une clé 'clé' dans parametres.csv (séparateur '|')."""
    dfp = load_parametres()
    val = dfp.loc[dfp["clé"] == key, "valeur"]
    if val.empty:
        return []
    return [x.strip() for x in str(val.values[0]).split(sep) if str(x).strip() != ""]

# ----------- RÈGLES/POIDS DE SCORING (paramétrables) -----------
def read_scoring_params():
    dfp = load_parametres()
    def gv(k, d):
        v = dfp.loc[dfp["clé"] == k, "valeur"]
        return float(v.values[0]) if not v.empty and str(v.values[0]).strip() != "" else d

    params_s = {
        "seuil_vip": gv("Seuil_VIP", 500000.0),
        "poids_inter": gv("Poids_Interaction", 1.0),
        "poids_part": gv("Poids_Participation", 1.0),
        "poids_pay": gv("Poids_Paiement_regle", 2.0),
        "fenetre_jours": gv("Fenetre_interactions_jours", 90.0),
        "min_inter_chaud": gv("Interactions_min", 3.0),
        "min_part_chaud": gv("Participations_min", 1.0),
    }
    return params_s

def compute_scoring_contacts():
    """Calcule Score, Tags, Probabilité (avec pastille) pour chaque contact, en s'appuyant sur les CSV et parametres.csv."""
    contacts = df_or_empty("contacts", ["ID","Nom","Prénom","Email","Société","Type","Statut"])
    parts = df_or_empty("participations", ["ID","ID Contact","ID Événement","Rôle","Inscription","Arrivée"])
    inter = df_or_empty("interactions", ["ID","ID Contact","Date","Canal","Objet","Résumé","Résultat","Prochaine_Action","Échéance","Responsable"])
    pay = df_or_empty("paiements", ["ID","ID Contact","ID Événement","Date Paiement","Montant","Moyen","Statut"])

    if contacts.empty:
        return contacts  # rien à faire

    # Nettoyages min
    for col in ["Montant"]:
        if col in pay.columns:
            pay[col] = pd.to_numeric(pay[col], errors="coerce").fillna(0)

    # Paramètres
    P = read_scoring_params()
    today = pd.Timestamp.today().normalize()

    # Interactions récentes (dans la fenêtre)
    if "Date" in inter.columns:
        inter["_Date"] = pd.to_datetime(inter["Date"], errors="coerce")
        inter_recent = inter[inter["_Date"] >= (today - pd.Timedelta(days=int(P["fenetre_jours"])))]
    else:
        inter["_Date"] = pd.NaT
        inter_recent = inter.copy().iloc[0:0]

    # Agrégats
    nb_parts = parts.groupby("ID Contact")["ID"].count() if not parts.empty else pd.Series(dtype=float)
    nb_inter = inter.groupby("ID Contact")["ID"].count() if not inter.empty else pd.Series(dtype=float)
    nb_inter_recent = inter_recent.groupby("ID Contact")["ID"].count() if not inter_recent.empty else pd.Series(dtype=float)
    ca_regle = pay.loc[pay.get("Statut","").astype(str).str.lower().eq("réglé"), :].groupby("ID Contact")["Montant"].sum() if not pay.empty else pd.Series(dtype=float)

    # Assemblage
    contacts = contacts.copy()
    contacts["Interactions"] = contacts["ID"].map(nb_inter).fillna(0).astype(int)
    contacts["Interactions_recent"] = contacts["ID"].map(nb_inter_recent).fillna(0).astype(int)
    contacts["Participations"] = contacts["ID"].map(nb_parts).fillna(0).astype(int)
    contacts["CA_réglé"] = contacts["ID"].map(ca_regle).fillna(0).astype(float)

    # Score composite
    contacts["Score_composite"] = (
        contacts["Interactions"] * P["poids_inter"] +
        contacts["Participations"] * P["poids_part"] +
        (contacts["CA_réglé"] / 1000.0) * P["poids_pay"]
    ).round(2)

    # Tags
    tags = []
    for _, r in contacts.iterrows():
        t = []
        if r["CA_réglé"] >= P["seuil_vip"]:
            t.append("VIP (CA élevé)")
        if r["Participations"] >= 3 and str(r.get("Statut","")).lower() not in ("client","membre"):
            t.append("Régulier-non-converti")
        # Ambassadeur : beaucoup d'interactions + participation
        if r["Interactions"] >= 5 and r["Participations"] >= 2:
            t.append("Ambassadeur")
        tags.append(", ".join(t))
    contacts["Tags"] = tags

    # Probabilité de conversion
    probas = []
    for _, r in contacts.iterrows():
        p = 0.2
        if r["Interactions_recent"] >= P["min_inter_chaud"] and r["Participations"] >= P["min_part_chaud"]:
            p = 0.8
            # Paiement partiel = bonus
            # si la colonne Statut existe et qu'au moins un paiement partiel récent existe
            # (pour simplifier, nous boostons directement)
            p = 0.9
        elif r["Interactions"] >= 2:
            p = 0.5
        if p >= 0.8:
            badge = "🟢"
        elif p >= 0.5:
            badge = "🟠"
        else:
            badge = "🔴"
        probas.append(f"{int(p*100)}% {badge}")
    contacts["Proba_conversion"] = probas

    return contacts

# ---------------------------------------------------------
# PAGE RAPPORTS — KPI Contacts + Participations + CA
# ---------------------------------------------------------
def page_rapports():
    st.header("📊 Rapports & KPI")

    contacts = df_or_empty("contacts", ["ID"])
    parts = df_or_empty("participations", ["ID","ID Contact","ID Événement"])
    pay = df_or_empty("paiements", ["ID","ID Contact","ID Événement","Montant","Statut","Date Paiement"])

    # KPI Réel
    kpi_contacts = len(contacts)
    kpi_participations = len(parts)
    if not pay.empty:
        pay["Montant"] = pd.to_numeric(pay["Montant"], errors="coerce").fillna(0)
        kpi_ca = pay.loc[pay.get("Statut","").astype(str).str.lower().eq("réglé"), "Montant"].sum()
    else:
        kpi_ca = 0.0

    # Objectifs (parametres.csv → bloc clé=valeur séparé par lignes dans Cibles)
    cibles_blob = get_param("Cibles", "")
    cibles = {}
    if isinstance(cibles_blob, str) and cibles_blob.strip():
        for line in cibles_blob.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                try:
                    cibles[k.strip()] = float(v.strip())
                except:
                    cibles[k.strip()] = v.strip()

    year = datetime.now().year
    target_contacts = cibles.get(f"kpi_target_contacts_total_year_{year}", 0)
    target_parts = cibles.get(f"kpi_target_participations_total_year_{year}", 0)
    target_ca = cibles.get(f"kpi_target_ca_regle_year_{year}", 0)

    df_kpi = pd.DataFrame({
        "KPI": ["contacts_total","participations_total","ca_regle"],
        "Objectif": [target_contacts, target_parts, target_ca],
        "Réel": [kpi_contacts, kpi_participations, kpi_ca],
    })
    df_kpi["Écart"] = df_kpi["Réel"] - df_kpi["Objectif"]

    st.subheader("🎯 Objectifs vs Réel")
    st.dataframe(df_kpi, use_container_width=True)

    # Graphiques simples
    st.subheader("📈 Visuels")
    c1, c2 = st.columns(2)
    with c1:
        st.metric("👥 Contacts", kpi_contacts)
        st.metric("✅ Participations", kpi_participations)
    with c2:
        st.metric("💰 CA réglé (FCFA)", f"{kpi_ca:,.0f}")

    # Export Excel du rapport
    import io
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        df_kpi.to_excel(writer, sheet_name="KPI", index=False)
    st.download_button("⬇️ Exporter le rapport (Excel)", data=out.getvalue(),
                       file_name=f"Rapport_KPI_{year}.xlsx", mime="application/vnd.ms-excel")
# ---------------------------------------------------------
# PAGE ADMIN — Paramétrage complet + Migration + Maintenance
# ---------------------------------------------------------
def page_admin():
    st.header("⚙️ Admin — Paramètres, Migration & Maintenance")

    # ===== Paramètres (édition front complète) =====
    st.subheader("🛠️ Paramètres (éditables ici, stockés dans parametres.csv)")
    dfp = load_parametres().copy()

    st.caption("Astuce : double-cliquez dans une cellule pour modifier. Les listes sont séparées par ‘|’.")
    edited = st.data_editor(
        dfp,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "clé": st.column_config.TextColumn("Clé"),
            "valeur": st.column_config.TextColumn("Valeur"),
        },
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("💾 Enregistrer (avec sauvegarde & rollback sécurisé)"):
            ok = save_parametres(edited)
            if ok:
                st.success("Paramètres mis à jour. Les nouvelles règles seront prises en compte.")
    with c2:
        if st.button("⏪ Restaurer le dernier backup"):
            dfb = restore_last_backup()
            st.dataframe(dfb, use_container_width=True)
    with c3:
        if st.button("📂 Ouvrir le fichier local parametres.csv"):
            st.download_button("Télécharger parametres.csv", data=open(PARAM_FILE, "rb").read(),
                               file_name="parametres.csv")

    st.markdown("---")

    # ===== Migration / Import-Export =====
    st.subheader("📦 Migration — Import/Export Global & Multi-onglets")

    mode = st.radio("Mode de migration", ["Import Excel global (.xlsx)", "Import Excel multi-onglets (.xlsx)", "Export Excel global"], horizontal=True)

    import io
    import pandas as pd

    if mode.startswith("Import Excel"):
        up = st.file_uploader("Fichier Excel", type=["xlsx"])
        if st.button("Importer maintenant") and up:
            log = {"contacts":0,"interactions":0,"events":0,"participations":0,"paiements":0,"certifications":0,"rejects":[]}
            try:
                xls = pd.ExcelFile(up)

                def ensure_cols(df, needed):
                    for c in needed:
                        if c not in df.columns:
                            df[c] = np.nan
                    return df[needed]

                if mode == "Import Excel multi-onglets (.xlsx)":
                    # Feuilles par nom standard
                    sheets = {
                        "contacts": ["ID","Nom","Prénom","Email","Téléphone","Société","Type","Statut"],
                        "interactions": ["ID","ID Contact","Date","Canal","Objet","Résumé","Résultat","Prochaine_Action","Échéance","Responsable"],
                        "events": ["ID","Nom","Type","Date","Lieu","Coût","Recette"],
                        "participations": ["ID","ID Contact","ID Événement","Rôle","Inscription","Arrivée","Temps_Present","Feedback","Note","Commentaire"],
                        "paiements": ["ID","ID Contact","ID Événement","Date Paiement","Montant","Moyen","Statut","Référence","Notes"],
                        "certifications": ["ID","ID Contact","Type","Date Examen","Résultat","Score","Date Obtention","Validité","Renouvellement","Notes"],
                    }
                    for tab, cols in sheets.items():
                        if tab.capitalize() in xls.sheet_names or tab in xls.sheet_names or tab.title() in xls.sheet_names or tab.upper() in xls.sheet_names:
                            sn = tab.capitalize() if tab.capitalize() in xls.sheet_names else (tab.title() if tab.title() in xls.sheet_names else (tab.upper() if tab.upper() in xls.sheet_names else tab))
                            df_new = pd.read_excel(xls, sheet_name=sn, dtype=str)
                            df_new = ensure_cols(df_new, cols)
                            base = df_or_empty(tab if tab!="events" else "events", cols)
                            merged = pd.concat([base, df_new], ignore_index=True)
                            save_df(tab if tab!="events" else "events", merged)
                            log[tab if tab!="events" else "events"] = len(df_new)
                else:
                    # Global : première feuille, colonne __TABLE__
                    sheet = xls.sheet_names[0]
                    gdf = pd.read_excel(xls, sheet_name=sheet, dtype=str)
                    if "__TABLE__" not in gdf.columns:
                        raise ValueError("Colonne '__TABLE__' absente du modèle global.")
                    # Répartition
                    for tab in ["contacts","interactions","events","participations","paiements","certifications"]:
                        sub = gdf[gdf["__TABLE__"].str.lower()==tab].copy()
                        if sub.empty: 
                            continue
                        base = df_or_empty(tab)
                        merged = pd.concat([base, sub], ignore_index=True)
                        save_df(tab, merged)
                        log[tab] = len(sub)

                st.success("✅ Import terminé")
                st.json(log)
            except Exception as e:
                st.error(f"❌ Erreur d'import : {e}")

    elif mode == "Export Excel global":
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="xlsxwriter") as w:
            df_or_empty("contacts").to_excel(w, sheet_name="Contacts", index=False)
            df_or_empty("interactions").to_excel(w, sheet_name="Interactions", index=False)
            df_or_empty("events").to_excel(w, sheet_name="Événements", index=False)
            df_or_empty("participations").to_excel(w, sheet_name="Participations", index=False)
            df_or_empty("paiements").to_excel(w, sheet_name="Paiements", index=False)
            df_or_empty("certifications").to_excel(w, sheet_name="Certifications", index=False)
        st.download_button("⬇️ Télécharger export.xlsx", data=out.getvalue(),
                           file_name="IIBA_export_global.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    st.markdown("---")

    # ===== Maintenance : Reset/Purge =====
    st.subheader("🧹 Maintenance")
    if st.button("🗑️ Réinitialiser la base (supprimer tous les CSV)"):
        for f in DATA_FILES.values():
            if os.path.exists(f):
                os.remove(f)
        st.warning("Base réinitialisée. Rechargez l'application.")

    purge_id = st.text_input("ID à purger (Contact/Événement/…)")
    if st.button("🔎 Purger l'ID"):
        found = False
        for name, f in DATA_FILES.items():
            df = df_or_empty(name)
            if not df.empty and "ID" in df.columns and purge_id in df["ID"].astype(str).values:
                df = df[df["ID"].astype(str) != purge_id]
                save_df(name, df)
                found = True
        if found:
            st.success(f"ID {purge_id} supprimé de la base.")
        else:
            st.info("ID non trouvé.")

# ---------------------------------------------------------
# ROUTAGE
# ---------------------------------------------------------
def main():
    st.set_page_config(page_title="IIBA Cameroun — CRM", layout="wide")
    st.sidebar.title("Navigation")
    choix = st.sidebar.radio("Aller à", ["CRM (Grille centrale)", "Événements", "Rapports", "Admin"])

    if choix == "CRM (Grille centrale)":
        page_crm()
    elif choix == "Événements":
        page_evenements()
    elif choix == "Rapports":
        page_rapports()
    else:
        page_admin()

if __name__ == "__main__":
    main()
