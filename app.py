# -*- coding: utf-8 -*-
# ------------------------------------------------------------
# IIBA Cameroun — CRM Streamlit (monofichier stabilisé)
# Version : Mix ancien design + logiques avancées
# Correctifs : set_page_config en tête, CRM/Événements robustes,
# Rapports Objectifs vs Réel (Contacts/Participations/CA),
# Admin : édition parametres.csv (backup/rollback), migration.
# ------------------------------------------------------------

import streamlit as st
st.set_page_config(page_title="IIBA Cameroun — CRM", layout="wide")

import pandas as pd
import numpy as np
from datetime import datetime
import os
from pathlib import Path
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import io

# =========================
# FICHIERS & CONSTANTES
# =========================
DATA_DIR = Path(".")
PARAM_FILE = DATA_DIR / "parametres.csv"
BACKUP_DIR = DATA_DIR / "backups"
BACKUP_DIR.mkdir(exist_ok=True)

DATA_FILES = {
    "contacts": "contacts.csv",
    "events": "events.csv",
    "participations": "participations.csv",
    "interactions": "interactions.csv",
    "paiements": "paiements.csv",
    "certifications": "certifications.csv",
}

# Clés "critiques" attendues dans parametres.csv
CRITICAL_KEYS = [
    # Listes (séparées par |)
    "Genres", "Types_contact", "Statuts_engagement", "Secteurs", "Pays", "Villes",
    "Sources", "Canaux", "Resultats_interaction", "Types_evenements", "Lieux",
    "Statuts_paiement", "Moyens_paiement", "Types_certification", "Entreprises_cibles",
    # Règles scoring & proba
    "Seuil_VIP", "Poids_Interaction", "Poids_Participation", "Poids_Paiement_regle",
    "Fenetre_interactions_jours", "Interactions_min", "Participations_min",
    # Affichage / KPI / Cibles
    "Colonnes_CRM", "KPI_visibles", "KPI_activés", "Cibles"
]

# =========================
# HELPERS PARAMETRES
# =========================
def load_parametres() -> pd.DataFrame:
    """Charge parametres.csv, le crée s'il n'existe pas (avec clés critiques)."""
    if not PARAM_FILE.exists():
        # init par défaut minimal (valeurs vides autorisées)
        df = pd.DataFrame({"clé": CRITICAL_KEYS, "valeur": [""] * len(CRITICAL_KEYS)})
        df.to_csv(PARAM_FILE, index=False, encoding="utf-8")
    df = pd.read_csv(PARAM_FILE, encoding="utf-8")
    # Structure minimale
    if not {"clé", "valeur"}.issubset(df.columns):
        st.error("⚠️ parametres.csv corrompu — tentative de restauration depuis backup…")
        _ = restore_last_backup()
        df = pd.read_csv(PARAM_FILE, encoding="utf-8")
    return df

def save_parametres(df_new: pd.DataFrame) -> bool:
    """Sauvegarde parametres.csv avec backup + validation des clés critiques."""
    try:
        if not {"clé", "valeur"}.issubset(df_new.columns):
            st.error("❌ Structure invalide : colonnes attendues 'clé' et 'valeur'.")
            return False

        missing = [k for k in CRITICAL_KEYS if k not in df_new["clé"].values]
        if missing:
            st.error(f"❌ Sauvegarde annulée. Clés critiques manquantes : {missing}")
            return False

        # Backup avant écrasement
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = BACKUP_DIR / f"parametres_backup_{ts}.csv"
        if PARAM_FILE.exists():
            pd.read_csv(PARAM_FILE, encoding="utf-8").to_csv(backup_path, index=False, encoding="utf-8")

        # Save
        df_new.to_csv(PARAM_FILE, index=False, encoding="utf-8")
        st.success("✅ Paramètres sauvegardés (backup créé).")
        return True
    except Exception as e:
        st.error(f"⚠️ Erreur sauvegarde paramètres : {e}")
        return False

def restore_last_backup() -> pd.DataFrame:
    """Restaure le dernier backup disponible."""
    backups = sorted(BACKUP_DIR.glob("parametres_backup_*.csv"), reverse=True)
    if not backups:
        st.error("❌ Aucun backup disponible.")
        return pd.DataFrame(columns=["clé", "valeur"])
    last = backups[0]
    st.warning(f"⏪ Restauration depuis {last.name}")
    dfb = pd.read_csv(last, encoding="utf-8")
    dfb.to_csv(PARAM_FILE, index=False, encoding="utf-8")
    return dfb

def get_param(key: str, default=None):
    dfp = load_parametres()
    s = dfp.loc[dfp["clé"] == key, "valeur"]
    if not s.empty:
        return s.values[0]
    return default

def to_list_param(key: str, sep="|"):
    """Lit une clé de parametres.csv et renvoie une liste (séparateur '|')."""
    raw = get_param(key, "")
    if not isinstance(raw, str):
        return []
    return [x.strip() for x in raw.split(sep) if str(x).strip() != ""]

# =========================
# ACCÈS CSV DONNÉES
# =========================
def df_or_empty(name: str, cols=None) -> pd.DataFrame:
    path = DATA_FILES[name]
    if os.path.exists(path):
        df = pd.read_csv(path, encoding="utf-8")
        # s'assurer des colonnes minimales
        if cols:
            for c in cols:
                if c not in df.columns:
                    df[c] = np.nan
        return df
    else:
        return pd.DataFrame(columns=cols or [])

def save_df(name: str, df: pd.DataFrame):
    df.to_csv(DATA_FILES[name], index=False, encoding="utf-8")

# =========================
# SCORING / TAGS / PROBA
# =========================
def read_scoring_params():
    def _getf(k, dflt):
        try:
            v = float(get_param(k, dflt))
            return v
        except:
            return float(dflt)
    return {
        "seuil_vip": _getf("Seuil_VIP", 500000.0),
        "poids_inter": _getf("Poids_Interaction", 1.0),
        "poids_part": _getf("Poids_Participation", 1.0),
        "poids_pay": _getf("Poids_Paiement_regle", 2.0),
        "fenetre_jours": _getf("Fenetre_interactions_jours", 90.0),
        "min_inter_chaud": _getf("Interactions_min", 3.0),
        "min_part_chaud": _getf("Participations_min", 1.0),
    }

def compute_scoring_contacts() -> pd.DataFrame:
    """Construit la vue contacts enrichie : interactions/participations/CA/score, tags, proba."""
    contacts = df_or_empty("contacts", ["ID","Nom","Prénom","Email","Téléphone","Société","Type","Statut"])
    parts = df_or_empty("participations", ["ID","ID Contact","ID Événement"])
    inter = df_or_empty("interactions", ["ID","ID Contact","Date","Canal","Objet","Résumé","Résultat","Prochaine_Action","Échéance","Responsable"])
    pay = df_or_empty("paiements", ["ID","ID Contact","ID Événement","Date Paiement","Montant","Moyen","Statut"])

    if contacts.empty:
        return contacts

    # Nettoyage montants
    if "Montant" in pay.columns:
        pay["Montant"] = pd.to_numeric(pay["Montant"], errors="coerce").fillna(0)

    P = read_scoring_params()
    today = pd.Timestamp.today().normalize()

    # Interactions récentes
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

    out = contacts.copy()
    out["Interactions"] = out["ID"].map(nb_inter).fillna(0).astype(int)
    out["Interactions_recent"] = out["ID"].map(nb_inter_recent).fillna(0).astype(int)
    out["Participations"] = out["ID"].map(nb_parts).fillna(0).astype(int)
    out["CA_réglé"] = out["ID"].map(ca_regle).fillna(0).astype(float)

    out["Score_composite"] = (
        out["Interactions"] * P["poids_inter"] +
        out["Participations"] * P["poids_part"] +
        (out["CA_réglé"] / 1000.0) * P["poids_pay"]
    ).round(2)

    tags = []
    probas = []
    for _, r in out.iterrows():
        t = []
        if r["CA_réglé"] >= P["seuil_vip"]:
            t.append("VIP (CA élevé)")
        if r["Participations"] >= 3 and str(r.get("Statut","")).lower() not in ("client","membre"):
            t.append("Régulier-non-converti")
        if r["Interactions"] >= 5 and r["Participations"] >= 2:
            t.append("Ambassadeur")
        tags.append(", ".join(t))

        # proba simple
        p = 0.2
        if r["Interactions_recent"] >= P["min_inter_chaud"] and r["Participations"] >= P["min_part_chaud"]:
            p = 0.9
        elif r["Interactions"] >= 2:
            p = 0.5
        badge = "🟢" if p >= 0.8 else ("🟠" if p >= 0.5 else "🔴")
        probas.append(f"{int(p*100)}% {badge}")

    out["Tags"] = tags
    out["Proba_conversion"] = probas
    return out

# =========================
# PAGE CRM (robuste)
# =========================
def page_crm():
    st.header("👥 CRM — Grille centrale")

    df = compute_scoring_contacts()
    if df is None or df.empty or len(df.columns) == 0:
        st.info("ℹ️ Aucun contact enregistré pour le moment.")
        return

    # Colonnes préférées si disponibles
    preferred_cols = ["ID","Nom","Prénom","Société","Type","Statut","Email",
                      "Interactions","Participations","CA_réglé","Score_composite","Proba_conversion","Tags"]
    cols = [c for c in preferred_cols if c in df.columns]
    if cols:
        df = df[cols]

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
    gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
    if len(df.columns) > 0:
        gb.configure_selection("single", use_checkbox=True)
    gridOptions = gb.build()

    grid_response = AgGrid(
        df,
        gridOptions=gridOptions,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        height=430,
        fit_columns_on_grid_load=True
    )

    selected = grid_response.get("selected_rows", [])
    st.subheader("📌 Fiche Contact")
    if selected:
        st.json(selected[0])
    else:
        st.info("Sélectionnez un contact dans la grille.")

# =========================
# PAGE ÉVÉNEMENTS (robuste)
# =========================
def page_evenements():
    st.header("📅 Gestion des Événements")

    df = df_or_empty("events", ["ID","Nom","Type","Date","Lieu","Coût","Recette"])
    if df.empty:
        st.info("ℹ️ Aucun événement enregistré pour le moment.")
        return

    # Sélecteur sécurisé
    ev_names = df["Nom"].dropna().astype(str).unique().tolist()
    ev_choice = st.selectbox("Sélectionnez un événement :", [""] + ev_names)
    if ev_choice:
        selected_event = df[df["Nom"] == ev_choice].iloc[0].to_dict()
        st.write("📌 Événement sélectionné :", selected_event)

    # Grille en lecture
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(editable=False, filter=True, sortable=True, resizable=True)
    gb.configure_pagination(paginationPageSize=20)
    gridOptions = gb.build()
    AgGrid(df, gridOptions=gridOptions, height=360, update_mode=GridUpdateMode.NO_UPDATE)
# =========================
# PAGE RAPPORTS (KPI & export)
# =========================
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

    # Cibles dans parametres.csv (clé=valeur lignes dans "Cibles")
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
    target_contacts = float(cibles.get(f"kpi_target_contacts_total_year_{year}", 0))
    target_parts = float(cibles.get(f"kpi_target_participations_total_year_{year}", 0))
    target_ca = float(cibles.get(f"kpi_target_ca_regle_year_{year}", 0))

    df_kpi = pd.DataFrame({
        "KPI": ["contacts_total","participations_total","ca_regle"],
        "Objectif": [target_contacts, target_parts, target_ca],
        "Réel": [kpi_contacts, kpi_participations, kpi_ca],
    })
    df_kpi["Écart"] = df_kpi["Réel"] - df_kpi["Objectif"]

    st.subheader("🎯 Objectifs vs Réel")
    st.dataframe(df_kpi, use_container_width=True)

    st.subheader("📈 Indicateurs clés")
    c1, c2, c3 = st.columns(3)
    c1.metric("👥 Contacts", kpi_contacts)
    c2.metric("✅ Participations", kpi_participations)
    c3.metric("💰 CA réglé (FCFA)", f"{kpi_ca:,.0f}")

    # Export Excel du rapport
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        df_kpi.to_excel(writer, sheet_name="KPI", index=False)
    st.download_button(
        "⬇️ Exporter le rapport (Excel)",
        data=out.getvalue(),
        file_name=f"Rapport_KPI_{year}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# =========================
# PAGE ADMIN — Paramètres (édition/backup/rollback)
# =========================
def page_admin():
    st.header("⚙️ Admin — Paramètres, Migration & Maintenance")

    # ---- Paramètres : édition front complète ----
    st.subheader("🛠️ Paramètres (éditables ici)")
    dfp = load_parametres().copy()

    st.caption("Astuce : double-cliquez dans une cellule pour modifier. Les listes utilisent le séparateur ‘|’.")
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

    colA, colB, colC = st.columns(3)
    with colA:
        if st.button("💾 Enregistrer (backup + rollback)"):
            ok = save_parametres(edited)
            if ok:
                st.success("Paramètres enregistrés. Rechargez la page pour tout appliquer.")
    with colB:
        if st.button("⏪ Restaurer le dernier backup"):
            dfb = restore_last_backup()
            st.dataframe(dfb, use_container_width=True)
    with colC:
        st.download_button(
            "📥 Télécharger parametres.csv",
            data=open(PARAM_FILE, "rb").read(),
            file_name="parametres.csv",
            mime="text/csv"
        )

    st.markdown("---")

    # ---- Migration / Import-Export ----
    st.subheader("📦 Migration — Import/Export Global & Multi-onglets")

    mode = st.radio("Mode", ["Import Excel global (.xlsx)", "Import Excel multi-onglets (.xlsx)", "Export Excel global"], horizontal=True)

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
                    sheets = {
                        "contacts": ["ID","Nom","Prénom","Email","Téléphone","Société","Type","Statut"],
                        "interactions": ["ID","ID Contact","Date","Canal","Objet","Résumé","Résultat","Prochaine_Action","Échéance","Responsable"],
                        "events": ["ID","Nom","Type","Date","Lieu","Coût","Recette"],
                        "participations": ["ID","ID Contact","ID Événement","Rôle","Inscription","Arrivée","Temps_Present","Feedback","Note","Commentaire"],
                        "paiements": ["ID","ID Contact","ID Événement","Date Paiement","Montant","Moyen","Statut","Référence","Notes"],
                        "certifications": ["ID","ID Contact","Type","Date Examen","Résultat","Score","Date Obtention","Validité","Renouvellement","Notes"],
                    }
                    for tab, cols in sheets.items():
                        # détection souple du nom d’onglet
                        candidates = [tab, tab.capitalize(), tab.title(), tab.upper()]
                        sheet_name = next((c for c in candidates if c in xls.sheet_names), None)
                        if sheet_name:
                            df_new = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
                            df_new = ensure_cols(df_new, cols)
                            base = df_or_empty(tab if tab != "events" else "events", cols)
                            merged = pd.concat([base, df_new], ignore_index=True)
                            save_df(tab if tab != "events" else "events", merged)
                            log[tab if tab != "events" else "events"] = len(df_new)
                else:
                    # Global : 1ère feuille avec __TABLE__
                    sheet = xls.sheet_names[0]
                    gdf = pd.read_excel(xls, sheet_name=sheet, dtype=str)
                    if "__TABLE__" not in gdf.columns:
                        raise ValueError("Colonne '__TABLE__' absente du modèle global.")
                    for tab in ["contacts","interactions","events","participations","paiements","certifications"]:
                        sub = gdf[gdf["__TABLE__"].astype(str).str.lower()==tab].copy()
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
        st.download_button(
            "⬇️ Télécharger export.xlsx",
            data=out.getvalue(),
            file_name="IIBA_export_global.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    st.markdown("---")
# =========================
# ADMIN — Maintenance (Reset / Purge)
# =========================
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

# =========================
# ROUTAGE
# =========================
def main():
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Aller à", ["CRM (Grille centrale)", "Événements", "Rapports", "Admin"])

    if page == "CRM (Grille centrale)":
        page_crm()
    elif page == "Événements":
        page_evenements()
    elif page == "Rapports":
        page_rapports()
    else:
        page_admin()

if __name__ == "__main__":
    main()
