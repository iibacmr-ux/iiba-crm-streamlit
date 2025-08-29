# pages/01_CRM.py — CRM (Grille centrale) IIBA Cameroun
from __future__ import annotations

import io
from datetime import date, datetime, timedelta
import pandas as pd
import streamlit as st

# --- AgGrid (pagination + filtres avancés)
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode
    HAS_AGGRID = True
except Exception:
    HAS_AGGRID = False

# --- Modules internes : robustes aux hot-reload
import importlib

# _shared : fonctions communes (global filter, load/save, id, params, sets…)
try:
    SH = importlib.import_module("_shared")
    SH = importlib.reload(SH)
except Exception as e:
    st.error(f"Échec import _shared : {e}")
    SH = None

# storage_backend : sauvegarde avec verrou optimiste (CSV ou GSheets)
try:
    SB = importlib.import_module("storage_backend")
    SB = importlib.reload(SB)
except Exception as e:
    st.error(f"Échec import storage_backend : {e}")
    SB = None

# Aliases/fallbacks (on évite les plantages si une API manque)
def _get(name, default=None):
    return getattr(SH, name, default) if SH else default

load_all_tables             = _get("load_all_tables",            lambda: {})
apply_global_filters        = _get("apply_global_filters",       lambda df, *_: df)
generate_id                 = _get("generate_id",                lambda p, df, col, w=3: f"{p}_{str(len(df)+1).zfill(w)}")
PARAMS                      = _get("PARAMS",                     {})
SET                         = _get("SET",                        { "types_contact": [], "canaux": [], "resultats_inter": [], "moyens_paiement": [], "statuts_paiement": [], "types_certif": [] })
AUDIT_COLS                  = _get("AUDIT_COLS",                 ["Created_At","Created_By","Updated_At","Updated_By"])
parse_date                  = _get("parse_date",                 lambda s: None)
email_ok                    = _get("email_ok",                   lambda s: True)
phone_ok                    = _get("phone_ok",                   lambda s: True)

save_df_target = getattr(SB, "save_df_target", None)  # peut être None si import raté

# WS_FUNC pour GSheets (optionnel). S’il n’existe pas et que backend=gsheets, storage_backend lèvera un message clair.
WS_FUNC = st.session_state.get("WS_FUNC", None)

# =============== Helpers de la page ===============

def _now_iso():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def _stamp_create(row: dict, user: dict | None):
    """Ajoute les colonnes d’audit lors d’une création."""
    row = dict(row)
    now = _now_iso()
    uid = (user or {}).get("UserID", "system")
    row.setdefault("Created_At", now)
    row.setdefault("Created_By", uid)
    row["Updated_At"] = row.get("Updated_At", now)
    row["Updated_By"] = row.get("Updated_By", uid)
    return row

def _stamp_update(row: dict, user: dict | None):
    """Met à jour Updated_* lors d’une édition."""
    row = dict(row)
    row["Updated_At"] = _now_iso()
    row["Updated_By"] = (user or {}).get("UserID", "system")
    return row

def _statusbar(df: pd.DataFrame, context: str):
    """Status bar agrégée (compteurs, sommes)."""
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(f"{context} — lignes", len(df))
    if {"CA_réglé","Impayé"}.issubset(df.columns):
        ca = pd.to_numeric(df["CA_réglé"], errors="coerce").fillna(0).sum()
        imp = pd.to_numeric(df["Impayé"], errors="coerce").fillna(0).sum()
        c2.metric("💰 CA réglé (grid)", f"{int(ca):,} FCFA".replace(",", " "))
        c3.metric("❌ Impayés (grid)", f"{int(imp):,} FCFA".replace(",", " "))
    else:
        c2.metric("—", "—")
        c3.metric("—", "—")
    c4.metric("Horodatage", _now_iso())

def _aggrid(df: pd.DataFrame, page_size=20, key="grid", side_bar=True, single_select=True, style_cols: dict | None=None):
    """AgGrid générique avec pagination + filtres + sélection simple."""
    if not HAS_AGGRID:
        st.info("Installez `streamlit-aggrid` pour la pagination et les filtres avancés.")
        st.dataframe(df, use_container_width=True)
        return {"selected_rows": []}

    gob = GridOptionsBuilder.from_dataframe(df)
    gob.configure_default_column(filter=True, sortable=True, resizable=True)
    gob.configure_pagination(paginationAutoPageSize=False, paginationPageSize=page_size)
    gob.configure_selection("single" if single_select else "multiple", use_checkbox=single_select)
    if side_bar:
        gob.configure_side_bar()

    if style_cols:
        for col, js in style_cols.items():
            gob.configure_column(col, cellStyle=js)

    grid = AgGrid(
        df,
        gridOptions=gob.build(),
        height=520,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        key=key,
        allow_unsafe_jscode=True
    )
    return grid

def _contact_display_label(row: pd.Series) -> str:
    return f"{row.get('ID','')} — {row.get('Prénom','')} {row.get('Nom','')} — {row.get('Société','')}"

def _ensure_cols(df: pd.DataFrame, want: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in want:
        if c not in df.columns:
            df[c] = ""
    return df

# =============== UI/UX de la page ===============

st.title("👥 CRM — Grille centrale (Contacts)")

# Afficher (si dispo) le panneau de filtre global côté CRM (utile car chaque page Streamlit est indépendante)
if hasattr(SH, "render_global_filter_panel"):
    try:
        dfs_for_filters = load_all_tables()
        SH.render_global_filter_panel(dfs_for_filters)
    except Exception as e:
        st.sidebar.warning(f"Filtre global indisponible : {e}")

# Auth requise 
user = st.session_state.get("auth_user") or st.session_state.get("user")
st.session_state["user"] = user
if not user: 
    st.info("🔐 Veuillez vous connecter depuis la page principale pour accéder au CRM.")
    st.stop()


user = st.session_state.get("user", {})

# Chargement (cache) de toutes les tables nécessaires à la page
dfs = load_all_tables(use_cache_only=True)
df_contacts     = _ensure_cols(dfs.get("contacts", pd.DataFrame()),      ["ID","Nom","Prénom","Société","Email","Type","Statut","Top20","Date_Creation"])
df_inter        = _ensure_cols(dfs.get("interactions", pd.DataFrame()),  ["ID_Interaction","ID","Date","Canal","Objet","Résumé","Résultat","Prochaine_Action","Relance","Responsable"])
df_events       = _ensure_cols(dfs.get("evenements", pd.DataFrame()),    ["ID_Événement","Nom_Événement","Type","Date","Lieu","Cout_Total"])
df_parts        = _ensure_cols(dfs.get("participations", pd.DataFrame()),["ID_Participation","ID","ID_Événement","Rôle","Feedback","Note"])
df_pay          = _ensure_cols(dfs.get("paiements", pd.DataFrame()),     ["ID_Paiement","ID","ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence"])
df_cert         = _ensure_cols(dfs.get("certifications", pd.DataFrame()),["ID_Certif","ID","Type_Certif","Date_Examen","Résultat","Score","Date_Obtention"])
df_entreprises  = _ensure_cols(dfs.get("entreprises", pd.DataFrame()),   ["ID_Entreprise","Nom_Entreprise"])

# --------- Agrégats “proba/score/totaux” (identique monofichier, condensé) ---------
def aggregates_for_contacts(today=None):
    """
    Agrégats robustes (tolérants aux colonnes manquantes).
    Prérequis: les DataFrames globaux df_contacts, df_inter, df_parts, df_pay, df_cert existent.
    """
    from datetime import date, timedelta
    today = today or date.today()

    # --- Paramètres (avec valeurs par défaut si manquants dans PARAMS) ---
    vip_thr      = float(PARAMS.get("vip_threshold", "500000"))
    w_int        = float(PARAMS.get("score_w_interaction", "1"))
    w_part       = float(PARAMS.get("score_w_participation", "1"))
    w_pay        = float(PARAMS.get("score_w_payment_regle", "2"))
    lookback     = int(PARAMS.get("interactions_lookback_days", "90"))
    hot_int_min  = int(PARAMS.get("rule_hot_interactions_recent_min", "3"))
    hot_part_min = int(PARAMS.get("rule_hot_participations_min", "1"))
    hot_partiel  = str(PARAMS.get("rule_hot_payment_partial_counts_as_hot", "1")).lower() in ("1","true","vrai","yes")

    # --- Aides locales ---
    def _ensure_cols(df: pd.DataFrame, cols: list) -> pd.DataFrame:
        """Ajoute les colonnes manquantes avec valeur vide."""
        for c in cols:
            if c not in df.columns:
                df[c] = ""  # valeur neutre
        return df

    # --- Base Contacts ---
    base = df_contacts.copy()
    if base is None or base.empty or "ID" not in base.columns:
        return pd.DataFrame(columns=["ID", "Interactions", "Interactions_recent", "Dernier_contact",
                                     "Resp_principal", "Participations", "A_animé_ou_invité",
                                     "CA_total", "CA_réglé", "Impayé", "Paiements_regles_n",
                                     "A_certification", "Score_composite", "Tags", "Proba_conversion"])
    base["ID"] = base["ID"].astype(str).str.strip()

    # ---------- Interactions ----------
    if df_inter is not None and not df_inter.empty:
        inter_df = df_inter.copy()
        inter_df = _ensure_cols(inter_df, ["ID_Interaction", "ID", "Date", "Responsable"])
        inter_df["ID"] = inter_df["ID"].astype(str).str.strip()
        inter_df["_d"] = pd.to_datetime(inter_df["Date"], errors="coerce")

        inter_count   = inter_df.groupby("ID")["ID_Interaction"].count()
        last_contact  = inter_df.groupby("ID")["_d"].max()
        recent_cut_ts = pd.Timestamp(today - timedelta(days=lookback))
        recent_inter  = inter_df.loc[inter_df["_d"] >= recent_cut_ts].groupby("ID")["ID_Interaction"].count()

        tmp = inter_df.groupby(["ID","Responsable"])["ID_Interaction"].count().reset_index()
        if not tmp.empty:
            idx = tmp.groupby("ID")["ID_Interaction"].idxmax()
            resp_max = tmp.loc[idx].set_index("ID")["Responsable"]
        else:
            resp_max = pd.Series(dtype=str)
    else:
        inter_count  = pd.Series(dtype=int)
        last_contact = pd.Series(dtype="datetime64[ns]")
        recent_inter = pd.Series(dtype=int)
        resp_max     = pd.Series(dtype=str)

    # ---------- Participations ----------
    if df_parts is not None and not df_parts.empty:
        parts_df = df_parts.copy()
        parts_df = _ensure_cols(parts_df, ["ID_Participation","ID","Rôle"])
        parts_df["ID"] = parts_df["ID"].astype(str).str.strip()
        parts_count = parts_df.groupby("ID")["ID_Participation"].count()
        has_anim    = parts_df.assign(_anim=parts_df["Rôle"].isin(["Animateur","Invité"])) \
                              .groupby("ID")["_anim"].any()
    else:
        parts_count = pd.Series(dtype=int)
        has_anim    = pd.Series(dtype=bool)

    # ---------- Paiements ----------
    if df_pay is not None and not df_pay.empty:
        pay = df_pay.copy()
        pay = _ensure_cols(pay, ["ID","Montant","Statut"])
        pay["ID"] = pay["ID"].astype(str).str.strip()
        pay["Montant"] = pd.to_numeric(pay["Montant"], errors="coerce").fillna(0.0)
        total_pay     = pay.groupby("ID")["Montant"].sum()
        # Si la colonne Statut n'existe pas / est vide, on considère tout comme non réglé par défaut
        if "Statut" in pay.columns and pay["Statut"].notna().any():
            pay_regle     = pay[pay["Statut"]=="Réglé"].groupby("ID")["Montant"].sum()
            pay_impaye    = pay[pay["Statut"]!="Réglé"].groupby("ID")["Montant"].sum()
            pay_reg_count = pay[pay["Statut"]=="Réglé"].groupby("ID")["Montant"].count()
        else:
            pay_regle     = pd.Series(dtype=float)  # aucun réglé
            pay_impaye    = total_pay               # tout impayé
            pay_reg_count = pd.Series(dtype=int)
    else:
        total_pay     = pd.Series(dtype=float)
        pay_regle     = pd.Series(dtype=float)
        pay_impaye    = pd.Series(dtype=float)
        pay_reg_count = pd.Series(dtype=int)

    # ---------- Certifications ----------
    if df_cert is not None and not df_cert.empty:
        cert_df = df_cert.copy()
        cert_df = _ensure_cols(cert_df, ["ID","ID_Certif","Résultat"])
        cert_df["ID"] = cert_df["ID"].astype(str).str.strip()
        has_cert = cert_df[cert_df["Résultat"]=="Réussi"].groupby("ID")["ID_Certif"].count() > 0
    else:
        has_cert = pd.Series(dtype=bool)

    # ---------- Assemblage ----------
    ag = pd.DataFrame(index=base["ID"])
    ag["Interactions"]        = ag.index.map(inter_count).fillna(0).astype(int)
    ag["Interactions_recent"] = ag.index.map(recent_inter).fillna(0).astype(int)

    # ✅ FIX: conversion via Series pour utiliser .dt
    lc = ag.index.to_series().map(last_contact)
    lc = pd.to_datetime(lc, errors="coerce")
    ag["Dernier_contact"]     = lc.dt.date

    ag["Resp_principal"]      = ag.index.map(resp_max).fillna("")
    ag["Participations"]      = ag.index.map(parts_count).fillna(0).astype(int)
    ag["A_animé_ou_invité"]   = ag.index.map(has_anim).fillna(False)
    ag["CA_total"]            = ag.index.map(total_pay).fillna(0.0)
    ag["CA_réglé"]            = ag.index.map(pay_regle).fillna(0.0)
    ag["Impayé"]              = ag.index.map(pay_impaye).fillna(0.0)
    ag["Paiements_regles_n"]  = ag.index.map(pay_reg_count).fillna(0).astype(int)
    ag["A_certification"]     = ag.index.map(has_cert).fillna(False)

    ag["Score_composite"] = (w_int * ag["Interactions"]
                             + w_part * ag["Participations"]
                             + w_pay  * ag["Paiements_regles_n"]).round(2)

    # ---------- Tags ----------
    if "Top20" in base.columns:
        top20_ids = set(base.loc[base["Top20"]==True, "ID"])
    else:
        top20_ids = set()
    type_series = base["Type"].astype(str).str.strip() if "Type" in base.columns else pd.Series("", index=base.index)

    def make_tags(row):
        tags = []
        if row.name in top20_ids:
            tags.append("Prospect Top-20")
        # régulier non converti: beaucoup de participations mais CA réglé nul
        if row["Participations"] >= 3 and row["CA_réglé"] <= 0 and type_series.reindex([row.name]).eq("Prospect").any():
            tags.append("Régulier-non-converti")
        if row["A_animé_ou_invité"] or row["Participations"] >= 4:
            tags.append("Futur formateur")
        if row["A_certification"]:
            tags.append("Ambassadeur (certifié)")
        if row["CA_réglé"] >= vip_thr:
            tags.append("VIP (CA élevé)")
        return ", ".join(tags)

    ag["Tags"] = ag.apply(make_tags, axis=1)

    # ---------- Probabilité de conversion ----------
    membres_ids = set(base.loc[type_series=="Membre","ID"]) if not base.empty else set()

    def proba(row):
        if row.name in membres_ids:
            return "Converti"
        chaud = (row["Interactions_recent"] >= hot_int_min and row["Participations"] >= hot_part_min)
        if hot_partiel and row["Impayé"] > 0 and row["CA_réglé"] == 0:
            chaud = True
        tiede = (row["Interactions_recent"] >= 1 or row["Participations"] >= 1)
        if chaud: return "Chaud"
        if tiede: return "Tiède"
        return "Froid"

    ag["Proba_conversion"] = ag.apply(proba, axis=1)

    return ag.reset_index(names="ID")



# --------- Filtres grille (locaux) ----------
colf1, colf2, colf3, colf4 = st.columns([2,1,1,1])
q            = colf1.text_input("🔎 Recherche (nom, société, email)…", "")
page_size    = colf2.selectbox("Taille de page", [20,50,100,200], index=0)
type_filtre  = colf3.selectbox("Type", ["Tous"] + list(SET.get("types_contact", [])))
top20_only   = colf4.checkbox("Top-20 uniquement", value=False)

# — fusion des agrégats au niveau contact
ag = aggregates_for_contacts()
dfc = df_contacts.copy()
if not dfc.empty:
    dfc = dfc.merge(ag, on="ID", how="left")

# — application filtre global (si défini via _shared)
try:
    dfc = apply_global_filters(dfc, "contacts")
except Exception:
    pass

# — filtres locaux
if q:
    qs = q.lower()
    dfc = dfc[dfc.apply(lambda r: any(qs in str(r.get(k,"")).lower() for k in ("Nom","Prénom","Société","Email")), axis=1)]
if type_filtre and type_filtre != "Tous":
    dfc = dfc[dfc["Type"] == type_filtre]
if top20_only and "Top20" in dfc.columns:
    dfc = dfc[dfc["Top20"].astype(str).str.lower().isin(["true","1","yes"])]

# — colonnes par défaut + paramétrables
def _parse_cols(s, defaults):
    cols = [c.strip() for c in str(s or "").split(",") if c.strip()]
    return [c for c in cols if c in dfc.columns] or defaults

defaults = ["ID","Nom","Prénom","Société","Type","Statut","Email",
            "Interactions","Participations","CA_réglé","Impayé","Resp_principal","A_animé_ou_invité",
            "Score_composite","Proba_conversion","Tags"]
defaults += [c for c in AUDIT_COLS if c in dfc.columns]

table_cols = _parse_cols(PARAMS.get("grid_crm_columns",""), defaults)

# — status bar agrégée (toujours visible)
_statusbar(dfc[table_cols], "Contacts")

# — Selecteur maître (ID — Nom Prénom — Entreprise)
sel_options = dfc.apply(_contact_display_label, axis=1).tolist() if not dfc.empty else []
id_map = dict(zip(sel_options, dfc["ID"])) if not dfc.empty else {}
sel_label = st.selectbox("Contact sélectionné (sélecteur maître)", [""] + sel_options, index=0, key="select_contact_label_crm")
if sel_label:
    st.session_state["selected_contact_id"] = id_map.get(sel_label)

# — Grille paginée + filtres avancés
proba_style = JsCode("""
  function(params) {
    const v = params.value;
    let color = null;
    if (v === 'Chaud') color = '#10B981';
    else if (v === 'Tiède') color = '#F59E0B';
    else if (v === 'Froid') color = '#EF4444';
    else if (v === 'Converti') color = '#6366F1';
    if (color){
      return { color: 'white', 'font-weight':'600', 'text-align':'center', 'border-radius':'12px', 'background-color': color };
    }
    return {};
  }
""") if HAS_AGGRID else None

style_map = {"Proba_conversion": proba_style} if proba_style else None
grid = _aggrid(dfc[table_cols], page_size=page_size, key="crm_grid", side_bar=True, single_select=True, style_cols=style_map)

selected_rows = grid.get("selected_rows", []) 
if grid and len(selected_rows) > 0 and selected_rows:
    row0 = grid["selected_rows"][0]
    if "ID" in row0:
        st.session_state["selected_contact_id"] = row0["ID"]

st.markdown("---")
cL, cR = st.columns([1,2])

# =============== Colonne gauche : FICHE CONTACT (CRUD) ===============
with cL:
    st.subheader("Fiche Contact")

    sel_id = st.session_state.get("selected_contact_id", None)
    if sel_id and not dfc.empty and (dfc["ID"] == sel_id).any():
        d = dfc[dfc["ID"] == sel_id].iloc[0].to_dict()

        col_top = st.columns(2)
        if col_top[0].button("➕ Nouveau contact", use_container_width=True):
            st.session_state["selected_contact_id"] = None
            st.rerun()
        if col_top[1].button("🧬 Dupliquer ce contact", use_container_width=True, disabled=not bool(sel_id)):
            src = df_contacts[df_contacts["ID"] == sel_id]
            if not src.empty:
                clone = src.iloc[0].to_dict()
                new_id = generate_id("CNT", df_contacts, "ID")
                clone["ID"] = new_id
                clone = _stamp_create(clone, user)
                df_new = pd.concat([df_contacts, pd.DataFrame([clone])], ignore_index=True)
                if save_df_target:
                    save_df_target("contacts", df_new, SH.PATHS if hasattr(SH, "PATHS") else None, WS_FUNC)
                st.session_state["selected_contact_id"] = new_id
                st.success(f"Contact dupliqué sous l'ID {new_id}")
                st.rerun()

        with st.form("edit_contact_form", clear_on_submit=False):
            st.text_input("ID", value=d.get("ID",""), disabled=True)

            a1, a2 = st.columns(2)
            nom     = a1.text_input("Nom *", d.get("Nom",""))
            prenom  = a2.text_input("Prénom", d.get("Prénom",""))

            b1, b2 = st.columns(2)
            genre  = b1.selectbox("Genre", SET.get("genres",["Homme","Femme","Autre"]),
                                  index=(SET.get("genres",[]).index(d.get("Genre","Homme")) if d.get("Genre","Homme") in SET.get("genres",[]) else 0))
            titre  = b2.text_input("Titre / Position", d.get("Titre",""))

            c1, c2 = st.columns(2)
            # Entreprise (dropdown depuis la table entreprises)
            entreprises_opts = [""] + SH.get_column_as_list(df_entreprises, "Nom_Entreprise") if hasattr(SH,"get_column_as_list") else [""] + sorted(df_entreprises["Nom_Entreprise"].dropna().astype(str).unique().tolist())
            societe = c1.selectbox("Société", entreprises_opts,
                                   index=(entreprises_opts.index(d.get("Société","")) if d.get("Société","") in entreprises_opts else 0))
            secteur = c2.selectbox("Secteur", SET.get("secteurs",["Autre"]),
                                   index=(SET.get("secteurs",[]).index(d.get("Secteur","Autre")) if d.get("Secteur","Autre") in SET.get("secteurs",[]) else len(SET.get("secteurs",["Autre"]))-1))

            d1, d2, d3 = st.columns(3)
            email    = d1.text_input("Email", d.get("Email",""))
            tel      = d2.text_input("Téléphone", d.get("Téléphone",""))
            linkedin = d3.text_input("LinkedIn", d.get("LinkedIn",""))

            e1, e2, e3 = st.columns(3)
            villes = SET.get("villes",["Autres"])
            payses = SET.get("pays",["Cameroun"])
            typesc = SET.get("types_contact",["Prospect","Membre"])
            ville   = e1.selectbox("Ville", villes, index=(villes.index(d.get("Ville","Autres")) if d.get("Ville","Autres") in villes else len(villes)-1))
            pays    = e2.selectbox("Pays", payses, index=(payses.index(d.get("Pays","Cameroun")) if d.get("Pays","Cameroun") in payses else 0))
            typec   = e3.selectbox("Type", typesc, index=(typesc.index(d.get("Type","Prospect")) if d.get("Type","Prospect") in typesc else 0))

            f1, f2, f3 = st.columns(3)
            sources = SET.get("sources",["LinkedIn","Recommandation","Autre"])
            statuts = SET.get("statuts_engagement",["Actif","Inactif"])
            source  = f1.selectbox("Source", sources, index=(sources.index(d.get("Source","LinkedIn")) if d.get("Source","LinkedIn") in sources else 0))
            statut  = f2.selectbox("Statut", statuts, index=(statuts.index(d.get("Statut","Actif")) if d.get("Statut","Actif") in statuts else 0))
            score   = f3.number_input("Score IIBA", value=float(d.get("Score_Engagement") or 0), step=1.0)

            dc = st.date_input("Date de création", value=parse_date(d.get("Date_Creation")) or date.today())
            notes = st.text_area("Notes", d.get("Notes",""))
            top20 = st.checkbox("Top-20 entreprise", value=str(d.get("Top20","")).lower() in ("1","true","yes"))

            ok_update = st.form_submit_button("💾 Enregistrer le contact")

            if ok_update:
                if not str(nom).strip():
                    st.error("❌ Le nom du contact est obligatoire.")
                    st.stop()
                if not email_ok(email):
                    st.error("Email invalide.")
                    st.stop()
                if not phone_ok(tel):
                    st.error("Téléphone invalide.")
                    st.stop()

                # Remonte au DF source des contacts (pas dfc)
                dfc_idx = df_contacts.index[df_contacts["ID"] == sel_id]
                if len(dfc_idx) == 0:
                    st.error("Contact introuvable (rafraîchissez).")
                    st.stop()
                i = dfc_idx[0]
                new_row = {
                    "ID": sel_id, "Nom": nom, "Prénom": prenom, "Genre": genre, "Titre": titre,
                    "Société": societe, "Secteur": secteur, "Email": email, "Téléphone": tel,
                    "LinkedIn": linkedin, "Ville": ville, "Pays": pays, "Type": typec,
                    "Source": source, "Statut": statut, "Score_Engagement": int(score),
                    "Date_Creation": dc.isoformat(), "Notes": notes, "Top20": top20
                }
                existing = df_contacts.loc[i].to_dict()
                existing.update(new_row)
                existing = _stamp_update(existing, user)
                df_contacts.loc[i] = existing

                if save_df_target:
                    try:
                        save_df_target("contacts", df_contacts, getattr(SH, "PATHS", None), WS_FUNC)
                        st.cache_data.clear()  # force une relecture au prochain run
                    except Exception as e:
                        st.error(f"Échec sauvegarde (contacts) : {e}")
                        st.stop()

                st.success("Contact mis à jour.")
                st.rerun()

    else:
        st.info("Sélectionnez un contact via le sélecteur maître ou la grille ci-dessous.")
        with st.expander("➕ Créer un nouveau contact"):
            with st.form("create_contact_form"):
                a1, a2 = st.columns(2)
                nom_new    = a1.text_input("Nom *", "")
                prenom_new = a2.text_input("Prénom", "")

                b1, b2 = st.columns(2)
                genre_new = b1.selectbox("Genre", SET.get("genres",["Homme","Femme","Autre"]), index=0)
                titre_new = b2.text_input("Titre / Position", "")

                c1, c2 = st.columns(2)
                entreprises_opts = [""] + (SH.get_column_as_list(df_entreprises, "Nom_Entreprise") if hasattr(SH,"get_column_as_list") else sorted(df_entreprises["Nom_Entreprise"].dropna().astype(str).unique().tolist()))
                societe_new = c1.selectbox("Société", entreprises_opts, index=0)
                secteur_new = c2.selectbox("Secteur", SET.get("secteurs",["Autre"]), index=len(SET.get("secteurs",["Autre"]))-1)

                d1, d2, d3 = st.columns(3)
                email_new    = d1.text_input("Email", "")
                tel_new      = d2.text_input("Téléphone", "")
                linkedin_new = d3.text_input("LinkedIn", "")

                e1, e2, e3 = st.columns(3)
                villes = SET.get("villes",["Autres"])
                payses = SET.get("pays",["Cameroun"])
                typesc = SET.get("types_contact",["Prospect","Membre"])
                ville_new = e1.selectbox("Ville", villes, index=len(villes)-1)
                pays_new  = e2.selectbox("Pays",  payses, index=0)
                typec_new = e3.selectbox("Type",  typesc, index=0)

                f1, f2, f3 = st.columns(3)
                sources = SET.get("sources",["LinkedIn","Recommandation","Autre"])
                statuts = SET.get("statuts_engagement",["Actif","Inactif"])
                source_new = f1.selectbox("Source", sources, index=0)
                statut_new = f2.selectbox("Statut", statuts, index=0)
                score_new  = f3.number_input("Score IIBA", value=0.0, step=1.0)

                dc_new  = st.date_input("Date de création", value=date.today())
                notes_new = st.text_area("Notes", "")
                top20_new = st.checkbox("Top-20 entreprise", value=False)

                ok_new = st.form_submit_button("💾 Créer le contact")
                if ok_new:
                    if not str(nom_new).strip():
                        st.error("❌ Le nom du contact est obligatoire.")
                        st.stop()
                    if not email_ok(email_new):
                        st.error("Email invalide.")
                        st.stop()
                    if not phone_ok(tel_new):
                        st.error("Téléphone invalide.")
                        st.stop()

                    new_id = generate_id("CNT", df_contacts, "ID")
                    row = {
                        "ID": new_id, "Nom": nom_new, "Prénom": prenom_new, "Genre": genre_new, "Titre": titre_new,
                        "Société": societe_new, "Secteur": secteur_new, "Email": email_new, "Téléphone": tel_new,
                        "LinkedIn": linkedin_new, "Ville": ville_new, "Pays": pays_new, "Type": typec_new,
                        "Source": source_new, "Statut": statut_new, "Score_Engagement": int(score_new),
                        "Date_Creation": dc_new.isoformat(), "Notes": notes_new, "Top20": top20_new
                    }
                    row = _stamp_create(row, user)
                    df_new = pd.concat([df_contacts, pd.DataFrame([row])], ignore_index=True)
                    if save_df_target:
                        try:
                            save_df_target("contacts", df_new, getattr(SH, "PATHS", None), WS_FUNC)
                            st.cache_data.clear()  # force une relecture au prochain run
                        except Exception as e:
                            st.error(f"Échec sauvegarde (contacts) : {e}")
                            st.stop()
                    st.session_state["selected_contact_id"] = new_id
                    st.success(f"Contact créé ({new_id}).")
                    st.rerun()

# =============== Colonne droite : ACTIONS LIÉES (CRUD) ===============
with cR:
    st.subheader("Actions liées au contact sélectionné")
    sel_id = st.session_state.get("selected_contact_id")

    tabs = st.tabs(["➕ Interaction","➕ Participation","➕ Paiement","➕ Certification","📑 Historique & sous-tables"])

    # --- INTERACTION ---
    with tabs[0]:
        if not sel_id:
            st.info("Sélectionnez d’abord un contact.")
        else:
            with st.form("form_add_inter"):
                a1, a2, a3 = st.columns(3)
                dti   = a1.date_input("Date", value=date.today())
                canal = a2.selectbox("Canal", SET.get("canaux",["Appel","Email","WhatsApp"]))
                resp  = a3.text_input("Responsable", value=user.get("UserID","IIBA"))

                obj = st.text_input("Objet")
                resu = st.selectbox("Résultat", SET.get("resultats_inter",["Positif","Négatif","À suivre","Sans suite"]))
                resume = st.text_area("Résumé")
                add_rel = st.checkbox("Planifier une relance ?")
                rel = st.date_input("Relance", value=date.today()) if add_rel else None

                ok = st.form_submit_button("💾 Enregistrer l'interaction")
                if ok:
                    nid = generate_id("INT", df_inter, "ID_Interaction")
                    row = {
                        "ID_Interaction": nid, "ID": sel_id, "Date": dti.isoformat(), "Canal": canal, "Objet": obj,
                        "Résumé": resume, "Résultat": resu, "Prochaine_Action": "", "Relance": rel.isoformat() if rel else "",
                        "Responsable": resp
                    }
                    row = _stamp_create(row, user)
                    df_new = pd.concat([df_inter, pd.DataFrame([row])], ignore_index=True)
                    if save_df_target:
                        try:
                            save_df_target("inter", df_new, getattr(SH, "PATHS", None), WS_FUNC)
                            st.cache_data.clear()  # force une relecture au prochain run
                        except Exception as e:
                            st.error(f"Échec sauvegarde (interactions) : {e}")
                            st.stop()
                    st.success(f"Interaction enregistrée ({nid}).")
                    st.rerun()

    # --- PARTICIPATION ---
    with tabs[1]:
        if not sel_id:
            st.info("Sélectionnez d’abord un contact.")
        else:
            if df_events.empty:
                st.warning("Créez d’abord un événement dans la page Événements.")
            else:
                with st.form("form_add_part"):
                    e1, e2 = st.columns(2)
                    ide  = e1.selectbox("Événement", df_events["ID_Événement"].tolist())
                    role = e2.selectbox("Rôle", ["Participant","Animateur","Invité"])
                    f1, f2 = st.columns(2)
                    fb   = f1.selectbox("Feedback", ["Très satisfait","Satisfait","Moyen","Insatisfait"])
                    note = f2.number_input("Note (1-5)", min_value=1, max_value=5, value=5)
                    okp = st.form_submit_button("💾 Enregistrer la participation")
                    if okp:
                        nid = generate_id("PAR", df_parts, "ID_Participation")
                        row = {"ID_Participation":nid,"ID":sel_id,"ID_Événement":ide,"Rôle":role,
                               "Feedback":fb,"Note":str(note)}
                        row = _stamp_create(row, user)
                        df_new = pd.concat([df_parts, pd.DataFrame([row])], ignore_index=True)
                        if save_df_target:
                            try:
                                save_df_target("parts", df_new, getattr(SH, "PATHS", None), WS_FUNC)
                            except Exception as e:
                                st.error(f"Échec sauvegarde (participations) : {e}")
                                st.stop()
                        st.success(f"Participation ajoutée ({nid}).")
                        st.rerun()

    # --- PAIEMENT ---
    with tabs[2]:
        if not sel_id:
            st.info("Sélectionnez d’abord un contact.")
        else:
            if df_events.empty:
                st.warning("Créez d’abord un événement.")
            else:
                with st.form("form_add_pay"):
                    p1, p2 = st.columns(2)
                    ide = p1.selectbox("Événement", df_events["ID_Événement"].tolist())
                    dtp = p2.date_input("Date paiement", value=date.today())
                    p3, p4, p5 = st.columns(3)
                    montant = p3.number_input("Montant (FCFA)", min_value=0, step=1000)
                    moyen   = p4.selectbox("Moyen", SET.get("moyens_paiement",["Mobile Money","Virement","CB","Cash"]))
                    statut  = p5.selectbox("Statut", SET.get("statuts_paiement",["Réglé","Partiel","Non payé"]))
                    ref = st.text_input("Référence")
                    okpay = st.form_submit_button("💾 Enregistrer le paiement")
                    if okpay:
                        nid = generate_id("PAY", df_pay, "ID_Paiement")
                        row = {"ID_Paiement":nid,"ID":sel_id,"ID_Événement":ide,"Date_Paiement":dtp.isoformat(),
                               "Montant":str(montant),"Moyen":moyen,"Statut":statut,"Référence":ref}
                        row = _stamp_create(row, user)
                        df_new = pd.concat([df_pay, pd.DataFrame([row])], ignore_index=True)
                        if save_df_target:
                            try:
                                save_df_target("pay", df_new, getattr(SH, "PATHS", None), WS_FUNC)
                                st.cache_data.clear()  # force une relecture au prochain run
                            except Exception as e:
                                st.error(f"Échec sauvegarde (paiements) : {e}")
                                st.stop()
                        st.success(f"Paiement enregistré ({nid}).")
                        st.rerun()

    # --- CERTIFICATION ---
    with tabs[3]:
        if not sel_id:
            st.info("Sélectionnez d’abord un contact.")
        else:
            with st.form("form_add_cert"):
                c1, c2, c3 = st.columns(3)
                tc  = c1.selectbox("Type Certification", SET.get("types_certif",["ECBA","CCBA","CBAP","PBA"]))
                dte = c2.date_input("Date Examen", value=date.today())
                res = c3.selectbox("Résultat", ["Réussi","Échoué","En cours","Reporté"])
                s1, s2 = st.columns(2)
                sc  = s1.number_input("Score", min_value=0, max_value=100, value=0)
                has_dto = s2.checkbox("Renseigner une date d'obtention ?")
                dto = st.date_input("Date Obtention", value=date.today()) if has_dto else None

                okc = st.form_submit_button("💾 Enregistrer la certification")
                if okc:
                    nid = generate_id("CER", df_cert, "ID_Certif")
                    row = {"ID_Certif":nid,"ID":sel_id,"Type_Certif":tc,"Date_Examen":dte.isoformat(),"Résultat":res,
                           "Score":str(sc),"Date_Obtention":(dto.isoformat() if dto else "")}
                    row = _stamp_create(row, user)
                    df_new = pd.concat([df_cert, pd.DataFrame([row])], ignore_index=True)
                    if save_df_target:
                        try:
                            save_df_target("cert", df_new, getattr(SH, "PATHS", None), WS_FUNC)
                            st.cache_data.clear()  # force une relecture au prochain run
                        except Exception as e:
                            st.error(f"Échec sauvegarde (certifications) : {e}")
                            st.stop()
                    st.success(f"Certification enregistrée ({nid}).")
                    st.rerun()

    # --- HISTORIQUE & sous-tables (grilles paginées + filtres + status bar) ---
    with tabs[4]:
        if not sel_id:
            st.info("Sélectionnez d’abord un contact.")
        else:
            st.markdown("### 🧾 Historique du contact sélectionné")

            # Interactions
            sub_inter = df_inter[df_inter["ID"] == sel_id].copy()
            st.caption(f"Interactions ({len(sub_inter)})")
            _statusbar(_ensure_cols(sub_inter, AUDIT_COLS), "Interactions")
            _aggrid(sub_inter, page_size=20, key=f"grid_inter_{sel_id}")

            # Participations
            sub_parts = df_parts[df_parts["ID"] == sel_id].copy()
            st.caption(f"Participations ({len(sub_parts)})")
            _statusbar(_ensure_cols(sub_parts, AUDIT_COLS), "Participations")
            _aggrid(sub_parts, page_size=20, key=f"grid_parts_{sel_id}")

            # Paiements
            sub_pay = df_pay[df_pay["ID"] == sel_id].copy()
            st.caption(f"Paiements ({len(sub_pay)})")
            _statusbar(_ensure_cols(sub_pay, AUDIT_COLS), "Paiements")
            _aggrid(sub_pay, page_size=20, key=f"grid_pay_{sel_id}")

            # Certifications
            sub_cert = df_cert[df_cert["ID"] == sel_id].copy()
            st.caption(f"Certifications ({len(sub_cert)})")
            _statusbar(_ensure_cols(sub_cert, AUDIT_COLS), "Certifications")
            _aggrid(sub_cert, page_size=20, key=f"grid_cert_{sel_id}")
