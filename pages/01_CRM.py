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

# Imports
from _shared import (
    PATHS, C_COLS, I_COLS, PART_COLS, PAY_COLS, CERT_COLS, E_COLS,
    load_all_tables, get_global_filters, apply_global_filters,
    generate_id, to_int_safe, PARAMS, get_param_list, make_event_label_map,
    enrich_with_event_cols, atomic_upsert, atomic_append_row
)


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
from collections.abc import Mapping
def map_to_datetime_column(df, index_source, series_or_other, col_name, errors="coerce"):
    """
    Aligne `series_or_other` sur l'index_source, convertit en datetime.date
    et affecte le résultat à df[col_name].

    - index_source : Index Pandas ou itérable de valeurs à mapper
    - series_or_other : Series, dict, callable, DataFrame (1 col) ou valeur simple
    - col_name : nom de la colonne à créer dans df
    - errors : comportement pd.to_datetime en cas d'erreur ('coerce', 'raise', 'ignore')
    """
    # Détermination du mapping sûr selon le type
    if isinstance(series_or_other, pd.Series):
        values = pd.Series(index_source).map(series_or_other.to_dict())
    elif isinstance(series_or_other, Mapping):
        values = pd.Series(index_source).map(series_or_other)
    elif callable(series_or_other):
        values = pd.Series(index_source).map(series_or_other)
    elif isinstance(series_or_other, pd.DataFrame):
        if series_or_other.shape[1] >= 1:
            col = series_or_other.columns[0]
            values = pd.Series(index_source).map(series_or_other[col].to_dict())
        else:
            values = pd.NaT
    else:
        values = pd.NaT

    df[col_name] = pd.to_datetime(values, errors=errors).dt.date
    return df


def aggregates_for_contacts(today=None):
    today = today or date.today()
    vip_thr = float(PARAMS.get("vip_threshold", "500000"))
    w_int   = float(PARAMS.get("score_w_interaction", "1"))
    w_part  = float(PARAMS.get("score_w_participation", "1"))
    w_pay   = float(PARAMS.get("score_w_payment_regle", "2"))
    lookback = int(PARAMS.get("interactions_lookback_days", "90"))
    hot_int_min = int(PARAMS.get("rule_hot_interactions_recent_min", "3"))
    hot_part_min = int(PARAMS.get("rule_hot_participations_min", "1"))
    hot_partiel = PARAMS.get("rule_hot_payment_partial_counts_as_hot", "1") in ("1","true","True")

    inter_count = df_inter.groupby("ID")["ID_Interaction"].count() if not df_inter.empty else pd.Series(dtype=int)
    inter_dates = pd.to_datetime(df_inter["Date"], errors="coerce") if not df_inter.empty else pd.Series(dtype="datetime64[ns]")
    last_contact = df_inter.assign(_d=inter_dates).groupby("ID")["_d"].max() if not df_inter.empty else pd.Series(dtype="datetime64[ns]")
    recent_cut = today - timedelta(days=lookback)
    recent_inter = df_inter.assign(_d=inter_dates).loc[lambda d: d["_d"] >= pd.Timestamp(recent_cut)].groupby("ID")["ID_Interaction"].count() if not df_inter.empty else pd.Series(dtype=int)

    resp_max = pd.Series(dtype=str)
    if not df_inter.empty:
        tmp = df_inter.groupby(["ID","Responsable"])["ID_Interaction"].count().reset_index()
        idx = tmp.groupby("ID")["ID_Interaction"].idxmax()
        resp_max = tmp.loc[idx].set_index("ID")["Responsable"]

    parts_count = df_parts.groupby("ID")["ID_Participation"].count() if not df_parts.empty else pd.Series(dtype=int)
    has_anim = pd.Series(dtype=bool)
    if not df_parts.empty:
        has_anim = df_parts.assign(_anim=df_parts["Rôle"].isin(["Animateur","Invité"])).groupby("ID")["_anim"].any()

    pay_reg_count = pd.Series(dtype=int)
    if not df_pay.empty:
        pay = df_pay.copy()
        pay["Montant"] = pd.to_numeric(pay["Montant"], errors="coerce").fillna(0.0)
        total_pay = pay.groupby("ID")["Montant"].sum()
        pay_regle = pay[pay["Statut"]=="Réglé"].groupby("ID")["Montant"].sum()
        pay_impaye= pay[pay["Statut"]!="Réglé"].groupby("ID")["Montant"].sum()
        pay_reg_count = pay[pay["Statut"]=="Réglé"].groupby("ID")["Montant"].count()
    else:
        total_pay = pd.Series(dtype=float)
        pay_regle = pd.Series(dtype=float)
        pay_impaye= pd.Series(dtype=float)

    has_cert = pd.Series(dtype=bool)
    if not df_cert.empty:
        has_cert = df_cert[df_cert["Résultat"]=="Réussi"].groupby("ID")["ID_Certif"].count() > 0

    base = df_contacts.copy()
    if base.empty or "ID" not in base.columns:
        return pd.DataFrame(columns=["ID"])

    ag = pd.DataFrame(index=base["ID"])
    ag["Interactions"] = ag.index.map(inter_count).fillna(0).astype(int)
    ag["Interactions_recent"] = ag.index.map(recent_inter).fillna(0).astype(int)
    
    # ag["Dernier_contact"] = pd.to_datetime(ag.index.map(last_contact), errors="coerce").dt.date
    # je reprends le DataFrame complet modifié avec ajout de "Dernier_contact"
    ag = map_to_datetime_column(ag, ag.index, last_contact, "Dernier_contact")
    
    ag["Resp_principal"] = ag.index.map(resp_max).fillna("")
    ag["Participations"] = ag.index.map(parts_count).fillna(0).astype(int)
    ag["A_animé_ou_invité"] = ag.index.map(has_anim).fillna(False)
    ag["CA_total"] = ag.index.map(total_pay).fillna(0.0)
    ag["CA_réglé"] = ag.index.map(pay_regle).fillna(0.0)
    ag["Impayé"] = ag.index.map(pay_impaye).fillna(0.0)
    ag["Paiements_regles_n"] = ag.index.map(pay_reg_count).fillna(0).astype(int)
    ag["A_certification"] = ag.index.map(has_cert).fillna(False)
    ag["Score_composite"] = (w_int * ag["Interactions"] + w_part * ag["Participations"] + w_pay * ag["Paiements_regles_n"]).round(2)

    def make_tags(row):
        tags=[]
        # Prospect Top-20 : sur société marquée top-20
        top20_ids = set(base.loc[base.get("Top20", False)==True, "ID"])
        if row.name in top20_ids:
            tags.append("Prospect Top-20")
        # régulier non converti
        if row["Participations"] >= 3 and row.name in set(base[base.get("Type","")== "Prospect"]["ID"]) and row["CA_réglé"] <= 0:
            tags.append("Régulier-non-converti")
        if row["A_animé_ou_invité"] or row["Participations"] >= 4:
            tags.append("Futur formateur")
        if row["A_certification"]:
            tags.append("Ambassadeur (certifié)")
        if row["CA_réglé"] >= float(PARAMS.get("vip_threshold", "500000")):
            tags.append("VIP (CA élevé)")
        return ", ".join(tags)

    ag["Tags"] = ag.apply(make_tags, axis=1)

    def proba(row):
        if row.name in set(base[base.get("Type","")=="Membre"]["ID"]):
            return "Converti"
        chaud = (row["Interactions_recent"] >= hot_int_min and row["Participations"] >= hot_part_min)
        if (PARAMS.get("rule_hot_payment_partial_counts_as_hot","1") in ("1","true","True")) and row["Impayé"]>0 and row["CA_réglé"]==0:
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

# Récupération du DataFrame des lignes sélectionnées
selected_df = grid.selected_rows  # c'est un DataFrame Pandas
if selected_df is not None and not selected_df.empty:
    # On prend la première ligne sélectionnée
    row0 = selected_df.iloc[0]
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
# Dupliquer (new chatgpt)
# if st.button("Dupliquer ce contact") and selected_id:
#     ws = st.session_state.get("WS_FUNC")
#     df_fresh = ensure_df_source("contacts", C_COLS, PATHS, ws)
#     new_id = generate_id("CNT", df_fresh, "ID")
#     # Base = ligne sélectionnée nettoyée
#     base = df_contacts[df_contacts["ID"] == selected_id]
#     payload = base.iloc[0].to_dict() if not base.empty else {}
#     payload["ID"] = new_id
#     payload = {k: payload.get(k,"") for k in C_COLS}
#     df_after, created = atomic_upsert("contacts", C_COLS, "ID", payload,
#                          user_email=st.session_state.get("auth_user",{}).get("email","system"),
#                          ws_func=ws, paths=PATHS)
#     st.success(f"Contact dupliqué ({new_id})")
#     st.session_state["selected_contact_id"] = new_id
#     st.rerun()


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

            if st.button("💾 Enregistrer les modifications (le contact)") and selected_id:
                ws = st.session_state.get("WS_FUNC")
                upd = {
                    "ID": selected_id,
                    "Nom": nom, "Prénom": prenom, "Genre": genre, "Titre": titre,
                    "Société": societe, "Secteur": secteur, "Email": email, "Téléphone": tel,
                    "LinkedIn": linkedin, "Ville": ville, "Pays": pays, "Type": typ, "Source": source,
                    "Statut": statut, "Score_Engagement": score, "Notes": notes, "Top20": "1" if top20 else "",
                }
                df_after, created = atomic_upsert("contacts", C_COLS, "ID", upd,
                                     user_email=st.session_state.get("auth_user",{}).get("email","system"),
                                     ws_func=ws, paths=PATHS)
                st.success(f"Contact mis à jour ({selected_id})")
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

                if st.button("💾 Créer le contact"):
                    ws = st.session_state.get("WS_FUNC")
                    # Relecture fraîche + ID basé sur l'état courant
                    df_fresh = ensure_df_source("contacts", C_COLS, PATHS, ws)
                    new_id = generate_id("CNT", df_fresh, "ID")
                    new_row = {
                        "ID": new_id,
                        "Nom": nom, "Prénom": prenom, "Genre": genre, "Titre": titre,
                        "Société": societe, "Secteur": secteur, "Email": email, "Téléphone": tel,
                        "LinkedIn": linkedin, "Ville": ville, "Pays": pays, "Type": typ, "Source": source,
                        "Statut": statut, "Score_Engagement": score, "Notes": notes, "Top20": "1" if top20 else "",
                        "Date_Creation": datetime.utcnow().strftime("%Y-%m-%d"),
                    }
                    df_after, created = atomic_upsert(
                        "contacts", C_COLS, "ID", new_row,
                        user_email=st.session_state.get("auth_user",{}).get("email","system"),
                        ws_func=ws, paths=PATHS
                    )
                    st.success(f"Contact créé ({new_id})")
                    st.session_state["selected_contact_id"] = new_id
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
                canaux     = get_param_list("interaction_canaux", "Appel,Email,WhatsApp,LinkedIn,Visio,Présentiel")
                resultats  = get_param_list("interaction_resultats", "OK,Relancer,Pas intéressé,NRP")
                responsabs = get_param_list("responsables", "IIBA Cameroun,Admin,Equipe")

                a1, a2, a3 = st.columns(3)
                dti   = a1.date_input("Date", value=date.today())
                canal = a2.selectbox("Canal", options=canaux or ["—"], index=0)
                resp  = a3.selectbox("Responsable", options=responsabs or ["—"], index=0)

                obj = st.text_input("Objet")
                resu = st.selectbox("Résultat", options=resultats or ["—"], index=0)
                resume = st.text_area("Résumé")
                add_rel = st.checkbox("Planifier une relance ?")
                rel = st.date_input("Relance", value=date.today()) if add_rel else None

                if st.button("Enregistrer l’interaction"):
                    ws = st.session_state.get("WS_FUNC")
                    df_fresh_int = ensure_df_source("interactions", I_COLS, PATHS, ws)
                    new_id = generate_id("INT", df_fresh_int, "ID_Interaction")
                    row = {
                        "ID_Interaction": new_id,
                        "ID": selected_id,
                        "Date": str(date_interaction) if date_interaction else "",
                        "Canal": canal_sel, "Objet": objet, "Résumé": resume, "Résultat": result_sel,
                        "Prochaine_Action": prochaine_action, "Relance": relance, "Responsable": resp_sel,
                    }
                    atomic_append_row("interactions", I_COLS, row,
                        user_email=st.session_state.get("auth_user",{}).get("email","system"),
                        ws_func=ws, paths=PATHS)
                    st.success(f"Interaction enregistrée ({new_id})")
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
                    evt_map = make_event_label_map(df_events)  # {label->ID}
                    evt_labels = sorted(evt_map.keys()) if evt_map else ["—"]
                    label_sel_part = e1.selectbox("Événement", options=evt_labels, index=0, key="evt_part")
                    role = e2.selectbox("Rôle", ["Participant","Animateur","Invité"])
                    f1, f2 = st.columns(2)
                    fb   = f1.selectbox("Feedback", ["Très satisfait","Satisfait","Moyen","Insatisfait"])
                    note = f2.number_input("Note (1-5)", min_value=1, max_value=5, value=5)
                    comment_part = st.text_area("Commentaire (Participation)", key="comment_part")
                    
                    if st.button("💾 Enregistrer la participation"):
                        ws = st.session_state.get("WS_FUNC")
                        df_fresh_part = ensure_df_source("participations", PART_COLS, PATHS, ws)
                        new_id = generate_id("PAR", df_fresh_part, "ID_Participation")
                        row = {
                            "ID_Participation": new_id,
                            "ID": selected_id,
                            "ID_Événement": evt_map.get(label_sel_part, ""),
                            "Rôle": role,
                            "Feedback": fb,
                            "Note": note,
                            "Commentaire": comment_part,
                        }
                        atomic_append_row("participations", PART_COLS, row,
                            user_email=st.session_state.get("auth_user",{}).get("email","system"),
                            ws_func=ws, paths=PATHS)
                        st.success(f"Participation enregistrée ({new_id})")
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
                    moyens  = get_param_list("moyens_paiement", "Cash,Mobile Money,CB,Virement")
                    statuts = get_param_list("statuts_paiement", "Réglé,Partiel,En attente,Annulé")
                    p1, p2 = st.columns(2)
                    evt_map2 = make_event_label_map(df_events)
                    evt_labels2 = sorted(evt_map2.keys()) if evt_map2 else ["—"]
                    label_sel_pay = p1.selectbox("Événement", options=evt_labels2, index=0, key="evt_pay")
                    dtp = p2.date_input("Date paiement", value=date.today())
                    p3, p4, p5 = st.columns(3)
                    montant = p3.number_input("Montant (FCFA)", min_value=0, step=1000)
                    moyen   = p4.selectbox("Moyen", options=moyens or ["—"], index=0)
                    statut  = p5.selectbox("Statut", options=statuts or ["—"], index=0)
                    ref = st.text_input("Référence")
                    comment_pay  = st.text_area("Commentaire (Paiement)", key="comment_pay")
                    if st.button("💾 Enregistrer le paiement"):
                        ws = st.session_state.get("WS_FUNC")
                        df_fresh_pay = ensure_df_source("paiements", PAY_COLS, PATHS, ws)
                        new_id = generate_id("PAY", df_fresh_pay, "ID_Paiement")
                        row = {
                            "ID_Paiement": new_id,
                            "ID": selected_id,
                            "ID_Événement": evt_map2.get(label_sel_pay, ""),
                            "Date_Paiement": str(dtp) if dtp else "",
                            "Montant": str(montant or ""),
                            "Moyen": moyen, "Statut": statut, "Référence": ref,
                            "Commentaire": comment_pay,
                        }
                        atomic_append_row("paiements", PAY_COLS, row,
                            user_email=st.session_state.get("auth_user",{}).get("email","system"),
                            ws_func=ws, paths=PATHS)
                        st.success(f"Paiement enregistré ({new_id})")
                        st.rerun()
                    
                    
    # --- CERTIFICATION ---
    with tabs[3]:
        if not sel_id:
            st.info("Sélectionnez d’abord un contact.")
        else:
            with st.form("form_add_cert"):
                types_cert = get_param_list("types_certif", "ECBA,CCBA,CBAP,AAC,CBDA,CPOA")
                c1, c2, c3 = st.columns(3)
                tc  = c1.selectbox("Type Certification", options=types_cert or ["—"], index=0)
                dte = c2.date_input("Date Examen", value=date.today())
                res = c3.selectbox("Résultat", ["Réussi","Échoué","En cours","Reporté"])
                s1, s2 = st.columns(2)
                sc  = s1.number_input("Score", min_value=0, max_value=100, value=0)
                has_dto = s2.checkbox("Renseigner une date d'obtention ?")
                dto = st.date_input("Date Obtention", value=date.today()) if has_dto else None
                comment_cert = st.text_area("Commentaire (Certification)", key="comment_cert")

                if st.button("💾 Enregistrer la certification"):
                    ws = st.session_state.get("WS_FUNC")
                    df_fresh_cert = ensure_df_source("certifications", CERT_COLS, PATHS, ws)
                    new_id = generate_id("CER", df_fresh_cert, "ID_Certif")
                    row = {
                        "ID_Certif": new_id,
                        "ID": selected_id,
                        "Type_Certif": type_sel,
                        "Date_Examen": str(date_exam) if date_exam else "",
                        "Résultat": result_cert, "Score": score_cert,
                        "Date_Obtention": str(date_obt) if date_obt else "",
                        "Commentaire": comment_cert,   # <- ajouté
                    }
                    atomic_append_row("certifications", CERT_COLS, row,
                        user_email=st.session_state.get("auth_user",{}).get("email","system"),
                        ws_func=ws, paths=PATHS)
                    st.success(f"Certification enregistrée ({new_id})")
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
            
            # Historique enrichi (colonnes événement)
            df_parts_enriched = enrich_with_event_cols(df_parts_contact, df_events, "ID_Événement")
            st.dataframe(
                df_parts_enriched[["ID_Participation","Rôle","Nom_Événement","Type","Lieu","Date","Feedback","Note","Commentaire"]],
                use_container_width=True
            )

            # Paiements
            sub_pay = df_pay[df_pay["ID"] == sel_id].copy()
            st.caption(f"Paiements ({len(sub_pay)})")
            _statusbar(_ensure_cols(sub_pay, AUDIT_COLS), "Paiements")
            _aggrid(sub_pay, page_size=20, key=f"grid_pay_{sel_id}")
            
            # Historique enrichi (colonnes événement)
            df_pay_enriched = enrich_with_event_cols(df_pay_contact, df_events, "ID_Événement")
            st.dataframe(
                df_pay_enriched[["ID_Paiement","Référence","Nom_Événement","Type","Lieu","Date","Date_Paiement","Montant","Moyen","Statut","Commentaire"]],
                use_container_width=True
            )            

            # Certifications
            sub_cert = df_cert[df_cert["ID"] == sel_id].copy()
            st.caption(f"Certifications ({len(sub_cert)})")
            _statusbar(_ensure_cols(sub_cert, AUDIT_COLS), "Certifications")
            _aggrid(sub_cert, page_size=20, key=f"grid_cert_{sel_id}")
