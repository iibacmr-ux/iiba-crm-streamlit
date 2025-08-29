
from __future__ import annotations
import pandas as pd
import streamlit as st
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
    HAS_AG = True
except Exception:
    HAS_AG = False

from _shared import load_all_tables, generate_id, to_int_safe, E_COLS, PART_COLS, OP_COLS, AUDIT_COLS, save_df_target

st.set_page_config(page_title="Événements", page_icon="📅", layout="wide")
dfs = load_all_tables()
df_events = dfs["events"]; df_parts = dfs["parts"]; df_contacts = dfs["contacts"]
df_orgparts = dfs["orgparts"]; df_ent = dfs["entreprises"]
PATHS = dfs["PATHS"]; WS_FUNC = dfs["WS_FUNC"]

st.title("📅 Événements")

# Grille des événements
show_cols = ["ID_Événement","Nom_Événement","Type","Date","Lieu","Capacité","Coût_Total","Statut"]
for c in show_cols:
    if c not in df_events.columns: df_events[c] = ""
if HAS_AG:
    gb = GridOptionsBuilder.from_dataframe(df_events[show_cols])
    gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
    gb.configure_default_column(filter=True, sortable=True, resizable=True)
    gb.configure_grid_options(statusBar={"statusPanels":[
        {"statusPanel":"agTotalRowCountComponent","align":"left"},
        {"statusPanel":"agFilteredRowCountComponent"},
        {"statusPanel":"agSelectedRowCountComponent"},
        {"statusPanel":"agAggregationComponent"}
    ]})
    gb.configure_side_bar()
    gb.configure_selection("single", use_checkbox=True)
    grid_resp = AgGrid(df_events[show_cols], gridOptions=gb.build(), update_mode=GridUpdateMode.SELECTION_CHANGED, height=360, theme="streamlit")
    sel_rows = grid_resp.get("selected_rows", [])
else:
    st.dataframe(df_events[show_cols], use_container_width=True)
    sel_rows = []

sel_evt_id = sel_rows[0]["ID_Événement"] if sel_rows else ""

st.markdown("---")
st.subheader("📝 Créer / Modifier un événement")
row_init = {c:"" for c in E_COLS}
if sel_evt_id:
    src = df_events[df_events["ID_Événement"] == sel_evt_id]
    if not src.empty:
        row_init.update(src.iloc[0].to_dict())

with st.form("evt_form"):
    col1, col2 = st.columns(2)
    with col1:
        nom = st.text_input("Nom_Événement", value=row_init.get("Nom_Événement",""))
        typ = st.selectbox("Type", dfs["SET"]["types_evt"], index=0 if row_init.get("Type","") not in dfs["SET"]["types_evt"] else dfs["SET"]["types_evt"].index(row_init.get("Type","")))
        dte = st.text_input("Date (YYYY-MM-DD)", value=row_init.get("Date",""))
        lieu = st.text_input("Lieu", value=row_init.get("Lieu",""))
        cap = st.number_input("Capacité", min_value=0, step=1, value=to_int_safe(row_init.get("Capacité"),0))
    with col2:
        cs = st.number_input("Coût Salle", min_value=0, step=10000, value=to_int_safe(row_init.get("Cout_Salle"),0))
        cf = st.number_input("Coût Formateur", min_value=0, step=10000, value=to_int_safe(row_init.get("Cout_Formateur"),0))
        cl = st.number_input("Coût Logistique", min_value=0, step=10000, value=to_int_safe(row_init.get("Cout_Logistique"),0))
        cp = st.number_input("Coût Pub", min_value=0, step=10000, value=to_int_safe(row_init.get("Cout_Pub"),0))
        ca = st.number_input("Autres coûts", min_value=0, step=10000, value=to_int_safe(row_init.get("Cout_Autres"),0))
        statut = st.selectbox("Statut", ["Planifié","En cours","Terminé","Annulé"], index=(["Planifié","En cours","Terminé","Annulé"].index(row_init.get("Statut","Planifié")) if row_init.get("Statut","Planifié") in ["Planifié","En cours","Terminé","Annulé"] else 0))
    desc = st.text_area("Description", value=row_init.get("Description",""))
    ok = st.form_submit_button("💾 Enregistrer")

if ok:
    if not sel_evt_id:
        new_id = generate_id("EVT", df_events, "ID_Événement")
        row = {"ID_Événement":new_id,"Nom_Événement":nom,"Type":typ,"Date":dte,"Lieu":lieu,"Capacité":int(cap),
               "Cout_Salle":int(cs),"Cout_Formateur":int(cf),"Cout_Logistique":int(cl),"Cout_Pub":int(cp),"Cout_Autres":int(ca),
               "Coût_Total":"", "Statut":statut,"Description":desc}
        for c in AUDIT_COLS: row.setdefault(c,"")
        df_events = pd.concat([df_events, pd.DataFrame([row])], ignore_index=True)
        save_df_target("events", df_events, PATHS, WS_FUNC)
        st.success(f"Événement créé ({new_id}).")
    else:
        idx = df_events.index[df_events["ID_Événement"] == sel_evt_id]
        if len(idx):
            i = idx[0]
            df_events.loc[i,"Nom_Événement"] = nom
            df_events.loc[i,"Type"] = typ
            df_events.loc[i,"Date"] = dte
            df_events.loc[i,"Lieu"] = lieu
            df_events.loc[i,"Capacité"] = int(cap)
            df_events.loc[i,"Cout_Salle"] = int(cs)
            df_events.loc[i,"Cout_Formateur"] = int(cf)
            df_events.loc[i,"Cout_Logistique"] = int(cl)
            df_events.loc[i,"Cout_Pub"] = int(cp)
            df_events.loc[i,"Cout_Autres"] = int(ca)
            df_events.loc[i,"Statut"] = statut
            df_events.loc[i,"Description"] = desc
            save_df_target("events", df_events, PATHS, WS_FUNC)
            st.success(f"Événement mis à jour ({sel_evt_id}).")

st.markdown("---")
st.subheader("👥 Participants (contacts) & 🏢 Entreprises")

tab_pers, tab_emp, tab_org = st.tabs(["Participants (contacts)", "Entreprises via employés", "Participations officielles (orgparts)"])

with tab_pers:
    if not sel_evt_id:
        st.info("Sélectionnez un événement dans la grille.")
    else:
        parts = df_parts[df_parts["ID_Événement"] == sel_evt_id].copy()
        st.dataframe(parts, use_container_width=True)

with tab_emp:
    if not sel_evt_id:
        st.info("Sélectionnez un événement dans la grille.")
    else:
        # Entreprises via employés (participants)
        parts = df_parts[df_parts["ID_Événement"] == sel_evt_id].copy()
        if parts.empty or df_contacts.empty:
            st.info("Pas de participants ou pas de contacts.")
        else:
            comp_map = df_contacts.set_index("ID")["Société"]
            parts["Entreprise"] = parts["ID"].map(comp_map).fillna("")
            agg = parts.groupby("Entreprise")["ID_Participation"].count().reset_index().rename(columns={"ID_Participation":"Participants"})
            st.dataframe(agg.sort_values("Participants", ascending=False), use_container_width=True)

with tab_org:
    if not sel_evt_id:
        st.info("Sélectionnez un événement dans la grille.")
    else:
        org = df_orgparts[df_orgparts["ID_Événement"] == sel_evt_id].copy()
        if org.empty:
            st.info("Aucune participation officielle enregistrée.")
        else:
            # Join pour afficher Nom_Entreprise
            noms = df_ent.set_index("ID_Entreprise")["Nom_Entreprise"]
            org["Nom_Entreprise"] = org["ID_Entreprise"].map(noms).fillna(org["ID_Entreprise"])
            show = ["ID_OrgPart","ID_Entreprise","Nom_Entreprise","Type_Lien","Nb_Employés","Montant_Sponsor","Notes"]
            st.dataframe(org[show], use_container_width=True)

        st.markdown("—")
        st.subheader("➕ Ajouter une participation officielle")
        with st.form("add_orgpart"):
            if df_ent.empty:
                st.warning("Aucune entreprise définie.")
            lab_ent = [] if df_ent.empty else df_ent.apply(lambda r: f"{r['ID_Entreprise']} — {r.get('Nom_Entreprise','')}", axis=1).tolist()
            ent_map = {} if df_ent.empty else dict(zip(lab_ent, df_ent["ID_Entreprise"]))
            ent_label = st.selectbox("Entreprise", [""] + lab_ent, index=0)
            tlink = st.selectbox("Type de lien", dfs["SET"]["types_org_lien"])
            nb = st.number_input("Nb employés (déclarés)", min_value=0, step=1, value=0)
            sponsor = st.number_input("Montant sponsoring (FCFA)", min_value=0, step=100000, value=0)
            notes = st.text_area("Notes")
            ok7 = st.form_submit_button("💾 Enregistrer")
            if ok7 and ent_label and sel_evt_id:
                from _shared import generate_id
                nid = generate_id("ORG", df_orgparts, "ID_OrgPart")
                row = {"ID_OrgPart":nid,"ID_Entreprise":ent_map[ent_label],"ID_Événement":sel_evt_id,
                       "Type_Lien":tlink,"Nb_Employés":int(nb),"Montant_Sponsor":int(sponsor),"Notes":notes}
                for c in AUDIT_COLS: row.setdefault(c,"")
                df_orgparts = pd.concat([df_orgparts, pd.DataFrame([row])], ignore_index=True)
                save_df_target("orgparts", df_orgparts, PATHS, WS_FUNC)
                st.success("Participation officielle ajoutée.")
