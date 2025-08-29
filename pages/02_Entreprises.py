# pages/02_Entreprises.py
from __future__ import annotations
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from _shared import (
    load_all_tables, generate_id, to_int_safe, C_COLS, ENT_COLS, AUDIT_COLS
)

st.set_page_config(page_title="Entreprises", page_icon="🏢", layout="wide")
dfs = load_all_tables()
df_contacts = dfs["contacts"]; df_ent = dfs["entreprises"]
PATHS = dfs["PATHS"]; WS_FUNC = dfs["WS_FUNC"]; SET = dfs["SET"]

st.title("🏢 Entreprises")

# Sélecteur entreprise (label = Nom)
def _label_ent(r):
    return f"{r['ID_Entreprise']} — {r.get('Nom_Entreprise','')}"
options = [] if df_ent.empty else df_ent.apply(_label_ent, axis=1).tolist()
id_map = {} if df_ent.empty else dict(zip(options, df_ent["ID_Entreprise"]))

sel_label = st.selectbox("Entreprise sélectionnée", [""] + options, index=0)
sel_eid = id_map.get(sel_label, "") if sel_label else ""

col_left, col_right = st.columns([2,1])

with col_left:
    st.subheader("🗂️ Grille des entreprises")
    show_cols = ["ID_Entreprise","Nom_Entreprise","Secteur","Pays","Ville","Statut_Partenariat","CA_Annuel","Nb_Employés"]
    for c in show_cols:
        if c not in df_ent.columns: df_ent[c] = ""
    gb = GridOptionsBuilder.from_dataframe(df_ent[show_cols])
    gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
    gb.configure_default_column(filter=True, sortable=True, resizable=True)
    grid = AgGrid(df_ent[show_cols], gridOptions=gb.build(), update_mode=GridUpdateMode.NO_UPDATE, height=360, theme="streamlit")

with col_right:
    st.subheader("🎯 Statistiques rapides")
    total_entreprises = len(df_ent)
    partenaires_actifs = len(df_ent[df_ent.get("Statut_Partenariat","").isin(["Partenaire", "Client", "Partenaire Stratégique"])])
    prospects = len(df_ent[df_ent.get("Statut_Partenariat","") == "Prospect"])
    ca_total_ent = pd.to_numeric(df_ent.get("CA_Annuel", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Total", total_entreprises)
    c2.metric("Actifs", partenaires_actifs)
    c3.metric("Prospects", prospects)
    c4.metric("CA Cumulé", f"{ca_total_ent/1e9:.1f}B FCFA")

st.markdown("---")
st.subheader("📝 Fiche entreprise")

row_init = {c:"" for c in ENT_COLS}
if sel_eid:
    src = df_ent[df_ent["ID_Entreprise"] == sel_eid]
    if not src.empty:
        row_init.update(src.iloc[0].to_dict())

with st.form("ent_form"):
    col1, col2 = st.columns(2)
    with col1:
        nom = st.text_input("Nom_Entreprise", value=row_init.get("Nom_Entreprise",""))
        secteur = st.selectbox("Secteur", dfs["SET"]["secteurs"], index=0 if row_init.get("Secteur","") not in dfs["SET"]["secteurs"] else dfs["SET"]["secteurs"].index(row_init.get("Secteur","")))
        pays = st.selectbox("Pays", dfs["SET"]["pays"])
        ville = st.selectbox("Ville", dfs["SET"]["villes"])
        statutp = st.selectbox("Statut_Partenariat", ["Prospect","Partenaire","Client","Partenaire Stratégique","Inactif"], index=0 if row_init.get("Statut_Partenariat","Prospect")=="" else ["Prospect","Partenaire","Client","Partenaire Stratégique","Inactif"].index(row_init.get("Statut_Partenariat","Prospect")))
    with col2:
        ca = st.number_input("CA_Annuel (FCFA)", min_value=0, step=1000000, value=to_int_safe(row_init.get("CA_Annuel"),0))
        nbe = st.number_input("Nb_Employés", min_value=0, step=1, value=to_int_safe(row_init.get("Nb_Employés"),0))
        notes = st.text_area("Notes", value=row_init.get("Notes",""))
        # Contact principal existant : "ID - Nom Prénom - Entreprise"
        if df_contacts.empty:
            st.info("Aucun contact encore enregistré.")
            cp_label = ""
            cp_map = {}
        else:
            def _lab_c(r):
                return f"{r['ID']} - {r.get('Nom','')} {r.get('Prénom','')} - {r.get('Société','')}"
            opts = df_contacts.apply(_lab_c, axis=1).tolist()
            cp_map = dict(zip(opts, df_contacts["ID"]))
            cp_label = st.selectbox("Contact principal (existant)", [""] + opts, index=0)
    ok = st.form_submit_button("💾 Enregistrer")

if ok:
    from storage_backend import save_df_target
    if not sel_eid:
        new_id = generate_id("ENT", df_ent, "ID_Entreprise")
        row = {
            "ID_Entreprise": new_id, "Nom_Entreprise": nom, "Secteur": secteur, "Pays": pays, "Ville": ville,
            "Contact_Principal_ID": cp_map.get(cp_label,""), "CA_Annuel": int(ca), "Nb_Employés": int(nbe),
            "Statut_Partenariat": statutp, "Notes": notes
        }
        for c in AUDIT_COLS:
            row.setdefault(c,"")
        globals()["df_ent"] = pd.concat([df_ent, pd.DataFrame([row])], ignore_index=True)
        save_df_target("entreprises", df_ent, PATHS, WS_FUNC)
        st.success(f"Entreprise créée ({new_id}).")
    else:
        idx = df_ent.index[df_ent["ID_Entreprise"] == sel_eid]
        if len(idx):
            i = idx[0]
            df_ent.loc[i,"Nom_Entreprise"] = nom
            df_ent.loc[i,"Secteur"] = secteur
            df_ent.loc[i,"Pays"] = pays
            df_ent.loc[i,"Ville"] = ville
            df_ent.loc[i,"Contact_Principal_ID"] = cp_map.get(cp_label,"")
            df_ent.loc[i,"CA_Annuel"] = int(ca)
            df_ent.loc[i,"Nb_Employés"] = int(nbe)
            df_ent.loc[i,"Statut_Partenariat"] = statutp
            df_ent.loc[i,"Notes"] = notes
            save_df_target("entreprises", df_ent, PATHS, WS_FUNC)
            st.success(f"Entreprise mise à jour ({sel_eid}).")

st.markdown("---")
st.subheader("👥 Contacts employés de l'entreprise")
if sel_eid:
    nom_ent = df_ent.loc[df_ent["ID_Entreprise"] == sel_eid, "Nom_Entreprise"].values
    if len(nom_ent):
        comp_name = nom_ent[0]
        df_emp = df_contacts[df_contacts.get("Société","") == comp_name][["ID","Nom","Prénom","Email","Fonction"]]
        st.dataframe(df_emp, use_container_width=True)
    else:
        st.info("Nom entreprise introuvable.")
else:
    st.info("Sélectionnez une entreprise pour voir ses employés.")
