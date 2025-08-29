# pages/02_Entreprises.py
from __future__ import annotations
from datetime import date
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from _shared import (
    load_all_tables, generate_id, to_int_safe, C_COLS, ENT_COLS, AUDIT_COLS
)

st.set_page_config(page_title="Entreprises", page_icon="🏢", layout="wide")
dfs = load_all_tables()
df_contacts = dfs["contacts"]; df_ent = dfs["entreprises"]
df_events = dfs["events"]; df_parts = dfs["parts"]; df_pay = dfs["pay"]; df_cert = dfs["cert"]
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
            # préselection si déjà défini
            _cur = row_init.get("Contact_Principal_ID","")
            _lab_cur = ""
            if _cur:
                r = df_contacts[df_contacts["ID"]==_cur]
                if not r.empty:
                    r=r.iloc[0]
                    _lab_cur = f"{r['ID']} - {r.get('Nom','')} {r.get('Prénom','')} - {r.get('Société','')}"
            cp_label = st.selectbox("Contact principal (existant)", [""] + opts, index=([""]+opts).index(_lab_cur) if _lab_cur in ([""]+opts) else 0)
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

st.markdown("---")
st.subheader("🔗 Assignations rapides à l'entreprise")
if not sel_eid:
    st.info("Sélectionnez d'abord une entreprise.")
else:
    # Déterminer liste de cibles (Contact principal, Un employé, Tous employés)
    employees = pd.DataFrame(columns=["ID","Nom","Prénom","Société"])
    nom_ent = df_ent.loc[df_ent["ID_Entreprise"] == sel_eid, "Nom_Entreprise"]
    if not nom_ent.empty:
        comp_name = nom_ent.iloc[0]
        employees = df_contacts[df_contacts.get("Société","") == comp_name][["ID","Nom","Prénom","Société"]].copy()
    cp_id = row_init.get("Contact_Principal_ID","")
    targets = []
    if cp_id:
        rc = df_contacts[df_contacts["ID"]==cp_id]
        if not rc.empty:
            r=rc.iloc[0]
            targets.append(("Contact principal", cp_id, f"{r['Prénom']} {r['Nom']}"))
    # ajouter option "Un employé (sélection)"
    targets.append(("Un employé (sélection)", "single", ""))
    if not employees.empty:
        targets.append(("Tous les employés", "all", f"{len(employees)}"))

    tab_int, tab_part, tab_pay, tab_cert = st.tabs(["➕ Interactions", "➕ Participations", "➕ Paiements", "➕ Certifications"])

    with tab_int:
        with st.form("ent_add_inter"):
            who = st.selectbox("Cible", [t[0] for t in targets])
            single_id = ""
            if "Un employé" in who and not employees.empty:
                emp_opts = employees.apply(lambda r: f"{r['ID']} — {r['Prénom']} {r['Nom']}", axis=1).tolist()
                emp_map = dict(zip(emp_opts, employees["ID"]))
                _lab = st.selectbox("Employé", emp_opts, index=0)
                single_id = emp_map.get(_lab,"")
            dte = st.date_input("Date", value=date.today())
            canal = st.selectbox("Canal", ["Email","Téléphone","WhatsApp","LinkedIn","F2F","Autre"])
            obj = st.text_input("Objet")
            res = st.selectbox("Résultat", ["OK","À suivre","Sans suite","Refus"])
            notes = st.text_area("Notes")
            ok = st.form_submit_button("💾 Enregistrer")
            if ok:
                from storage_backend import save_df_target
                def _add_inter(cid):
                    nid = generate_id("INT", dfs["inter"], "ID_Interaction")
                    row = {"ID_Interaction":nid,"ID":cid,"Date":dte.isoformat(),"Canal":canal,"Objet":obj,"Résultat":res,"Relance":dte.isoformat(),"Responsable":"","Notes":notes}
                    for c in AUDIT_COLS: row.setdefault(c,"")
                    dfs["inter"] = pd.concat([dfs["inter"], pd.DataFrame([row])], ignore_index=True)
                if who.startswith("Contact principal") and cp_id:
                    _add_inter(cp_id)
                elif "Un employé" in who and single_id:
                    _add_inter(single_id)
                elif "Tous" in who and not employees.empty:
                    for cid in employees["ID"].tolist():
                        _add_inter(cid)
                save_df_target("inter", dfs["inter"], PATHS, WS_FUNC)
                st.success("Interaction(s) ajoutée(s).")

    with tab_part:
        with st.form("ent_add_part"):
            if df_events.empty:
                st.warning("Aucun événement défini.")
            ev_options = [] if df_events.empty else df_events.apply(lambda r: f"{r['ID_Événement']} — {r.get('Nom_Événement','')} ({r.get('Date','')})", axis=1).tolist()
            ev_map = {} if df_events.empty else dict(zip(ev_options, df_events["ID_Événement"]))
            ev_label = st.selectbox("Événement", [""] + ev_options, index=0)
            role = st.selectbox("Rôle", dfs["SET"]["roles_evt"])
            fb = st.text_area("Feedback")
            note = st.number_input("Note", min_value=0, max_value=100, value=0)
            who = st.selectbox("Cible", [t[0] for t in targets])
            single_id = ""
            if "Un employé" in who and not employees.empty:
                emp_opts = employees.apply(lambda r: f"{r['ID']} — {r['Prénom']} {r['Nom']}", axis=1).tolist()
                emp_map = dict(zip(emp_opts, employees["ID"]))
                _lab = st.selectbox("Employé", emp_opts, index=0)
                single_id = emp_map.get(_lab,"")
            ok = st.form_submit_button("💾 Enregistrer")
            if ok and ev_label:
                from storage_backend import save_df_target
                def _add_part(cid):
                    nid = generate_id("PAR", dfs["parts"], "ID_Participation")
                    row = {"ID_Participation":nid,"ID":cid,"ID_Événement":ev_map[ev_label],"Rôle":role,"Feedback":fb,"Note":int(note)}
                    for c in AUDIT_COLS: row.setdefault(c,"")
                    dfs["parts"] = pd.concat([dfs["parts"], pd.DataFrame([row])], ignore_index=True)
                if who.startswith("Contact principal") and cp_id:
                    _add_part(cp_id)
                elif "Un employé" in who and single_id:
                    _add_part(single_id)
                elif "Tous" in who and not employees.empty:
                    for cid in employees["ID"].tolist():
                        _add_part(cid)
                save_df_target("parts", dfs["parts"], PATHS, WS_FUNC)
                st.success("Participation(s) ajoutée(s).")

    with tab_pay:
        with st.form("ent_add_pay"):
            if df_events.empty:
                st.warning("Aucun événement défini.")
            ev_options = [] if df_events.empty else df_events.apply(lambda r: f"{r['ID_Événement']} — {r.get('Nom_Événement','')} ({r.get('Date','')})", axis=1).tolist()
            ev_map = {} if df_events.empty else dict(zip(ev_options, df_events["ID_Événement"]))
            ev_label = st.selectbox("Événement", [""] + ev_options, index=0)
            dte = st.date_input("Date paiement", value=date.today())
            montant = st.number_input("Montant (FCFA)", min_value=0, step=1000, value=0)
            moyen = st.selectbox("Moyen", dfs["SET"]["moyens_paiement"])
            statut = st.selectbox("Statut", dfs["SET"]["statuts_paiement"])
            ref = st.text_input("Référence")
            who = st.selectbox("Cible", [t[0] for t in targets])
            single_id = ""
            if "Un employé" in who and not employees.empty:
                emp_opts = employees.apply(lambda r: f"{r['ID']} — {r['Prénom']} {r['Nom']}", axis=1).tolist()
                emp_map = dict(zip(emp_opts, employees["ID"]))
                _lab = st.selectbox("Employé", emp_opts, index=0)
                single_id = emp_map.get(_lab,"")
            ok = st.form_submit_button("💾 Enregistrer")
            if ok and ev_label:
                from storage_backend import save_df_target
                def _add_pay(cid):
                    nid = generate_id("PAY", dfs["pay"], "ID_Paiement")
                    row = {"ID_Paiement":nid,"ID":cid,"ID_Événement":ev_map[ev_label],"Date_Paiement":dte.isoformat(),
                           "Montant":int(montant),"Moyen":moyen,"Statut":statut,"Référence":ref}
                    for c in AUDIT_COLS: row.setdefault(c,"")
                    dfs["pay"] = pd.concat([dfs["pay"], pd.DataFrame([row])], ignore_index=True)
                if who.startswith("Contact principal") and cp_id:
                    _add_pay(cp_id)
                elif "Un employé" in who and single_id:
                    _add_pay(single_id)
                elif "Tous" in who and not employees.empty:
                    for cid in employees["ID"].tolist():
                        _add_pay(cid)
                save_df_target("pay", dfs["pay"], PATHS, WS_FUNC)
                st.success("Paiement(s) ajouté(s).")

    with tab_cert:
        with st.form("ent_add_cert"):
            tc = st.selectbox("Type Certification", dfs["SET"]["types_certif"])
            dte = st.date_input("Date Examen", value=date.today())
            res = st.selectbox("Résultat", ["Réussi","Échoué","En cours","Reporté"])
            sc = st.number_input("Score", min_value=0, max_value=100, value=0)
            has_dto = st.checkbox("Renseigner une date d'obtention ?")
            dto = st.date_input("Date Obtention", value=date.today()) if has_dto else None
            who = st.selectbox("Cible", [t[0] for t in targets])
            single_id = ""
            if "Un employé" in who and not employees.empty:
                emp_opts = employees.apply(lambda r: f"{r['ID']} — {r['Prénom']} {r['Nom']}", axis=1).tolist()
                emp_map = dict(zip(emp_opts, employees["ID"]))
                _lab = st.selectbox("Employé", emp_opts, index=0)
                single_id = emp_map.get(_lab,"")
            ok = st.form_submit_button("💾 Enregistrer")
            if ok:
                from storage_backend import save_df_target
                def _add_cert(cid):
                    nid = generate_id("CER", dfs["cert"], "ID_Certif")
                    row = {"ID_Certif":nid,"ID":cid,"Type_Certif":tc,"Date_Examen":dte.isoformat(),"Résultat":res,"Score":str(sc),
                           "Date_Obtention":dto.isoformat() if dto else "","Validité":"","Renouvellement":"","Notes":""}
                    for c in AUDIT_COLS: row.setdefault(c,"")
                    dfs["cert"] = pd.concat([dfs["cert"], pd.DataFrame([row])], ignore_index=True)
                if who.startswith("Contact principal") and cp_id:
                    _add_cert(cp_id)
                elif "Un employé" in who and single_id:
                    _add_cert(single_id)
                elif "Tous" in who and not employees.empty:
                    for cid in employees["ID"].tolist():
                        _add_cert(cid)
                save_df_target("cert", dfs["cert"], PATHS, WS_FUNC)
                st.success("Certification(s) ajoutée(s).")
