# pages/01_CRM.py
from __future__ import annotations
from datetime import date
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
from _shared import (
    load_all_tables, aggregates_for_contacts, generate_id, parse_date, to_int_safe,
    C_COLS, INTER_COLS, PART_COLS, PAY_COLS, CERT_COLS, AUDIT_COLS
)

st.set_page_config(page_title="CRM — Grille centrale", page_icon="👥", layout="wide")
dfs = load_all_tables()
df_contacts = dfs["contacts"]; df_inter = dfs["inter"]; df_events = dfs["events"]
df_parts = dfs["parts"]; df_pay = dfs["pay"]; df_cert = dfs["cert"]
PATHS = dfs["PATHS"]; WS_FUNC = dfs["WS_FUNC"]
PARAMS = dfs["PARAMS"]; SET = dfs["SET"]

st.title("👥 CRM — Grille centrale (Contacts)")

# Filtres
colf1, colf2, colf3, colf4 = st.columns([2,1,1,1])
q = colf1.text_input("Recherche (nom, société, email)…","")
page_size = colf2.selectbox("Taille de page", [20,50,100,200], index=0)
type_filtre = colf3.selectbox("Type", ["Tous"] + SET["types_contact"])
top20_only = colf4.checkbox("Top-20 uniquement", value=False)

dfc = df_contacts.copy()
ag = aggregates_for_contacts(dfs)
dfc = dfc.merge(ag, on="ID", how="left")

if q:
    qs = q.lower()
    dfc = dfc[dfc.apply(lambda r: qs in str(r.get("Nom","")).lower() or qs in str(r.get("Prénom","")).lower()
                      or qs in str(r.get("Société","")).lower() or qs in str(r.get("Email","")).lower(), axis=1)]
if type_filtre != "Tous":
    dfc = dfc[dfc.get("Type","") == type_filtre]
if top20_only and "Top20" in dfc.columns:
    dfc = dfc[dfc["Top20"].astype(str).isin(["1","true","True","Yes","Oui"])]

default_cols = [
    "ID","Nom","Prénom","Société","Type","Statut","Email",
    "Interactions","Participations","CA_réglé","Impayé","Dernier_contact",
    "Score_composite","Proba_conversion","Tags"
]
default_cols += [c for c in AUDIT_COLS if c in dfc.columns]

# Sélecteur maître
def _label_contact(row):
    return f"{row['ID']} — {row.get('Prénom','')} {row.get('Nom','')} — {row.get('Société','')}"
options = [] if dfc.empty else dfc.apply(_label_contact, axis=1).tolist()
id_map = {} if dfc.empty else dict(zip(options, dfc["ID"]))

sel_label = st.selectbox("Contact sélectionné (sélecteur maître)", [""] + options, index=0, key="select_contact_label")
sel_id = id_map.get(sel_label, "") if sel_label else ""

# Grille (AgGrid)
if not dfc.empty:
    dfc_show = dfc[default_cols].copy()
    gb = GridOptionsBuilder.from_dataframe(dfc_show)
    gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=int(page_size))
    gb.configure_default_column(resizable=True, filter=True, sortable=True)
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
    """)
    gb.configure_column("Proba_conversion", cellStyle=proba_style)
    grid = AgGrid(
        dfc_show, gridOptions=gb.build(), update_mode=GridUpdateMode.NO_UPDATE,
        allow_unsafe_jscode=True, height=360, theme="streamlit"
    )
else:
    st.info("Aucun contact.")

st.markdown("---")
st.subheader("🛠 Actions rapides sur le contact sélectionné")

if not sel_id:
    st.warning("Sélectionnez d'abord un contact via le sélecteur ou la grille.")
else:
    tabs = st.tabs(["➕ Interaction", "➕ Participation", "➕ Paiement", "➕ Certification", "👁️ Vue 360°"])

    with tabs[0]:
        with st.form("add_inter"):
            dte = st.date_input("Date", value=date.today())
            canal = st.selectbox("Canal", ["Email","Téléphone","WhatsApp","LinkedIn","F2F","Autre"])
            obj = st.text_input("Objet")
            res = st.selectbox("Résultat", ["OK","À suivre","Sans suite","Refus"])
            rel = st.date_input("Relance", value=date.today())
            resp = st.text_input("Responsable", value="")
            notes = st.text_area("Notes")
            ok = st.form_submit_button("💾 Enregistrer l'interaction")
            if ok:
                nid = generate_id("INT", df_inter, "ID_Interaction")
                row = {"ID_Interaction":nid,"ID":sel_id,"Date":dte.isoformat(),"Canal":canal,"Objet":obj,
                       "Résultat":res,"Relance":rel.isoformat(),"Responsable":resp,"Notes":notes}
                for c in AUDIT_COLS:
                    row.setdefault(c, "")
                globals()["df_inter"] = pd.concat([df_inter, pd.DataFrame([row])], ignore_index=True)
                from storage_backend import save_df_target
                save_df_target("inter", df_inter, PATHS, WS_FUNC)
                st.success(f"Interaction enregistrée ({nid}).")

    with tabs[1]:
        with st.form("add_part"):
            if df_events.empty:
                st.warning("Aucun événement défini.")
            ev_options = [] if df_events.empty else df_events.apply(lambda r: f"{r['ID_Événement']} — {r.get('Nom_Événement','')} ({r.get('Date','')})", axis=1).tolist()
            ev_map = {} if df_events.empty else dict(zip(ev_options, df_events["ID_Événement"]))
            ev_label = st.selectbox("Événement", [""] + ev_options, index=0)
            role = st.selectbox("Rôle", dfs["SET"]["roles_evt"])
            fb = st.text_area("Feedback")
            note = st.number_input("Note", min_value=0, max_value=100, value=0)
            ok = st.form_submit_button("💾 Enregistrer la participation")
            if ok and ev_label:
                nid = generate_id("PAR", df_parts, "ID_Participation")
                row = {"ID_Participation":nid,"ID":sel_id,"ID_Événement":ev_map[ev_label],"Rôle":role,"Feedback":fb,"Note":int(note)}
                for c in AUDIT_COLS:
                    row.setdefault(c, "")
                globals()["df_parts"] = pd.concat([df_parts, pd.DataFrame([row])], ignore_index=True)
                from storage_backend import save_df_target
                save_df_target("parts", df_parts, PATHS, WS_FUNC)
                st.success(f"Participation enregistrée ({nid}).")

    with tabs[2]:
        with st.form("add_pay"):
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
            ok = st.form_submit_button("💾 Enregistrer le paiement")
            if ok and ev_label:
                nid = generate_id("PAY", df_pay, "ID_Paiement")
                row = {"ID_Paiement":nid,"ID":sel_id,"ID_Événement":ev_map[ev_label],"Date_Paiement":dte.isoformat(),
                       "Montant":int(montant),"Moyen":moyen,"Statut":statut,"Référence":ref}
                for c in AUDIT_COLS:
                    row.setdefault(c, "")
                globals()["df_pay"] = pd.concat([df_pay, pd.DataFrame([row])], ignore_index=True)
                from storage_backend import save_df_target
                save_df_target("pay", df_pay, PATHS, WS_FUNC)
                st.success(f"Paiement enregistré ({nid}).")

    with tabs[3]:
        with st.form("add_cert"):
            tc = st.selectbox("Type Certification", dfs["SET"]["types_certif"])
            dte = st.date_input("Date Examen", value=date.today())
            res = st.selectbox("Résultat", ["Réussi","Échoué","En cours","Reporté"])
            sc = st.number_input("Score", min_value=0, max_value=100, value=0)
            has_dto = st.checkbox("Renseigner une date d'obtention ?")
            dto = st.date_input("Date Obtention", value=date.today()) if has_dto else None
            ok = st.form_submit_button("💾 Enregistrer la certification")
            if ok:
                nid = generate_id("CER", df_cert, "ID_Certif")
                row = {"ID_Certif":nid,"ID":sel_id,"Type_Certif":tc,"Date_Examen":dte.isoformat(),"Résultat":res,"Score":str(sc),
                       "Date_Obtention":dto.isoformat() if dto else "","Validité":"","Renouvellement":"","Notes":""}
                for c in AUDIT_COLS:
                    row.setdefault(c, "")
                globals()["df_cert"] = pd.concat([df_cert, pd.DataFrame([row])], ignore_index=True)
                from storage_backend import save_df_target
                save_df_target("cert", df_cert, PATHS, WS_FUNC)
                st.success(f"Certification ajoutée ({nid}).")

    with tabs[4]:
        st.markdown("#### Vue 360°")
        if not df_inter.empty:
            st.write("**Interactions**")
            st.dataframe(df_inter[df_inter["ID"]==sel_id][["Date","Canal","Objet","Résultat","Relance","Responsable"]], use_container_width=True)
        if not df_parts.empty:
            st.write("**Participations**")
            dfp = df_parts[df_parts["ID"]==sel_id].copy()
            if not df_events.empty:
                ev_names = df_events.set_index("ID_Événement")["Nom_Événement"]
                dfp["Événement"] = dfp["ID_Événement"].map(ev_names)
            st.dataframe(dfp[["Événement","Rôle","Feedback","Note"]], use_container_width=True)
        if not df_pay.empty:
            st.write("**Paiements**")
            st.dataframe(df_pay[df_pay["ID"]==sel_id][["ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence"]], use_container_width=True)
        if not df_cert.empty:
            st.write("**Certifications**")
            st.dataframe(df_cert[df_cert["ID"]==sel_id][["Type_Certif","Date_Examen","Résultat","Score","Date_Obtention"]], use_container_width=True)
