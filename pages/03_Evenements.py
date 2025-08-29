# pages/03_Evenements.py
from __future__ import annotations
import pandas as pd
import streamlit as st
from _shared import (
    load_all_tables, generate_id, E_COLS, PART_COLS, PAY_COLS, AUDIT_COLS
)

st.set_page_config(page_title="Événements", page_icon="📅", layout="wide")
dfs = load_all_tables()
df_events = dfs["events"]; df_parts = dfs["parts"]; df_pay = dfs["pay"]; df_contacts = dfs["contacts"]; df_ent = dfs["entreprises"]
PATHS = dfs["PATHS"]; WS_FUNC = dfs["WS_FUNC"]; SET = dfs["SET"]

st.title("📅 Événements")

# Sélecteur
def _label_event(row):
    dat = row.get("Date","")
    nom = row.get("Nom_Événement","")
    typ = row.get("Type","")
    return f"{row['ID_Événement']} — {nom} — {typ} — {dat}"

options = [] if df_events.empty else df_events.apply(_label_event, axis=1).tolist()
id_map = {} if df_events.empty else dict(zip(options, df_events["ID_Événement"]))

sel_label = st.selectbox("Événement sélectionné (sélecteur maître)", ["— Aucun —"] + options, index=0)
sel_eid = id_map.get(sel_label, "") if sel_label and sel_label != "— Aucun —" else ""

st.markdown("---")
st.subheader("📝 Gérer un événement")

row_init = {c:"" for c in E_COLS}
if sel_eid:
    src = df_events[df_events["ID_Événement"] == sel_eid]
    if not src.empty:
        row_init.update(src.iloc[0].to_dict())

with st.form("event_form_main", clear_on_submit=False):
    c1, c2, c3 = st.columns(3)
    with c1:
        nom = st.text_input("Nom_Événement", value=row_init.get("Nom_Événement",""))
        typ = st.selectbox("Type", SET["types_evt"], index=0 if row_init.get("Type","") not in SET["types_evt"] else SET["types_evt"].index(row_init.get("Type","")))
        date_ev = st.date_input("Date", value=pd.to_datetime(row_init.get("Date","") or pd.Timestamp.today(), errors="coerce"))
    with c2:
        lieu = st.text_input("Lieu", value=row_init.get("Lieu",""))
        capacite = st.number_input("Capacité", min_value=0, step=1, value=int(float(row_init.get("Capacité") or 0)) if str(row_init.get("Capacité","")).strip() else 0)
        statut = st.selectbox("Statut", ["Planifié","Publié","Clos","Annulé"], index=0 if row_init.get("Statut","Planifié")=="" else ["Planifié","Publié","Clos","Annulé"].index(row_init.get("Statut","Planifié")))
    with c3:
        cout_salle = st.number_input("Cout_Salle", min_value=0, step=1000, value=int(float(row_init.get("Cout_Salle") or 0)) if str(row_init.get("Cout_Salle","")).strip() else 0)
        cout_form = st.number_input("Cout_Formateur", min_value=0, step=1000, value=int(float(row_init.get("Cout_Formateur") or 0)) if str(row_init.get("Cout_Formateur","")).strip() else 0)
        cout_log = st.number_input("Cout_Logistique", min_value=0, step=1000, value=int(float(row_init.get("Cout_Logistique") or 0)) if str(row_init.get("Cout_Logistique","")).strip() else 0)
        cout_pub = st.number_input("Cout_Pub", min_value=0, step=1000, value=int(float(row_init.get("Cout_Pub") or 0)) if str(row_init.get("Cout_Pub","")).strip() else 0)
        cout_aut = st.number_input("Cout_Autres", min_value=0, step=1000, value=int(float(row_init.get("Cout_Autres") or 0)) if str(row_init.get("Cout_Autres","")).strip() else 0)
    desc = st.text_area("Description", value=row_init.get("Description",""))
    colb1, colb2 = st.columns([1,1])
    if colb1.form_submit_button("💾 Enregistrer / Mettre à jour"):
        from storage_backend import save_df_target
        if not sel_eid:
            new_id = generate_id("EVT", df_events, "ID_Événement")
            row = {"ID_Événement":new_id,"Nom_Événement":nom,"Type":typ,"Date":str(date_ev.date()),
                   "Lieu":lieu,"Capacité":int(capacite),"Coût_Total":"",
                   "Cout_Salle":int(cout_salle),"Cout_Formateur":int(cout_form),"Cout_Logistique":int(cout_log),
                   "Cout_Pub":int(cout_pub),"Cout_Autres":int(cout_aut),"Statut":statut,"Description":desc}
            for c in AUDIT_COLS: row.setdefault(c,"")
            globals()["df_events"] = pd.concat([df_events, pd.DataFrame([row])], ignore_index=True)
            save_df_target("events", df_events, PATHS, WS_FUNC)
            st.success(f"Événement créé ({new_id}).")
        else:
            idx = df_events.index[df_events["ID_Événement"] == sel_eid]
            if len(idx):
                i = idx[0]
                df_events.loc[i,"Nom_Événement"] = nom
                df_events.loc[i,"Type"] = typ
                df_events.loc[i,"Date"] = str(date_ev.date())
                df_events.loc[i,"Lieu"] = lieu
                df_events.loc[i,"Capacité"] = int(capacite)
                df_events.loc[i,"Cout_Salle"] = int(cout_salle)
                df_events.loc[i,"Cout_Formateur"] = int(cout_form)
                df_events.loc[i,"Cout_Logistique"] = int(cout_log)
                df_events.loc[i,"Cout_Pub"] = int(cout_pub)
                df_events.loc[i,"Cout_Autres"] = int(cout_aut)
                df_events.loc[i,"Statut"] = statut
                df_events.loc[i,"Description"] = desc
                save_df_target("events", df_events, PATHS, WS_FUNC)
                st.success(f"Événement mis à jour ({sel_eid}).")

    if colb2.form_submit_button("🆕 Nouveau"):
        st.experimental_rerun()

st.markdown("---")
st.subheader("👥 Participants (contacts) de l'événement")
if df_parts.empty or df_contacts.empty:
    st.info("Aucune participation enregistrée.")
else:
    if sel_eid:
        dfp = df_parts[df_parts["ID_Événement"] == sel_eid].copy()
    else:
        dfp = df_parts.copy()
    if dfp.empty:
        st.info("Aucun participant pour l'instant.")
    else:
        names = df_contacts.set_index("ID").apply(lambda r: f"{r.get('Prénom','')} {r.get('Nom','')}", axis=1)
        dfp["Contact"] = dfp["ID"].map(names)
        st.dataframe(dfp[["ID_Participation","Contact","Rôle","Feedback","Note","ID"]], use_container_width=True)
