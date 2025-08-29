# pages/04_Rapports.py — Baseline rapports + pagination sur tableaux longs
from __future__ import annotations
import io
import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
from _shared import load_all_tables, filter_and_paginate, statusbar, parse_date

st.set_page_config(page_title="Rapports — IIBA Cameroun", page_icon="📈", layout="wide")
st.title("📈 Rapports & KPI — Baseline")

dfs = load_all_tables()
df_contacts = dfs["contacts"]
df_events   = dfs["events"]
df_parts    = dfs["parts"]
df_pay      = dfs["pay"]
df_cert     = dfs["cert"]

# --- Période ---
annees = ["Toutes"] + sorted(list({pd.to_datetime(x, errors="coerce").year for x in df_events.get("Date","") if str(x).strip()!="" if pd.to_datetime(x, errors="coerce") is not pd.NaT}))
mois = ["Tous"] + [str(i) for i in range(1,13)]
c1,c2 = st.columns(2)
annee = c1.selectbox("Année", annees, index=0)
mois_sel = c2.selectbox("Mois", mois, index=0)

st.markdown("### 📅 Événements (période filtrée)")
dfe = df_events.copy()
if "Date" in dfe.columns:
    dfe["_d"] = pd.to_datetime(dfe["Date"], errors="coerce")
    if annee != "Toutes":
        dfe = dfe[dfe["_d"].dt.year == int(annee)]
    if mois_sel != "Tous":
        dfe = dfe[dfe["_d"].dt.month == int(mois_sel)]
page_e, filt_e = filter_and_paginate(dfe.drop(columns=["_d"], errors="ignore"), key_prefix="rep_evt", page_size_default=20,
                                     suggested_filters=["Type","Ville","Pays"])
statusbar(filt_e, numeric_keys=["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total"])
st.dataframe(page_e, use_container_width=True, hide_index=True)

st.markdown("### 🎟 Participations (période via date événement)")
dfp = df_parts.copy()
if not dfp.empty and "ID_Événement" in dfp.columns and "Date" in df_events.columns:
    ev_dates = df_events.set_index("ID_Événement")["Date"].map(pd.to_datetime, na_action="ignore")
    dfp["_d_evt"] = dfp["ID_Événement"].map(ev_dates)
    if annee != "Toutes":
        dfp = dfp[dfp["_d_evt"].dt.year == int(annee)]
    if mois_sel != "Tous":
        dfp = dfp[dfp["_d_evt"].dt.month == int(mois_sel)]
page_p, filt_p = filter_and_paginate(dfp.drop(columns=["_d_evt"], errors="ignore"), key_prefix="rep_parts", page_size_default=20,
                                     suggested_filters=["Rôle"])
statusbar(filt_p, numeric_keys=[])
st.dataframe(page_p, use_container_width=True, hide_index=True)

st.markdown("### 💰 Paiements (période via Date_Paiement)")
dfpay = df_pay.copy()
if "Date_Paiement" in dfpay.columns:
    dfpay["_dp"] = pd.to_datetime(dfpay["Date_Paiement"], errors="coerce")
    if annee != "Toutes":
        dfpay = dfpay[dfpay["_dp"].dt.year == int(annee)]
    if mois_sel != "Tous":
        dfpay = dfpay[dfpay["_dp"].dt.month == int(mois_sel)]
page_pay, filt_pay = filter_and_paginate(dfpay.drop(columns=["_dp"], errors="ignore"), key_prefix="rep_pay", page_size_default=20,
                                         suggested_filters=["Statut"])
statusbar(filt_pay, numeric_keys=["Montant"])
st.dataframe(page_pay, use_container_width=True, hide_index=True)

st.markdown("### 🎓 Certifications (période via Date_Obtention/Examen)")
dfc = df_cert.copy()
if not dfc.empty:
    dfc["_do"] = pd.to_datetime(dfc.get("Date_Obtention",""), errors="coerce")
    dfc["_de"] = pd.to_datetime(dfc.get("Date_Examen",""), errors="coerce")
    mask = pd.Series(False, index=dfc.index)
    if annee != "Toutes":
        mask = mask | (dfc["_do"].dt.year == int(annee)) | (dfc["_de"].dt.year == int(annee))
    if mois_sel != "Tous":
        mask = mask | (dfc["_do"].dt.month == int(mois_sel)) | (dfc["_de"].dt.month == int(mois_sel))
    dfc = dfc[mask]
page_c, filt_c = filter_and_paginate(dfc.drop(columns=["_do","_de"], errors="ignore"), key_prefix="rep_cert", page_size_default=20,
                                     suggested_filters=["Résultat"])
statusbar(filt_c, numeric_keys=[])
st.dataframe(page_c, use_container_width=True, hide_index=True)

st.markdown("### ⬇ Export Excel (période)")
buf = io.BytesIO()
with pd.ExcelWriter(buf, engine="openpyxl") as writer:
    page_e.to_excel(writer, sheet_name="Événements", index=False)
    page_p.to_excel(writer, sheet_name="Participations", index=False)
    page_pay.to_excel(writer, sheet_name="Paiements", index=False)
    page_c.to_excel(writer, sheet_name="Certifications", index=False)
st.download_button("⬇ Télécharger", data=buf.getvalue(),
                   file_name=f"rapports_periode_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
