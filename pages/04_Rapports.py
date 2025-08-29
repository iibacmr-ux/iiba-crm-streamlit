# pages/04_Rapports.py — Rapports + sous-rapports avec filtres/pagination
from __future__ import annotations
import io
import streamlit as st
import pandas as pd
from datetime import datetime
from _shared import (
    load_all_tables, filter_and_paginate, statusbar, parse_date,
    export_filtered_excel, smart_suggested_filters
)

st.set_page_config(page_title="Rapports — IIBA Cameroun", page_icon="📈", layout="wide")
st.title("📈 Rapports & KPI — Période + Sous-rapports")

dfs = load_all_tables()
df_contacts = dfs["contacts"]
df_events   = dfs["events"]
df_parts    = dfs["parts"]
df_pay      = dfs["pay"]
df_cert     = dfs["cert"]
df_entre    = dfs["entreprises"]
df_ep       = dfs["entreprise_parts"]

# --- Sélecteurs de période ---
def _years_from(series):
    s = pd.to_datetime(series, errors="coerce").dt.year.dropna().astype(int)
    if s.empty: return []
    return sorted(s.unique().tolist())

years = sorted(set(_years_from(df_events.get("Date","")) |
                   set(_years_from(df_pay.get("Date_Paiement","")))))
annees = ["Toutes"] + [str(y) for y in years]
mois = ["Tous"] + [str(i) for i in range(1,13)]
c1,c2 = st.columns(2)
annee = c1.selectbox("Année", annees, index=0)
mois_sel = c2.selectbox("Mois", mois, index=0)

def _apply_period(df, date_col):
    if date_col not in df.columns: return df
    d = pd.to_datetime(df[date_col], errors="coerce")
    if annee != "Toutes":
        df = df[d.dt.year == int(annee)]
    if mois_sel != "Tous":
        df = df[d.dt.month == int(mois_sel)]
    return df

# === Événements ===
st.header("📅 Événements (période)")
dfe = _apply_period(df_events.copy(), "Date")
page_e, filt_e = filter_and_paginate(dfe, key_prefix="rep_evt", page_size_default=20,
                                     suggested_filters=["Type","Ville","Pays"])
statusbar(filt_e, numeric_keys=["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total"])
st.dataframe(page_e, use_container_width=True, hide_index=True)

# === Participations (via date d'événement) ===
st.header("🎟 Participations (période via date événement)")
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

# === Paiements (période via Date_Paiement) ===
st.header("💰 Paiements (période via Date_Paiement)")
dfpay = _apply_period(df_pay.copy(), "Date_Paiement")
page_pay, filt_pay = filter_and_paginate(dfpay, key_prefix="rep_pay", page_size_default=20,
                                         suggested_filters=["Statut"])
statusbar(filt_pay, numeric_keys=["Montant"])
st.dataframe(page_pay, use_container_width=True, hide_index=True)

# === Certifications (période) ===
st.header("🎓 Certifications (période via Date_Obtention/Examen)")
dfc = df_cert.copy()
if not dfc.empty:
    dfc["_do"] = pd.to_datetime(dfc.get("Date_Obtention",""), errors="coerce")
    dfc["_de"] = pd.to_datetime(dfc.get("Date_Examen",""), errors="coerce")
    mask = pd.Series(True, index=dfc.index)
    if annee != "Toutes":
        mask &= ((dfc["_do"].dt.year == int(annee)) | (dfc["_de"].dt.year == int(annee)))
    if mois_sel != "Tous":
        mask &= ((dfc["_do"].dt.month == int(mois_sel)) | (dfc["_de"].dt.month == int(mois_sel)))
    dfc = dfc[mask]
page_c, filt_c = filter_and_paginate(dfc.drop(columns=["_do","_de"], errors="ignore"), key_prefix="rep_cert", page_size_default=20,
                                     suggested_filters=["Résultat"])
statusbar(filt_c, numeric_keys=[])
st.dataframe(page_c, use_container_width=True, hide_index=True)

# === Sous-rapports spécifiques ===
st.header("📊 Sous-rapports spécifiques")

# Top entreprises par CA réglé (via paiements + contacts Entreprise) + sponsoring officiel
with st.expander("🏆 Top entreprises par CA réglé (incl. sponsoring officiel)", expanded=False):
    pay_ok = df_pay.copy()
    pay_ok["Montant"] = pd.to_numeric(pay_ok.get("Montant",0), errors="coerce").fillna(0)
    pay_ok = pay_ok[pay_ok.get("Statut","")=="Réglé"]
    # via employés
    if not pay_ok.empty and "ID" in pay_ok.columns and "Entreprise" in df_contacts.columns:
        merged = pay_ok.merge(df_contacts[["ID","Entreprise"]], on="ID", how="left")
        agg_emp = merged.groupby("Entreprise")["Montant"].sum().reset_index().rename(columns={"Montant":"CA_Regle_Employes"})
    else:
        agg_emp = pd.DataFrame(columns=["Entreprise","CA_Regle_Employes"])
    # sponsoring officiel (entreprise_parts -> Sponsoring_FCFA)
    ep = df_ep.copy()
    ep["Sponsoring_FCFA"] = pd.to_numeric(ep.get("Sponsoring_FCFA",0), errors="coerce").fillna(0)
    agg_off = ep.groupby("ID_Entreprise")["Sponsoring_FCFA"].sum().reset_index()
    agg_off = agg_off.merge(df_entre[["ID_Entreprise","Nom_Entreprise"]], on="ID_Entreprise", how="left")
    agg_off = agg_off.rename(columns={"Nom_Entreprise":"Entreprise"})
    # fusion
    top = pd.merge(agg_emp, agg_off[["Entreprise","Sponsoring_FCFA"]], on="Entreprise", how="outer").fillna(0)
    top["CA_Total"] = pd.to_numeric(top.get("CA_Regle_Employes",0), errors="coerce").fillna(0) + \
                      pd.to_numeric(top.get("Sponsoring_FCFA",0), errors="coerce").fillna(0)
    # filtres/pagination
    suggested = ["Entreprise"]
    page_top, filt_top = filter_and_paginate(top, key_prefix="rep_top_ca", page_size_default=20, suggested_filters=suggested)
    statusbar(filt_top, numeric_keys=["CA_Regle_Employes","Sponsoring_FCFA","CA_Total"])
    st.dataframe(page_top.sort_values("CA_Total", ascending=False), use_container_width=True, hide_index=True)

# Activité mensuelle (événements / participations / paiements réglés)
with st.expander("📆 Activité mensuelle (Événements / Participations / Paiements réglés)", expanded=False):
    # Événements par mois
    dfe2 = df_events.copy()
    dfe2["_mois"] = pd.to_datetime(dfe2.get("Date",""), errors="coerce").dt.to_period("M").astype(str)
    evm = dfe2["_mois"].value_counts().rename_axis("Mois").reset_index(name="Nb_Événements")
    page_evm, filt_evm = filter_and_paginate(evm, key_prefix="rep_act_evt", page_size_default=20,
                                             suggested_filters=["Mois"])
    statusbar(filt_evm, numeric_keys=["Nb_Événements"])
    st.dataframe(page_evm.sort_values("Mois"), use_container_width=True, hide_index=True)

    # Participations par mois (via Date événement)
    dfp2 = df_parts.copy()
    if not dfp2.empty and "ID_Événement" in dfp2.columns and "Date" in df_events.columns:
        ev_dates = df_events.set_index("ID_Événement")["Date"].map(pd.to_datetime, na_action="ignore")
        dfp2["_d_evt"] = dfp2["ID_Événement"].map(ev_dates)
        dfp2["_mois"] = pd.to_datetime(dfp2["_d_evt"], errors="coerce").dt.to_period("M").astype(str)
        pm = dfp2["_mois"].value_counts().rename_axis("Mois").reset_index(name="Nb_Participations")
    else:
        pm = pd.DataFrame(columns=["Mois","Nb_Participations"])
    page_pm, filt_pm = filter_and_paginate(pm, key_prefix="rep_act_parts", page_size_default=20,
                                           suggested_filters=["Mois"])
    statusbar(filt_pm, numeric_keys=["Nb_Participations"])
    st.dataframe(page_pm.sort_values("Mois"), use_container_width=True, hide_index=True)

    # Paiements réglés par mois
    dfpay2 = df_pay.copy()
    dfpay2 = dfpay2[dfpay2.get("Statut","")=="Réglé"].copy()
    dfpay2["Montant"] = pd.to_numeric(dfpay2.get("Montant",0), errors="coerce").fillna(0)
    dfpay2["_mois"] = pd.to_datetime(dfpay2.get("Date_Paiement",""), errors="coerce").dt.to_period("M").astype(str)
    pym = dfpay2.groupby("_mois")["Montant"].sum().reset_index().rename(columns={"_mois":"Mois","Montant":"CA_Regle"})
    page_pym, filt_pym = filter_and_paginate(pym, key_prefix="rep_act_pay", page_size_default=20,
                                             suggested_filters=["Mois"])
    statusbar(filt_pym, numeric_keys=["CA_Regle"])
    st.dataframe(page_pym.sort_values("Mois"), use_container_width=True, hide_index=True)

# Export des tables filtrées principales (non paginées)
st.subheader("⬇ Exports (filtres appliqués)")
export_filtered_excel({
    "Événements": filt_e,
    "Participations": filt_p,
    "Paiements": filt_pay,
    "Certifications": filt_c,
    "Top_Entreprises_CA": 'filt_top' in locals() and filt_top or pd.DataFrame(),
    "Activité_Mensuelle_Événements": 'filt_evm' in locals() and filt_evm or pd.DataFrame(),
    "Activité_Mensuelle_Participations": 'filt_pm' in locals() and filt_pm or pd.DataFrame(),
    "Activité_Mensuelle_Paiements": 'filt_pym' in locals() and filt_pym or pd.DataFrame(),
}, filename_prefix="rapports_filtres")
