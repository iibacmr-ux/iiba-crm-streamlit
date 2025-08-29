
# pages/04_Rapports.py — Rapports & KPI (baseline)
from __future__ import annotations
import pandas as pd
import streamlit as st
from _shared import load_all_tables, parse_date

st.set_page_config(page_title="Rapports", page_icon="📈", layout="wide")

dfs = load_all_tables()
dfc = dfs["contacts"]; dfi = dfs["inter"]; dfp = dfs["parts"]; dfpay = dfs["pay"]; dfe = dfs["events"]; dfent = dfs["entreprises"]; dfcert = dfs["cert"]

st.title("📈 Rapports — Synthèse opérationnelle (baseline)")

# Helper to coerce to date
if not dfi.empty and "Date" in dfi.columns: dfi["Date"] = dfi["Date"].apply(parse_date)
if not dfpay.empty and "Date_Paiement" in dfpay.columns: dfpay["Date_Paiement"] = dfpay["Date_Paiement"].apply(parse_date)
if not dfp.empty and "Date" in dfp.columns: dfp["Date"] = dfp["Date"].apply(parse_date)
if not dfe.empty and "Date" in dfe.columns: dfe["Date"] = dfe["Date"].apply(parse_date)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Contacts", len(dfc))
c2.metric("Interactions", len(dfi))
c3.metric("Participations", len(dfp))
total_regle = pd.to_numeric(dfpay[dfpay.get("Statut","")=="Réglé"].get("Montant",0), errors="coerce").fillna(0).sum()
c4.metric("CA réglé (cumul)", f"{int(total_regle):,} FCFA".replace(","," "))

st.markdown("---")
st.subheader("📅 Activité par mois (année courante)")
year = st.selectbox("Année", sorted(set([pd.Timestamp.today().year] + [x.year for x in pd.to_datetime(dfi.get("Date",""), errors="coerce").dropna().tolist()])), index=0)

def month_count(s, colname="Date"):
    s = pd.to_datetime(s, errors="coerce")
    s = s[s.dt.year == year]
    vc = s.dt.month.value_counts().sort_index()
    return vc if not vc.empty else pd.Series([0]*1, index=[pd.Timestamp.today().month])

cols = st.columns(4)
cols[0].bar_chart(month_count(dfi.get("Date","")))
cols[0].caption("Interactions / mois")
cols[1].bar_chart(month_count(dfp.get("Date","")) if "Date" in dfp.columns else pd.Series(dtype=int))
cols[1].caption("Participations / mois")
cols[2].bar_chart(month_count(dfpay.get("Date_Paiement","")) if "Date_Paiement" in dfpay.columns else pd.Series(dtype=int))
cols[2].caption("Paiements / mois")
cols[3].bar_chart(month_count(dfe.get("Date","")) if "Date" in dfe.columns else pd.Series(dtype=int))
cols[3].caption("Événements / mois")

st.markdown("---")
st.subheader("🏢 Top entreprises par CA réglé")
if dfc.empty or dfpay.empty:
    st.info("Pas assez de données pour ce tableau.")
else:
    # map contact -> entreprise
    comp_map = dfc.set_index("ID")["Société"]
    p = dfpay.copy()
    p["Montant"] = pd.to_numeric(p["Montant"], errors="coerce").fillna(0.0)
    p = p[p.get("Statut","")=="Réglé"]
    p["Entreprise"] = p["ID"].map(comp_map).fillna("")
    top = p.groupby("Entreprise")["Montant"].sum().reset_index().sort_values("Montant", ascending=False).head(20)
    st.dataframe(top, use_container_width=True)

st.markdown("---")
st.subheader("🎓 Certifications — Réussites par type")
if dfcert.empty:
    st.info("Aucune certification.")
else:
    ok = (dfcert.get("Résultat","")=="Réussi")
    ct = dfcert[ok].groupby("Type_Certif")["ID_Certif"].count().reset_index().rename(columns={"ID_Certif":"Réussites"})
    st.bar_chart(ct.set_index("Type_Certif"))

st.caption("⚠️ Ceci est une **version baseline** des Rapports. Pour coller 1:1 au monofichier initial, on peut intégrer vos vues complètes.")
