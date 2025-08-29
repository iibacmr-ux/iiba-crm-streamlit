# pages/04_Rapports.py
from __future__ import annotations
from datetime import datetime, date, timedelta
import io
import pandas as pd
import altair as alt
import streamlit as st
from _shared import load_all_tables, parse_date

st.set_page_config(page_title="Rapports & KPI", page_icon="📑", layout="wide")
dfs = load_all_tables()
df_contacts = dfs["contacts"]; df_events = dfs["events"]
df_parts = dfs["parts"]; df_pay = dfs["pay"]; df_cert = dfs["cert"]
PARAMS = dfs["PARAMS"]

st.title("📑 Rapports & KPI — IIBA Cameroun")

# Sélection période
years = ["Toutes"] + sorted(list({str(pd.to_datetime(x, errors="coerce").year) for x in list(df_events.get("Date","")) + list(df_pay.get("Date_Paiement","")) if str(x).strip()}))
months = ["Tous"] + [str(i) for i in range(1,13)]
colp1, colp2 = st.columns(2)
annee = colp1.selectbox("Année", years, index=0)
mois = colp2.selectbox("Mois", months, index=0)

def _safe_parse_series(s: pd.Series) -> pd.Series:
    return s.map(lambda x: parse_date(x) if pd.notna(x) and str(x).strip() != "" else None)

def _build_mask_from_dates(d: pd.Series, year_sel: str, month_sel: str) -> pd.Series:
    mask = pd.Series(True, index=d.index)
    if year_sel != "Toutes":
        y = int(year_sel)
        mask = mask & d.map(lambda x: isinstance(x, (datetime, date)) and x.year == y)
    if month_sel != "Tous":
        m = int(month_sel)
        mask = mask & d.map(lambda x: isinstance(x, (datetime, date)) and x.month == m)
    return mask.fillna(False)

def filtered_tables_for_period(year_sel: str, month_sel: str):
    if df_events.empty:
        dfe2 = df_events.copy()
    else:
        ev_dates = _safe_parse_series(df_events["Date"])
        mask_e = _build_mask_from_dates(ev_dates, year_sel, month_sel)
        dfe2 = df_events[mask_e].copy()

    if df_parts.empty:
        dfp2 = df_parts.copy()
    else:
        dfp2 = df_parts.copy()
        if not df_events.empty:
            ev_dates_map = df_events.set_index("ID_Événement")["Date"].map(parse_date)
            dfp2["_d_evt"] = dfp2["ID_Événement"].map(ev_dates_map)
            mask_p = _build_mask_from_dates(dfp2["_d_evt"], year_sel, month_sel)
            dfp2 = dfp2[mask_p].copy()
        else:
            dfp2 = dfp2.iloc[0:0].copy()

    if df_pay.empty:
        dfpay2 = df_pay.copy()
    else:
        pay_dates = _safe_parse_series(df_pay["Date_Paiement"])
        mask_pay = _build_mask_from_dates(pay_dates, year_sel, month_sel)
        dfpay2 = df_pay[mask_pay].copy()

    if df_cert.empty:
        dfcert2 = df_cert.copy()
    else:
        obt = _safe_parse_series(df_cert["Date_Obtention"]) if "Date_Obtention" in df_cert.columns else pd.Series([None]*len(df_cert), index=df_cert.index)
        exa = _safe_parse_series(df_cert["Date_Examen"])    if "Date_Examen"    in df_cert.columns    else pd.Series([None]*len(df_cert), index=df_cert.index)
        mask_c = _build_mask_from_dates(obt, year_sel, month_sel) | _build_mask_from_dates(exa, year_sel, month_sel)
        dfcert2 = df_cert[mask_c.fillna(False)].copy()

    return dfe2, dfp2, dfpay2, dfcert2

def filtered_contacts_for_period(year_sel: str, month_sel: str,
                                 dfe_all: pd.DataFrame, dfi_all: pd.DataFrame,
                                 dfp_all: pd.DataFrame, dfpay_all: pd.DataFrame) -> pd.DataFrame:
    base = df_contacts.copy()
    if base.empty or "ID" not in base.columns:
        return base

    base["ID"] = base["ID"].astype(str).str.strip()
    if "Date_Creation" in base.columns:
        base["_dc"] = _safe_parse_series(base["Date_Creation"])
    else:
        base["_dc"] = pd.Series([None]*len(base), index=base.index)

    use_fallback = True

    if not use_fallback:
        mask = _build_mask_from_dates(base["_dc"], year_sel, month_sel)
        return base[mask].drop(columns=["_dc"], errors="ignore")

    if not dfi_all.empty and "Date" in dfi_all.columns and "ID" in dfi_all.columns:
        dfi = dfi_all.copy()
        dfi["ID"] = dfi["ID"].astype(str).str.strip()
        dfi["_di"] = pd.to_datetime(_safe_parse_series(dfi["Date"]), errors="coerce")
        first_inter = dfi.groupby("ID")["_di"].min()
    else:
        first_inter = pd.Series(dtype="datetime64[ns]")

    if (not dfp_all.empty and "ID" in dfp_all.columns and "ID_Événement" in dfp_all.columns
        and not dfe_all.empty and "ID_Événement" in dfe_all.columns and "Date" in dfe_all.columns):
        dfp = dfp_all.copy()
        dfp = dfp[dfp["ID_Événement"].notna()]
        dfp["ID"] = dfp["ID"].astype(str).str.strip()

        ev_dates = dfe_all.copy()
        ev_dates["_de"] = _safe_parse_series(ev_dates["Date"])
        ev_map = ev_dates.set_index("ID_Événement")["_de"]

        dfp["_de"] = dfp["ID_Événement"].map(ev_map)
        dfp["_de"] = pd.to_datetime(dfp["_de"], errors="coerce")
        first_part = dfp.groupby("ID")["_de"].min()
    else:
        first_part = pd.Series(dtype="datetime64[ns]")

    if not dfpay_all.empty and "Date_Paiement" in dfpay_all.columns and "ID" in dfpay_all.columns:
        dfpay = dfpay_all.copy()
        dfpay["ID"] = dfpay["ID"].astype(str).str.strip()
        dfpay["_dp"] = pd.to_datetime(_safe_parse_series(dfpay["Date_Paiement"]), errors="coerce")
        first_pay = dfpay.groupby("ID")["_dp"].min()
    else:
        first_pay = pd.Series(dtype="datetime64[ns]")

    def _first_valid_date(dc, fi, fp, fpay):
        cands = []
        for v in (dc, fi, fp, fpay):
            if pd.isna(v) or v in (None, ""):
                continue
            if isinstance(v, pd.Timestamp):
                v = v.to_pydatetime()
            if isinstance(v, datetime):
                cands.append(v.date())
            elif isinstance(v, date):
                cands.append(v)
        return min(cands) if cands else None

    ref_dates = {}
    ids = base["ID"].tolist()
    s_dc = base.set_index("ID")["_dc"] if "ID" in base.columns else pd.Series(dtype=object)

    for cid in ids:
        dc   = s_dc.get(cid, None) if not s_dc.empty else None
        fi   = first_inter.get(cid, None) if not first_inter.empty else None
        fp   = first_part.get(cid, None)  if not first_part.empty else None
        fpay = first_pay.get(cid, None)   if not first_pay.empty else None
        ref_dates[cid] = _first_valid_date(dc, fi, fp, fpay)

    base["_ref"] = base["ID"].map(ref_dates)
    mask = _build_mask_from_dates(base["_ref"], year_sel, month_sel)
    return base[mask].drop(columns=["_dc","_ref"], errors="ignore")

def event_financials(dfe2, dfpay2):
    rec_by_evt = pd.Series(dtype=float)
    if not dfpay2.empty:
        r = dfpay2[dfpay2.get("Statut","")=="Réglé"].copy()
        r["Montant"] = pd.to_numeric(r["Montant"], errors='coerce').fillna(0)
        rec_by_evt = r.groupby("ID_Événement")["Montant"].sum()
    ev = dfe2 if not dfe2.empty else df_events.copy()
    if ev.empty:
        return pd.DataFrame(columns=["ID_Événement","Nom_Événement","Type","Date","Coût_Total","Recette","Bénéfice"])
    for c in ["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total","Coût_Total"]:
        if c not in ev.columns: ev[c] = 0
        ev[c] = pd.to_numeric(ev[c], errors='coerce').fillna(0)
    ev["Cout_Total"] = ev["Coût_Total"].where(ev.get("Coût_Total",0)>0, ev[["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres"]].sum(axis=1))
    ev = ev.set_index("ID_Événement")
    rep = pd.DataFrame({
        "Nom_Événement": ev.get("Nom_Événement",""),
        "Type": ev.get("Type",""),
        "Date": ev.get("Date",""),
        "Coût_Total": ev["Cout_Total"]
    })
    rep["Recette"] = rec_by_evt.reindex(rep.index, fill_value=0)
    rep["Bénéfice"] = rep["Recette"] - rep["Coût_Total"]
    return rep.reset_index()

dfe2, dfp2, dfpay2, dfcert2 = filtered_tables_for_period(annee, mois)
dfc2 = filtered_contacts_for_period(annee, mois, df_events, dfs["inter"], dfs["parts"], df_pay)

total_contacts = len(dfc2)
prospects_actifs = len(dfc2[(dfc2.get("Type","")== "Prospect") & (dfc2.get("Statut","")== "Actif")])
membres = len(dfc2[dfc2.get("Type","")=="Membre"])
events_count = len(dfe2)
parts_total = len(dfp2)
if not dfpay2.empty:
    dfpay2["Montant"] = pd.to_numeric(dfpay2["Montant"], errors='coerce').fillna(0)
    ca_regle = float(dfpay2[dfpay2["Statut"]=="Réglé"]["Montant"].sum())
    impayes = float(dfpay2[dfpay2["Statut"]!="Réglé"]["Montant"].sum())
else:
    ca_regle = 0.0
    impayes = 0.0
denom_prospects = max(1, len(dfc2[dfc2.get("Type","")=="Prospect"]))
taux_conv = (membres / denom_prospects) * 100

df_inter = dfs["inter"]
if not df_inter.empty:
    di = _safe_parse_series(df_inter["Date"])
    mask_i = _build_mask_from_dates(di, annee, mois)
    dfi2 = df_inter[mask_i].copy()
else:
    dfi2 = df_inter.copy()

ids_contacts_periode = set(dfc2.get("ID", pd.Series([], dtype=str)).astype(str))
ids_inter = set(dfi2.get("ID", pd.Series([], dtype=str)).astype(str)) if not dfi2.empty else set()
ids_parts = set(dfp2.get("ID", pd.Series([], dtype=str)).astype(str)) if not dfp2.empty else set()
ids_engaged = (ids_inter | ids_parts) & ids_contacts_periode
engagement_n = len(ids_engaged)
engagement_rate = (engagement_n / max(1, len(ids_contacts_periode))) * 100

kpis = {
    "contacts_total":        ("👥 Contacts (créés / période)", total_contacts),
    "prospects_actifs":      ("🧲 Prospects actifs (période)", prospects_actifs),
    "membres":               ("🏆 Membres (période)", membres),
    "events_count":          ("📅 Événements (période)", events_count),
    "participations_total":  ("🎟 Participations (période)", parts_total),
    "ca_regle":              ("💰 CA réglé (période)", f"{int(ca_regle):,} FCFA".replace(",", " ")),
    "impayes":               ("❌ Impayés (période)", f"{int(impayes):,} FCFA".replace(",", " ")),
    "taux_conv":             ("🔄 Taux conversion (période)", f"{taux_conv:.1f}%"),
    "engagement":            ("🙌 Engagement (période)", f"{engagement_rate:.1f}%"),
}
enabled = list(kpis.keys())
ncols = 4
rows = [enabled[i:i+ncols] for i in range(0, len(enabled), ncols)]
for row in rows:
    cols = st.columns(len(row))
    for col, k in zip(cols, row):
        label, value = kpis[k]
        col.metric(label, value)

ev_fin = event_financials(dfe2, dfpay2)
if not ev_fin.empty:
    chart1 = alt.Chart(ev_fin.melt(id_vars=["Nom_Événement"], value_vars=["Recette","Coût_Total","Bénéfice"])).mark_bar().encode(
        x=alt.X("Nom_Événement:N", sort="-y", title="Événement"),
        y=alt.Y('value:Q', title='Montant (FCFA)'),
        color=alt.Color('variable:N', title='Indicateur'),
        tooltip=['Nom_Événement', 'variable', 'value']
    ).properties(height=300, title='CA vs Coût vs Bénéfice (période)')
    st.altair_chart(chart1, use_container_width=True)

buf = io.BytesIO()
with pd.ExcelWriter(buf, engine='openpyxl') as writer:
    dfc2.to_excel(writer, sheet_name="Contacts(période)", index=False)
    dfe2.to_excel(writer, sheet_name="Événements(période)", index=False)
    dfp2.to_excel(writer, sheet_name="Participations(période)", index=False)
    dfpay2.to_excel(writer, sheet_name="Paiements(période)", index=False)
    dfcert2.to_excel(writer, sheet_name="Certifications(période)", index=False)
    ev_fin.to_excel(writer, sheet_name="Finance(période)", index=False)
st.download_button("⬇ Export Rapport Excel (période)", buf.getvalue(), "rapport_iiba_cameroon_periode.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
