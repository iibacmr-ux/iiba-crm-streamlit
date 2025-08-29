# pages/00_Admin.py — Listes, KPI cibles, Import/Export Excel (toutes tables)
from __future__ import annotations
import io
import streamlit as st
import pandas as pd
from _shared import load_all_tables, save_table, filter_and_paginate, statusbar

st.set_page_config(page_title="Admin — IIBA Cameroun", page_icon="🛠️", layout="wide")
st.title("🛠️ Administration")

dfs = load_all_tables()

st.header("📦 Export/Import Excel (toutes tables)")
c1, c2 = st.columns(2)
with c1:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for name, df in dfs.items():
            try:
                df.to_excel(writer, sheet_name=name[:31], index=False)
            except Exception:
                pd.DataFrame().to_excel(writer, sheet_name=name[:31], index=False)
    st.download_button("⬇ Exporter toutes les tables (Excel)", buf.getvalue(),
                       file_name="iiba_crm_all_tables.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
with c2:
    up = st.file_uploader("Importer un Excel (mêmes feuilles/colonnes)", type=["xlsx"])
    if up is not None:
        try:
            x = pd.ExcelFile(up)
            changed = False
            for sheet in x.sheet_names:
                try:
                    df_new = pd.read_excel(x, sheet_name=sheet, dtype=str).fillna("")
                    dfs[sheet] = df_new
                    save_table(sheet, df_new)
                    changed = True
                except Exception:
                    pass
            if changed:
                st.success("Import terminé. Les tables ont été mises à jour.")
        except Exception as e:
            st.error(f"Import échoué: {e}")

st.header("📋 Listes de valeurs (édition rapide)")
tab_cats, tab_kpi = st.tabs(["Listes", "KPI / Paramètres"])

with tab_cats:
    # Exemple : types lien org, secteurs, fonctions, pays, villes… (si présents dans params ou une table dédiée)
    st.caption("Éditez vos listes dans la table 'parametres' (clé/valeur).")
    dfp = dfs.get("params", pd.DataFrame(columns=["key","value"])).copy()
    page_p, filt_p = filter_and_paginate(dfp, key_prefix="adm_params", page_size_default=20,
                                         suggested_filters=["key"])
    statusbar(filt_p, numeric_keys=[])
    st.dataframe(page_p, use_container_width=True, hide_index=True)

with tab_kpi:
    st.caption("KPI cibles et paramètres divers (scoring, seuils, objectifs, etc.).")
    dfp = dfs.get("params", pd.DataFrame(columns=["key","value"])).copy()
    page_p, filt_p = filter_and_paginate(dfp, key_prefix="adm_kpi", page_size_default=20,
                                         suggested_filters=["key"])
    statusbar(filt_p, numeric_keys=[])
    st.dataframe(page_p, use_container_width=True, hide_index=True)
