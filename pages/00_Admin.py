# pages/00_Admin.py — Listes, KPI cibles, Import/Export Excel (toutes tables) + filtres/pagination
from __future__ import annotations
import io
import streamlit as st
import pandas as pd
from _shared import load_all_tables, save_table, filter_and_paginate, statusbar, export_filtered_excel, smart_suggested_filters

st.set_page_config(page_title="Admin — IIBA Cameroun", page_icon="🛠️", layout="wide")
st.title("🛠️ Administration")

if "auth_user" not in st.session_state:
    st.info("🔐 Veuillez vous connecter depuis la page principale pour accéder à cette section.")
    st.stop()

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

st.header("📋 Listes de valeurs & KPI / Paramètres")
tab_cats, tab_kpi, tab_tech = st.tabs(["Listes", "KPI / Paramètres", "Tech (diagnostic data)"])

with tab_cats:
    st.caption("Éditez vos listes dans la table 'parametres' (clé/valeur).")
    dfp = dfs.get("params", pd.DataFrame(columns=["key","value"])).copy()
    suggested = ["key"]
    page_p, filt_p = filter_and_paginate(dfp, key_prefix="adm_params", page_size_default=20,
                                         suggested_filters=suggested)
    statusbar(filt_p, numeric_keys=[])
    st.dataframe(page_p, use_container_width=True, hide_index=True)

with tab_kpi:
    st.caption("KPI cibles et paramètres divers (scoring, seuils, objectifs, etc.).")
    dfp = dfs.get("params", pd.DataFrame(columns=["key","value"])).copy()
    suggested = ["key"]
    page_p, filt_p = filter_and_paginate(dfp, key_prefix="adm_kpi", page_size_default=20,
                                         suggested_filters=suggested)
    statusbar(filt_p, numeric_keys=[])
    st.dataframe(page_p, use_container_width=True, hide_index=True)

with tab_tech:
    st.caption("Aperçu rapide des autres tables (filtrées/paginées).")
    for name in ["contacts","entreprises","events","parts","pay","cert","inter","entreprise_parts"]:
        st.markdown(f"#### Table : {name}")
        df = dfs.get(name, pd.DataFrame())
        suggested = smart_suggested_filters(df)
        page_t, filt_t = filter_and_paginate(df, key_prefix=f"adm_{name}", page_size_default=20,
                                             suggested_filters=suggested)
        # Choix auto des sommes numériques usuelles
        numeric_keys = []
        if name == "pay": numeric_keys = ["Montant"]
        if name == "entreprises": numeric_keys = ["CA_Annuel","Nb_Employes"]
        if name == "entreprise_parts": numeric_keys = ["Nb_Employes","Sponsoring_FCFA"]
        statusbar(filt_t, numeric_keys=numeric_keys)
        st.dataframe(page_t, use_container_width=True, hide_index=True)

st.subheader("⬇ Export des tables filtrées (depuis l'onglet Tech)")
# Exemple d'export combiné des dernières grilles filtrées si nécessaire : on exporte tout brut
export_filtered_excel({k:v for k,v in dfs.items()}, filename_prefix="admin_tables_brut")
