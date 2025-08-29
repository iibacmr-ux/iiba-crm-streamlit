
from __future__ import annotations
import io
import pandas as pd
import streamlit as st
from _shared import load_all_tables, save_df_target

st.set_page_config(page_title="Admin", page_icon="🛠️", layout="wide")
dfs = load_all_tables()
PATHS = dfs["PATHS"]; WS_FUNC = dfs["WS_FUNC"]

st.title("🛠️ Administration — Paramètres & Données")

st.subheader("📋 Listes de valeurs (Paramètres)")
df_params = dfs["params"].copy()

def _val(key, default=""):
    r = df_params[df_params["cle"]==key]
    return (r.iloc[0]["val"] if not r.empty else default)

def _set(key, value):
    nonlocal_df = st.session_state.setdefault("_params_work", df_params.copy())
    r = nonlocal_df[nonlocal_df["cle"]==key]
    if r.empty:
        nonlocal_df = pd.concat([nonlocal_df, pd.DataFrame([{"cle":key,"val":value}])], ignore_index=True)
    else:
        nonlocal_df.loc[r.index[0],"val"] = value
    st.session_state["_params_work"] = nonlocal_df

with st.form("params_form"):
    col1, col2, col3 = st.columns(3)
    with col1:
        secteurs = st.text_area("Secteurs (séparés par des virgules)", value=_val("secteurs", ",".join(dfs["SET"]["secteurs"])))
        fonctions = st.text_area("Fonctions", value=_val("fonctions", ",".join(dfs["SET"]["fonctions"])))
        types_evt = st.text_area("Types d'événement", value=_val("types_evt", ",".join(dfs["SET"]["types_evt"])))
    with col2:
        pays = st.text_area("Pays", value=_val("pays", ",".join(dfs["SET"]["pays"])))
        villes = st.text_area("Villes", value=_val("villes", ",".join(dfs["SET"]["villes"])))
        roles_evt = st.text_area("Rôles événement", value=_val("roles_evt", ",".join(dfs["SET"]["roles_evt"])))
    with col3:
        moyens = st.text_area("Moyens de paiement", value=_val("moyens_paiement", ",".join(dfs["SET"]["moyens_paiement"])))
        statuts_pay = st.text_area("Statuts paiement", value=_val("statuts_paiement", ",".join(dfs["SET"]["statuts_paiement"])))
        types_cert = st.text_area("Types de certification", value=_val("types_certif", ",".join(dfs["SET"]["types_certif"])))
        types_org = st.text_area("Types de lien entreprise–événement", value=_val("types_org_lien", ",".join(dfs["SET"]["types_org_lien"])))

    ok = st.form_submit_button("💾 Enregistrer les paramètres")
    if ok:
        dfw = st.session_state.get("_params_work", df_params.copy())
        # rafraîchir depuis les champs
        mapping = {
            "secteurs": secteurs, "fonctions": fonctions, "types_evt": types_evt,
            "pays": pays, "villes": villes, "roles_evt": roles_evt,
            "moyens_paiement": moyens, "statuts_paiement": statuts_pay,
            "types_certif": types_cert, "types_org_lien": types_org
        }
        for k, v in mapping.items():
            r = dfw[dfw["cle"]==k]
            if r.empty:
                dfw = pd.concat([dfw, pd.DataFrame([{"cle":k,"val":v}])], ignore_index=True)
            else:
                dfw.loc[r.index[0],"val"] = v
        save_df_target("params", dfw, PATHS, WS_FUNC)
        st.success("Paramètres enregistrés.")

st.markdown("---")
st.subheader("⬇️ Export complet (.xlsx) / ⬆️ Import CSV (table par table)")

# Export Excel
buf = io.BytesIO()
with pd.ExcelWriter(buf, engine="openpyxl") as writer:
    for key in ["contacts","entreprises","events","inter","parts","pay","cert","orgparts","params"]:
        dfs[key].to_excel(writer, sheet_name=key, index=False)
st.download_button("⬇️ Exporter toutes les tables (Excel)", buf.getvalue(),
                   file_name="iiba_crm_export.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Import CSV rapide
imp_col1, imp_col2 = st.columns(2)
with imp_col1:
    st.write("Importer un CSV dans une table :")
    target = st.selectbox("Table cible", ["contacts","entreprises","events","inter","parts","pay","cert","orgparts","params"])
    up = st.file_uploader("CSV (UTF-8)", type=["csv"], key="imp_csv")
    if st.button("📥 Importer"):
        if up is None:
            st.error("Choisissez un fichier CSV.")
        else:
            try:
                newdf = pd.read_csv(up, dtype=str).fillna("")
                dfs[target] = newdf
                save_df_target(target, newdf, PATHS, WS_FUNC)
                st.success(f"Table '{target}' importée avec succès.")
            except Exception as e:
                st.error(f"Erreur d'import: {e}")

with imp_col2:
    st.write("Tables actuelles (taille) :")
    for key in ["contacts","entreprises","events","inter","parts","pay","cert","orgparts","params"]:
        st.caption(f"• {key}: {len(dfs[key])} lignes")
