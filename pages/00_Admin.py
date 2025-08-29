# pages/00_Admin.py
from __future__ import annotations
import io
import pandas as pd
import streamlit as st
from _shared import load_all_tables, AUDIT_COLS
from storage_backend import save_df_target

st.set_page_config(page_title="Admin — Paramètres", page_icon="🛠️", layout="wide")
dfs = load_all_tables()
df_params = dfs["params"]
PATHS = dfs["PATHS"]; WS_FUNC = dfs["WS_FUNC"]

st.title("🛠️ Administration & Paramètres")

st.markdown("Gérez ici les **listes de valeurs** utilisées dans les dropdowns (Fonctions, Secteurs, Pays, Villes, Types d'événement, Rôles, Moyens/Statuts de paiement, Types de certification), ainsi que quelques **KPI cibles**.")

# Helper to get & set single-line CSV lists in df_params
def get_param(key: str, default: str = "") -> str:
    if df_params.empty:
        return default
    _df = df_params.copy()
    cols = [c.lower() for c in _df.columns]
    if "cle" in cols and "val" in cols:
        _df.columns = [c.lower() for c in _df.columns]
        m = _df[_df["cle"] == key]
        if not m.empty:
            return str(m.iloc[0]["val"])
    elif "key" in cols and "value" in cols:
        _df.columns = [c.lower() for c in _df.columns]
        m = _df[_df["key"] == key]
        if not m.empty:
            return str(m.iloc[0]["value"])
    return default

def set_param(key: str, value: str):
    global df_params
    cols = [c.lower() for c in df_params.columns]
    if df_params.empty:
        df_params = pd.DataFrame(columns=["cle","val"] + AUDIT_COLS)
        cols = ["cle","val"] + AUDIT_COLS
    df_params.columns = [c.lower() for c in df_params.columns]
    if "cle" in df_params.columns and "val" in df_params.columns:
        mask = (df_params["cle"] == key)
        if mask.any():
            df_params.loc[mask, "val"] = value
        else:
            row = {"cle": key, "val": value}
            for c in AUDIT_COLS: row.setdefault(c,"")
            df_params = pd.concat([df_params, pd.DataFrame([row])], ignore_index=True)
    else:
        # fallback "key"/"value"
        if "key" not in df_params.columns or "value" not in df_params.columns:
            df_params = pd.DataFrame(columns=["key","value"] + AUDIT_COLS)
        mask = (df_params["key"] == key)
        if mask.any():
            df_params.loc[mask, "value"] = value
        else:
            row = {"key": key, "value": value}
            for c in AUDIT_COLS: row.setdefault(c,"")
            df_params = pd.concat([df_params, pd.DataFrame([row])], ignore_index=True)

# UI — List editors
st.subheader("📋 Listes pour dropdowns")
lists = [
    ("Fonctions", "fonctions"),
    ("Secteurs", "secteurs"),
    ("Pays", "pays"),
    ("Villes", "villes"),
    ("Types d'événement", "types_evt"),
    ("Rôles événement", "roles_evt"),
    ("Moyens de paiement", "moyens_paiement"),
    ("Statuts de paiement", "statuts_paiement"),
    ("Types de certification", "types_certif"),
]
cols = st.columns(3)
for i, (label, key) in enumerate(lists):
    with cols[i % 3]:
        val = get_param(key, "")
        new = st.text_area(label, value=val, placeholder="Valeurs séparées par des virgules")
        if st.button(f"💾 Enregistrer {label}", key=f"save_{key}"):
            set_param(key, new)
            save_df_target("params", df_params, PATHS, WS_FUNC)
            st.success(f"{label} mis à jour.")

st.markdown("---")
st.subheader("🎯 KPI cibles (année courante)")
y = pd.Timestamp.today().year
kpis = [
    (f"kpi_target_contacts_total_year_{y}", "Contacts créés (année)"),
    (f"kpi_target_participations_total_year_{y}", "Participations (année)"),
    (f"kpi_target_ca_regle_year_{y}", "CA réglé (FCFA, année)"),
]
c1,c2,c3 = st.columns(3)
for (key, label), col in zip(kpis, [c1,c2,c3]):
    v = get_param(key, "0")
    new = col.text_input(label, value=str(v), key=f"inp_{key}")
    if col.button(f"💾 Enregistrer {label}", key=f"btn_{key}"):
        set_param(key, new)
        save_df_target("params", df_params, PATHS, WS_FUNC)
        st.success(f"{label} mis à jour.")

st.markdown("---")
st.subheader("📤 Export / 📥 Import des paramètres")
colx, coly = st.columns(2)
with colx:
    if st.button("⬇ Exporter params.csv"):
        buf = io.StringIO()
        df_params.to_csv(buf, index=False)
        st.download_button("Télécharger params.csv", buf.getvalue(), file_name="params.csv", mime="text/csv", use_container_width=True)
with coly:
    up = st.file_uploader("Importer params.csv", type=["csv"])
    if up is not None:
        try:
            imp = pd.read_csv(up).fillna("")
            # Normaliser colonnes
            cols = [c.lower() for c in imp.columns]
            if "cle" in cols and "val" in cols:
                pass
            elif "key" in cols and "value" in cols:
                imp = imp.rename(columns={"key":"cle","value":"val"})
            elif len(imp.columns)>=2:
                imp = imp.rename(columns={imp.columns[0]:"cle", imp.columns[1]:"val"})
            # concat/sur-écrire par clé
            base = df_params.copy()
            base.columns = [c.lower() for c in base.columns]
            if "cle" not in base.columns or "val" not in base.columns:
                base = pd.DataFrame(columns=["cle","val"] + AUDIT_COLS)
            # merge en priorisant import
            merged = pd.concat([base[["cle","val"]], imp[["cle","val"]]], ignore_index=True)
            merged = merged.drop_duplicates(subset=["cle"], keep="last")
            # reconstruire df_params complet
            df_params = merged.copy()
            for c in AUDIT_COLS:
                if c not in df_params.columns: df_params[c] = ""
            save_df_target("params", df_params, PATHS, WS_FUNC)
            st.success("Paramètres importés.")
        except Exception as e:
            st.error(f"Import impossible : {e}")
