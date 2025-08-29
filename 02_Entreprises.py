# pages/02_Entreprises.py — Grille + Vue 360° + Interactions officielles
from __future__ import annotations
import streamlit as st
import pandas as pd
from _shared import load_all_tables, statusbar, filter_and_paginate

st.set_page_config(page_title="Entreprises — IIBA Cameroun", page_icon="🏢", layout="wide")
st.title("🏢 Entreprises")

dfs = load_all_tables()
dfe = dfs["entreprises"].copy()
dfc = dfs["contacts"].copy()
dfi = dfs["inter"].copy()
dfp = dfs["parts"].copy()
dfpay = dfs["pay"].copy()
dfcert = dfs["cert"].copy()
df_ep = dfs["entreprise_parts"].copy()

# ===== Grille avec filtres + pagination =====
suggested = ["Secteur","Pays","Ville"]
page_df, filtered_df = filter_and_paginate(dfe, key_prefix="ent", page_size_default=20, suggested_filters=suggested)
statusbar(filtered_df, numeric_keys=["CA_Annuel","Nb_Employes"])
st.dataframe(page_df, use_container_width=True, hide_index=True)

# ===== Sélection d'une entreprise =====
sel_ids = ["—"] + filtered_df.get("ID_Entreprise", pd.Series([], dtype=str)).astype(str).tolist()
sel_ent = st.selectbox("Sélectionner une entreprise (ID_Entreprise)", options=sel_ids, index=0, key="ent_sel")
if sel_ent and sel_ent != "—":
    row = dfe[dfe["ID_Entreprise"].astype(str)==sel_ent]
    if not row.empty:
        ent = row.iloc[0].to_dict()
        st.subheader(f"🗂️ Fiche — {ent.get('Nom_Entreprise','(sans nom)')}")
        c1,c2,c3,c4 = st.columns(4)
        c1.text_input("ID_Entreprise", ent.get("ID_Entreprise",""), disabled=True)
        c2.text_input("Créé le", ent.get("Created_At",""), disabled=True)
        c3.text_input("Modifié le", ent.get("Updated_At",""), disabled=True)
        c4.text_input("Secteur", ent.get("Secteur",""), disabled=True)

        st.markdown("---")
        tab_emp, tab_off, tab360 = st.tabs(["👥 Employés","🏢 Interactions officielles","🔭 Vue 360° Entreprise"])

        with tab_emp:
            # Employés = contacts liés (colonne 'Entreprise' égale au nom de l'entreprise)
            nom_ent = ent.get("Nom_Entreprise","")
            sub_emp = dfc[dfc.get("Entreprise","")==nom_ent].copy()
            st.caption(f"Employés liés à : **{nom_ent}**")
            page_emp, emp_filtered = filter_and_paginate(sub_emp, key_prefix="ent_emp", page_size_default=20,
                                                         suggested_filters=["Type","Statut","Fonction","Ville","Pays"])
            statusbar(emp_filtered, numeric_keys=[])
            st.dataframe(page_emp, use_container_width=True, hide_index=True)

        with tab_off:
            # Interactions officielles = interactions avec Cible='Entreprise' & ID_Cible = ID_Entreprise
            inte = dfi[(dfi.get("Cible","")=="Entreprise") & (dfi.get("ID_Cible","")==sel_ent)].copy()
            page_int, int_filtered = filter_and_paginate(inte, key_prefix="ent_official", page_size_default=20,
                                                         suggested_filters=["Canal","Responsable"])
            statusbar(int_filtered, numeric_keys=[])
            st.dataframe(page_int, use_container_width=True, hide_index=True)

        with tab360:
            # Agrégats employés (interactions / paiements / certifs / participations)
            nom_ent = ent.get("Nom_Entreprise","")
            emp_ids = set(dfc[dfc.get("Entreprise","")==nom_ent]["ID"].astype(str))
            # interactions des employés (Cible=Contact et ID in emp_ids) OU anciennes lignes sans Cible
            inter_emp = dfi[((dfi.get("Cible","")=="Contact") & (dfi.get("ID_Cible","").astype(str).isin(emp_ids)))
                            | ((dfi.get("Cible","")=="") & (dfi.get("ID","").astype(str).isin(emp_ids)))].copy()
            pay_emp = dfpay[dfpay.get("ID","").astype(str).isin(emp_ids)].copy()
            cert_emp = dfcert[dfcert.get("ID","").astype(str).isin(emp_ids)].copy()
            parts_emp = dfs["parts"][dfs["parts"].get("ID","").astype(str).isin(emp_ids)].copy()

            st.write(f"**Employés liés** : {len(emp_ids)}")
            st.write(f"**Interactions (employés)** : {len(inter_emp)}")
            if not pay_emp.empty:
                pay_emp["Montant"] = pd.to_numeric(pay_emp["Montant"], errors="coerce").fillna(0)
            st.write(f"**Paiements (employés)** : {int(pay_emp.get('Montant',pd.Series(dtype=float)).sum()):,} FCFA".replace(",", " "))
            st.write(f"**Certifications réussies (employés)** : {len(cert_emp[cert_emp.get('Résultat','')=='Réussi'])}")
            st.write(f"**Participations (employés)** : {len(parts_emp)}")

            st.markdown("##### Détails — Interactions d'employés")
            p_i, f_i = filter_and_paginate(inter_emp, key_prefix="ent360_inter", page_size_default=20,
                                           suggested_filters=["Canal","Responsable"])
            statusbar(f_i, numeric_keys=[])
            st.dataframe(p_i, use_container_width=True, hide_index=True)

            st.markdown("##### Détails — Paiements d'employés")
            p_p, f_p = filter_and_paginate(pay_emp, key_prefix="ent360_pay", page_size_default=20,
                                           suggested_filters=["Statut"])
            statusbar(f_p, numeric_keys=["Montant"])
            st.dataframe(p_p, use_container_width=True, hide_index=True)

            st.markdown("##### Détails — Certifications d'employés")
            p_c, f_c = filter_and_paginate(cert_emp, key_prefix="ent360_cert", page_size_default=20,
                                           suggested_filters=["Résultat"])
            statusbar(f_c, numeric_keys=[])
            st.dataframe(p_c, use_container_width=True, hide_index=True)
