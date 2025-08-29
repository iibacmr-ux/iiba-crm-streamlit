# pages/01_Contacts.py
from __future__ import annotations
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st

from storage_backend import ensure_df_source, save_df_target
from gs_client import read_service_account_secret, get_gspread_client, make_ws_func
from ui_common import require_login, aggrid_table

st.set_page_config(page_title="CRM — Contacts", page_icon="👤", layout="wide")
require_login()

BACKEND = st.secrets.get("storage_backend", "csv")
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
PATHS = {
    "contacts": DATA_DIR / "contacts.csv",
    "entreprises": DATA_DIR / "entreprises.csv",
    "inter": DATA_DIR / "interactions.csv",
    "pay": DATA_DIR / "paiements.csv",
    "cert": DATA_DIR / "certifications.csv",
    "parts": DATA_DIR / "participations.csv",
    "events": DATA_DIR / "evenements.csv",
    "params": DATA_DIR / "parametres.csv",
}

WS_FUNC = None
if BACKEND == "gsheets":
    try:
        info = read_service_account_secret()
        GC = get_gspread_client(info)
        WS_FUNC = make_ws_func(GC)
    except Exception as e:
        st.error(f"Initialisation Google Sheets échouée : {e}")
        st.stop()

C_COLS = ["ID","Civilité","Nom","Prénom","Email","Téléphone","Entreprise","Fonction",
          "Adresse","Ville","Pays","Notes","Created_At","Created_By","Updated_At","Updated_By"]
E_COLS = ["ID_Entreprise","Raison_Sociale","CA_Annuel","Nb_Employés","Secteur","Contact_Principal",
          "Adresse","Ville","Pays","Site_Web","Notes","Created_At","Created_By","Updated_At","Updated_By"]
EV_COLS = ["ID_Événement","Titre","Date","Heure","Lieu","Ville","Pays","Description","Type",
           "Created_At","Created_By","Updated_At","Updated_By"]
P_COLS = ["ID_Participation","Cible_Type","Cible_ID","ID_Événement","Role","Created_At","Created_By","Updated_At","Updated_By"]
I_COLS = ["ID_Interaction","ID_Contact","Date","Canal","Objet","Notes","Created_At","Created_By","Updated_At","Updated_By"]
PAY_COLS = ["ID_Paiement","ID_Contact","Date","Montant","Devise","Moyen","Notes","Created_At","Created_By","Updated_At","Updated_By"]
CERT_COLS = ["ID_Certif","ID_Contact","Type","N°","Date","Expiration","Notes","Created_At","Created_By","Updated_At","Updated_By"]
P_PARAMS = ["Param","Valeur","Created_At","Created_By","Updated_At","Updated_By"]

df_contacts = ensure_df_source("contacts", C_COLS, PATHS, WS_FUNC)
df_entreprises = ensure_df_source("entreprises", E_COLS, PATHS, WS_FUNC)
df_events = ensure_df_source("events", EV_COLS, PATHS, WS_FUNC)
df_parts = ensure_df_source("parts", P_COLS, PATHS, WS_FUNC)
df_inter = ensure_df_source("inter", I_COLS, PATHS, WS_FUNC)
df_pay = ensure_df_source("pay", PAY_COLS, PATHS, WS_FUNC)
df_cert = ensure_df_source("cert", CERT_COLS, PATHS, WS_FUNC)
df_params = ensure_df_source("params", P_PARAMS, PATHS, WS_FUNC)

st.sidebar.checkbox("⚠️ Forcer la sauvegarde (ignore verrou)", value=False, key="override_save")

st.title("Contacts")

grid = aggrid_table(df_contacts, page_size=20, selection='single')
sel = grid.selected_rows[0] if grid.selected_rows else None
st.caption(f"{len(df_contacts)} contact(s)")

st.markdown("---")
st.subheader("Créer / Modifier un contact")

entre_opts = [""] + sorted([e for e in df_entreprises["Raison_Sociale"].dropna().astype(str).unique() if e])

def list_vals(key):
    return [""] + sorted(df_params[df_params["Param"]==key]["Valeur"].dropna().astype(str).unique().tolist())
fonctions = list_vals("Fonction")
pays_list = list_vals("Pays")
villes = list_vals("Ville")

with st.form("contact_form", clear_on_submit=False):
    row_init = sel or {}
    colA, colB, colC = st.columns(3)
    with colA:
        civilite = st.selectbox("Civilité", ["","M.","Mme","Dr"], index=(["","M.","Mme","Dr"].index(row_init.get("Civilité","")) if row_init.get("Civilité","") in ["","M.","Mme","Dr"] else 0))
        nom = st.text_input("Nom", row_init.get("Nom","")).strip()
        prenom = st.text_input("Prénom", row_init.get("Prénom","")).strip()
    with colB:
        email = st.text_input("Email", row_init.get("Email","")).strip()
        tel = st.text_input("Téléphone", row_init.get("Téléphone","")).strip()
        ent = st.selectbox("Entreprise", options=entre_opts, index=(entre_opts.index(row_init.get("Entreprise","")) if row_init.get("Entreprise","") in entre_opts else 0))
    with colC:
        fonction = st.selectbox("Fonction", fonctions, index=(fonctions.index(row_init.get("Fonction","")) if row_init.get("Fonction","") in fonctions else 0))
        pays = st.selectbox("Pays", pays_list, index=(pays_list.index(row_init.get("Pays","")) if row_init.get("Pays","") in pays_list else 0))
        ville = st.selectbox("Ville", villes, index=(villes.index(row_init.get("Ville","")) if row_init.get("Ville","") in villes else 0))
    adresse = st.text_area("Adresse", row_init.get("Adresse",""))
    notes = st.text_area("Notes", row_init.get("Notes",""))

    submitted = st.form_submit_button("Enregistrer")

    if submitted:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        if sel is None:
            new_id = f"C{int(datetime.utcnow().timestamp())}"
            new_row = {
                "ID": new_id, "Civilité": civilite, "Nom": nom, "Prénom": prenom,
                "Email": email, "Téléphone": tel, "Entreprise": ent, "Fonction": fonction,
                "Adresse": adresse, "Ville": ville, "Pays": pays, "Notes": notes,
                "Created_At": now, "Created_By": "ui", "Updated_At": now, "Updated_By": "ui",
            }
            df_contacts = pd.concat([df_contacts, pd.DataFrame([new_row])], ignore_index=True)
            save_df_target("contacts", df_contacts, PATHS, WS_FUNC, override=st.session_state.get("override_save", False))
            st.success(f"Contact {new_id} créé."); st.experimental_rerun()
        else:
            cid = sel["ID"]
            idx = df_contacts.index[df_contacts["ID"] == cid]
            if len(idx):
                i = idx[0]
                df_contacts.loc[i, ["Civilité","Nom","Prénom","Email","Téléphone","Entreprise","Fonction","Adresse","Ville","Pays","Notes","Updated_At","Updated_By"]] = \
                    [civilite,nom,prenom,email,tel,ent,fonction,adresse,ville,pays,notes, now,"ui"]
                save_df_target("contacts", df_contacts, PATHS, WS_FUNC, override=st.session_state.get("override_save", False))
                st.success(f"Contact {cid} mis à jour."); st.experimental_rerun()

st.markdown("---")
st.subheader("Relations")
if sel is None:
    st.info("Sélectionnez un contact dans la grille pour gérer ses relations.")
else:
    cid = sel["ID"]
    col1, col2 = st.columns(2)
    with col1:
        st.caption("📅 Participations à des événements")
        ev_labels = [f"{r['ID_Événement']} — {r['Titre']}" for _, r in df_events.iterrows()]
        ev_map = {lab: r["ID_Événement"] for lab, (_, r) in zip(ev_labels, df_events.iterrows())}
        add_ev = st.selectbox("Ajouter à l'événement", [""] + ev_labels, index=0)
        if st.button("Ajouter participation", disabled=(add_ev=="" or cid=="")):
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            new_row = {"ID_Participation": f"P{int(datetime.utcnow().timestamp())}","Cible_Type":"contact","Cible_ID": cid,
                       "ID_Événement": ev_map.get(add_ev,""), "Role":"participant",
                       "Created_At": now,"Created_By":"ui","Updated_At": now,"Updated_By":"ui"}
            df_parts = pd.concat([df_parts, pd.DataFrame([new_row])], ignore_index=True)
            save_df_target("parts", df_parts, PATHS, WS_FUNC, override=st.session_state.get("override_save", False)); st.experimental_rerun()
        st.dataframe(df_parts[df_parts["Cible_Type"].eq("contact") & df_parts["Cible_ID"].eq(cid)], use_container_width=True, height=200)
    with col2:
        st.caption("💬 Interactions")
        with st.form("add_inter"):
            datei = st.date_input("Date")
            canal = st.selectbox("Canal", ["Email","Téléphone","Réunion","Autre"])
            obj = st.text_input("Objet").strip()
            notei = st.text_area("Notes")
            ok = st.form_submit_button("Ajouter interaction")
        if ok:
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            new_row = {"ID_Interaction": f"I{int(datetime.utcnow().timestamp())}",
                       "ID_Contact": cid, "Date": str(datei), "Canal": canal, "Objet": obj, "Notes": notei,
                       "Created_At": now,"Created_By":"ui","Updated_At": now,"Updated_By":"ui"}
            df_inter = pd.concat([df_inter, pd.DataFrame([new_row])], ignore_index=True)
            save_df_target("inter", df_inter, PATHS, WS_FUNC, override=st.session_state.get("override_save", False))
            st.success("Interaction ajoutée."); st.experimental_rerun()

    col3, col4 = st.columns(2)
    with col3:
        st.caption("💳 Paiements")
        with st.form("add_pay"):
            datep = st.date_input("Date paiement")
            montant = st.number_input("Montant", min_value=0, step=1000, value=0)
            devise = st.text_input("Devise", value="FCFA")
            moyen = st.selectbox("Moyen", ["Espèces","Mobile Money","Virement","Carte"])
            notesp = st.text_area("Notes")
            okp = st.form_submit_button("Ajouter paiement")
        if okp:
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            new_row = {"ID_Paiement": f"PAY{int(datetime.utcnow().timestamp())}","ID_Contact": cid,
                       "Date": str(datep),"Montant": str(montant),"Devise": devise,"Moyen": moyen,"Notes": notesp,
                       "Created_At": now,"Created_By":"ui","Updated_At": now,"Updated_By":"ui"}
            df_pay = pd.concat([df_pay, pd.DataFrame([new_row])], ignore_index=True)
            save_df_target("pay", df_pay, PATHS, WS_FUNC, override=st.session_state.get("override_save", False))
            st.success("Paiement ajouté."); st.experimental_rerun()
        st.dataframe(df_pay[df_pay["ID_Contact"].eq(cid)], use_container_width=True, height=200)
    with col4:
        st.caption("🎖️ Certifications")
        with st.form("add_cert"):
            typec = st.text_input("Type/Programme").strip()
            num = st.text_input("N°").strip()
            datec = st.date_input("Date obtention")
            exp = st.date_input("Expiration")
            notec = st.text_area("Notes")
            okc = st.form_submit_button("Ajouter certification")
        if okc:
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            new_row = {"ID_Certif": f"CERT{int(datetime.utcnow().timestamp())}","ID_Contact": cid,
                       "Type": typec,"N°": num,"Date": str(datec),"Expiration": str(exp),"Notes": notec,
                       "Created_At": now,"Created_By":"ui","Updated_At": now,"Updated_By":"ui"}
            df_cert = pd.concat([df_cert, pd.DataFrame([new_row])], ignore_index=True)
            save_df_target("cert", df_cert, PATHS, WS_FUNC, override=st.session_state.get("override_save", False))
            st.success("Certification ajoutée."); st.experimental_rerun()
        st.dataframe(df_cert[df_cert["ID_Contact"].eq(cid)], use_container_width=True, height=200)
